package cromwell.backend.impl.aws

import akka.actor.{Actor, ActorLogging}
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider}
import com.amazonaws.retry.RetryPolicy
import com.amazonaws.services.ecs.AmazonECSAsyncClient
import com.amazonaws.services.ecs.model._
import cromwell.backend.MemorySize
import wdl4s.parser.MemoryUnit

import scala.collection.JavaConverters._

object AwsTaskRunner {
  var cacheKeyToTaskDefinition: Map[String, TaskDefinition] = Map.empty
}


trait AwsTaskRunner {
  self: ActorLogging with Actor =>

  import AwsTaskRunner._

  def awsConfiguration: AwsConfiguration

  val containerName = "cromwell-container-name"

  lazy val awsAttributes: AwsAttributes = awsConfiguration.awsAttributes

  lazy val credentials = new AWSCredentials {
    override def getAWSAccessKeyId: String = awsAttributes.accessKeyId

    override def getAWSSecretKey: String = awsAttributes.secretKey
  }

  final val clientConfiguration = new ClientConfiguration()
  clientConfiguration.setMaxErrorRetry(25)
  clientConfiguration.setRetryPolicy(new RetryPolicy(null, null, 25, true))

  lazy val credentialsProvider = new AWSCredentialsProvider {
    override def refresh(): Unit = ()

    override def getCredentials: AWSCredentials = credentials
  }

  lazy val ecsAsyncClient = new AmazonECSAsyncClient(credentialsProvider, clientConfiguration)

  // http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/ecs/model/ContainerDefinition.html#withCpu-java.lang.Integer-
  private val CpuUnitsPerCore = 1024

  private def getOrCreateTaskDefinition(image: String, memorySize: MemorySize, cpu: Int): TaskDefinition = {
    val cacheKey = s"$image $memorySize $cpu"

    if (!cacheKeyToTaskDefinition.contains(cacheKey)) {
      // TODO clean up task definitions and use the Batch API to override memory and CPU.

      cacheKey.intern.synchronized {
        if (!cacheKeyToTaskDefinition.contains(cacheKey)) {
          log.info(s"Cache miss for $cacheKey, creating new task definition.")
          val volumeProperties = new HostVolumeProperties().withSourcePath(awsAttributes.hostMountPoint)
          val cromwellVolume = "cromwell-volume"
          val volume = new Volume().withHost(volumeProperties).withName(cromwellVolume)

          val mountPoint = new MountPoint().withSourceVolume(cromwellVolume).withContainerPath(awsAttributes.containerMountPoint).withReadOnly(false)
          val accessKey = new KeyValuePair().withName("AWS_ACCESS_KEY_ID").withValue(awsAttributes.accessKeyId)
          val secretAccessKey = new KeyValuePair().withName("AWS_SECRET_ACCESS_KEY").withValue(awsAttributes.secretKey)

          val containerDefinition = new ContainerDefinition()
            .withName(containerName)
            .withCommand("/bin/sh", "-xv", "-c", "echo Default command")
            .withImage(image)
            .withMemory(memorySize.to(MemoryUnit.MiB).amount.toInt)
            .withCpu(cpu * CpuUnitsPerCore)
            .withMountPoints(mountPoint)
            .withEnvironment(accessKey, secretAccessKey)
            .withEssential(true)

          val registerTaskDefinitionRequest = new RegisterTaskDefinitionRequest()
            .withFamily("family-cromwell")
            .withContainerDefinitions(containerDefinition)
            .withVolumes(volume)

          try {
            val taskDefinition = ecsAsyncClient.registerTaskDefinition(registerTaskDefinitionRequest).getTaskDefinition
            cacheKeyToTaskDefinition += cacheKey -> taskDefinition
          } catch {
            case e: ClientException if e.getMessage.contains(
                "Too many concurrent attempts to create a new revision of the specified family.") =>
              throw new AwsNonFatalException(e.getMessage, e)
          }
        }
      }
    }
    cacheKeyToTaskDefinition(cacheKey)
  }

  def doRunTask(command: String, dockerImage: String, memorySize: MemorySize, cpu: Int, awsAttributes: AwsAttributes): RunTaskResult = {
    val taskDefinition = getOrCreateTaskDefinition(dockerImage, memorySize, cpu)

    val commandOverride = new ContainerOverride()
      .withCommand("/bin/sh", "-c", command)
      .withName(containerName)
    val taskOverride = new TaskOverride().withContainerOverrides(commandOverride)

    val taskResult =
      ecsAsyncClient.runTask(new RunTaskRequest()
        .withTaskDefinition(taskDefinition.getTaskDefinitionArn)
        .withCluster(awsAttributes.clusterName)
        .withOverrides(taskOverride)
      )

    log.info("task result: {}", taskResult)
    taskResult
  }

  def runTask(command: String, dockerImage: String, memorySize: MemorySize, cpu: Int, awsAttributes: AwsAttributes): Task = {
    def retryRunTask(count: Int = 1): Task = {
      try {
        val taskResult = doRunTask(command, dockerImage, memorySize, cpu, awsAttributes)
        taskResult.getTasks.asScala.headOption match {
          case Some(headTask) => headTask
          case None =>
            val failures = taskResult.getFailures.asScala
            throw new AwsNonFatalException(s"task was not created due to failures:\n${failures.mkString("\n")}")
        }

      } catch {
        case e: AwsNonFatalException if count <= 120 =>
          log.warning(s"""Caught exception during attempt $count, "${e.getMessage}", retrying in 1 second""")
          Thread.sleep(1000L)
          retryRunTask(count + 1)
      }
    }

    val task = retryRunTask()
    waitUntilDone(task)
  }

  protected def describeTasks(task: Task): DescribeTasksResult = {
    val taskArn = task.getTaskArn
    log.info("Checking status for {}", taskArn)
    describeTasks(taskArn)
  }

  protected def describeTasks(taskArn: String): DescribeTasksResult = {
    val describeTasksRequest = new DescribeTasksRequest()
      .withCluster(awsAttributes.clusterName)
      .withTasks(List(taskArn).asJava)

    ecsAsyncClient.describeTasks(describeTasksRequest)
  }

  protected def isStopped(describedTask: Task): Boolean = {
    describedTask.getLastStatus == DesiredStatus.STOPPED.toString
  }

  protected def isSuccess(describedTask: Task): Boolean = {
    val containerReturnCodeOption = for {
      container <- describedTask.getContainers.asScala.headOption
      containerReturnCode <- Option(container.getExitCode)
    } yield containerReturnCode.toInt
    containerReturnCodeOption.isDefined
  }

  protected def containerReasonExit(describedTask: Task): Option[String] = {
    for {
      container <- describedTask.getContainers.asScala.headOption
      containerReason <- Option(container.getReason)
    } yield containerReason
  }

  protected def waitUntilDone(task: Task): Task = {
    val describedTasks = describeTasks(task)
    val taskDescription = describedTasks.getTasks.asScala.headOption
    taskDescription match {
      case Some(td) if td.getLastStatus == DesiredStatus.STOPPED.toString =>
        logResult(describedTasks)
        td
      case notStopped =>
        log.info(s"Still waiting for completion. Last known status: {}", notStopped.map(_.getLastStatus).getOrElse("UNKNOWN"))
        Thread.sleep(5000)
        waitUntilDone(task)
    }
  }

  private def logResult(taskDescription: DescribeTasksResult): Unit = {
    taskDescription.getFailures.asScala.toList match {
      case Nil => log.info("complete: {}", taskDescription)
      case failures => log.error("failures: {}\n{}", failures.map(_.getReason).mkString("\n"), taskDescription)
    }
  }
}
