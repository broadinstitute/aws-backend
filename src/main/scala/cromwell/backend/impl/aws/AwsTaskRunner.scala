package cromwell.backend.impl.aws

import akka.actor.{Actor, ActorLogging}
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider}
import com.amazonaws.retry.RetryPolicy
import com.amazonaws.services.ecs.AmazonECSAsyncClient
import com.amazonaws.services.ecs.model._
import cromwell.backend.MemorySize
import cromwell.backend.impl.aws.util.AwsSdkAsyncHandler
import wdl4s.parser.MemoryUnit

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration


trait AwsTaskRunner {
  self: ActorLogging with Actor =>

  var cacheKeyToTaskDefinition: Map[String, TaskDefinition] = Map.empty

  val containerName = "cromwell-container-name"

  private def getOrCreateTaskDefinition(image: String, memorySize: MemorySize, cpu: Int): TaskDefinition = {
    val cacheKey = s"$image $memorySize $cpu"

    if (!cacheKeyToTaskDefinition.contains(cacheKey)) {
      // DANGER total hack.  Squirrels away a TaskDefinition for a Docker image name + memory size + cpu triplet.
      // Task definitions are never cleaned up.

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
            .withCpu(cpu)
            .withMountPoints(mountPoint)
            .withEnvironment(accessKey, secretAccessKey)
            .withEssential(true)

          val registerTaskDefinitionRequest = new RegisterTaskDefinitionRequest()
            .withFamily("family-cromwell")
            .withContainerDefinitions(containerDefinition)
            .withVolumes(volume)

          val taskDefinition = ecsAsyncClient.registerTaskDefinition(registerTaskDefinitionRequest).getTaskDefinition
          cacheKeyToTaskDefinition += cacheKey -> taskDefinition
        }
      }
    }
    cacheKeyToTaskDefinition(cacheKey)
  }

  def runTaskAsync(command: String, dockerImage: String, memorySize: MemorySize, cpu: Int, awsAttributes: AwsAttributes): RunTaskResult = {
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
    val taskResult = runTaskAsync(command, dockerImage, memorySize, cpu, awsAttributes)
    // Error checking needed here, if something is wrong getTasks will be empty.
    waitUntilDone(taskResult.getTasks.asScala.head)
  }

  def awsConfiguration: AwsConfiguration

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

  protected def describeTasks(task: Task): DescribeTasksResult = {
    val taskArn = task.getTaskArn
    log.info("Checking status for {}", taskArn)
    val describeTasksRequest = new DescribeTasksRequest()
      .withCluster(awsAttributes.clusterName)
      .withTasks(List(taskArn).asJava)

    val resultHandler = new AwsSdkAsyncHandler[DescribeTasksRequest, DescribeTasksResult]()
    val _ = ecsAsyncClient.describeTasksAsync(describeTasksRequest, resultHandler)

    val describedTasks = Await.result(resultHandler.future, Duration.Inf)
    describedTasks.result
  }

  protected def isStopped(describedTasks: DescribeTasksResult): Boolean = {
    describedTasks.getTasks.asScala.headOption match {
      case Some(describedTask) if isStopped(describedTask) => true
      case None => false // TODO: If we don't find our task... then we should error?
      case _ => false
    }
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
