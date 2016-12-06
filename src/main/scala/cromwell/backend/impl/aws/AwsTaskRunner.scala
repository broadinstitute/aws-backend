package cromwell.backend.impl.aws

import akka.actor.{Actor, ActorLogging}
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider}
import com.amazonaws.retry.RetryPolicy
import com.amazonaws.services.ecs.AmazonECSAsyncClient
import com.amazonaws.services.ecs.model._
import cromwell.backend.impl.aws.util.AwsSdkAsyncHandler

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration


trait AwsTaskRunner {
  self: ActorLogging with Actor =>

  var imageToTaskDefinition: Map[String, TaskDefinition] = Map.empty

  val containerName = "cromwell-container-name"

  private def getOrCreateTaskDefinition(image: String): TaskDefinition = {
    if (!imageToTaskDefinition.contains(image)) {
      // DANGER total hack.  This calls into question the whole AWS backend approach.
      // It also completely punts on handling memory or CPU resources and task definitions
      // are no longer being cleaned up.
      image.intern.synchronized {
        if (!imageToTaskDefinition.contains(image)) {
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
            .withMemory(awsAttributes.containerMemoryMib)
            .withMountPoints(mountPoint)
            .withEnvironment(accessKey, secretAccessKey)
            .withEssential(true)

          val registerTaskDefinitionRequest = new RegisterTaskDefinitionRequest()
            .withFamily("family-cromwell")
            .withContainerDefinitions(containerDefinition)
            .withVolumes(volume)

          val taskDefinition = ecsAsyncClient.registerTaskDefinition(registerTaskDefinitionRequest).getTaskDefinition
          imageToTaskDefinition += image -> taskDefinition
        }
      }
    }
    imageToTaskDefinition(image)
  }

  def runTask(command: String, dockerImage: String, awsAttributes: AwsAttributes): Task = {

    val taskDefinition = getOrCreateTaskDefinition(dockerImage)

    val commandOverride = new ContainerOverride()
      .withCommand("/bin/sh", "-xv", "-c", command)
      .withName(containerName)
    val taskOverride = new TaskOverride().withContainerOverrides(commandOverride)

    val taskResult =
      ecsAsyncClient.runTask(new RunTaskRequest()
        .withTaskDefinition(taskDefinition.getTaskDefinitionArn)
        .withCluster(awsAttributes.clusterName)
        .withOverrides(taskOverride)
      )

    log.info("task result: {}", taskResult)
    // Error checking needed here, if something is wrong getTasks will be empty.
    waitUntilDone(taskResult.getTasks.asScala.head)
  }

  def awsConfiguration: AwsConfiguration

  val awsAttributes: AwsAttributes = awsConfiguration.awsAttributes

  val credentials = new AWSCredentials {
    override def getAWSAccessKeyId: String = awsAttributes.accessKeyId

    override def getAWSSecretKey: String = awsAttributes.secretKey
  }

  val clientConfiguration = new ClientConfiguration()
  clientConfiguration.setMaxErrorRetry(25)
  clientConfiguration.setRetryPolicy(new RetryPolicy(null, null, 25, true))

  val credentialsProvider = new AWSCredentialsProvider {
    override def refresh(): Unit = ()

    override def getCredentials: AWSCredentials = credentials
  }

  val ecsAsyncClient = new AmazonECSAsyncClient(credentialsProvider, clientConfiguration)


  protected def waitUntilDone(task: Task): Task = {
    val taskArn = task.getTaskArn
    log.info("Checking status for {}", taskArn)
    val describeTasksRequest = new DescribeTasksRequest()
      .withCluster(awsAttributes.clusterName)
      .withTasks(List(taskArn).asJava)

    val resultHandler = new AwsSdkAsyncHandler[DescribeTasksRequest, DescribeTasksResult]()
    val _ = ecsAsyncClient.describeTasksAsync(describeTasksRequest, resultHandler)

    val describedTasks = Await.result(resultHandler.future, Duration.Inf)
    val taskDescription = describedTasks.result.getTasks.asScala.headOption
    taskDescription match {
      case Some(td) if td.getLastStatus == DesiredStatus.STOPPED.toString =>
        logResult(describedTasks.result)
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
