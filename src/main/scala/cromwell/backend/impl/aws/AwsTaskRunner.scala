package cromwell.backend.impl.aws

import akka.actor.{Actor, ActorLogging}
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.ecs.AmazonECSAsyncClient
import com.amazonaws.services.ecs.model._
import cromwell.backend.impl.aws.util.AwsSdkAsyncHandler

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration


trait AwsTaskRunner { self: ActorLogging with Actor =>

  def registerTaskDefinition(name: String, command: String, dockerImage: String, awsAttributes: AwsAttributes): TaskDefinition = {
    val volumeProperties = new HostVolumeProperties().withSourcePath(awsAttributes.mountPoint)
    val cromwellVolume = "cromwell-volume"
    val volume = new Volume().withHost(volumeProperties).withName(cromwellVolume)

    val mountPoint = new MountPoint().withSourceVolume(cromwellVolume).withContainerPath(awsAttributes.mountPoint)
    val accessKey = new KeyValuePair().withName("AWS_ACCESS_KEY_ID").withValue(awsAttributes.accessKeyId)
    val secretAccessKey = new KeyValuePair().withName("AWS_SECRET_ACCESS_KEY").withValue(awsAttributes.secretKey)

    val containerDefinition = new ContainerDefinition()
      .withName(name)
      .withCommand("/bin/sh", "-xv", "-c", command)
      .withImage(dockerImage)
      .withMemory(awsAttributes.containerMemoryMib)
      .withMountPoints(mountPoint)
      .withEnvironment(accessKey, secretAccessKey)
      .withEssential(true)

    val registerTaskDefinitionRequest = new RegisterTaskDefinitionRequest()
      .withFamily("family-" + name)
      .withContainerDefinitions(containerDefinition)
      .withVolumes(volume)

    ecsAsyncClient.registerTaskDefinition(registerTaskDefinitionRequest).getTaskDefinition
  }

  def deregisterTaskDefinition(taskDefinition: TaskDefinition): Unit = {
    val deregisterTaskDefinitionRequest = new DeregisterTaskDefinitionRequest()
      .withTaskDefinition(taskDefinition.getTaskDefinitionArn)
    ecsAsyncClient.deregisterTaskDefinition(deregisterTaskDefinitionRequest)
  }

  def runTask(taskDefinition: TaskDefinition): Task = {
    val taskResult =
      ecsAsyncClient.runTask(new RunTaskRequest()
        .withTaskDefinition(taskDefinition.getTaskDefinitionArn)
        .withCluster(awsAttributes.clusterName)
      )

    log.info("task result is {}", taskResult)
    // Error checking needed here, if something is wrong getTasks will be empty.
    waitUntilDone(taskResult.getTasks.asScala.head)
  }

  def awsConfiguration: AwsConfiguration

  val awsAttributes = awsConfiguration.awsAttributes

  val credentials = new AWSCredentials {
    override def getAWSAccessKeyId: String = awsAttributes.accessKeyId

    override def getAWSSecretKey: String = awsAttributes.secretKey
  }

  val ecsAsyncClient = new AmazonECSAsyncClient(credentials)


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
        Thread.sleep(2000)
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
