package cromwell.backend.impl.aws

import akka.actor.{Actor, ActorLogging}
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider}
import com.amazonaws.retry.RetryPolicy
import com.amazonaws.services.batch._
import com.amazonaws.services.batch.model._
import cromwell.backend.MemorySize
import wdl4s.parser.MemoryUnit

import scala.collection.JavaConverters._

object AwsJobRunner {
  var cacheKeyToJobDefinitionArn: Map[String, String] = Map.empty
}

trait AwsJobRunner {
  self: ActorLogging with Actor =>

  import AwsJobRunner._

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

  lazy val batchClient: AWSBatch = new AWSBatchClient(credentialsProvider, clientConfiguration)

  private def getOrCreateJobDefinitionArn(image: String): String = {
    val cacheKey = image

    if (!cacheKeyToJobDefinitionArn.contains(cacheKey)) {
      cacheKey.intern.synchronized {
        if (!cacheKeyToJobDefinitionArn.contains(cacheKey)) {
          log.info(s"Cache miss for $cacheKey, creating new job definition.")

          val host = new Host()
            .withSourcePath(awsAttributes.hostMountPoint)

          val volume = new Volume()
            .withName("cromwell-volume")
            .withHost(host)

          val mountPoint = new MountPoint()
            .withSourceVolume(volume.getName)
            .withContainerPath(awsAttributes.containerMountPoint)
            .withReadOnly(false)

          val containerProperties = new ContainerProperties()
            .withCommand("")
            .withVcpus(0)
            .withMemory(0)
            .withImage(image)
            .withVolumes(volume)
            .withMountPoints(mountPoint)

          val registerJobDefinitionRequest = new RegisterJobDefinitionRequest()
            .withJobDefinitionName("cromwell-job-definition")
            .withType(JobDefinitionType.Container)
            .withContainerProperties(containerProperties)

          try {
            val jobDefinitionArn = batchClient.registerJobDefinition(registerJobDefinitionRequest).getJobDefinitionArn
            cacheKeyToJobDefinitionArn += cacheKey -> jobDefinitionArn
          } catch {
            case e: ClientException if e.getMessage.contains(
                "Too many concurrent attempts to create a new revision of the specified family.") =>
              throw new AwsNonFatalException(e.getMessage, e)
          }
        }
      }
    }
    cacheKeyToJobDefinitionArn(cacheKey)
  }

  def submitJob(command: String, dockerImage: String, memorySize: MemorySize, cpu: Int,
                awsAttributes: AwsAttributes, keyValues: Map[String, String] = Map.empty): SubmitJobResult = {
    val jobDefinitionArn = getOrCreateJobDefinitionArn(dockerImage)

    val keyValuePairs = keyValues.toSeq map {
      case (key, value) => new KeyValuePair().withName(key).withValue(value)
    }

    val containerOverrides = new ContainerOverrides()
      .withCommand("/bin/sh", "-c", command)
      .withMemory(memorySize.to(MemoryUnit.MiB).amount.toInt)
      .withVcpus(cpu)
      .withEnvironment(keyValuePairs: _*)

    val submitJobRequest = new SubmitJobRequest()
      .withJobName("cromwell-job")
      .withJobDefinition(jobDefinitionArn)
      .withJobQueue(awsAttributes.jobQueueName)
      .withContainerOverrides(containerOverrides)

    val submitJobResult = batchClient.submitJob(submitJobRequest)

    log.info("submit job result: {}", submitJobResult)
    submitJobResult
  }

  protected def describeJob(jobId: String): DescribeJobsResult = {
    val describeJobsRequest = new DescribeJobsRequest()
      .withJobs(jobId)

    batchClient.describeJobs(describeJobsRequest)
  }

  protected def isTerminal(jobDetail: JobDetail): Boolean = {
    jobDetail.getStatus == JobStatus.SUCCEEDED.toString || jobDetail.getStatus == JobStatus.FAILED.toString
  }

  protected def isSuccess(jobDetail: JobDetail): Boolean = {
    jobDetail.getStatus match {
      case status if status == JobStatus.SUCCEEDED.toString => true
      case status if status == JobStatus.FAILED.toString => false
      case _ => throw new RuntimeException(s"Unknown status in $jobDetail")
    }
  }

  protected def containerReasonExit(jobDetail: JobDetail): Option[String] = {
    Option(jobDetail.getContainer.getReason)
  }

  private val RetryableFailureReasons = Seq(
    "CannotCreateContainerError: API error (500): devicemapper: Error running deviceResume dm_task_run failed",
    "CannotPullContainerError: failed to register layer: devicemapper: Error running deviceResume dm_task_run failed"
  )

  protected def isRetryableReason(reason: String): Boolean = {
    RetryableFailureReasons.contains(reason.trim)
  }

  protected def runJobAndWait(command: String, dockerImage: String, memorySize: MemorySize, cpu: Int,
                    awsAttributes: AwsAttributes): Unit = {

    val awsKeys = Map(
      "AWS_ACCESS_KEY_ID" -> awsAttributes.accessKeyId,
      "AWS_SECRET_ACCESS_KEY" -> awsAttributes.secretKey)

    def retryRunJob(count: Int = 1): Unit = {
      val submitJobResult = submitJob(command, dockerImage, memorySize, cpu, awsAttributes, awsKeys)
      val jobId = submitJobResult.getJobId
      val jobDetail = waitUntilDone(jobId)
      if (!isSuccess(jobDetail)) {
        val reason = containerReasonExit(jobDetail).getOrElse(s"unknown error: $jobDetail")
        if (isRetryableReason(reason)) {
          log.info(s"Retrying job $jobId in 1 second due to attempt $count failure: $reason")
          Thread.sleep(1000L)
          retryRunJob(count + 1)
        } else {
          throw new RuntimeException(s"job $jobId failed: $reason")
        }
      }
    }

    retryRunJob()
  }

  private def waitUntilDone(jobId: String): JobDetail = {
    val describedJobs = describeJob(jobId)
    val jobDetailOption = describedJobs.getJobs.asScala.headOption
    jobDetailOption match {
      case Some(jobDetail) if isTerminal(jobDetail) =>
        log.info("Completed jobId {} {}{}", jobDetail.getJobId, jobDetail.getStatus,
          Option(jobDetail.getStatusReason).map(": " + _).getOrElse(""))
        jobDetail
      case notStopped =>
        log.info(s"Still waiting for completion of jobId {}. Last known status: {}",
          jobId, notStopped.map(_.getStatus).getOrElse("UNKNOWN"))
        Thread.sleep(5000)
        waitUntilDone(jobId)
    }
  }
}
