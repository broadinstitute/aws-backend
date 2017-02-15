package cromwell.backend.impl.aws

import java.time.{Instant, OffsetDateTime, ZoneId}

import com.amazonaws.services.batch.model._
import cromwell.backend.async.{ExecutionHandle, FailedNonRetryableExecutionHandle, FailedRetryableExecutionHandle, PendingExecutionHandle}
import cromwell.backend.standard.{StandardAsyncExecutionActor, StandardAsyncExecutionActorParams, StandardAsyncJob}
import cromwell.backend.validation.{CpuValidation, DockerValidation, MemoryValidation, RuntimeAttributesValidation}
import cromwell.backend.{BackendInitializationData, BackendJobLifecycleActor}
import cromwell.core.ExecutionEvent
import cromwell.core.path.{MappedPath, Path, PathFactory}
import cromwell.core.retry.SimpleExponentialBackoff
import wdl4s.values.{WdlFile, WdlSingleFile}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

case class AwsRunStatus(jobDetail: JobDetail) {
  override lazy val toString: String = jobDetail.getStatus
}

class AwsAsyncJobExecutionActor(override val standardParams: StandardAsyncExecutionActorParams)
  extends BackendJobLifecycleActor with StandardAsyncExecutionActor with AwsJobRunner {

  override type StandardAsyncRunInfo = Any
  override type StandardAsyncRunStatus = AwsRunStatus

  override lazy val executeOrRecoverBackOff = SimpleExponentialBackoff(
    initialInterval = 3.seconds, maxInterval = 20.seconds, multiplier = 1.1)

  override lazy val pollBackOff = SimpleExponentialBackoff(
    initialInterval = 30.seconds, maxInterval = 600.seconds, multiplier = 1.1)

  lazy val awsBackendInitializationData: AwsBackendInitializationData = {
    BackendInitializationData.as[AwsBackendInitializationData](standardParams.backendInitializationDataOption)
  }

  override lazy val awsConfiguration: AwsConfiguration = awsBackendInitializationData.awsConfiguration

  override def execute(): ExecutionHandle = {
    val scriptFile = jobPaths.script
    scriptFile.parent.createDirectories().chmod("rwxrwxrwx")
    scriptFile.write(commandScriptContents)

    val cromwellCommand = redirectOutputs(s"/bin/bash ${jobPaths.script}")
    val docker = RuntimeAttributesValidation.extract(DockerValidation.instance, validatedRuntimeAttributes)
    val memory = RuntimeAttributesValidation.extract(MemoryValidation.instance, validatedRuntimeAttributes)
    val cpu = RuntimeAttributesValidation.extract(CpuValidation.instance, validatedRuntimeAttributes)
    val runJobResult = submitJob(cromwellCommand, docker, memory, cpu, awsConfiguration.awsAttributes)

    log.info("AWS submission completed:\n{}", runJobResult)
    val jobId = runJobResult.getJobId

    PendingExecutionHandle(jobDescriptor, StandardAsyncJob(jobId), None, None)
  }

  override def pollStatus(handle: StandardAsyncPendingExecutionHandle): AwsRunStatus = {
    val jobId = handle.pendingJob.jobId
    val describeJobsResult = describeJob(jobId)

    val jobs = describeJobsResult.getJobs.asScala

    jobs.headOption match {
      case Some(jobDetail) => AwsRunStatus(jobDetail)
      case None =>
        // This is purposefully not a fatal exception as there can be transient eventual consistency failures.
        throw new AwsNonFatalException(s"Could not find job for arn $jobId")
    }
  }

  override def isTerminal(runStatus: AwsRunStatus): Boolean = isTerminal(runStatus.jobDetail)

  override def isSuccess(runStatus: AwsRunStatus): Boolean = isSuccess(runStatus.jobDetail)

  override def mapCommandLineWdlFile(wdlFile: WdlFile): WdlFile = {
    val path = PathFactory.buildPath(wdlFile.value, workflowPaths.pathBuilders)
    WdlSingleFile(path.pathAsString)
  }

  private def hostAbsoluteFilePath(wdlPath: Path): Path = {
    jobPaths.callExecutionRoot.resolve(wdlPath).toAbsolutePath
  }

  override def mapOutputWdlFile(wdlFile: WdlFile): WdlFile = {
    val outputPath = PathFactory.buildPath(wdlFile.value, workflowPaths.pathBuilders)
    outputPath match {
      case path: MappedPath =>
        // This was a pass through, for example: output { File outFile = inFile }
        WdlSingleFile(path.pathAsString)
      case path if !hostAbsoluteFilePath(path).exists =>
        throw new RuntimeException(s"Could not process output, file not found: ${hostAbsoluteFilePath(path)}")
      case path => WdlFile(hostAbsoluteFilePath(path).pathAsString)
    }
  }

  override def handleExecutionSuccess(runStatus: AwsRunStatus,
                                      handle: StandardAsyncPendingExecutionHandle,
                                      returnCode: Int): ExecutionHandle = {
    log.info("AWS job completed!\n{}", runStatus.jobDetail)
    super.handleExecutionSuccess(runStatus, handle, returnCode)
  }

  override def handleExecutionFailure(runStatus: AwsRunStatus,
                                      handle: StandardAsyncPendingExecutionHandle,
                                      returnCode: Option[Int]): ExecutionHandle = {
    log.info("AWS job failed!\n{}", runStatus.jobDetail)
    val reason = containerReasonExit(runStatus.jobDetail).getOrElse(s"unknown error: ${runStatus.jobDetail}")
    if (isRetryableReason(reason)) {
      FailedRetryableExecutionHandle(
        new Exception(s"Job ${runStatus.jobDetail.getJobId} failed for retryable reason: $reason"), returnCode)
    } else {
      FailedNonRetryableExecutionHandle(
        new Exception(s"Job ${runStatus.jobDetail.getJobId} failed for reason: $reason"), returnCode)
    }
  }

  override def getTerminalEvents(runStatus: AwsRunStatus): Seq[ExecutionEvent] = {
    Seq(
      "createdAt" -> Option(runStatus.jobDetail.getCreatedAt),
      "startedAt" -> Option(runStatus.jobDetail.getStartedAt),
      "stoppedAt" -> Option(runStatus.jobDetail.getStoppedAt)
    ) collect {
      case (name, Some(epochMillis)) => new ExecutionEvent(name, dateToTime(epochMillis))
    }
  }

  private def dateToTime(epochMillis: Long): OffsetDateTime =
    OffsetDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneId.systemDefault)
}
