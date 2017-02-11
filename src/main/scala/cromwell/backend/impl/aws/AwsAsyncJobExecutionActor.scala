package cromwell.backend.impl.aws

import java.time.{OffsetDateTime, ZoneId}
import java.util.Date

import com.amazonaws.services.ecs.model.Task
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

case class AwsRunStatus(task: Task) {
  override lazy val toString: String = task.getLastStatus
}

class AwsAsyncJobExecutionActor(override val standardParams: StandardAsyncExecutionActorParams)
  extends BackendJobLifecycleActor with StandardAsyncExecutionActor with AwsTaskRunner {

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
    val runTaskResult = doRunTask(cromwellCommand, docker, memory, cpu, awsConfiguration.awsAttributes)

    log.info("AWS submission completed:\n{}", runTaskResult)
    val taskArn = runTaskResult.getTasks.asScala.headOption match {
      case Some(task) => task.getTaskArn
      case None =>
        val failures = runTaskResult.getFailures.asScala
        // This is purposefully not a fatal exception as there can be transient eventual consistency failures.
        throw new AwsNonFatalException(s"Task creation failed due to failures:\n${failures.mkString("\n")}")
    }

    PendingExecutionHandle(jobDescriptor, StandardAsyncJob(taskArn), None, None)
  }

  override def pollStatus(handle: StandardAsyncPendingExecutionHandle): AwsRunStatus = {
    val taskArn = handle.pendingJob.jobId
    val describeTasksResult = describeTasks(taskArn)

    val tasks = describeTasksResult.getTasks.asScala

    tasks.headOption match {
      case Some(t) => AwsRunStatus(t)
      case None =>
        // This is purposefully not a fatal exception as there can be transient eventual consistency failures.
        throw new AwsNonFatalException(s"Could not find task for arn $taskArn")
    }
  }

  override def isTerminal(runStatus: AwsRunStatus): Boolean = isStopped(runStatus.task)

  override def isSuccess(runStatus: AwsRunStatus): Boolean = isSuccess(runStatus.task)

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
    log.info("AWS task completed!\n{}", runStatus.task)
    super.handleExecutionSuccess(runStatus, handle, returnCode)
  }

  private val RetryableFailureReasons = Seq(
    "CannotPullContainerError: failed to register layer: devicemapper: Error running deviceResume dm_task_run failed"
  )

  override def handleExecutionFailure(runStatus: AwsRunStatus,
                                      handle: StandardAsyncPendingExecutionHandle,
                                      returnCode: Option[Int]): ExecutionHandle = {
    log.info("AWS task failed!\n{}", runStatus.task)
    val reason = containerReasonExit(runStatus.task).getOrElse("unknown")
    reason match {
      case _ if RetryableFailureReasons.contains(reason) =>
        FailedRetryableExecutionHandle(new Exception(s"Task failed for retryable reason: $reason"), returnCode)
      case _ =>
        FailedNonRetryableExecutionHandle(new Exception(s"Task failed for reason: $reason"), returnCode)
    }
  }

  override def getTerminalEvents(runStatus: AwsRunStatus): Seq[ExecutionEvent] = {
    Seq(
      "createdAt" -> Option(runStatus.task.getCreatedAt),
      "startedAt" -> Option(runStatus.task.getStartedAt),
      "stoppedAt" -> Option(runStatus.task.getStoppedAt)
    ) collect {
      case (name, Some(date)) => new ExecutionEvent(name, dateToTime(date))
    }
  }

  private def dateToTime(date: Date): OffsetDateTime = OffsetDateTime.ofInstant(date.toInstant, ZoneId.systemDefault)
}
