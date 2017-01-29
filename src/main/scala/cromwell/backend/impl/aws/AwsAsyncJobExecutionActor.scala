package cromwell.backend.impl.aws

import java.time.{OffsetDateTime, ZoneId}
import java.util.Date

import com.amazonaws.services.ecs.model.{DescribeTasksRequest, Task}
import cromwell.backend.async.{ExecutionHandle, FailedNonRetryableExecutionHandle, PendingExecutionHandle}
import cromwell.backend.standard.{StandardAsyncExecutionActor, StandardAsyncExecutionActorParams, StandardAsyncJob}
import cromwell.backend.validation.{DockerValidation, RuntimeAttributesValidation}
import cromwell.backend.{BackendInitializationData, BackendJobLifecycleActor}
import cromwell.core.path.{DefaultPathBuilder, Path, PathFactory}
import cromwell.core.retry.SimpleExponentialBackoff
import cromwell.core.{CromwellFatalException, ExecutionEvent}
import wdl4s.values.{WdlFile, WdlSingleFile}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

case class AwsRunStatus(task: Task) {
  override lazy val toString: String = task.getLastStatus
}

class AwsAsyncJobExecutionActor(override val standardParams: StandardAsyncExecutionActorParams)
  extends BackendJobLifecycleActor with StandardAsyncExecutionActor with AwsTaskRunner {

  lazy val awsBackendInitializationData: AwsBackendInitializationData = {
    BackendInitializationData.as[AwsBackendInitializationData](standardParams.backendInitializationDataOption)
  }

  override lazy val awsConfiguration: AwsConfiguration = awsBackendInitializationData.awsConfiguration

  override type StandardAsyncRunInfo = Any
  override type StandardAsyncRunStatus = AwsRunStatus

  override lazy val executeOrRecoverBackOff = SimpleExponentialBackoff(
    initialInterval = 3.seconds, maxInterval = 20.seconds, multiplier = 1.1)

  override lazy val pollBackOff = SimpleExponentialBackoff(
    initialInterval = 30.seconds, maxInterval = 600.seconds, multiplier = 1.1)

  private lazy val workflowDirectory = DefaultPathBuilder.get(awsConfiguration.awsAttributes.containerMountPoint)
    .resolve(jobDescriptor.workflowDescriptor.id.id.toString)

  private lazy val workflowInputs = workflowDirectory.resolve("workflow-inputs")

  override def execute(): ExecutionHandle = {
    val scriptFile = jobPaths.script
    scriptFile.parent.createDirectories().chmod("rwxrwxrwx")
    scriptFile.write(commandScriptContents)

    val cromwellCommand = redirectOutputs(s"/bin/bash ${jobPaths.script}")
    val docker = RuntimeAttributesValidation.extract(DockerValidation.instance, validatedRuntimeAttributes)
    val runTaskResult = runTaskAsync(cromwellCommand, docker, awsConfiguration.awsAttributes)

    log.info("AWS submission completed:\n{}", runTaskResult)
    val taskArn = runTaskResult.getTasks.asScala.head.getTaskArn

    PendingExecutionHandle(jobDescriptor, StandardAsyncJob(taskArn), None, None)
  }

  override def pollStatus(handle: StandardAsyncPendingExecutionHandle): AwsRunStatus = {
    val taskArn = handle.pendingJob.jobId

    val describeTasksRequest = new DescribeTasksRequest()
      .withCluster(awsAttributes.clusterName)
      .withTasks(List(taskArn).asJava)

    val describeTasksResult = ecsAsyncClient.describeTasks(describeTasksRequest)

    val tasks = describeTasksResult.getTasks.asScala
    val task = tasks.headOption.getOrElse(
      throw CromwellFatalException(new RuntimeException(s"Task $taskArn not found.")))
    AwsRunStatus(task)
  }

  override def isTerminal(runStatus: AwsRunStatus): Boolean = isStopped(runStatus.task)

  override def isSuccess(runStatus: AwsRunStatus): Boolean = isSuccess(runStatus.task)

  override def mapCommandLineWdlFile(wdlFile: WdlFile): WdlFile = {
    wdlFile match {
      case WdlSingleFile(value) if AwsFile.isS3File(value) =>
        // Any input file that looks like an S3 file must be a workflow input.
        // TODO: GLOB: .toString or .toRealString?
        WdlSingleFile(workflowInputs.resolve(AwsFile(value).toLocalPath).toString)
      case _ =>
        // TODO: GLOB: Slightly better understanding. The file path for the command line is _always_ .toString.
        // This method should not return a URI, as you can't for ex: cat file:///path/to/file, only cat /path/to/file
        WdlSingleFile(workflowPaths.buildPath(wdlFile.value).toString)
    }
  }

  // TODO: GLOB: JobPaths should _really_ differentiate java.nio.Paths between in and out of container, aka on and off host
  private def hostAbsoluteFilePath(pathString: String): Path = {
    val wdlPath = PathFactory.buildPath(pathString, workflowPaths.pathBuilders)
    jobPaths.callExecutionRoot.resolve(wdlPath).toAbsolutePath
  }

  override def mapOutputWdlFile(wdlFile: WdlFile): WdlFile = {
    wdlFile match {
      // TODO: GLOB: .toString or .toRealString for all of these?
      case WdlSingleFile(value) if AwsFile.isS3File(value) =>
        WdlSingleFile(workflowInputs.resolve(AwsFile(value).toLocalPath).pathAsString)
      case fileNotFound: WdlFile if !hostAbsoluteFilePath(fileNotFound.valueString).exists =>
        throw new RuntimeException("Could not process output, file not found: " +
          s"${hostAbsoluteFilePath(fileNotFound.valueString).pathAsString}")
      case _ => WdlFile(hostAbsoluteFilePath(wdlFile.valueString).pathAsString)
    }
  }

  override def handleExecutionSuccess(runStatus: AwsRunStatus,
                                      handle: StandardAsyncPendingExecutionHandle,
                                      returnCode: Int): ExecutionHandle = {
    log.info("AWS task completed!\n{}", runStatus.task)
    super.handleExecutionSuccess(runStatus, handle, returnCode)
  }

  override def handleExecutionFailure(runStatus: AwsRunStatus,
                                      handle: StandardAsyncPendingExecutionHandle,
                                      returnCode: Option[Int]): ExecutionHandle = {
    log.info("AWS task failed!\n{}", runStatus.task)
    val reason = containerReasonExit(runStatus.task).getOrElse("unknown")
    FailedNonRetryableExecutionHandle(new Exception(s"Task failed for reason: $reason"), returnCode)
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
