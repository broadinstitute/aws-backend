package cromwell.backend.impl.aws

import java.nio.file.Paths
import java.util.Base64

import akka.actor.Props
import cromwell.backend.BackendJobExecutionActor.{BackendJobExecutionResponse, JobSucceededResponse}
import cromwell.backend.wdl.OutputEvaluator
import cromwell.backend.{BackendConfigurationDescriptor, BackendJobDescriptor, BackendJobExecutionActor}
import wdl4s.values.{WdlFile, WdlSingleFile, WdlValue}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}


class AwsJobExecutionActor(override val jobDescriptor: BackendJobDescriptor,
                           override val configurationDescriptor: BackendConfigurationDescriptor,
                           val awsConfiguration: AwsConfiguration) extends BackendJobExecutionActor with AwsTaskRunner {

  override def execute: Future[BackendJobExecutionResponse] = {

    val workflowDirectory = Paths.get(awsConfiguration.awsAttributes.containerMountPoint)
      .resolve(jobDescriptor.workflowDescriptor.id.id.toString)

    val workflowInputs = workflowDirectory.resolve("workflow-inputs")

    val inputs = jobDescriptor.inputDeclarations.collect {
      case (key, WdlSingleFile(value)) if AwsFile.isS3File(value) =>
        // Any input file that looks like an S3 file must be a workflow input.
        key -> WdlSingleFile(workflowInputs.resolve(AwsFile(value).toLocalPath).toString)
      case kv => kv
    }

    log.info(s"inputs: {}", inputs)
    val docker = jobDescriptor.runtimeAttributes("docker").valueString

    val functions = AwsStandardLibraryFunctions(jobDescriptor, awsAttributes)
    val callDir = functions.callRootPath

    val task = jobDescriptor.call.task
    val userCommand = task.instantiateCommand(inputs, functions).get
    val encoded = new String(Base64.getEncoder.encode(userCommand.getBytes))
    log.info(s"Encoded command is $encoded")

    val cromwellCommand = List(
      s"mkdir -m 777 -p $callDir/detritus",
      s"cd $callDir",
      s"echo $encoded | base64 -d > detritus/command.sh",
      s"(/bin/bash -e detritus/command.sh 1> detritus/stdout.txt 2> detritus/stderr.txt)\necho $$? > detritus/rc.txt\n"
    ).mkString(" && ")

    log.info(s"Command is $cromwellCommand")

    runTask(cromwellCommand, docker, awsConfiguration.awsAttributes)

    def postMapper(wdlValue: WdlValue): Try[WdlValue] = Try {
      val mapped = wdlValue match {
        case WdlSingleFile(value) if AwsFile.isS3File(value) => WdlSingleFile(workflowInputs.resolve(AwsFile(value).toLocalPath).toString)
        case WdlSingleFile(value) if !Paths.get(value).isAbsolute => WdlFile(callDir.resolve(value).toString)
        case x => x
      }
      mapped
    }

    OutputEvaluator.evaluateOutputs(jobDescriptor, functions, postMapper) match {
      case Success(outputs) =>
        Future.successful(JobSucceededResponse(jobDescriptor.key, Option(0), outputs, None, Seq.empty))
      case Failure(x) =>
        Future.failed(x)
    }
  }
}

object AwsJobExecutionActor {

  def props(jobDescriptor: BackendJobDescriptor,
            configurationDescriptor: BackendConfigurationDescriptor,
            awsConfiguration: AwsConfiguration): Props = Props(new AwsJobExecutionActor(jobDescriptor, configurationDescriptor, awsConfiguration))
}
