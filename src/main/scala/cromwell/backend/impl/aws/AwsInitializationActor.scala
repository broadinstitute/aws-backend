package cromwell.backend.impl.aws

import com.typesafe.config.Config
import cromwell.backend.io.{JobPaths, WorkflowPaths}
import cromwell.backend.standard.{StandardInitializationActor, StandardInitializationActorParams, StandardInitializationData, StandardValidatedRuntimeAttributesBuilder}
import cromwell.backend.validation.DockerValidation
import cromwell.backend.{BackendInitializationData, BackendJobDescriptorKey, BackendWorkflowDescriptor}
import cromwell.core.JobKey
import cromwell.core.path.{DefaultPathBuilder, Path, PathBuilder}
import wdl4s.values.WdlSingleFile

import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.Try

case class AwsBackendInitializationData
(
  override val workflowPaths: WorkflowPaths,
  override val runtimeAttributesBuilder: StandardValidatedRuntimeAttributesBuilder,
  awsConfiguration: AwsConfiguration
) extends StandardInitializationData(workflowPaths, runtimeAttributesBuilder, classOf[AwsExpressionFunctions])

class AwsInitializationActor(standardParams: StandardInitializationActorParams)
  extends StandardInitializationActor(standardParams) with AwsTaskRunner {

  override val awsConfiguration = AwsConfiguration(configurationDescriptor)

  override def runtimeAttributesBuilder: StandardValidatedRuntimeAttributesBuilder = {
    super.runtimeAttributesBuilder.withValidation(DockerValidation.instance)
  }

  /** Copy inputs down from their S3 locations to the workflow inputs directory on the pre-mounted EFS volume. */
  override def beforeAll(): Future[Option[BackendInitializationData]] = {

    Future.fromTry(Try {
      val awsAttributes = awsConfiguration.awsAttributes

      val workflowDirectory =
        DefaultPathBuilder.get(awsAttributes.containerMountPoint).resolve(workflowDescriptor.id.id.toString)
      val workflowInputsDirectory = workflowDirectory.resolve("workflow-inputs")

      val prepareWorkflowInputDirectory = List(
        s"cd ${awsAttributes.containerMountPoint}",
        s"mkdir -m 777 -p $workflowInputsDirectory",
        s"chmod 777 $workflowDirectory",
        s"cd $workflowInputsDirectory")

      // Workflow inputs have to be S3 cp'd onesie twosie
      val localizeWorkflowInputs = workflowDescriptor.knownValues.values.collect {
        case WdlSingleFile(value) =>
          val awsFile = AwsFile(value)
          val parentDirectory = awsFile.toLocalPath.parent
          List(
            s"mkdir -m 777 -p $parentDirectory",
            s"(cd $parentDirectory && /usr/bin/aws s3 cp $value .)"
          ).mkString(" && ")
      } toList
      val commands = (prepareWorkflowInputDirectory ++ localizeWorkflowInputs).mkString(" && ")
      log.info("initialization commands: {}", commands)

      runTask(commands, AwsBackendActorFactory.AwsCliImage, awsAttributes)
      Option(AwsBackendInitializationData(workflowPaths, runtimeAttributesBuilder, awsConfiguration))
    })
  }

  override lazy val workflowPaths: WorkflowPaths = {
    new WorkflowPaths {
      outer =>
      override lazy val workflowDescriptor: BackendWorkflowDescriptor = standardParams.workflowDescriptor
      override lazy val config: Config = standardParams.configurationDescriptor.backendConfig
      override lazy val pathBuilders: List[PathBuilder] = WorkflowPaths.DefaultPathBuilders

      // TODO: GLOB: Can we use the standard workflow root?
      override lazy val workflowRoot: Path =
        DefaultPathBuilder.get(awsAttributes.containerMountPoint).resolve(workflowDescriptor.id.id.toString)

      override def toJobPaths(backendJobDescriptorKey: BackendJobDescriptorKey,
                              jobWorkflowDescriptor: BackendWorkflowDescriptor): JobPaths = {
        new JobPaths with WorkflowPaths {

          // TODO: GLOB: Can we use the standard workflow and call root?
          override lazy val workflowRoot: Path =
            DefaultPathBuilder.get(awsAttributes.containerMountPoint).resolve(jobWorkflowDescriptor.id.id.toString)
          private lazy val sanitizedJobKey = AwsExpressionFunctions.sanitizedJobKey(backendJobDescriptorKey)
          override lazy val callRoot: Path = workflowRoot.resolve(sanitizedJobKey)

          override lazy val returnCodeFilename: String = "detritus/rc.txt"
          override lazy val stdoutFilename: String = "detritus/stdout.txt"
          override lazy val stderrFilename: String = "detritus/stderr.txt"
          override lazy val scriptFilename: String = "detritus/command.sh"
          override lazy val jobKey: JobKey = backendJobDescriptorKey
          override lazy val workflowDescriptor: BackendWorkflowDescriptor = jobWorkflowDescriptor
          override lazy val config: Config = outer.config
          override lazy val pathBuilders: List[PathBuilder] = outer.pathBuilders

          override def toJobPaths(jobKey: BackendJobDescriptorKey,
                                  jobWorkflowDescriptor: BackendWorkflowDescriptor): JobPaths =
            outer.toJobPaths(jobKey, jobWorkflowDescriptor)
        }
      }
    }
  }

  /**
    * Validate that this WorkflowBackendActor can run all of the calls that it's been assigned
    */
  override def validate(): Future[Unit] = Future.successful(()) // Everything is awesome, always.  Hardcode that accordingly.
}
