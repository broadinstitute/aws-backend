package cromwell.backend.impl.aws

import com.typesafe.config.Config
import cromwell.backend.io.{JobPaths, WorkflowPaths}
import cromwell.backend.standard.{StandardInitializationActor, StandardInitializationActorParams, StandardInitializationData, StandardValidatedRuntimeAttributesBuilder}
import cromwell.backend.validation.{CpuValidation, DockerValidation, MemoryValidation}
import cromwell.backend.{BackendInitializationData, BackendJobDescriptorKey, BackendWorkflowDescriptor, MemorySize}
import cromwell.core.JobKey
import cromwell.core.path._
import wdl4s.parser.MemoryUnit
import wdl4s.values.{WdlArray, WdlSingleFile, WdlValue}

import scala.collection.immutable.Seq
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
  awsInitializationActor =>

  override val awsConfiguration = AwsConfiguration(configurationDescriptor)

  override def runtimeAttributesBuilder: StandardValidatedRuntimeAttributesBuilder = {
    super.runtimeAttributesBuilder.withValidation(
      DockerValidation.instance,
      MemoryValidation.withDefaultMemory(MemorySize(awsAttributes.containerMemoryMib.toDouble, MemoryUnit.MiB)),
      CpuValidation.default
    )
  }

  lazy val workflowRootDirectory: Path = DefaultPathBuilder
    .get(awsAttributes.containerMountPoint)
    .resolve(workflowDescriptor.rootWorkflowId.toString)

  lazy val workflowInputsDirectory: Path = workflowRootDirectory.resolve("workflow-inputs")

  lazy val workflowInputsPathBuilder: PathBuilder = new MappedPathBuilder("s3://", workflowRootDirectory.pathAsString)

  override lazy val pathBuilders: List[PathBuilder] = List(workflowInputsPathBuilder, DefaultPathBuilder)

  override lazy val initializationData: StandardInitializationData =
    AwsBackendInitializationData(workflowPaths, runtimeAttributesBuilder, awsConfiguration)

  /** Copy inputs down from their S3 locations to the workflow inputs directory on the pre-mounted EFS volume. */
  override def beforeAll(): Future[Option[BackendInitializationData]] = {

    def pathsFromWdlValue(wdlValue: WdlValue): Seq[Path] = wdlValue match {
      case WdlSingleFile(value) => Seq(PathFactory.buildPath(value, pathBuilders))
      case a: WdlArray => a.value.toList flatMap pathsFromWdlValue
      case _ => Seq.empty
    }

    Future.fromTry(Try {
      val awsAttributes = awsConfiguration.awsAttributes

      // Workflow inputs have to be S3 cp'd onesie twosie
      val paths = workflowDescriptor.knownValues.values flatMap pathsFromWdlValue
      val localizeWorkflowInputs: Seq[String] = paths collect {
        case path: MappedPath =>
          val parentDirectory = path.parent
          List(
            s"mkdir -m 777 -p $parentDirectory",
            s"(cd $parentDirectory && /usr/bin/aws s3 cp ${path.prefixedPathAsString} .)"
          ).mkString(" && \\\n    ")
      } toList
      val commands = localizeWorkflowInputs.mkString(" && \\\n")
      log.info("initialization commands: {}", commands)

      val allPermissions = "rwxrwxrwx"
      workflowRootDirectory.createDirectories().chmod(allPermissions)
      workflowInputsDirectory.createDirectories().chmod(allPermissions)
      val localizationScript = workflowInputsDirectory.createTempFile("localization", ".sh").chmod(allPermissions)
      localizationScript.write(commands)

      runTask(s"sh ${localizationScript.pathWithoutScheme}", AwsBackendActorFactory.AwsCliImage,
        MemorySize(awsAttributes.containerMemoryMib.toDouble, MemoryUnit.MiB), 1, awsAttributes)
      Option(initializationData)
    })
  }

  override lazy val workflowPaths: WorkflowPaths = {
    new WorkflowPaths {
      workflowPaths =>
      override lazy val workflowDescriptor: BackendWorkflowDescriptor = standardParams.workflowDescriptor
      override lazy val config: Config = standardParams.configurationDescriptor.backendConfig
      override lazy val pathBuilders: List[PathBuilder] = awsInitializationActor.pathBuilders

      // TODO: Switch AwsAttributes.root's name and config key. Then this override is no longer needed.
      override lazy val executionRootString: String = workflowRootDirectory.resolve("cromwell-executions").pathAsString

      override def toJobPaths(backendJobDescriptorKey: BackendJobDescriptorKey,
                              jobWorkflowDescriptor: BackendWorkflowDescriptor): JobPaths = {
        new JobPaths with WorkflowPaths {

          override lazy val workflowDescriptor: BackendWorkflowDescriptor = jobWorkflowDescriptor
          override lazy val config: Config = workflowPaths.config
          override lazy val pathBuilders: List[PathBuilder] = workflowPaths.pathBuilders

          // TODO: Switch AwsAttributes.root's name and config key. Then this override is no longer needed.
          override lazy val executionRootString: String = workflowPaths.executionRootString

          private lazy val sanitizedJobKey = AwsExpressionFunctions.sanitizedJobKey(backendJobDescriptorKey)
          override lazy val callRoot: Path = workflowRoot.resolve(sanitizedJobKey)

          override lazy val jobKey: JobKey = backendJobDescriptorKey

          override def toJobPaths(backendJobDescriptorKey: BackendJobDescriptorKey,
                                  jobWorkflowDescriptor: BackendWorkflowDescriptor): JobPaths =
            workflowPaths.toJobPaths(backendJobDescriptorKey, jobWorkflowDescriptor)
        }
      }
    }
  }

  /**
    * Validate that this WorkflowBackendActor can run all of the calls that it's been assigned
    */
  override def validate(): Future[Unit] = Future.successful(()) // Everything is awesome, always.  Hardcode that accordingly.
}
