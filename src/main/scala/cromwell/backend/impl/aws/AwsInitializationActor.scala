package cromwell.backend.impl.aws

import cromwell.backend.BackendInitializationData
import cromwell.backend.io.WorkflowPaths
import cromwell.backend.standard.{StandardInitializationActor, StandardInitializationActorParams, StandardInitializationData, StandardValidatedRuntimeAttributesBuilder}
import cromwell.backend.validation.{CpuValidation, DockerValidation, MemoryValidation, RuntimeAttributesKeys}
import cromwell.core.path._
import wdl4s.values.{WdlArray, WdlSingleFile, WdlValue}

import scala.collection.immutable.Seq
import scala.concurrent.Future

case class AwsBackendInitializationData
(
  override val workflowPaths: WorkflowPaths,
  override val runtimeAttributesBuilder: StandardValidatedRuntimeAttributesBuilder,
  awsConfiguration: AwsConfiguration
) extends StandardInitializationData(workflowPaths, runtimeAttributesBuilder, classOf[AwsExpressionFunctions])

class AwsInitializationActor(standardParams: StandardInitializationActorParams)
  extends StandardInitializationActor(standardParams) with AwsJobRunner with AwsBucketTransfer {
  awsInitializationActor =>

  override val awsConfiguration = AwsConfiguration(configurationDescriptor)

  override def runtimeAttributesBuilder: StandardValidatedRuntimeAttributesBuilder = {
    super.runtimeAttributesBuilder.withValidation(
      DockerValidation.instance,
      MemoryValidation.withDefaultMemory(RuntimeAttributesKeys.MemoryKey, awsAttributes.containerMemory),
      CpuValidation.instance.withDefault(CpuValidation.default)
    )
  }

  lazy val workflowRootDirectory: Path = DefaultPathBuilder
    .get(awsAttributes.containerMountPoint)
    .resolve(workflowDescriptor.rootWorkflowId.toString)

  lazy val workflowInputsDirectory: Path = workflowRootDirectory.resolve("workflow-inputs")

  lazy val workflowInputsPathBuilder: PathBuilder = new MappedPathBuilder("s3://", workflowRootDirectory.pathAsString)

  override lazy val pathBuilders: Future[List[PathBuilder]] = Future.successful(List(workflowInputsPathBuilder, DefaultPathBuilder))

  override lazy val initializationData: Future[StandardInitializationData] =
    workflowPaths map { wps => AwsBackendInitializationData(wps, runtimeAttributesBuilder, awsConfiguration) }

  /** Copy inputs down from their S3 locations to the workflow inputs directory on the pre-mounted EFS volume. */
  override def beforeAll(): Future[Option[BackendInitializationData]] = {
    pathBuilders flatMap { pbs =>

      def pathsFromWdlValue(wdlValue: WdlValue): Seq[Path] = wdlValue match {
        case WdlSingleFile(value) => Seq(PathFactory.buildPath(value, pbs))
        case a: WdlArray => a.value.toList flatMap pathsFromWdlValue
        case _ => Seq.empty
      }

      // Workflow inputs have to be S3 cp'd onesie twosie
      val paths = workflowDescriptor.knownValues.values flatMap pathsFromWdlValue
      val mappedPaths = paths collect { case path: MappedPath => path }
      val mappedPathsByParent = mappedPaths.toSeq.distinct.groupBy(_.parent)
      val localizeWorkflowInputs = mappedPathsByParent map {
        case (parentDirectory, childPaths) =>
          s"""|mkdir -m 777 -p $parentDirectory && \\
              |(cd $parentDirectory && \\
              |${childPaths.map(path => s"/usr/bin/aws s3 cp ${path.prefixedPathAsString} .").mkString(" && \\\n")}) \\
              |""".stripMargin
      }
      val commands = localizeWorkflowInputs.mkString(" && \\\n")
      log.info("initialization commands: {}", commands)

      val allPermissions = "rwxrwxrwx"
      workflowRootDirectory.createDirectories().chmod(allPermissions)
      workflowInputsDirectory.createDirectories().chmod(allPermissions)
      val localizationDirectory = workflowInputsDirectory.resolve("localization")
      localizationDirectory.createDirectories().chmod(allPermissions)
      val localizationScript = localizationDirectory.createTempFile("localization", ".sh").chmod(allPermissions)
      localizationScript.write(commands)

      runBucketTransferScript(localizationScript)
      initializationData

    } map Option.apply
  }

  override lazy val workflowPaths: Future[WorkflowPaths] = awsInitializationActor.pathBuilders map { pbs =>
    AwsWorkflowPaths(
      standardParams.workflowDescriptor,
      standardParams.configurationDescriptor.backendConfig,
      DefaultPathBuilder.get(awsAttributes.hostMountPoint),
      pbs
    )
  }

  /**
    * Validate that this WorkflowBackendActor can run all of the calls that it's been assigned
    */
  override def validate(): Future[Unit] = Future.successful(()) // Everything is awesome, always.  Hardcode that accordingly.
}
