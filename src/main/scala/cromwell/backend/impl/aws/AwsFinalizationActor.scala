package cromwell.backend.impl.aws

import cromwell.backend._
import cromwell.backend.standard.{StandardFinalizationActor, StandardFinalizationActorParams}
import cromwell.core.JobOutput
import cromwell.core.path.{DefaultPathBuilder, Path}
import wdl4s.parser.MemoryUnit
import wdl4s.values.WdlSingleFile

import scala.concurrent.Future

case class AwsFinalizationActor(override val standardParams: StandardFinalizationActorParams
                               ) extends StandardFinalizationActor(standardParams) with AwsTaskRunner {

  lazy val awsBackendInitializationData: AwsBackendInitializationData = {
    BackendInitializationData.as[AwsBackendInitializationData](initializationDataOption)
  }

  override lazy val awsConfiguration: AwsConfiguration = awsBackendInitializationData.awsConfiguration

  lazy val workflowRootDirectory: Path = DefaultPathBuilder
    .get(awsAttributes.containerMountPoint)
    .resolve(workflowDescriptor.rootWorkflowId.toString)

  lazy val workflowInputsDirectory: Path = workflowRootDirectory.resolve("workflow-inputs")

  // Copy outputs from EFS to the output bucket
  override def afterAll(): Future[Unit] = {
    if (initializationDataOption.isDefined) {
      val outputBucket = awsConfiguration.awsAttributes.root + "/workflow-outputs"
      val commands = workflowOutputs.values.collect({ case JobOutput(WdlSingleFile(f)) =>
        val outputPath = awsBackendInitializationData.workflowPaths.getPath(f).get
        val relativePath = DefaultPathBuilder.get(awsAttributes.hostMountPoint).relativize(outputPath)
        s"/usr/bin/aws s3 cp $f $outputBucket/$relativePath" }).mkString(" && \\\n")

      log.info("finalization commands: {}", commands)

      val allPermissions = "rwxrwxrwx"
      val delocalizationScript = workflowInputsDirectory.createTempFile("delocalization", ".sh").chmod(allPermissions)
      delocalizationScript.write(commands)

      runTask(s"sh ${delocalizationScript.pathWithoutScheme}", AwsBackendActorFactory.AwsCliImage,
        MemorySize(awsAttributes.containerMemoryMib.toDouble, MemoryUnit.MiB), 1, awsAttributes)
    }
    super.afterAll()
  }
}
