package cromwell.backend.impl.aws

import java.nio.file.Paths

import akka.actor.Props
import cromwell.backend.{BackendConfigurationDescriptor, BackendWorkflowDescriptor, BackendWorkflowFinalizationActor, JobExecutionMap}
import cromwell.core.{CallOutputs, JobOutput}
import wdl4s.TaskCall
import wdl4s.values.WdlSingleFile

import scala.concurrent.Future


case class AwsFinalizationActor(override val workflowDescriptor: BackendWorkflowDescriptor,
                                override val calls: Set[TaskCall],
                                awsConfiguration: AwsConfiguration,
                                executionStore: JobExecutionMap,
                                outputStore: CallOutputs) extends BackendWorkflowFinalizationActor with AwsTaskRunner {

  // Copy outputs from EFS to the output bucket
  override def afterAll(): Future[Unit] = {

    val outputBucket = awsConfiguration.awsAttributes.root + "/workflow-outputs"
    val commands = outputStore.values.collect({ case JobOutput(WdlSingleFile(f)) =>
      val relativePath = Paths.get(awsAttributes.hostMountPoint).relativize(Paths.get(f))
      s"/usr/bin/aws s3 cp $f $outputBucket/$relativePath" }).mkString(" && ")

    log.info("finalization commands: {}", commands)

    runTask(commands, AwsBackendActorFactory.AwsCliImage, awsConfiguration.awsAttributes)
    Future.successful(())
  }

  override val configurationDescriptor: BackendConfigurationDescriptor = awsConfiguration.configurationDescriptor
}

object AwsFinalizationActor {
  def props(workflowDescriptor: BackendWorkflowDescriptor, calls: Set[TaskCall], awsConfiguration: AwsConfiguration, executionStore: JobExecutionMap, outputStore: CallOutputs) =
    Props(AwsFinalizationActor(workflowDescriptor, calls, awsConfiguration, executionStore, outputStore))
}
