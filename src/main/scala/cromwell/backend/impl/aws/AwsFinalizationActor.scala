package cromwell.backend.impl.aws

import java.nio.file.Paths

import akka.actor.Props
import cromwell.backend.{BackendWorkflowDescriptor, BackendWorkflowFinalizationActor}
import cromwell.core.OutputStore.OutputEntry
import cromwell.core.WorkflowOptions.FinalWorkflowOutputsDir
import cromwell.core.{ExecutionStore, OutputStore}
import wdl4s.Call
import wdl4s.values.WdlSingleFile

import scala.concurrent.Future


case class AwsFinalizationActor(override val workflowDescriptor: BackendWorkflowDescriptor,
                                override val calls: Set[Call],
                                awsConfiguration: AwsConfiguration,
                                executionStore: ExecutionStore,
                                outputStore: OutputStore) extends BackendWorkflowFinalizationActor with AwsTaskRunner {

  // Copy outputs from EFS to the output bucket
  override def afterAll(): Future[Unit] = {

    val outputBucket = workflowDescriptor.getWorkflowOption(FinalWorkflowOutputsDir).get
    val commands = outputStore.store.values.flatMap { _ collect { case OutputEntry(_, _, Some(WdlSingleFile(f))) =>
      val relativePath = Paths.get(awsAttributes.hostMountPoint).relativize(Paths.get(f))
      s"/usr/bin/aws s3 cp $f $outputBucket/$relativePath" }
    }.mkString(" && ")

    log.info("finalization commands: {}", commands)

    val taskDefinition = registerTaskDefinition("delocalizer-" + workflowDescriptor.id.id, commands, AwsBackendActorFactory.AwsCliImage, awsConfiguration.awsAttributes)
    runTask(taskDefinition)
    deregisterTaskDefinition(taskDefinition)
    Future.successful(())
  }

  override val configurationDescriptor = awsConfiguration.configurationDescriptor

}

object AwsFinalizationActor {
  def props(workflowDescriptor: BackendWorkflowDescriptor, calls: Set[Call], awsConfiguration: AwsConfiguration, executionStore: ExecutionStore, outputStore: OutputStore) =
    Props(AwsFinalizationActor(workflowDescriptor, calls, awsConfiguration, executionStore, outputStore))
}
