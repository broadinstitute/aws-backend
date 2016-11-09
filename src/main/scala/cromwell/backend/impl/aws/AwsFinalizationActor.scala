package cromwell.backend.impl.aws

import java.nio.file.Path

import akka.actor.Props
import cromwell.backend.{BackendWorkflowDescriptor, BackendWorkflowFinalizationActor}
import cromwell.core.{ExecutionStore, OutputStore}
import wdl4s.{Call, ReportableSymbol}

import scala.concurrent.Future
import scala.language.postfixOps


case class AwsFinalizationActor(override val workflowDescriptor: BackendWorkflowDescriptor,
                                override val calls: Set[Call],
                                awsConfiguration: AwsConfiguration,
                                executionStore: ExecutionStore,
                                outputStore: OutputStore) extends BackendWorkflowFinalizationActor with AwsTaskRunner {

  // Copy inputs from EFS to the output bucket
  override def afterAll(): Future[Unit] = {

    def buildSourceAndDestinationPaths(reportableOutputs: Seq[ReportableSymbol]): Seq[(Path, Path)] = ???

    val sourceAndDestinationPaths = buildSourceAndDestinationPaths(workflowDescriptor.workflowNamespace.workflow.outputs)

    val commands = sourceAndDestinationPaths.collect { case (sourcePath, destinationPath) => s"/usr/bin/aws s3 cp $sourcePath $destinationPath" } toList

    val taskDefinition = registerTaskDefinition("delocalizer-" + workflowDescriptor.id.id, commands.mkString(" && "), AwsBackendActorFactory.AwsCliImage, awsConfiguration.awsAttributes)
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
