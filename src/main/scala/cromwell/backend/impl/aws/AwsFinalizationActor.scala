package cromwell.backend.impl.aws

import akka.actor.Props
import cromwell.backend.{BackendWorkflowDescriptor, BackendWorkflowFinalizationActor}
import cromwell.core.{ExecutionStore, OutputStore}
import wdl4s.Call

import scala.concurrent.Future


case class AwsFinalizationActor(override val workflowDescriptor: BackendWorkflowDescriptor,
                                override val calls: Set[Call],
                                awsConfiguration: AwsConfiguration,
                                executionStore: ExecutionStore,
                                outputStore: OutputStore) extends BackendWorkflowFinalizationActor {

  // Copy inputs from EFS to the output bucket
  override def afterAll(): Future[Unit] = ???

  override val configurationDescriptor = awsConfiguration.configurationDescriptor

}

object AwsFinalizationActor {
  def props(workflowDescriptor: BackendWorkflowDescriptor, calls: Set[Call], awsConfiguration: AwsConfiguration, executionStore: ExecutionStore, outputStore: OutputStore) =
    Props(AwsFinalizationActor(workflowDescriptor, calls, awsConfiguration, executionStore, outputStore))
}
