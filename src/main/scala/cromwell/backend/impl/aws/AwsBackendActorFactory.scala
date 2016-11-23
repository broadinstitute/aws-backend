package cromwell.backend.impl.aws

import akka.actor.{ActorRef, Props}
import cromwell.backend.{BackendConfigurationDescriptor, BackendInitializationData, BackendJobDescriptor, BackendLifecycleActorFactory, BackendWorkflowDescriptor}
import cromwell.core.{ExecutionStore, OutputStore}
import wdl4s.Call

object AwsBackendActorFactory {
  val AwsCliImage = "garland/aws-cli-docker:latest"
}

case class AwsBackendActorFactory(name: String, configurationDescriptor: BackendConfigurationDescriptor) extends BackendLifecycleActorFactory {

  val awsConfiguration = AwsConfiguration(configurationDescriptor)

  override def workflowInitializationActorProps(workflowDescriptor: BackendWorkflowDescriptor, calls: Set[Call], serviceRegistryActor: ActorRef): Option[Props] = {
    Option(AwsInitializationActor.props(workflowDescriptor, calls, serviceRegistryActor, awsConfiguration))
  }

  override def jobExecutionActorProps(jobDescriptor: BackendJobDescriptor,
                                      initializationData: Option[BackendInitializationData],
                                      serviceRegistryActor: ActorRef,
                                      backendSingletonActor: Option[ActorRef]): Props = AwsJobExecutionActor.props(jobDescriptor, configurationDescriptor, awsConfiguration)

  override def workflowFinalizationActorProps(workflowDescriptor: BackendWorkflowDescriptor, calls: Set[Call], executionStore: ExecutionStore, outputStore: OutputStore, initializationData: Option[BackendInitializationData]): Option[Props] = {
    Option(AwsFinalizationActor.props(workflowDescriptor, calls, awsConfiguration, executionStore, outputStore))
  }
}
