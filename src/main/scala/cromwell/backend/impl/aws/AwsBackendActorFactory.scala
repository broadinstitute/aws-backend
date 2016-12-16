package cromwell.backend.impl.aws

import akka.actor.{ActorRef, Props}
import cromwell.backend._
import cromwell.core.CallOutputs
import cromwell.core.JobExecutionToken.JobExecutionTokenType
import net.ceedubs.ficus.Ficus._
import wdl4s.TaskCall
import wdl4s.expression.WdlStandardLibraryFunctions


object AwsBackendActorFactory {
  val AwsCliImage = "garland/aws-cli-docker:latest"
}

case class AwsBackendActorFactory(name: String, configurationDescriptor: BackendConfigurationDescriptor) extends BackendLifecycleActorFactory {

  val awsConfiguration = AwsConfiguration(configurationDescriptor)

  override def workflowInitializationActorProps(workflowDescriptor: BackendWorkflowDescriptor,
                                                calls: Set[TaskCall],
                                                serviceRegistryActor: ActorRef): Option[Props] = {
    Option(AwsInitializationActor.props(workflowDescriptor, calls, serviceRegistryActor, awsConfiguration))
  }

  override def jobExecutionActorProps(jobDescriptor: BackendJobDescriptor,
                                      initializationData: Option[BackendInitializationData],
                                      serviceRegistryActor: ActorRef,
                                      backendSingletonActor: Option[ActorRef]): Props = AwsJobExecutionActor.props(jobDescriptor, configurationDescriptor, awsConfiguration)

  override def workflowFinalizationActorProps(workflowDescriptor: BackendWorkflowDescriptor,
                                              calls: Set[TaskCall],
                                              jobExecutionMap: JobExecutionMap,
                                              workflowOutputs: CallOutputs,
                                              initializationData: Option[BackendInitializationData]): Option[Props] = {
    Option(AwsFinalizationActor.props(workflowDescriptor, calls, awsConfiguration, jobExecutionMap, workflowOutputs))
  }

  override def jobExecutionTokenType: JobExecutionTokenType = {
    val concurrentJobLimit = configurationDescriptor.backendConfig.as[Option[Int]]("concurrent-job-limit").orElse(Option(10))

    JobExecutionTokenType("AWS Backend", concurrentJobLimit)
  }

  override def expressionLanguageFunctions(workflowDescriptor: BackendWorkflowDescriptor, jobKey: BackendJobDescriptorKey, initializationData: Option[BackendInitializationData]): WdlStandardLibraryFunctions = {
    val jobDescriptor = BackendJobDescriptor(workflowDescriptor, jobKey, Map.empty, Map.empty)
    AwsStandardLibraryFunctions(jobDescriptor, awsConfiguration.awsAttributes)
  }
}
