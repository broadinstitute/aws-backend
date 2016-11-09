package cromwell.backend.impl.aws

import akka.actor.{ActorRef, Props}
import cromwell.backend.{BackendConfigurationDescriptor, BackendInitializationData, BackendWorkflowDescriptor, BackendWorkflowInitializationActor}
import cromwell.core.WorkflowOptions
import wdl4s.Call
import wdl4s.values.{WdlFile, WdlValue}

import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.{Success, Try}


class AwsInitializationActor(override val workflowDescriptor: BackendWorkflowDescriptor,
                             override val calls: Set[Call],
                             override val serviceRegistryActor: ActorRef,
                             val awsConfiguration: AwsConfiguration) extends BackendWorkflowInitializationActor with AwsTaskRunner {

  override protected def runtimeAttributeValidators: Map[String, (Option[WdlValue]) => Boolean] = Map.empty // Might be more rigorous

  override protected def coerceDefaultRuntimeAttributes(options: WorkflowOptions): Try[Map[String, WdlValue]] = Success(Map.empty)

  /** Copy inputs down from their S3 locations to the workflow inputs directory on the pre-mounted EFS volume. */
  override def beforeAll(): Future[Option[BackendInitializationData]] = {

    // The calls within a workflow could be "jailed" to the subdirectory corresponding to their workflow, but
    // this would require some SSM shenanigans to mkdir the <mount point>/<workflow id> directory prior to
    // allocating the Volume.  Totally doable but not worth it for a POC.
    val awsAttributes = awsConfiguration.awsAttributes

    val workflowInputsDirectory = s"${workflowDescriptor.id.id}/workflow-inputs"

    val prepareWorkflowInputDirectory = List(
      s"cd ${awsAttributes.containerMountPoint}",
      s"mkdir -p $workflowInputsDirectory",
      s"cd $workflowInputsDirectory")

    // Workflow inputs have to be S3 cp'd onesie twosie
    // FIXME strip the 's3://' protocol prefix from input file paths and copy with the remainder of the path to avoid collisions
    val localizeWorkflowInputs = workflowDescriptor.inputs.values.collect { case file: WdlFile => s"/usr/bin/aws s3 cp ${file.value} ." } toList
    val commands = (prepareWorkflowInputDirectory ++ localizeWorkflowInputs).mkString(" && ")

    val taskDefinition = registerTaskDefinition("localize-workflow-inputs", commands, AwsBackendActorFactory.AwsCliImage, awsAttributes)
    runTask(taskDefinition)
    // deregisterTaskDefinition(taskDefinition)
    Future.successful(None)
  }

  /**
    * Validate that this WorkflowBackendActor can run all of the calls that it's been assigned
    */
  override def validate(): Future[Unit] = Future.successful(()) // Everything is awesome, always.  Hardcode that accordingly.

  /**
    * The configuration for the backend, in the context of the entire Cromwell configuration file.
    */
  override protected def configurationDescriptor: BackendConfigurationDescriptor = awsConfiguration.configurationDescriptor
}

object AwsInitializationActor {
  def props(workflowDescriptor: BackendWorkflowDescriptor, calls: Set[Call], serviceRegistryActor: ActorRef, awsConfiguration: AwsConfiguration): Props =
    Props(new AwsInitializationActor(workflowDescriptor, calls, serviceRegistryActor, awsConfiguration))
}
