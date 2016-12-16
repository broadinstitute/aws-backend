package cromwell.backend.impl.aws

import java.nio.file.Paths

import akka.actor.{ActorRef, Props}
import cromwell.backend.{BackendConfigurationDescriptor, BackendInitializationData, BackendWorkflowDescriptor, BackendWorkflowInitializationActor}
import cromwell.core.WorkflowOptions
import wdl4s.TaskCall
import wdl4s.values.{WdlSingleFile, WdlValue}

import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.{Success, Try}


class AwsInitializationActor(override val workflowDescriptor: BackendWorkflowDescriptor,
                             override val calls: Set[TaskCall],
                             override val serviceRegistryActor: ActorRef,
                             val awsConfiguration: AwsConfiguration) extends BackendWorkflowInitializationActor with AwsTaskRunner {

  override protected def runtimeAttributeValidators: Map[String, (Option[WdlValue]) => Boolean] = Map.empty // Might be more rigorous

  override protected def coerceDefaultRuntimeAttributes(options: WorkflowOptions): Try[Map[String, WdlValue]] = Success(Map.empty)

  /** Copy inputs down from their S3 locations to the workflow inputs directory on the pre-mounted EFS volume. */
  override def beforeAll(): Future[Option[BackendInitializationData]] = {

    Future.fromTry(Try {
      val awsAttributes = awsConfiguration.awsAttributes

      val workflowDirectory = Paths.get(awsAttributes.containerMountPoint).resolve(workflowDescriptor.id.id.toString)
      val workflowInputsDirectory = workflowDirectory.resolve("workflow-inputs")

      val prepareWorkflowInputDirectory = List(
        s"cd ${awsAttributes.containerMountPoint}",
        s"mkdir -m 777 -p $workflowInputsDirectory",
        s"chmod 777 $workflowDirectory",
        s"cd $workflowInputsDirectory")

      // Workflow inputs have to be S3 cp'd onesie twosie
      val localizeWorkflowInputs = workflowDescriptor.inputs.values.collect {
        case WdlSingleFile(value) =>
          val awsFile = AwsFile(value)
          val parentDirectory = awsFile.toLocalPath.getParent
          List(
            s"mkdir -m 777 -p $parentDirectory",
            s"(cd $parentDirectory && /usr/bin/aws s3 cp $value .)"
          ).mkString(" && ")
      } toList
      val commands = (prepareWorkflowInputDirectory ++ localizeWorkflowInputs).mkString(" && ")
      log.info("initialization commands: {}", commands)

      runTask(commands, AwsBackendActorFactory.AwsCliImage, awsAttributes)
      None
    })
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
  def props(workflowDescriptor: BackendWorkflowDescriptor, calls: Set[TaskCall], serviceRegistryActor: ActorRef, awsConfiguration: AwsConfiguration): Props =
    Props(new AwsInitializationActor(workflowDescriptor, calls, serviceRegistryActor, awsConfiguration))
}
