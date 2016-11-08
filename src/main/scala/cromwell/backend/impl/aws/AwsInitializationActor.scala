package cromwell.backend.impl.aws

import akka.actor.{ActorRef, Props}
import cromwell.backend.{BackendConfigurationDescriptor, BackendInitializationData, BackendWorkflowDescriptor, BackendWorkflowInitializationActor}
import cromwell.core.WorkflowOptions
import wdl4s.Call
import wdl4s.values.WdlValue

import scala.concurrent.Future
import scala.util.Try


class AwsInitializationActor(override val workflowDescriptor: BackendWorkflowDescriptor,
                             override val calls: Set[Call],
                             override val serviceRegistryActor: ActorRef) extends BackendWorkflowInitializationActor {

  override protected def runtimeAttributeValidators: Map[String, (Option[WdlValue]) => Boolean] = ???

  override protected def coerceDefaultRuntimeAttributes(options: WorkflowOptions): Try[Map[String, WdlValue]] = ???

  /**
    * A call which happens before anything else runs
    */
  override def beforeAll(): Future[Option[BackendInitializationData]] = ???

  /**
    * Validate that this WorkflowBackendActor can run all of the calls that it's been assigned
    */
  override def validate(): Future[Unit] = ???

  /**
    * The configuration for the backend, in the context of the entire Cromwell configuration file.
    */
  override protected def configurationDescriptor: BackendConfigurationDescriptor = ???
}

object AwsInitializationActor {
  def props(workflowDescriptor: BackendWorkflowDescriptor, calls: Set[Call], serviceRegistryActor: ActorRef): Props =
    Props(new AwsInitializationActor(workflowDescriptor, calls, serviceRegistryActor))
}
