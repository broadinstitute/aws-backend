package cromwell.backend.impl.aws

import cromwell.backend.{BackendInitializationData, BackendWorkflowDescriptor}
import cromwell.core.{ExecutionStore, OutputStore}
import wdl4s.Call


class AwsFinalizationActor {

}

object AwsFinalizationActor {
  def props(workflowDescriptor: BackendWorkflowDescriptor, calls: Set[Call], executionStore: ExecutionStore, outputStore: OutputStore) = ???
}
