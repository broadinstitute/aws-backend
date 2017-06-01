package cromwell.backend.impl.aws

import com.typesafe.config.Config
import cromwell.backend.{BackendJobDescriptorKey, BackendWorkflowDescriptor}
import cromwell.backend.io.{JobPaths, WorkflowPaths}
import cromwell.core.JobKey
import cromwell.core.path.{Path, PathBuilder}

case class AwsWorkflowPaths(override val workflowDescriptor: BackendWorkflowDescriptor,
                            override val config: Config,
                            hostMountPoint: Path,
                            pathBuilders: List[PathBuilder]) extends WorkflowPaths {

  override lazy val executionRootString: String = {
    val workflowId = workflowDescriptor.rootWorkflowId.toString()
    hostMountPoint.resolve(workflowId).resolve("cromwell-executions").pathAsString
  }

  override def toJobPaths(jobKey: BackendJobDescriptorKey,
                          ignored: BackendWorkflowDescriptor): JobPaths =
    AwsJobPaths(jobKey, workflowRoot, this)

  override protected def toJobPaths(workflowPaths: WorkflowPaths, jobKey: BackendJobDescriptorKey): JobPaths =
    AwsJobPaths(jobKey, workflowRoot, workflowPaths)

  override protected def withDescriptor(workflowDescriptor: BackendWorkflowDescriptor): WorkflowPaths =
    AwsWorkflowPaths(workflowDescriptor, config, hostMountPoint, pathBuilders)
}

case class AwsJobPaths(
                        backendJobDescriptorKey: BackendJobDescriptorKey,
                        workflowRoot: Path,
                        override val workflowPaths: WorkflowPaths
                      ) extends JobPaths {

  private lazy val sanitizedJobKey = AwsExpressionFunctions.sanitizedJobKey(backendJobDescriptorKey)

  override lazy val callRoot: Path = workflowRoot.resolve(sanitizedJobKey)

  override lazy val jobKey: JobKey = backendJobDescriptorKey
}
