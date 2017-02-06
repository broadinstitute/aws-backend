package cromwell.backend.impl.aws

import cromwell.backend.standard.{StandardExpressionFunctions, StandardExpressionFunctionsParams}
import cromwell.backend.{BackendJobDescriptor, BackendJobDescriptorKey}
import cromwell.core.path.{DefaultPath, Path}

class AwsExpressionFunctions(standardParams: StandardExpressionFunctionsParams)
  extends StandardExpressionFunctions(standardParams) {

  override lazy val writeDirectory: Path = callContext.root.resolve("tempdir")

  override def postMapping(path: Path) = {
    path match {
      case _: DefaultPath if !path.isAbsolute => callContext.root.resolve(path)
      case _ => path
    }
  }
}

object AwsExpressionFunctions {

  // ECS does not like dots or colons
  def sanitizedJobDescriptor(jobDescriptor: BackendJobDescriptor): String = sanitizedJobKey(jobDescriptor.key)

  // ECS does not like dots or colons
  def sanitizedJobKey(jobKey: BackendJobDescriptorKey): String = jobKey.tag.replaceAll(":", "-").replaceAll("\\.", "-")
}
