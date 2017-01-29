package cromwell.backend.impl.aws

import cromwell.backend.standard.{StandardExpressionFunctions, StandardExpressionFunctionsParams}
import cromwell.backend.{BackendJobDescriptor, BackendJobDescriptorKey}
import cromwell.core.path.{DefaultPathBuilder, Path}

class AwsExpressionFunctions(standardParams: StandardExpressionFunctionsParams)
  extends StandardExpressionFunctions(standardParams) {

  override lazy val writeDirectory: Path = callContext.root.resolve("tempdir")

  // TODO: GLOB: "mappings... mappings everywhere." Look at SFS/JES and see what we need.

  override def preMapping(str: String): String = {
    if (AwsFile.isS3File(str)) {
      str
    } else if (DefaultPathBuilder.get(str).isAbsolute) {
      str
    } else {
      callContext.root.resolve(str.stripPrefix("/")).pathAsString
    }
  }

  override def postMapping(path: Path): Path = super.postMapping(path)
}

object AwsExpressionFunctions {

  // ECS does not like dots or colons
  def sanitizedJobDescriptor(jobDescriptor: BackendJobDescriptor): String = sanitizedJobKey(jobDescriptor.key)

  // ECS does not like dots or colons
  def sanitizedJobKey(jobKey: BackendJobDescriptorKey): String = jobKey.tag.replaceAll(":", "-").replaceAll("\\.", "-")
}
