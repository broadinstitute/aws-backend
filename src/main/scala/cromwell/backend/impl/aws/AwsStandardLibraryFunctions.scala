package cromwell.backend.impl.aws

import java.nio.file.Paths

import cromwell.backend.BackendJobDescriptor
import cromwell.backend.wdl.{ReadLikeFunctions, WriteFunctions}
import cromwell.core.path.{DefaultPathBuilder, PathBuilder}
import wdl4s.expression.{PureStandardLibraryFunctionsLike, WdlStandardLibraryFunctions}
import wdl4s.values.{WdlFile, WdlSingleFile, WdlValue}

import scala.util.Try

case class AwsStandardLibraryFunctions(jobDescriptor: BackendJobDescriptor, awsAttributes: AwsAttributes) extends
  WdlStandardLibraryFunctions with PureStandardLibraryFunctionsLike with ReadLikeFunctions with WriteFunctions {

  val workflowRootPath = Paths.get(awsAttributes.containerMountPoint).resolve(jobDescriptor.workflowDescriptor.id.id.toString)

  // ECS does not like dots or colons
  val sanitizedJobDescriptor = jobDescriptor.key.tag.replaceAll(":", "-").replaceAll("\\.", "-")

  val callRootPath = workflowRootPath.resolve(sanitizedJobDescriptor)

  val detritusPath = callRootPath.resolve("detritus")

  override val writeDirectory = callRootPath.resolve("tempdir")

  override def writeTempFile(path: String, prefix: String, suffix: String, content: String): String =
    super[WriteFunctions].writeTempFile(path, prefix, suffix, content)

  private def detritusFile(name: String): Try[WdlSingleFile] = Try { WdlSingleFile(detritusPath.resolve(name).toString) }

  override def stdout(params: Seq[Try[WdlValue]]): Try[WdlFile] = detritusFile("stdout.txt")

  override def stderr(params: Seq[Try[WdlValue]]): Try[WdlFile] = detritusFile("stderr.txt")

  override def pathBuilders: List[PathBuilder] = List(DefaultPathBuilder)
}
