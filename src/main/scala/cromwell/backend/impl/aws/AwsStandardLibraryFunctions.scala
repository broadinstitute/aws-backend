package cromwell.backend.impl.aws

import java.nio.file.{Path, Paths}

import better.files.File
import cromwell.backend.BackendJobDescriptor
import cromwell.backend.wdl.{ReadLikeFunctions, WriteFunctions}
import cromwell.core.path.PathBuilder
import wdl4s.TsvSerializable
import wdl4s.expression.WdlStandardLibraryFunctions
import wdl4s.values.{WdlArray, WdlFile, WdlMap, WdlObject, WdlSingleFile, WdlValue}

import scala.language.postfixOps
import scala.util.Try

case class AwsStandardLibraryFunctions(jobDescriptor: BackendJobDescriptor, awsAttributes: AwsAttributes) extends
  WdlStandardLibraryFunctions with ReadLikeFunctions with WriteFunctions {

  val workflowRootPath: Path = Paths.get(awsAttributes.containerMountPoint).resolve(jobDescriptor.workflowDescriptor.id.id.toString)

  val callRootPath: Path = workflowRootPath.resolve(AwsStandardLibraryFunctions.sanitizedJobDescriptor(jobDescriptor))

  val detritusPath: Path = callRootPath.resolve("detritus")

  override val writeDirectory: Path = callRootPath.resolve("tempdir")

  override def writeTempFile(path: String, prefix: String, suffix: String, content: String): String =
    super[WriteFunctions].writeTempFile(path, prefix, suffix, content)

  private def detritusFile(name: String): Try[WdlSingleFile] = Try { WdlSingleFile(detritusPath.resolve(name).toString) }

  override def stdout(params: Seq[Try[WdlValue]]): Try[WdlFile] = detritusFile("stdout.txt")

  override def stderr(params: Seq[Try[WdlValue]]): Try[WdlFile] = detritusFile("stderr.txt")

  override def pathBuilders: List[PathBuilder] = List(AwsPathBuilder(callRootPath.toString))

  override def globPath(glob: String): String = callRootPath.toString

  override def glob(path: String, pattern: String): Seq[String] = {
    File(callRootPath).glob(s"**/$pattern") map { _.pathAsString } toSeq
  }

  private lazy val _writeDirectory = File(writeDirectory).createDirectories()

  private def writeContent(baseName: String, content: String): Try[WdlFile] = {
    import wdl4s.values._

    val tmpFile = _writeDirectory / s"$baseName-${content.md5Sum}.tmp"

    Try {
      if (tmpFile.notExists) tmpFile.write(content)
    } map { _ =>
      // TODO everything in this class from _writeDirectory down was copy/pasted from WriteFunctions because of a
      // fairly recent change to the line below that the current WriteFunctions structure doesn't allow me to override
      // less clumsily.  I'm not sure what the intent behind that change was so I currently can't give a name to
      // the concept in order to refactor this.
      // WdlFile(tmpFile.uri.toString)
      WdlFile(tmpFile.toString)
    }
  }

  private def writeToTsv(params: Seq[Try[WdlValue]], wdlClass: Class[_ <: WdlValue with TsvSerializable]) = {
    for {
      singleArgument <- extractSingleArgument(params)
      downcast <- Try(wdlClass.cast(singleArgument))
      tsvSerialized <- downcast.tsvSerialize
      file <- writeContent(wdlClass.getSimpleName.toLowerCase, tsvSerialized)
    } yield file
  }

  override def write_lines(params: Seq[Try[WdlValue]]): Try[WdlFile] = writeToTsv(params, classOf[WdlArray])
  override def write_map(params: Seq[Try[WdlValue]]): Try[WdlFile] = writeToTsv(params, classOf[WdlMap])
  override def write_object(params: Seq[Try[WdlValue]]): Try[WdlFile] = writeToTsv(params, classOf[WdlObject])
  override def write_objects(params: Seq[Try[WdlValue]]): Try[WdlFile] = writeToTsv(params, classOf[WdlArray])
  override def write_tsv(params: Seq[Try[WdlValue]]): Try[WdlFile] = writeToTsv(params, classOf[WdlArray])
}

object AwsStandardLibraryFunctions {

  // ECS does not like dots or colons
  def sanitizedJobDescriptor(jobDescriptor: BackendJobDescriptor): String = jobDescriptor.key.tag.replaceAll(":", "-").replaceAll("\\.", "-")
}

case class AwsPathBuilder(callDir: String) extends PathBuilder {
  override val name = "AwsPathBuilder"

  override def build(pathAsString: String): Try[Path] = Try {
    val maybeAbsolute = Paths.get(pathAsString)
    if (maybeAbsolute.isAbsolute) maybeAbsolute else Paths.get(callDir).resolve(pathAsString)
  }
}
