package cromwell.backend.impl.aws

import java.nio.file.{Path, Paths}

import com.amazonaws.services.s3.AmazonS3URI

case class AwsFile(s3Path: String) {
  def toLocalPath: Path = {
    val s3 = new AmazonS3URI(s3Path)
    Paths.get(s3.getBucket).resolve(s3.getKey)
  }
}


object AwsFile {
  def isS3File(filePath: String): Boolean = filePath.startsWith("s3://")
}
