package cromwell.backend.impl.aws

import com.amazonaws.services.s3.AmazonS3URI
import cromwell.core.path.{DefaultPathBuilder, Path}

case class AwsFile(s3Path: String) {
  def toLocalPath: Path = {
    val s3 = new AmazonS3URI(s3Path)
    DefaultPathBuilder.get(s3.getBucket).resolve(s3.getKey)
  }
}


object AwsFile {
  def isS3File(filePath: String): Boolean = filePath.startsWith("s3://")
}
