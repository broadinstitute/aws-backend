package cromwell.backend.impl.aws

import cats.data.Validated._
import cats.syntax.cartesian._
import com.typesafe.config.Config
import lenthall.config.ValidatedConfig._
import wdl4s.ExceptionWithErrors


case class AwsAttributes(accessKeyId: String, secretKey: String, clusterName: String, containerMemoryMib: Int)

object AwsAttributes {

  private val awsKeys = Set(
    "access-key-id",
    "secret-key",
    "cluster-name",
    "container-memory-mib"
  )

  private val context = "AWS"

  def apply(backendConfig: Config): AwsAttributes = {
    backendConfig.warnNotRecognized(awsKeys, context)

    val accessKeyId = backendConfig.validateString("access-key-id")
    val secretKey = backendConfig.validateString("secret-key")
    val clusterName = backendConfig.validateString("cluster-name")
    val containerMemoryMib = backendConfig.validateInt("container-memory-mib")

    (accessKeyId |@| secretKey |@| clusterName |@| containerMemoryMib) map {
      AwsAttributes(_, _, _, _)
    } match {
      case Valid(r) => r
      case Invalid(f) =>
        throw new IllegalArgumentException with ExceptionWithErrors {
          override val message = "AWS Configuration is not valid: Errors"
          override val errors = f
        }
    }
  }
}
