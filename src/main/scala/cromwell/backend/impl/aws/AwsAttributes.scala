package cromwell.backend.impl.aws

import cats.data.Validated._
import cats.syntax.cartesian._
import com.typesafe.config.{Config, ConfigValue}
import lenthall.exception.MessageAggregation
import lenthall.validation.ErrorOr._
import lenthall.validation.Validation._
import net.ceedubs.ficus.Ficus._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

case class AwsAttributes(root: String, accessKeyId: String, secretKey: String, clusterName: String, containerMemoryMib: Int, hostMountPoint: String, containerMountPoint: String)

object AwsAttributes {
  lazy val Logger: Logger = LoggerFactory.getLogger("AwsAttributes")

  private val awsKeys = Set(
    "root",
    "access-key-id",
    "secret-key",
    "cluster-name",
    "container-memory-mib",
    "host-mount-point",
    "container-mount-point",
    "concurrent-job-limit"
  )

  private val context = "AWS"

  def apply(backendConfig: Config): AwsAttributes = {
    val configKeys = backendConfig.entrySet().toSet map { entry: java.util.Map.Entry[String, ConfigValue] => entry.getKey }
    warnNotRecognized(configKeys, awsKeys, context, Logger)

    val root: ErrorOr[String] = validate { backendConfig.as[String]("root") }
    val accessKeyId: ErrorOr[String] = validate { backendConfig.as[String]("access-key-id") }
    val secretKey: ErrorOr[String] = validate { backendConfig.as[String]("secret-key") }
    val clusterName: ErrorOr[String] = validate { backendConfig.as[String]("cluster-name") }
    val containerMemoryMib: ErrorOr[Int] = validate { backendConfig.as[Int]("container-memory-mib") }
    val hostMountPoint: ErrorOr[String] = validate { backendConfig.as[String]("host-mount-point") }
    val containerMountPoint: ErrorOr[String] = validate { backendConfig.as[String]("container-mount-point") }

    (root |@| accessKeyId |@| secretKey |@| clusterName |@| containerMemoryMib |@| hostMountPoint |@| containerMountPoint) map {
      AwsAttributes(_, _, _, _, _, _, _)
    } match {
      case Valid(r) => r
      case Invalid(f) =>
        throw new IllegalArgumentException with MessageAggregation {
          override def exceptionContext: String = "AWS Configuration is not valid: Errors"
          override val errorMessages: Traversable[String] = f.toList
        }
    }
  }
}
