package cromwell.backend.impl.aws

import cats.data.Validated._
import cats.syntax.cartesian._
import com.typesafe.config.{Config, ConfigValue}
import cromwell.backend.MemorySize
import lenthall.exception.MessageAggregation
import lenthall.validation.ErrorOr._
import lenthall.validation.Validation._
import net.ceedubs.ficus.Ficus._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

case class AwsAttributes(cloudOutputsRoot: String, accessKeyId: String, secretKey: String, jobQueueName: String,
                         containerMemory: String, hostMountPoint: String, containerMountPoint: String) {

  lazy val containerMemorySize = MemorySize.parse(containerMemory).get
}

object AwsAttributes {
  lazy val Logger: Logger = LoggerFactory.getLogger("AwsAttributes")

  private val awsKeys = Set(
    "cloud-outputs-root",
    "access-key-id",
    "secret-key",
    "job-queue-name",
    "container-memory",
    "host-mount-point",
    "container-mount-point",
    "concurrent-job-limit"
  )

  private val context = "AWS"

  def apply(backendConfig: Config): AwsAttributes = {
    val configKeys = backendConfig.entrySet().toSet map { entry: java.util.Map.Entry[String, ConfigValue] => entry.getKey }
    warnNotRecognized(configKeys, awsKeys, context, Logger)

    val cloudOutputsRoot: ErrorOr[String] = validate { backendConfig.as[String]("cloud-outputs-root") }
    val accessKeyId: ErrorOr[String] = validate { backendConfig.as[String]("access-key-id") }
    val secretKey: ErrorOr[String] = validate { backendConfig.as[String]("secret-key") }
    val jobQueueName: ErrorOr[String] = validate { backendConfig.as[String]("job-queue-name") }
    val containerMemory: ErrorOr[String] = validate { backendConfig.as[String]("container-memory") }
    val hostMountPoint: ErrorOr[String] = validate { backendConfig.as[String]("host-mount-point") }
    val containerMountPoint: ErrorOr[String] = validate { backendConfig.as[String]("container-mount-point") }

    val errorOrAttributes = cloudOutputsRoot |@| accessKeyId |@| secretKey |@| jobQueueName |@| containerMemory |@| hostMountPoint |@| containerMountPoint

    errorOrAttributes map AwsAttributes.apply match {
      case Valid(r) => r
      case Invalid(f) =>
        throw new IllegalArgumentException with MessageAggregation {
          override def exceptionContext: String = "AWS Configuration is not valid: Errors"
          override val errorMessages: Traversable[String] = f.toList
        }
    }
  }
}
