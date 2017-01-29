package cromwell.backend.impl.aws

import akka.event.LoggingAdapter
import cromwell.backend._
import cromwell.backend.callcaching.FileHashingActor.{FileHashingFunction, SingleFileHashRequest}
import cromwell.backend.standard._
import cromwell.core.JobExecutionToken.JobExecutionTokenType
import net.ceedubs.ficus.Ficus._

import scala.util.Try

object AwsBackendActorFactory {
  val AwsCliImage = "garland/aws-cli-docker:latest"
  val JobIdKey = "aws_task_arn"

  // TODO: GLOB: MD5 is computationally expensive. Make the SFS hashing options available to other backends?
  private def md5HashingFunction(singleFileHashRequest: SingleFileHashRequest,
                                 loggingAdapter: LoggingAdapter): Try[String] = {
    val initializationData =
      BackendInitializationData.as[StandardInitializationData](singleFileHashRequest.initializationData)
    val pathTry = initializationData.workflowPaths.getPath(singleFileHashRequest.file.value)
    pathTry map { _.md5 }
  }
}

case class AwsBackendActorFactory(name: String, configurationDescriptor: BackendConfigurationDescriptor)
  extends StandardLifecycleActorFactory {

  lazy val awsConfiguration = AwsConfiguration(configurationDescriptor)

  override lazy val initializationActorClass: Class[_ <: StandardInitializationActor] = classOf[AwsInitializationActor]

  override lazy val asyncExecutionActorClass: Class[_ <: StandardAsyncExecutionActor] =
    classOf[AwsAsyncJobExecutionActor]

  override lazy val finalizationActorClassOption: Option[Class[_ <: StandardFinalizationActor]] =
    Option(classOf[AwsFinalizationActor])

  override lazy val jobIdKey: String = AwsBackendActorFactory.JobIdKey

  override def jobExecutionTokenType: JobExecutionTokenType = {
    val concurrentJobLimit = configurationDescriptor.backendConfig.as[Option[Int]]("concurrent-job-limit").orElse(Option(10))

    JobExecutionTokenType("AWS Backend", concurrentJobLimit)
  }

  override lazy val fileHashingFunction: Option[FileHashingFunction] =
    Option(FileHashingFunction(AwsBackendActorFactory.md5HashingFunction))
}
