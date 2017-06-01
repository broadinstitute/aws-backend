package cromwell.backend.impl.aws

import cromwell.backend._
import cromwell.backend.standard._
import cromwell.core.JobExecutionToken.JobExecutionTokenType
import net.ceedubs.ficus.Ficus._

object AwsBackendActorFactory {
  val AwsCliImage = "garland/aws-cli-docker:latest"
  val JobIdKey = "aws_task_arn"
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
}
