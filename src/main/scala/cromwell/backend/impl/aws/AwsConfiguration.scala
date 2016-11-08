package cromwell.backend.impl.aws

import cromwell.backend.BackendConfigurationDescriptor

case class AwsConfiguration(configurationDescriptor: BackendConfigurationDescriptor) {
  val awsAttributes = AwsAttributes(configurationDescriptor.backendConfig)
}
