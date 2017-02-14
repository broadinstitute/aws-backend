package cromwell.backend.impl.aws

class AwsNonFatalException(message: String, cause: Throwable = null) extends Exception(message, cause)
