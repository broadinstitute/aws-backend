package cromwell.backend.impl.aws

import cromwell.backend.MemorySize
import cromwell.core.path.Path
import wdl4s.parser.MemoryUnit

/**
  * This adds redirection of standard streams and rc file capture like `StandardAsyncExecutionActor#redirectOutputs`
  * for the benefit of localization and delocalization in the initialization and finalization actors.
  */
trait AwsBucketTransfer {
  self: AwsJobRunner =>

  def runBucketTransferScript(script: Path) = {
    val parentDirectory = script.parent
    val stdout = parentDirectory.resolve("stdout").pathWithoutScheme
    val stderr = parentDirectory.resolve("stderr").pathWithoutScheme
    val rc = parentDirectory.resolve("rc").pathWithoutScheme
    val scriptPath = script.pathWithoutScheme

    runJobAndWait(s"sh $scriptPath > $stdout 2> $stderr < /dev/null || echo -1 > $rc", AwsBackendActorFactory.AwsCliImage,
      MemorySize(awsAttributes.containerMemoryMib.toDouble, MemoryUnit.MiB), 1, awsAttributes)
  }
}
