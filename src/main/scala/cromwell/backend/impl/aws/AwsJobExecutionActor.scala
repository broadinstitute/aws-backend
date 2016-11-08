package cromwell.backend.impl.aws

import akka.actor.Props
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.ecs.AmazonECSAsyncClient
import com.amazonaws.services.ecs.model._
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClient
import cromwell.backend.BackendJobExecutionActor.{BackendJobExecutionResponse, SucceededResponse}
import cromwell.backend.impl.aws.util.AwsSdkAsyncHandler
import cromwell.backend.{BackendConfigurationDescriptor, BackendJobDescriptor, BackendJobExecutionActor}
import net.ceedubs.ficus.Ficus._
import wdl4s.expression.PureStandardLibraryFunctions

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}


class AwsJobExecutionActor(override val jobDescriptor: BackendJobDescriptor,
                           override val configurationDescriptor: BackendConfigurationDescriptor) extends BackendJobExecutionActor {

  val awsAccessKeyId = configurationDescriptor.backendConfig.as[String]("access-key-id")
  val awsSecretKey = configurationDescriptor.backendConfig.as[String]("secret-key")
  val awsClusterName = configurationDescriptor.backendConfig.as[String]("cluster-name")
  val awsContainerMemoryMib = configurationDescriptor.backendConfig.as[Int]("container-memory-mib")
  val awsMyBucket = configurationDescriptor.backendConfig.as[String]("my-bucket")

  val credentials = new AWSCredentials {
    override def getAWSAccessKeyId: String = awsAccessKeyId

    override def getAWSSecretKey: String = awsSecretKey
  }

  val ecsAsyncClient = new AmazonECSAsyncClient(credentials)

  val ec2Client = new AmazonEC2Client(credentials)

  val ssmClient = new AWSSimpleSystemsManagementClient()

  override def execute: Future[BackendJobExecutionResponse] = {

    val taskDefinition = registerTaskDefinition()
    // This prints out the plaintext secret key
    // log.info("Task definition: {}", taskDefinition)

    val taskResult =
      ecsAsyncClient.runTask(new RunTaskRequest()
        .withTaskDefinition(taskDefinition.getTaskDefinitionArn)
        .withCluster(awsClusterName)
      )

    log.info("task result is {}", taskResult)
    // Error checking needed here, if something is wrong getTasks will be empty.
    waitUntilDone(taskResult.getTasks.asScala.head)

    deregisterTaskDefinition(taskDefinition)

    Future.successful(SucceededResponse(jobDescriptor.key, Option(0), Map.empty, None, Seq.empty))
  }

  private def waitUntilDone(task: Task): Task = {
    val taskArn = task.getTaskArn
    log.info("Checking status for {}", taskArn)
    val describeTasksRequest = new DescribeTasksRequest()
      .withCluster(awsClusterName)
      .withTasks(List(taskArn).asJava)

    val resultHandler = new AwsSdkAsyncHandler[DescribeTasksRequest, DescribeTasksResult]()
    val _ = ecsAsyncClient.describeTasksAsync(describeTasksRequest, resultHandler)

    val describedTasks = Await.result(resultHandler.future, Duration.Inf)
    val taskDescription = describedTasks.result.getTasks.asScala.headOption
    taskDescription match {
      case Some(td) if td.getLastStatus == DesiredStatus.STOPPED.toString =>
        logResult(describedTasks.result)
        td
      case notStopped =>
        log.info(s"Still waiting for completion. Last known status: {}", notStopped.map(_.getLastStatus).getOrElse("UNKNOWN"))
        Thread.sleep(2000)
        waitUntilDone(task)
    }
  }

  private def registerTaskDefinition(): TaskDefinition = {
    import AwsJobExecutionActor.MountPoint
    val volumeProperties = new HostVolumeProperties().withSourcePath("/tmp")
    val iodirName = "iodir"
    val volume = new Volume().withHost(volumeProperties).withName(iodirName)
    val input = s"$awsMyBucket/gumby.png"

    val mountPoint = new MountPoint().withSourceVolume(iodirName).withContainerPath(MountPoint)
    val accessKey = new KeyValuePair().withName("AWS_ACCESS_KEY_ID").withValue(awsAccessKeyId)
    val secretAccessKey = new KeyValuePair().withName("AWS_SECRET_ACCESS_KEY").withValue(awsSecretKey)

    val sanitizedJobDescriptor = jobDescriptor.toString.replaceAll(raw"[\\\.:]", "-")

    val localizeCommand = List(
      s"cd $MountPoint",
      s"mkdir $sanitizedJobDescriptor",
      s"cd $sanitizedJobDescriptor",
      s"/usr/bin/aws s3 cp $input .",
      "echo $? > localize.rc.tmp",
      "mv localize.rc.tmp localize.rc").mkString(" && ")

    val localizationContainerDefinition = new ContainerDefinition()
      .withName("localize-v1-" + sanitizedJobDescriptor)
      .withCommand("/bin/sh", "-xv", "-c", localizeCommand)
      .withImage("garland/aws-cli-docker")
      .withMemory(awsContainerMemoryMib)
      .withMountPoints(mountPoint)
      .withEnvironment(accessKey, secretAccessKey)
      .withEssential(false)

    val command = jobDescriptor.call.task.instantiateCommand(jobDescriptor.inputDeclarations, PureStandardLibraryFunctions, identity).get
    log.info(s"User command is $command")

    val userCommand = s"""

cd $MountPoint
# This can't safely cd to containerCommandDir since that directory is created by the preceding container and
# may not exist yet.  Instead look for the localization rc in the appropriate subdirectory of containerIoDir.
# Once that file materializes the script can cd to containerCommandDir.
while ! [ -f $sanitizedJobDescriptor/localize.rc ]
do
  echo "waiting for localization..." >> userCommand.log
  sleep 5
done

cd $sanitizedJobDescriptor

if [ `cat localize.rc` -eq 0 ]
then
  echo 'Running user command "$command"' >> userCommand.log
  $command
  echo $$? > userCommand.rc.tmp
else
  echo 'Found non-zero localization return code, skipping user command.' >> userCommand.log
  cp localize.rc.tmp userCommand.rc.tmp
fi

mv userCommand.rc.tmp userCommand.rc
       """.stripMargin

    val image = jobDescriptor.call.task.runtimeAttributes.attrs("docker").evaluate(Map.empty, PureStandardLibraryFunctions).get.valueString
    val userCommandContainerDefinition = new ContainerDefinition()
      .withName("user-command-v1-" + sanitizedJobDescriptor)
      .withCommand("/bin/sh", "-xv", "-c", userCommand)
      .withImage(image)
      .withMemory(awsContainerMemoryMib) // TODO AWS AwsRuntimeAttributes
      .withMountPoints(mountPoint)
      .withEssential(false)

val delocalizationCommand = s"""

cd $MountPoint
while ! [ -f $sanitizedJobDescriptor/userCommand.rc ]
do
  echo "waiting for user command..." >> delocalization.log
  sleep 5
done

cd $sanitizedJobDescriptor

echo "Running delocalization... " >> delocalization.log
/usr/bin/aws s3 cp . $awsMyBucket --recursive
# Clean up after running, this container instance (and currently this volume) gets recycled!
rm -rf *

   """.stripMargin

    val delocalizationContainerDefinition = new ContainerDefinition()
      .withName("delocalize-v1-" + sanitizedJobDescriptor)
      .withCommand("/bin/sh", "-xv", "-c", delocalizationCommand)
      .withImage("garland/aws-cli-docker")
      .withMemory(awsContainerMemoryMib)
      .withMountPoints(mountPoint)
      .withEnvironment(accessKey, secretAccessKey)
      // The delocalization is the only container marked essential, so the task will not exit until this container
      // has finished running.
      .withEssential(true)

    val taskRequest = new RegisterTaskDefinitionRequest()
      .withFamily("cromwell-" + sanitizedJobDescriptor)
      .withContainerDefinitions(localizationContainerDefinition, userCommandContainerDefinition, delocalizationContainerDefinition)
      .withVolumes(volume)

    ecsAsyncClient.registerTaskDefinition(taskRequest).getTaskDefinition
  }

  private def deregisterTaskDefinition(taskDefinition: TaskDefinition) = {
    ecsAsyncClient.deregisterTaskDefinition(new DeregisterTaskDefinitionRequest().withTaskDefinition(taskDefinition.getTaskDefinitionArn))
  }

  private def logResult(taskDescription: DescribeTasksResult): Unit = {
    taskDescription.getFailures.asScala.toList match {
      case Nil => log.info("complete: {}", taskDescription)
      case failures => log.error("failures: {}\n{}", failures.map(_.getReason).mkString("\n"), taskDescription)
    }
  }
}

object AwsJobExecutionActor {

  /*
   EFS:

   <ol>
     <li>Create the EFS filesystem via `aws efs create-file-system`</li>
     <li>Create a mount target in the appropriate availability zone.  I used this AWS console EFS UI for this but it
         appears the CLI can do this too.  The correct VPC will need to be entered here, which is available via
         ECS -> EC2 instance on the UI.  The default security group added here allows all traffic, which gets the job
         done but is probably more permissive than required.</li>
     <li>The EC2 instance needs to be added to a security group that allows "appropriate" traffic.  I attempted to
         restrict traffic to the NFS port per the instructions
         <a href="https://aws.amazon.com/blogs/compute/using-amazon-efs-to-persist-data-from-amazon-ecs-containers/">here</a>
         (or more specifically <a href="https://github.com/awslabs/amazon-ecs-amazon-efs/blob/master/amazon-efs-ecs.json">here</a>),
         but this caused hangs and timeouts attempting to mount.  I added a security group to the EC2 instance that
         allows all traffic and that got past the problem, but is again probably more permissive than required.</li>
   </ol>

   This monstrosity is the mount command:

   <pre>sudo mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 $(curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone).fs-f9b86eb0.efs.us-east-1.amazonaws.com:/ /usr/share/iodir
   */

  val MountPoint = "/usr/share/iodir"

  def props(jobDescriptor: BackendJobDescriptor,
            configurationDescriptor: BackendConfigurationDescriptor): Props = Props(new AwsJobExecutionActor(jobDescriptor, configurationDescriptor))
}
