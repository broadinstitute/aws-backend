package cromwell.backend.impl.aws

import java.nio.file.Paths

import akka.actor.Props
import com.amazonaws.services.s3.AmazonS3URI
import cromwell.backend.BackendJobExecutionActor.{BackendJobExecutionResponse, SucceededResponse}
import cromwell.backend.{BackendConfigurationDescriptor, BackendJobDescriptor, BackendJobExecutionActor}
import wdl4s.values.WdlSingleFile

import scala.concurrent.Future


class AwsJobExecutionActor(override val jobDescriptor: BackendJobDescriptor,
                           override val configurationDescriptor: BackendConfigurationDescriptor,
                           val awsConfiguration: AwsConfiguration) extends BackendJobExecutionActor with AwsTaskRunner {

  override def execute: Future[BackendJobExecutionResponse] = {

    val callDir = Paths.get(awsConfiguration.awsAttributes.containerMountPoint).resolve(jobDescriptor.workflowDescriptor.id.id.toString).resolve("workflow-inputs")

    val files = jobDescriptor.inputDeclarations.values.collect {
      case WdlSingleFile(value) if AwsFile.isS3File(value) =>
        AwsFile(value).toLocalPath(callDir.toString)
    }

    log.info(s"I see some files at $files")

//    val taskDefinition = registerTaskDefinition("user-command-" + jobDescriptor.toString, ???, ???, awsConfiguration.awsAttributes)
//
//    runTask(taskDefinition)
//
//    deregisterTaskDefinition(taskDefinition)
//    Future.successful(SucceededResponse(jobDescriptor.key, ???, ???, ???, ???))

    Future.successful(SucceededResponse(jobDescriptor.key, Option(0), Map.empty, None, Seq.empty))
  }
}

object AwsJobExecutionActor {

  /*
   EFS:

   <ol>
     <li>Create the EFS filesystem via `aws efs create-file-system`</li>
     <li>Create a mount target in the appropriate availability zone.  I used the AWS console EFS UI for this but it
         appears the CLI can do it too.  The correct VPC will need to be entered here, which is available via
         ECS -> EC2 instance on the UI.  The default security group added here allows all traffic, which gets the job
         done but is probably more permissive than required.</li>
     <li>The EC2 instance needs to be added to a security group that allows "appropriate" traffic.  I attempted to
         restrict traffic to the NFS port per the instructions
         <a href="https://aws.amazon.com/blogs/compute/using-amazon-efs-to-persist-data-from-amazon-ecs-containers/">here</a>
         (or more specifically <a href="https://github.com/awslabs/amazon-ecs-amazon-efs/blob/master/amazon-efs-ecs.json">here</a>),
         but this caused hangs and timeouts attempting to mount.  I added a security group to the EC2 instance that
         allows all traffic and that got past the problem, but this is again probably more permissive than required.</li>
      <li>SSH into the EC2 instance and mount the EFS volume:
        <pre>
sudo mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 $(curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone).fs-f9b86eb0.efs.us-east-1.amazonaws.com:/ /usr/share/iodir
        <pre>
      </li>
      <li>
         Restart the Docker daemon on the EC2 instance.  This is required to avoid some
         <a href="https://forums.docker.com/t/docker-fails-to-mount-v-volume-from-nfs-mounted-directory/582/3">weird behavior</a>
         where the NFS filesystem does not look the same inside and outside of containers.

         <pre>sudo service docker restart</pre>
      </li>
      <li>
         Restart the ECS agent:
         <pre>sudo start ecs</pre>
      </li>
   </ol>

   */

  def props(jobDescriptor: BackendJobDescriptor,
            configurationDescriptor: BackendConfigurationDescriptor,
            awsConfiguration: AwsConfiguration): Props = Props(new AwsJobExecutionActor(jobDescriptor, configurationDescriptor, awsConfiguration))
}
