package cromwell.backend.impl.aws

import akka.actor.Props
import cromwell.backend.BackendJobExecutionActor.{BackendJobExecutionResponse, SucceededResponse}
import cromwell.backend.{BackendConfigurationDescriptor, BackendJobDescriptor, BackendJobExecutionActor}

import scala.concurrent.Future


class AwsJobExecutionActor(override val jobDescriptor: BackendJobDescriptor,
                           override val configurationDescriptor: BackendConfigurationDescriptor,
                           val awsConfiguration: AwsConfiguration) extends BackendJobExecutionActor with AwsTaskRunner {

  override def execute: Future[BackendJobExecutionResponse] = {

    val taskDefinition = registerTaskDefinition("user-command-" + jobDescriptor.toString, ???, ???, awsConfiguration.awsAttributes)

    runTask(taskDefinition)

    deregisterTaskDefinition(taskDefinition)
    Future.successful(SucceededResponse(jobDescriptor.key, ???, ???, ???, ???))

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

  def props(jobDescriptor: BackendJobDescriptor,
            configurationDescriptor: BackendConfigurationDescriptor,
            awsConfiguration: AwsConfiguration): Props = Props(new AwsJobExecutionActor(jobDescriptor, configurationDescriptor, awsConfiguration))
}
