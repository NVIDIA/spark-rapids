# Get Started with Rapids on AWS EMR

This is a getting started guide for Rapids on AWS EMR. At the end of this guide, the user will be able to run a sample Apache Spark application that runs on NVIDIA GPUs on AWS EMR.

For more information on AWS EMR, please see the [AWS documentation](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-what-is-emr.html).

## Configure and Launch AWS EMR with GPU Nodes


###  Launch EMR Cluster using AWS CLI

For Master node with m5.xlarge and two core nodes 2x g4dn.2xlarge  (will change to template)

```
aws emr create-cluster \
--release-label emr-6.1.0 \
--service-role EMR_DefaultRole \
--applications Name=Hadoop Name=Spark Name=Livy \
--ec2-attributes '{"KeyName":"miangangz-aws-east-1-eng","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-7e41d850","EmrManagedSlaveSecurityGroup":"sg-0d6f9a040b3668dd2","EmrManagedMasterSecurityGroup":"sg-042b0e48a5cfe0772"}' \
--instance-groups '[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master - 1"},{"InstanceCount":2,"InstanceGroupType":"CORE","InstanceType":"g4dn.xlarge","Name":"Core - 2"}]' \
--custom-ami-id ami-0947d2ba12ee1ff75 \
--additional-info "{
        'developmentOption' : {
            'platformGpgCheck': false,
            'platformRepoUrl' : 'http://awssupportdatasvcs.com/bootstrap-actions/rapids_preview_for_nvidia/620_platform',
            'appsGpgCheck': false,
            'applicationRepoUrl' : 'http://awssupportdatasvcs.com/bootstrap-actions/rapids_preview_for_nvidia/620_apps'
        }
    }" \
--bootstrap-actions Name='Prepare for Rapids ITests',Path=s3://awssupportdatasvcs.com/bootstrap-actions/rapids_preview_for_nvidia/install/prepare_itests_ba.sh \
--configurations file:///home/ec2-user/preview_config.json \
--steps Type=CUSTOM_JAR,Name=CustomJAR,ActionOnFailure=CONTINUE,Jar=s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar,Args=["s3://awssupportdatasvcs.com/bootstrap-actions/rapids_preview_for_nvidia/step/run_rapids_itests.sh"] \
--log-uri 's3n://aws-logs-354625738399-us-east-1/elasticmapreduce/' \
--name 'Miangangz-spark3-620' \
--region us-east-1
```

Fill with actual value for KeyName, SubnetId, EmrManagedSlaveSecurityGroup, EmrManagedMasterSecurityGroup, S3 bucket for logs, name and region. 

And download the JSON configuration file [preview_config.json](preview_config.json) in raw format to the local file storage where you will issue the above AWS CLI command.  

###  Launch EMR Cluster using AWS Console (GUI)

Go to the AWS Management Console and select the `EMR` service from the "Analytics" section. Choose the region you want to launch your cluster in, e.g. US West Oregon, using the dropdown menu in the top right corner. Click `Create cluster` and select `Go to advanced options`, which will bring up a detailed cluster configuration page.

#### Step 1:  Software, Configuration and Steps

Select **emr-6.2.0** or latest EMR version for the release, uncheck all the software options, and then check **Hadoop 3.2.1**, **Spark 3.0.1** and **Livy 0.7.0**.

In the "Edit software settings" field, add the followings JSON file from S3 (). You can also customize and upload to you own S3 bucket.

In the "Steps" field, add Custom JAR with following JAR file (s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar) and  Arguments ("s3://awssupportdatasvcs.com/bootstrap-actions/rapids_preview_for_nvidia/step/run_rapids_itests.sh")


![Step 1: Step 1:  Software, Configuration and Steps](pics/Rapids_EMR_GUI_1.PNG)

#### Step 2: Hardware

Select the desired VPC and availability zone in the "Network" and "EC2 Subnet" fields respectively. (Default network and subnet are ok)

In the "Core" node row, change the "Instance type" to **g4dn.xlarge**, **g4dn.2xlarge**, or **p3.2xlarge** and ensure "Instance count" is set to **2**. Keep the default "Master" node instance type of **m5.xlarge** and ignore the unnecessary "Task" node configuration.

![Step 2: Hardware](pics/Rapids_EMR_GUI_2.PNG)

#### Step 3:  General Cluster Settings

Enter a custom "Cluster name" and make a note of the s3 folder that cluster logs will be written to.

*Optionally* add key-value "Tags", configure a "Custom AMI", or add custom "Bootstrap Actions"  for the EMR cluster on this page.

![Step 3: General Cluster Settings](pics/Rapids_EMR_GUI_3.PNG)

####  Step 4: Security

Select an existing "EC2 key pair" that will be used to authenticate SSH access to the cluster's nodes. If you do not have access to an EC2 key pair, follow these instructions to [create an EC2 key pair](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html#having-ec2-create-your-key-pair).

*Optionally* set custom security groups in the "EC2 security groups" tab.

In the "EC2 security groups" tab, confirm that the security group chosen for the "Master" node allows for SSH access. Follow these instructions to [allow inbound SSH traffic](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/authorizing-access-to-an-instance.html) if the security group does not allow it yet.

![Step 4: Security](pics/Rapids_EMR_GUI_4.PNG)

#### Finish Cluster Configuration

The EMR cluster management page displays the status of multiple clusters or detailed information about a chosen cluster. In the detailed cluster view, the "Summary" and "Hardware" tabs can be used to monitor the status of master and core nodes as they provision and initialize.

When the cluster is ready, a green-dot will appear next to the cluster name and the "Status" column will display **Waiting, cluster ready**.

In the cluster's "Summary" tab, find the "Master public DNS" field and click the `SSH` button. Follow the instructions to SSH to the new cluster's master node.

![Finish Cluster Configuration](pics/Rapids_EMR_GUI_5.PNG)


### Running an example joint operation using Spark Shell

SSH to the EMR cluster's master node, get into sparks shell and run the sql join example to verify GPU operation.

```
spark-shell
```

Running following Scala code in Spark Shell

```
val data = 1 to 10000
val df1 = sc.parallelize(data).toDF()
val df2 = sc.parallelize(data).toDF()
val out = df1.as("df1").join(df2.as("df2"), $"df1.value" === $"df2.value")
out.count()
out.explain()
```

The output should have GPU operstions 

```
scala> out.explain()
== Physical Plan ==
*(3) GpuColumnarToRow false
+- GpuShuffledHashJoin [value#2], [value#8], Inner, BuildRight
   :- GpuCoalesceBatches TargetSize(2147483647)
   :  +- GpuColumnarExchange gpuhashpartitioning(value#2, 48), true, [id=#207]
   :     +- GpuRowToColumnar TargetSize(2147483647)
   :        +- *(1) SerializeFromObject [input[0, int, false] AS value#2]
   :           +- Scan[obj#1]
   +- GpuCoalesceBatches RequireSingleBatch
      +- GpuColumnarExchange gpuhashpartitioning(value#8, 48), true, [id=#213]
         +- GpuRowToColumnar TargetSize(2147483647)
            +- *(2) SerializeFromObject [input[0, int, false] AS value#8]
               +- Scan[obj#7]

```


### Submit Spark jobs to a EMR Cluster Accelerated by GPUs

Similar to spark-submit for on-prem clusters, AWS EMR supports a Spark applicaton job to be submitted. The mortgage examples we use are also available as a spark application.  You can also use spark shell to run the scala code and pyspark to run the python code on master node through CLI.
 


### Running Mortgage ETL PySpark Example using EMR Notebook

An EMR Notebook is a "serverless" Jupyter notebook. Unlike a traditional notebook, the contents of an EMR Notebook itself—the equations, visualizations, queries, models, code, and narrative text—are saved in Amazon S3 separately from the cluster that runs the code. This provides an EMR Notebook with durable storage, efficient access, and flexibility.

You can use the following step-by-step guide to run the example mortgage dataset using Rapids on Amazon EMR GPU clusters. For more examples, please refer to [NVIDIA/spark-rapids](https://github.com/NVIDIA/spark-rapids/)


#### Create EMR Notebook and Connect to EMR GPU Cluster 

Go to the AWS Management Console and select Notebooks on the left column. Click the Create notebook button. You can then click "Choose an existing cluster" and pick the right cluster after click Choose button.

Then you can lauch the Jupyter Notebook from EMR Notebook instance. 


#### Download and Run GPU Mortgage ETL Example (using 1 year data)

Download this Mortgate ETL PySpark Notebook from [here](Mortgage-ETL-GPU-EMR.ipynb). Make sure to use PySpark as kernel. 


