# Get Started with Rapids on AWS EMR

This is a getting started guide for Rapids on AWS EMR. At the end of this guide, the user will be able to run a sample Apache Spark application that runs on NVIDIA GPUs on AWS EMR.

The current EMR 6.2.0 release suppport Spark version 3.0.1 and Nvidia spark-rapids 0.2.0. For more details of supported applications, please see the [EMR release notes](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-6x.html).  

For more information on AWS EMR, please see the [AWS documentation](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-what-is-emr.html).

## Configure and Launch AWS EMR with GPU Nodes

###  Launch EMR Cluster using AWS CLI

Following steps is based on AWS EMR document - ["Using the Nvidia Spark-RAPIDS Accelerator for Spark"](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-rapids.html) 


You can use AWS CLI to launch a cluster with one Master node (m5.xlarge) and two nodes 2x g4dn.2xlarge  

```
 aws emr create-cluster \
--release-label emr-6.2.0 \
--applications Name=Hadoop Name=Spark Name=Livy Name=JupyterEnterpriseGateway \
--service-role EMR_DefaultRole \
--ec2-attributes KeyName=my-key-pair,InstanceProfile=EMR_EC2_DefaultRole \
--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m4.4xlarge \                 
                  InstanceGroupType=CORE,InstanceCount=1,InstanceType=g4dn.2xlarge \    
                  InstanceGroupType=TASK,InstanceCount=1,InstanceType=g4dn.2xlarge \
--configurations file:///my-configurations.json \
--bootstrap-actions Name='My Spark Rapids Bootstrap action',Path=s3://my-bucket/my-bootstrap-action.sh
```

Please fill with actual value for KeyName and file paths. You can further customize  SubnetId, EmrManagedSlaveSecurityGroup, EmrManagedMasterSecurityGroup, name and region etc. 


###  Launch EMR Cluster using AWS Console (GUI)

Go to the AWS Management Console and select the `EMR` service from the "Analytics" section. Choose the region you want to launch your cluster in, e.g. US West Oregon, using the dropdown menu in the top right corner. Click `Create cluster` and select `Go to advanced options`, which will bring up a detailed cluster configuration page.

#### Step 1:  Software, Configuration and Steps

Select **emr-6.2.0** or latest EMR version for the release, uncheck all the software options, and then check **Hadoop 3.2.1**, **Spark 3.0.1** and **Livy 0.7.0**.

In the "Edit software settings" field, copy and paste the configurions from the [EMR document](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-rapids.html). You can create a JSON file on you own S3 bucket.

![Step 1: Step 1:  Software, Configuration and Steps](pics/Rapids_EMR_GUI_1.PNG)

#### Step 2: Hardware

Select the desired VPC and availability zone in the "Network" and "EC2 Subnet" fields respectively. (Default network and subnet are ok)

In the "Core" node row, change the "Instance type" to **g4dn.xlarge**, **g4dn.2xlarge**, or **p3.2xlarge** and ensure "Instance count" is set to **1** for each. Keep the default "Master" node instance type of **m5.xlarge**.

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
```

### Submit Spark jobs to a EMR Cluster Accelerated by GPUs

Similar to spark-submit for on-prem clusters, AWS EMR supports a Spark applicaton job to be submitted. The mortgage examples we use are also available as a spark application.  You can also use spark shell to run the scala code and pyspark to run the python code on master node through CLI.
 


### Running GPU Accelerated Mortgage ETL and XGBoost Example using EMR Notebook

An EMR Notebook is a "serverless" Jupyter notebook. Unlike a traditional notebook, the contents of an EMR Notebook itself—the equations, visualizations, queries, models, code, and narrative text—are saved in Amazon S3 separately from the cluster that runs the code. This provides an EMR Notebook with durable storage, efficient access, and flexibility.

You can use the following step-by-step guide to run the example mortgage dataset using Rapids on Amazon EMR GPU clusters. For more examples, please refer to [NVIDIA/spark-rapids](https://github.com/NVIDIA/spark-rapids/)

![Create EMR Notebook](pics/EMR_notebook_2.png)

#### Create EMR Notebook and Connect to EMR GPU Cluster 

Go to the AWS Management Console and select Notebooks on the left column. Click the Create notebook button. You can then click "Choose an existing cluster" and pick the right cluster after click Choose button. Once the instance is ready,  launch the Jupyter from EMR Notebook instance. 

![Create EMR Notebook](pics/EMR_notebook_1.png)

#### Runn Mortgage ETL PySpark Notebook on EMR GPU Cluster 

Download [the Mortgate ETL PySpark Notebook](Mortgage-ETL-GPU-EMR.ipynb). Make sure to use PySpark as kernel. This example use 1 year (year 2000) data for a two node g4dn GPU cluster. You can adjust settings in the notebook for full mortgage dataset ETL. 


#### Runn Mortgage Xgboost Scala Notebook on EMR GPU Cluster 

Please refer to this [quick start guide](https://github.com/NVIDIA/spark-xgboost-examples/blob/spark-2/getting-started-guides/csp/aws/Using_EMR_Notebook.md) to running GPU accelerated XGBoost on EMR Spark Cluster.
