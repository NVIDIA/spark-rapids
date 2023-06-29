---
layout: page
title: GCP Dataproc
nav_order: 4
parent: Getting-Started
---

# Getting Started with RAPIDS Accelerator on GCP Dataproc
 [Google Cloud Dataproc](https://cloud.google.com/dataproc) is Google Cloud's fully managed Apache
 Spark and Hadoop service. The quick start guide will go through:
 
* [Create a Dataproc Cluster Accelerated by GPUs](#create-a-dataproc-cluster-accelerated-by-gpus)
  * [Create a Dataproc Cluster using T4's](#create-a-dataproc-cluster-using-t4s)
  * [Build custom Dataproc image to reduce cluster initialization time](#build-a-custom-dataproc-image-to-reduce-cluster-init-time)
  * [Create a Dataproc Cluster using MIG with A100's](#create-a-dataproc-cluster-using-mig-with-a100s)
  * [Cluster creation troubleshooting](#cluster-creation-troubleshooting)
* [Run PySpark or Scala ETL and XGBoost training Notebook on a Dataproc Cluster Accelerated by
  GPUs](#run-pyspark-or-scala-notebook-on-a-dataproc-cluster-accelerated-by-gpus)
* [Submit the same sample ETL application as a Spark job to a Dataproc Cluster Accelerated by
  GPUs](#submit-spark-jobs-to-a-dataproc-cluster-accelerated-by-gpus)

We provide some RAPIDS tools to analyze the clusters and the applications running on [Google Cloud Dataproc](https://cloud.google.com/dataproc) including:
* [Diagnosing a GPU Cluster](#diagnosing-a-gpu-cluster)
* [Bootstrap GPU cluster with optimized settings](#bootstrap-gpu-cluster-with-optimized-settings)
* [Qualify CPU workloads for GPU acceleration](#qualify-cpu-workloads-for-gpu-acceleration)
* [Tune applications on GPU cluster](#tune-applications-on-gpu-cluster)

The Prerequisites of the RAPIDS tools including:
* gcloud CLI is installed: https://cloud.google.com/sdk/docs/install
* python 3.8+
* `pip install spark-rapids-user-tools`

## Create a Dataproc Cluster Accelerated by GPUs

You can use [Cloud Shell](https://cloud.google.com/shell) to execute shell commands that will
create a Dataproc cluster.  Cloud Shell contains command line tools for interacting with Google
Cloud Platform, including gcloud and gsutil.  Alternatively, you can install [GCloud
SDK](https://cloud.google.com/sdk/install) on your machine.  From the Cloud Shell, users will need
to enable services within your project.  Enable the Compute and Dataproc APIs in order to access
Dataproc, and enable the Storage API as you’ll need a Google Cloud Storage bucket to house your
data.  This may take several minutes.

```bash
gcloud services enable compute.googleapis.com
gcloud services enable dataproc.googleapis.com
gcloud services enable storage-api.googleapis.com
``` 

After the command line environment is set up, log in to your GCP account.  You can now create a
Dataproc cluster. Dataproc supports multiple different GPU types depending on your use case.
Generally, T4 is a good option for use with the RAPIDS Accelerator for Spark. We also support
MIG on the Ampere architecture GPUs like the A100. Using
[MIG](https://docs.nvidia.com/datacenter/tesla/mig-user-guide/) you can request an A100 and split
it up into multiple different compute instances, and it runs like you have multiple separate GPUs.

The example configurations below will allow users to run any of the [notebook
demos](../demo/GCP) on GCP. Adjust the sizes and
number of GPU based on your needs.

The script below will initialize with the following:

* [GPU Driver and RAPIDS Acclerator for Apache Spark](https://github.com/GoogleCloudDataproc/initialization-actions/tree/master/spark-rapids) through
  initialization actions (please note it takes up to 1 week for the latest init script to be merged
  into the GCP Dataproc public GCS bucket)

  To make changes to example configuration, make a copy of `spark-rapids.sh` and add the RAPIDS
  Accelerator related parameters according to [tuning guide](../tuning-guide.md) and modify the
  `--initialization-actions` parameter to point to the updated version.
* Configuration for [GPU scheduling and isolation](yarn-gpu.md)
* [Local SSD](https://cloud.google.com/dataproc/docs/concepts/compute/dataproc-local-ssds) is
  recommended for Spark scratch space to improve IO
* Component gateway enabled for accessing Web UIs hosted on the cluster

### Create a Dataproc Cluster Using T4's
* One 16-core master node and 5 32-core worker nodes
* Two NVIDIA T4 for each worker node
Note that with image-version 2.1 Secure Boot is enabled by default and not all operating systems currently have
support for installing GPU drivers that are properly signed.  Please follow 
[this document](https://cloud.google.com/compute/docs/gpus/install-drivers-gpu#secure-boot) for more details on how
to set up supported operating systems with secure boot and signed GPU drivers.  For unsupported operating
systems you can disable secure boot by adding the `--no-shielded-secure-boot` configuration while creating the cluster. This
should allow the cluster to boot correctly with unsigned GPU drivers.

```bash
    export REGION=[Your Preferred GCP Region]
    export GCS_BUCKET=[Your GCS Bucket]
    export CLUSTER_NAME=[Your Cluster Name]
    # Number of GPUs to attach to each worker node in the cluster
    export NUM_GPUS=2
    # Number of Spark worker nodes in the cluster
    export NUM_WORKERS=5

gcloud dataproc clusters create $CLUSTER_NAME  \
    --region=$REGION \
    --image-version=2.0-ubuntu18 \
    --master-machine-type=n2-standard-16 \
    --num-workers=$NUM_WORKERS \
    --worker-accelerator=type=nvidia-tesla-t4,count=$NUM_GPUS \
    --worker-machine-type=n1-highmem-32 \
    --num-worker-local-ssds=4 \
    --initialization-actions=gs://goog-dataproc-initialization-actions-${REGION}/spark-rapids/spark-rapids.sh \
    --optional-components=JUPYTER,ZEPPELIN \
    --metadata=rapids-runtime=SPARK \
    --bucket=$GCS_BUCKET \
    --enable-component-gateway \
    --subnet=default
```

This takes around 10-15 minutes to complete.  You can navigate to the [Dataproc clusters tab](https://console.cloud.google.com/dataproc/clusters) in the
Google Cloud Console to see the progress.

![Dataproc Cluster](../img/GCP/dataproc-cluster.png)

To reduce initialization time to 4-5 minutes, create a custom Dataproc image using
[this guide](#build-a-custom-dataproc-image-to-reduce-cluster-init-time).

### Build a Custom Dataproc Image to Reduce Cluster Init Time
Building a custom Dataproc image that already has NVIDIA drivers, the CUDA toolkit, and the RAPIDS Accelerator for Apache Spark preinstalled and preconfigured will reduce cluster initialization time to 3-4 minutes. The custom image
can also be used in an air gap environment. In this example, we will utilize [these instructions
from GCP](https://cloud.google.com/dataproc/docs/guides/dataproc-images) to create a custom image.

Google provides the `generate_custom_image.py` [script](https://github.com/GoogleCloudDataproc/custom-images/blob/master/generate_custom_image.py) that:
- Launches a temporary Compute Engine VM instance with the specified Dataproc base image.
- Then runs the customization script inside the VM instance to install custom packages and/or
update configurations.
- After the customization script finishes, it shuts down the VM instance and creates a Dataproc
  custom image from the disk of the VM instance.
- The temporary VM is deleted after the custom image is created.
- The custom image is saved and can be used to create Dataproc clusters.


The following example clones the [Dataproc custom-images](https://github.com/GoogleCloudDataproc/custom-images) repository, downloads the [`spark-rapids.sh`](https://github.com/GoogleCloudDataproc/initialization-actions/tree/master/spark-rapids) script locally, and then creates a custom Dataproc image with Spark RAPIDS resources using the downlaoded resources. The `spark-rapids.sh` script is passed as the customization script and installs the RAPIDS Accelerator for Apache Spark along with NVIDIA drivers and other depencies. Custom image generation may take 20-25 minutes to complete.

```bash
git clone https://github.com/GoogleCloudDataproc/custom-images
cd custom-images
wget https://raw.githubusercontent.com/GoogleCloudDataproc/initialization-actions/master/spark-rapids/spark-rapids.sh

export ZONE=[Your Preferred GCP Zone]
export GCS_BUCKET=[Your GCS Bucket]
export CUSTOMIZATION_SCRIPT=./spark-rapids.sh
export IMAGE_NAME=sample-20-ubuntu18-gpu-t4
export DATAPROC_VERSION=2.0-ubuntu18
export GPU_NAME=nvidia-tesla-t4
export GPU_COUNT=1

python generate_custom_image.py \
    --image-name $IMAGE_NAME \
    --dataproc-version $DATAPROC_VERSION \
    --customization-script $CUSTOMIZATION_SCRIPT \
    --no-smoke-test \
    --zone $ZONE \
    --gcs-bucket $GCS_BUCKET \
    --machine-type n1-standard-4 \
    --accelerator type=$GPU_NAME,count=$GPU_COUNT \
    --disk-size 200 \
    --subnet default 
```

See [here](https://cloud.google.com/dataproc/docs/guides/dataproc-images#running_the_code) for
details on `generate_custom_image.py` script configuration and
[here](https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-versions) for dataproc
version information.

The custom `sample-20-ubuntu18-gpu-t4` image is now ready and can be viewed in the GCP console under
`Compute Engine > Storage > Images`.

Let's launch a cluster using the `sample-20-ubuntu18-gpu-t4` custom image:

```bash 
export REGION=[Your Preferred GCP Region]
export GCS_BUCKET=[Your GCS Bucket]
export CLUSTER_NAME=[Your Cluster Name]
export IMAGE_NAME=sample-20-ubuntu18-gpu-t4
export NUM_GPUS=1
export NUM_WORKERS=2

gcloud dataproc clusters create $CLUSTER_NAME  \
    --region=$REGION \
    --image=$IMAGE_NAME \
    --master-machine-type=n2-standard-4 \
    --num-workers=$NUM_WORKERS \
    --worker-accelerator=type=nvidia-tesla-t4,count=$NUM_GPUS \
    --worker-machine-type=n1-standard-4 \
    --num-worker-local-ssds=1 \
    --optional-components=JUPYTER,ZEPPELIN \
    --metadata=rapids-runtime=SPARK \
    --bucket=$GCS_BUCKET \
    --enable-component-gateway \
    --subnet=default 
```

There are no initiailization actions that need to be configured because NVIDIA drivers and Spark RAPIDS resources are already installed in the custom image. The new cluster should be up and running within 3-4 minutes!

### Create a Dataproc Cluster using MIG with A100's
* 1x 16-core master node and 5 12-core worker nodes
* 1x NVIDIA A100 for each worker node, split into 2 MIG instances using
[instance profile 3g.20gb](https://docs.nvidia.com/datacenter/tesla/mig-user-guide/#a100-profiles).

```bash
    export REGION=[Your Preferred GCP Region]
    export ZONE=[Your Preferred GCP Zone]
    export GCS_BUCKET=[Your GCS Bucket]
    export CLUSTER_NAME=[Your Cluster Name]
    # Number of GPUs to attach to each worker node in the cluster
    export NUM_GPUS=1
    # Number of Spark worker nodes in the cluster
    export NUM_WORKERS=4

gcloud dataproc clusters create $CLUSTER_NAME  \
    --region=$REGION \
    --zone=$ZONE \
    --image-version=2.0-ubuntu18 \
    --master-machine-type=n2-standard-16 \
    --num-workers=$NUM_WORKERS \
    --worker-accelerator=type=nvidia-tesla-a100,count=$NUM_GPUS \
    --worker-machine-type=a2-highgpu-1g \
    --num-worker-local-ssds=4 \
    --initialization-actions=gs://goog-dataproc-initialization-actions-${REGION}/spark-rapids/spark-rapids.sh \
    --metadata=startup-script-url=gs://goog-dataproc-initialization-actions-${REGION}/spark-rapids/mig.sh \
    --optional-components=JUPYTER,ZEPPELIN \
    --metadata=rapids-runtime=SPARK \
    --bucket=$GCS_BUCKET \
    --enable-component-gateway \
    --subnet=default
```

To change the MIG instance profile you can specify either the profile id or profile name via the
metadata parameter `MIG_CGI`. Below is an example of using a profile name and a profile id.

```bash
    --metadata=^:^MIG_CGI='3g.20gb,9'
```

This may take around 10-15 minutes to complete.  You can navigate to the Dataproc clusters tab in
the Google Cloud Console to see the progress.

![Dataproc Cluster](../img/GCP/dataproc-cluster.png)

To reduce initialization time to 4-5 minutes, create a custom Dataproc image using
[this](#build-a-custom-dataproc-image-to-reduce-cluster-init-time) guide.

### Cluster Creation Troubleshooting
If you encounter an error related to GPUs not being available because of your account quotas, please
go to this page for updating your quotas: [Quotas and limits](https://cloud.google.com/compute/quotas).

If you encounter an error related to GPUs not available in the specific region or zone, you will
need to update the REGION or ZONE parameter in the cluster creation command.

## Run PySpark or Scala Notebook on a Dataproc Cluster Accelerated by GPUs
To use notebooks with a Dataproc cluster, click on the cluster name under the Dataproc cluster tab
and navigate to the "Web Interfaces" tab.  Under "Web Interfaces", click on the JupyterLab or
Jupyter link. Download the sample [Mortgage ETL on GPU Jupyter
Notebook](../demo/GCP/Mortgage-ETL.ipynb) and upload it to Jupyter.

To get example data for the sample notebook, please refer to these
[instructions](https://github.com/NVIDIA/spark-rapids-examples/blob/main/docs/get-started/xgboost-examples/dataset/mortgage.md).
Download the desired data, decompress it, and upload the csv files to a GCS bucket.

![Dataproc Web Interfaces](../img/GCP/dataproc-service.png)

The sample notebook will transcode the CSV files into Parquet files before running an ETL query
that prepares the dataset for training.  The ETL query splits the data, saving 20% of the data in
a separate GCS location training for evaluation.  Using the default notebook configuration the
first stage should take ~110 seconds (1/3 of CPU execution time with same config) and the second
stage takes ~170 seconds (1/7 of CPU execution time with same config).  The notebook depends on
the pre-compiled [RAPIDS plugin for Apache Spark](https://mvnrepository.com/artifact/com.nvidia/rapids-4-spark) which is pre-downloaded and pre-configured by
the GCP Dataproc [Spark RAPIDS init
script](https://github.com/GoogleCloudDataproc/initialization-actions/tree/master/spark-rapids).

Once the data is prepared, we use the [Mortgage XGBoost4j Scala
Notebook](../demo/GCP/mortgage-xgboost4j-gpu-scala.ipynb) in Dataproc's Jupyter notebook to execute
the training job on GPUs. Scala based XGBoost examples use [DLMC
XGBoost](https://github.com/dmlc/xgboost). For a PySpark based XGBoost example, please refer to
[Spark-RAPIDS-examples](https://github.com/NVIDIA/spark-rapids-examples/blob/main/docs/get-started/xgboost-examples/on-prem-cluster/yarn-python.md) that
makes sure the required libraries are installed.

The training time should be around 680 seconds (1/7 of CPU execution time with same config). This
is shown under cell:

```scala
// Start training
println("\n------ Training ------")
val (xgbClassificationModel, _) = benchmark("train") {
  xgbClassifier.fit(trainSet)
}
```

## Submit Spark jobs to a Dataproc Cluster Accelerated by GPUs
Similar to `spark-submit` for on-prem clusters, Dataproc supports submitting Spark applications to
Dataproc clusters.  The previous mortgage examples are also available as a [Spark
application](https://github.com/NVIDIA/spark-rapids-examples/tree/main/examples/XGBoost-Examples).

Follow these
[instructions](https://github.com/NVIDIA/spark-rapids-examples/blob/main/docs/get-started/xgboost-examples/building-sample-apps/scala.md)
to Build the
[xgboost-example](https://github.com/NVIDIA/spark-rapids-examples/blob/main/docs/get-started/xgboost-examples)
jars. Upload the `sample_xgboost_apps-${VERSION}-SNAPSHOT-jar-with-dependencies.jar` to a GCS
bucket by dragging and dropping the jar file from your local machine into the GCS web console or by running:
```
gsutil cp aggregator/target/sample_xgboost_apps-${VERSION}-SNAPSHOT-jar-with-dependencies.jar gs://${GCS_BUCKET}/scala/
```

Submit the Spark XGBoost application to dataproc using the following command:
```bash
export REGION=[Your Preferred GCP Region]
export GCS_BUCKET=[Your GCS Bucket]
export CLUSTER_NAME=[Your Cluster Name]
export VERSION=[Your jar version]
export SPARK_NUM_EXECUTORS=20
export SPARK_EXECUTOR_MEMORY=20G
export SPARK_EXECUTOR_MEMORYOVERHEAD=16G
export SPARK_NUM_CORES_PER_EXECUTOR=7
export DATA_PATH=gs://${GCS_BUCKET}/mortgage_full

gcloud dataproc jobs submit spark \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --class=com.nvidia.spark.examples.mortgage.GPUMain \
    --jars=gs://${GCS_BUCKET}/scala/sample_xgboost_apps-${VERSION}-SNAPSHOT-jar-with-dependencies.jar \
    --properties=spark.executor.cores=${SPARK_NUM_CORES_PER_EXECUTOR},spark.task.cpus=${SPARK_NUM_CORES_PER_EXECUTOR},spark.executor.memory=${SPARK_EXECUTOR_MEMORY},spark.executor.memoryOverhead=${SPARK_EXECUTOR_MEMORYOVERHEAD},spark.executor.resource.gpu.amount=1,spark.task.resource.gpu.amount=1,spark.rapids.sql.batchSizeBytes=512M,spark.rapids.sql.reader.batchSizeBytes=768M,spark.rapids.sql.variableFloatAgg.enabled=true,spark.rapids.memory.gpu.pooling.enabled=false,spark.dynamicAllocation.enabled=false \
    -- \
    -dataPath=train::${DATA_PATH}/train \
    -dataPath=trans::${DATA_PATH}/eval \
    -format=parquet \
    -numWorkers=${SPARK_NUM_EXECUTORS} \
    -treeMethod=gpu_hist \
    -numRound=100 \
    -maxDepth=8   
``` 

## Diagnosing a GPU Cluster

The diagnostic tool can be run to check a GPU cluster with RAPIDS Accelerator for Apache Spark
is healthy and ready for Spark jobs, such as checking the version of installed NVIDIA driver,
cuda-toolkit, RAPIDS Accelerator and running Spark test jobs etc. This tool also can
be used by the front line support team for basic diagnostic and troubleshooting before escalating
to NVIDIA RAPIDS Accelerator for Apache Spark engineering team.

Usage: `spark_rapids_dataproc diagnostic --cluster <cluster-name> --region <region>`

Help (to see all options available): `spark_rapids_dataproc diagnostic --help`

Example output:

```text
*** Running diagnostic function "nv_driver" ***
Warning: Permanently added 'compute.9009746126288801979' (ECDSA) to the list of known hosts.
Fri Oct 14 05:17:55 2022
+-----------------------------------------------------------------------------+
| NVIDIA-SMI 460.106.00   Driver Version: 460.106.00   CUDA Version: 11.2     |
|-------------------------------+----------------------+----------------------+
| GPU  Name        Persistence-M| Bus-Id        Disp.A | Volatile Uncorr. ECC |
| Fan  Temp  Perf  Pwr:Usage/Cap|         Memory-Usage | GPU-Util  Compute M. |
|                               |                      |               MIG M. |
|===============================+======================+======================|
|   0  Tesla T4            On   | 00000000:00:04.0 Off |                    0 |
| N/A   48C    P8    10W /  70W |      0MiB / 15109MiB |      0%      Default |
|                               |                      |                  N/A |
+-------------------------------+----------------------+----------------------+

+-----------------------------------------------------------------------------+
| Processes:                                                                  |
|  GPU   GI   CI        PID   Type   Process name                  GPU Memory |
|        ID   ID                                                   Usage      |
|=============================================================================|
|  No running processes found                                                 |
+-----------------------------------------------------------------------------+
NVRM version: NVIDIA UNIX x86_64 Kernel Module  460.106.00  Tue Sep 28 12:05:58 UTC 2021
GCC version:  gcc version 7.5.0 (Ubuntu 7.5.0-3ubuntu1~18.04)
Connection to 34.68.242.247 closed.
*** Check "nv_driver": PASS ***
*** Running diagnostic function "nv_driver" ***
Warning: Permanently added 'compute.6788823627063447738' (ECDSA) to the list of known hosts.
Fri Oct 14 05:18:02 2022
+-----------------------------------------------------------------------------+
| NVIDIA-SMI 460.106.00   Driver Version: 460.106.00   CUDA Version: 11.2     |
|-------------------------------+----------------------+----------------------+
| GPU  Name        Persistence-M| Bus-Id        Disp.A | Volatile Uncorr. ECC |
| Fan  Temp  Perf  Pwr:Usage/Cap|         Memory-Usage | GPU-Util  Compute M. |
|                               |                      |               MIG M. |
|===============================+======================+======================|
|   0  Tesla T4            On   | 00000000:00:04.0 Off |                    0 |
| N/A   35C    P8     9W /  70W |      0MiB / 15109MiB |      0%      Default |
|                               |                      |                  N/A |
+-------------------------------+----------------------+----------------------+

+-----------------------------------------------------------------------------+
| Processes:                                                                  |
|  GPU   GI   CI        PID   Type   Process name                  GPU Memory |
|        ID   ID                                                   Usage      |
|=============================================================================|
|  No running processes found                                                 |
+-----------------------------------------------------------------------------+
NVRM version: NVIDIA UNIX x86_64 Kernel Module  460.106.00  Tue Sep 28 12:05:58 UTC 2021
GCC version:  gcc version 7.5.0 (Ubuntu 7.5.0-3ubuntu1~18.04)
Connection to 34.123.223.104 closed.
*** Check "nv_driver": PASS ***
*** Running diagnostic function "cuda_version" ***
Connection to 34.68.242.247 closed.
found cuda major version: 11
*** Check "cuda_version": PASS ***
*** Running diagnostic function "cuda_version" ***
Connection to 34.123.223.104 closed.
found cuda major version: 11
*** Check "cuda_version": PASS ***
...
********************************************************************************
Overall check result: PASS
```

Please note that the diagnostic tool supports the following:

* Dataproc 2.0 with image of Debian 10 or Ubuntu 18.04 (Rocky8 support is coming soon)
* GPU clusters that must have 1 worker node at least. Single node cluster (1 master, 0 workers) is
  not supported

## Bootstrap GPU Cluster with Optimized Settings

The bootstrap tool will apply optimized settings for the RAPIDS Accelerator on Apache Spark on a 
GPU cluster for Dataproc.  The tool will fetch the characteristics of the cluster -- including 
number of workers, worker cores, worker memory, and GPU accelerator type and count.  It will use
the cluster properties to then determine the optimal settings for running GPU-accelerated Spark 
applications.

Usage: `spark_rapids_dataproc bootstrap --cluster <cluster-name> --region <region>`

Help (to see all options available): `spark_rapids_dataproc bootstrap --help`

Example output: 
```
##### BEGIN : RAPIDS bootstrap settings for gpu-cluster
spark.executor.cores=16
spark.executor.memory=32768m
spark.executor.memoryOverhead=7372m
spark.rapids.sql.concurrentGpuTasks=2
spark.rapids.memory.pinnedPool.size=4096m
spark.sql.files.maxPartitionBytes=512m
spark.task.resource.gpu.amount=0.0625
##### END : RAPIDS bootstrap settings for gpu-cluster
```

A detailed description for bootstrap settings with usage information is available in the
[RAPIDS Accelerator for Apache Spark Configuration](https://nvidia.github.io/spark-rapids/docs/configs.html) 
and [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html) page.

## Qualify CPU Workloads for GPU Acceleration

The [qualification tool](https://pypi.org/project/spark-rapids-user-tools/) is launched on a Dataproc cluster that has applications that have already run.
The tool will output the applications recommended for acceleration along with estimated speed-up
and cost saving metrics.  Additionally, it will provide information on how to launch a GPU-
accelerated cluster to take advantage of the speed-up and cost savings.

Usage: `spark_rapids_dataproc qualification --cluster <cluster-name> --region <region>`

Help (to see all options available): `spark_rapids_dataproc qualification --help`

Example output:
```
+----+------------+--------------------------------+----------------------+-----------------+-----------------+---------------+-----------------+
|    | App Name   | App ID                         | Recommendation       |   Estimated GPU |   Estimated GPU |           App |   Estimated GPU |
|    |            |                                |                      |         Speedup |     Duration(s) |   Duration(s) |      Savings(%) |
|----+------------+--------------------------------+----------------------+-----------------+-----------------+---------------+-----------------|
|  0 | query24    | application_1664888311321_0011 | Strongly Recommended |            3.49 |          257.18 |        897.68 |           59.70 |
|  1 | query78    | application_1664888311321_0009 | Strongly Recommended |            3.35 |          113.89 |        382.35 |           58.10 |
|  2 | query23    | application_1664888311321_0010 | Strongly Recommended |            3.08 |          325.77 |       1004.28 |           54.37 |
|  3 | query64    | application_1664888311321_0008 | Strongly Recommended |            2.91 |          150.81 |        440.30 |           51.82 |
|  4 | query50    | application_1664888311321_0003 | Recommended          |            2.47 |          101.54 |        250.95 |           43.08 |
|  5 | query16    | application_1664888311321_0005 | Recommended          |            2.36 |          106.33 |        251.95 |           40.63 |
|  6 | query38    | application_1664888311321_0004 | Recommended          |            2.29 |           67.37 |        154.33 |           38.59 |
|  7 | query87    | application_1664888311321_0006 | Recommended          |            2.25 |           75.67 |        170.69 |           37.64 |
|  8 | query51    | application_1664888311321_0002 | Recommended          |            1.53 |           53.94 |         82.63 |            8.18 |
+----+------------+--------------------------------+----------------------+-----------------+-----------------+---------------+-----------------+
To launch a GPU-accelerated cluster with Spark RAPIDS, add the following to your cluster creation script:
        --initialization-actions=gs://goog-dataproc-initialization-actions-${REGION}/spark-rapids/spark-rapids.sh \
        --worker-accelerator type=nvidia-tesla-t4,count=2 \
        --metadata gpu-driver-provider="NVIDIA" \
        --metadata rapids-runtime=SPARK \
        --cuda-version=11.5
```

Please refer [Qualification Tool](https://nvidia.github.io/spark-rapids/docs/spark-qualification-tool.html) guide for running qualification tool on more environment.

## Tune Applications on GPU Cluster

Once Spark applications have been run on the GPU cluster, the [profiling tool](https://nvidia.github.io/spark-rapids/docs/spark-profiling-tool.html) can be run to 
analyze the event logs of the applications to determine if more optimal settings should be
configured.  The tool will output a per-application set of config settings to be adjusted for
enhanced performance.

Usage: `spark_rapids_dataproc profiling --cluster <cluster-name> --region <region>`

Help (to see all options available): `spark_rapids_dataproc profiling --help`

Example output:
```
+--------------------------------+--------------------------------------------------+--------------------------------------------------------------------------------------------------+
| App ID                         | Recommendations                                  | Comments                                                                                         |
+================================+==================================================+==================================================================================================+
| application_1664894105643_0011 | --conf spark.executor.cores=16                   | - 'spark.task.resource.gpu.amount' was not set.                                                  |
|                                | --conf spark.executor.memory=32768m              | - 'spark.rapids.sql.concurrentGpuTasks' was not set.                                             |
|                                | --conf spark.executor.memoryOverhead=7372m       | - 'spark.rapids.memory.pinnedPool.size' was not set.                                             |
|                                | --conf spark.rapids.memory.pinnedPool.size=4096m | - 'spark.executor.memoryOverhead' was not set.                                                   |
|                                | --conf spark.rapids.sql.concurrentGpuTasks=2     | - 'spark.sql.files.maxPartitionBytes' was not set.                                               |
|                                | --conf spark.sql.files.maxPartitionBytes=1571m   | - 'spark.sql.shuffle.partitions' was not set.                                                    |
|                                | --conf spark.sql.shuffle.partitions=200          |                                                                                                  |
|                                | --conf spark.task.resource.gpu.amount=0.0625     |                                                                                                  |
+--------------------------------+--------------------------------------------------+--------------------------------------------------------------------------------------------------+
| application_1664894105643_0002 | --conf spark.executor.cores=16                   | - 'spark.task.resource.gpu.amount' was not set.                                                  |
|                                | --conf spark.executor.memory=32768m              | - 'spark.rapids.sql.concurrentGpuTasks' was not set.                                             |
|                                | --conf spark.executor.memoryOverhead=7372m       | - 'spark.rapids.memory.pinnedPool.size' was not set.                                             |
|                                | --conf spark.rapids.memory.pinnedPool.size=4096m | - 'spark.executor.memoryOverhead' was not set.                                                   |
|                                | --conf spark.rapids.sql.concurrentGpuTasks=2     | - 'spark.sql.files.maxPartitionBytes' was not set.                                               |
|                                | --conf spark.sql.files.maxPartitionBytes=3844m   | - 'spark.sql.shuffle.partitions' was not set.                                                    |
|                                | --conf spark.sql.shuffle.partitions=200          |                                                                                                  |
|                                | --conf spark.task.resource.gpu.amount=0.0625     |                                                                                                  |
+--------------------------------+--------------------------------------------------+--------------------------------------------------------------------------------------------------+
```
