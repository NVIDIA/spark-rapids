---
layout: page
title: GCP Dataproc
nav_order: 4
parent: Getting-Started
---

# Getting started with RAPIDS Accelerator on GCP Dataproc
 [Google Cloud Dataproc](https://cloud.google.com/dataproc) is Google Cloud's fully managed Apache
 Spark and Hadoop service.  This guide will walk through the steps to:

* [Spin up a Dataproc Cluster Accelerated by GPUs](#spin-up-a-dataproc-cluster-accelerated-by-gpus)
* [Run Pyspark or Scala ETL and XGBoost training Notebook on a Dataproc Cluster Accelerated by
  GPUs](#run-pyspark-or-scala-notebook-on-a-dataproc-cluster-accelerated-by-gpus)
* [Submit the same sample ETL application as a Spark job to a Dataproc Cluster Accelerated by
  GPUs](#submit-spark-jobs-to-a-dataproc-cluster-accelerated-by-gpus)
* [Build custom Dataproc image to accelerate cluster initialization time](#build-custom-dataproc-image-to-accelerate-cluster-init-time)

## Spin up a Dataproc Cluster Accelerated by GPUs
 
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

After the command line environment is setup, log in to your GCP account.  You can now create a
Dataproc cluster with the configuration shown below.  The configuration will allow users to run any
of the [notebook demos](https://github.com/NVIDIA/spark-rapids/tree/main/docs/demo/GCP) on
GCP.  Alternatively, users can also start 2*2T4 worker nodes.

The script below will initialize with the following: 

* [GPU Driver](https://github.com/GoogleCloudDataproc/initialization-actions/tree/master/gpu) and
  [RAPIDS Acclerator for Apache
  Spark](https://github.com/GoogleCloudDataproc/initialization-actions/tree/master/rapids) through
  initialization actions (please note it takes up to 1 week for the latest init script to be merged into the GCP
  Dataproc public GCS bucket)
* One 8-core master node and 5 32-core worker nodes
* Four NVIDIA T4 for each worker node
* [Local SSD](https://cloud.google.com/dataproc/docs/concepts/compute/dataproc-local-ssds) is
  recommended for Spark scratch space to improve IO
* Component gateway enabled for accessing Web UIs hosted on the cluster
* Configuration for [GPU scheduling and isolation](yarn-gpu.md)


```bash
    export REGION=[Your Preferred GCP Region]
    export GCS_BUCKET=[Your GCS Bucket]
    export CLUSTER_NAME=[Your Cluster Name]
    export NUM_GPUS=4
    export NUM_WORKERS=5

gcloud dataproc clusters create $CLUSTER_NAME  \
    --region $REGION \
    --image-version=2.0-ubuntu18 \
    --master-machine-type n1-standard-16 \
    --num-workers $NUM_WORKERS \
    --worker-accelerator type=nvidia-tesla-t4,count=$NUM_GPUS \
    --worker-machine-type n1-highmem-32\
    --num-worker-local-ssds 4 \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/gpu/install_gpu_driver.sh,gs://goog-dataproc-initialization-actions-${REGION}/rapids/rapids.sh \
    --optional-components=JUPYTER,ZEPPELIN \
    --metadata rapids-runtime=SPARK \
    --bucket $GCS_BUCKET \
    --enable-component-gateway 
``` 

This may take around 10-15 minutes to complete.  You can navigate to the Dataproc clusters tab in the
Google Cloud Console to see the progress.

![Dataproc Cluster](../img/GCP/dataproc-cluster.png)

If you'd like to further accelerate init time to 4-5 minutes, create a custom Dataproc image using
[this](#build-custom-dataproc-image-to-accelerate-cluster-init-time) guide. 

## Run PySpark or Scala Notebook on a Dataproc Cluster Accelerated by GPUs
To use notebooks with a Dataproc cluster, click on the cluster name under the Dataproc cluster tab
and navigate to the "Web Interfaces" tab.  Under "Web Interfaces", click on the JupyterLab or
Jupyter link to start to use sample [Mortgage ETL on GPU Jupyter
Notebook](../demo/GCP/Mortgage-ETL-GPU.ipynb) to process full 17 years [Mortgage
data](https://rapidsai.github.io/demos/datasets/mortgage-data).

![Dataproc Web Interfaces](../img/GCP/dataproc-service.png)

The notebook will first transcode CSV files into Parquet files and then run an ETL query to prepare
the dataset for training.  In the sample notebook, we use 2016 data as the evaluation set and the
rest as a training set, saving to respective GCS locations.  Using the default notebook
configuration the first stage should take ~110 seconds (1/3 of CPU execution time with same config)
and the second stage takes ~170 seconds (1/7 of CPU execution time with same config).  The notebook
depends on the pre-compiled [Spark RAPIDS SQL
plugin](https://mvnrepository.com/artifact/com.nvidia/rapids-4-spark) and
[cuDF](https://mvnrepository.com/artifact/ai.rapids/cudf), which are pre-downloaded by the GCP
Dataproc [RAPIDS init
script](https://github.com/GoogleCloudDataproc/initialization-actions/tree/master/rapids).

Once data is prepared, we use the [Mortgage XGBoost4j Scala
Notebook](../demo/GCP/mortgage-xgboost4j-gpu-scala.zpln) in Dataproc's Zeppelin service to execute
the training job on the GPU.  NVIDIA also ships [Spark
XGBoost4j](https://github.com/NVIDIA/spark-xgboost) which is based on [DMLC
xgboost](https://github.com/dmlc/xgboost).  Precompiled
[XGBoost4j](https://repo1.maven.org/maven2/com/nvidia/xgboost4j_3.0/) and [XGBoost4j
Spark](https://repo1.maven.org/maven2/com/nvidia/xgboost4j-spark_3.0/1.0.0-0.1.0/) libraries can be
downloaded from maven.  They are pre-downloaded by the GCP [RAPIDS init
action](https://github.com/GoogleCloudDataproc/initialization-actions/tree/master/rapids).  Since
github cannot render a Zeppelin notebook, we prepared a [Jupyter Notebook with Scala
code](../demo/GCP/mortgage-xgboost4j-gpu-scala.ipynb) for you to view the code content.

The training time should be around 480 seconds (1/10 of CPU execution time with same config).  This
is shown under cell:

```scala
// Start training
println("\n------ Training ------")
val (xgbClassificationModel, _) = benchmark("train") {
  xgbClassifier.fit(trainSet)
}
```

## Submit Spark jobs to a Dataproc Cluster Accelerated by GPUs
Similar to spark-submit for on-prem clusters, Dataproc supports a Spark applicaton job to be
submitted as a Dataproc job.  The mortgage examples we use above are also available as a [spark
application](https://github.com/NVIDIA/spark-xgboost-examples/tree/spark-3/examples/apps/scala).
After [building the jar
files](https://github.com/NVIDIA/spark-xgboost-examples/blob/spark-3/getting-started-guides/building-sample-apps/scala.md)
they are available through maven `mvn package -Dcuda.classifier=cuda11-0`. 

Place the jar file `sample_xgboost_apps-0.2.2.jar` under the `gs://$GCS_BUCKET/scala/` folder by
running `gsutil cp target/sample_xgboost_apps-0.2.2.jar gs://$GCS_BUCKET/scala/`.  To do this you
can either drag and drop files from your local machine into the GCP storage browser, or use the
gsutil cp as shown before to do this from a command line.  We can thereby submit the jar by:

```bash
export REGION=[Your Preferred GCP Region]
export GCS_BUCKET=[Your GCS Bucket]
export CLUSTER_NAME=[Your Cluster Name]
export SPARK_NUM_EXECUTORS=20
export SPARK_EXECUTOR_MEMORY=20G
export SPARK_EXECUTOR_MEMORYOVERHEAD=16G
export SPARK_NUM_CORES_PER_EXECUTOR=7
export DATA_PATH=gs://${GCS_BUCKET}/mortgage_full

gcloud dataproc jobs submit spark \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --class=com.nvidia.spark.examples.mortgage.GPUMain \
    --jars=gs://${GCS_BUCKET}/scala/sample_xgboost_apps-0.2.2.jar \
    --properties=spark.executor.cores=${SPARK_NUM_CORES_PER_EXECUTOR},spark.task.cpus=${SPARK_NUM_CORES_PER_EXECUTOR},spark.executor.memory=${SPARK_EXECUTOR_MEMORY},spark.executor.memoryOverhead=${SPARK_EXECUTOR_MEMORYOVERHEAD},spark.executor.resource.gpu.amount=1,spark.task.resource.gpu.amount=1,spark.rapids.sql.hasNans=false,spark.rapids.sql.batchSizeBytes=512M,spark.rapids.sql.reader.batchSizeBytes=768M,spark.rapids.sql.variableFloatAgg.enabled=true,spark.rapids.memory.gpu.pooling.enabled=false \
    -- \
    -dataPath=train::${DATA_PATH}/train \
    -dataPath=trans::${DATA_PATH}/test \
    -format=parquet \
    -numWorkers=${SPARK_NUM_EXECUTORS} \
    -treeMethod=gpu_hist \
    -numRound=100 \
    -maxDepth=8   
``` 

## Dataproc Hub in AI Platform Notebook to Dataproc cluster 
With the integration between AI Platform Notebooks and Dataproc, users can create a [Dataproc Hub
notebook](https://cloud.google.com/blog/products/data-analytics/administering-jupyter-notebooks-for-spark-workloads-on-dataproc).
The AI platform will connect to a Dataproc cluster through a yaml configuration.

In the future, users will be able to provision a Dataproc cluster through DataprocHub notebook.  You
can use example [pyspark notebooks](../demo/GCP/Mortgage-ETL-GPU.ipynb) to experiment.

## Build custom dataproc image to accelerate cluster init time
In order to accelerate cluster init time to 3-4 minutes, we need to build a custom Dataproc image
that already has NVIDIA drivers and CUDA toolkit installed, with RAPIDS deployed. The custom image 
could also be used in an air gap environment. In this section, we will be using [these
instructions from GCP](https://cloud.google.com/dataproc/docs/guides/dataproc-images) to create a
custom image. 

Currently, the [GPU Driver](https://github.com/GoogleCloudDataproc/initialization-actions/tree/master/gpu) 
initialization actions:
1. Configure YARN, the YARN node manager, GPU isolation and GPU exclusive mode.
2. Install GPU drivers.

Let's write a script to move as many of those to custom image. [gpu_dataproc_packages_ubuntu_sample.sh](gpu_dataproc_packages_ubuntu_sample.sh)
in this directory will be used to create the Dataproc image:

Google provides a `generate_custom_image.py` script that:
- Launches a temporary Compute Engine VM instance with the specified Dataproc base image.
- Then runs the customization script inside the VM instance to install custom packages and/or update
  configurations.
- After the customization script finishes, it shuts down the VM instance and creates a Dataproc
  custom image from the disk of the VM instance.
- The temporary VM is deleted after the custom image is created. 
- The custom image is saved and can be used to create Dataproc clusters.

Download `gpu_dataproc_packages_ubuntu_sample.sh` in this repo.  The script uses
Google's `generate_custom_image.py` script.  This step may take 20-25 minutes to complete.

```bash
git clone https://github.com/GoogleCloudDataproc/custom-images
cd custom-images

export CUSTOMIZATION_SCRIPT=/path/to/gpu_dataproc_packages_ubuntu_sample.sh
export ZONE=[Your Preferred GCP Zone]
export GCS_BUCKET=[Your GCS Bucket]
export IMAGE_NAME=a209-ubuntu18-gpu-t4
export DATAPROC_VERSION=2.0.9-ubuntu18
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
    --disk-size 200
```

See [here](https://cloud.google.com/dataproc/docs/guides/dataproc-images#running_the_code) for more
details on `generate_custom_image.py` script arguments and 
[here](https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-versions) for dataproc version description.

The image `sample-209-ubuntu18-gpu-t4` is now ready and can be viewed in the GCP console under
`Compute Engine > Storage > Images`. The next step is to launch the cluster using this new image and
new initialization actions (that do not install NVIDIA drivers since we are already past that step).

Here is the new custom GPU initialization action that needs to be run after cluster creation with 
custom images, save this as `addon.sh`

```bash
#!/bin/bash
chmod a+rwx -R /sys/fs/cgroup/cpu,cpuacct
chmod a+rwx -R /sys/fs/cgroup/devices
```

Move this to your own bucket. Lets launch the cluster:

```bash 
export REGION=[Your Preferred GCP Region]
export GCS_BUCKET=[Your GCS Bucket]
export CLUSTER_NAME=[Your Cluster Name]
export NUM_GPUS=1
export NUM_WORKERS=2

gcloud dataproc clusters create $CLUSTER_NAME  \
    --region $REGION \
    --image=sample-209-ubuntu18-gpu-t4 \
    --master-machine-type n1-standard-4 \
    --num-workers $NUM_WORKERS \
    --worker-accelerator type=nvidia-tesla-t4,count=$NUM_GPUS \
    --worker-machine-type n1-standard-4 \
    --num-worker-local-ssds 1 \
    --initialization-actions gs://$GCS_BUCKET/addon.sh \
    --optional-components=JUPYTER,ZEPPELIN \
    --metadata rapids-runtime=SPARK \
    --bucket $GCS_BUCKET \
    --enable-component-gateway 
```

The new cluster should be up and running within 3-4 minutes!
