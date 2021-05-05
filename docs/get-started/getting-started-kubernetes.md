---
layout: page
title: Kubernetes
nav_order: 5
parent: Getting-Started
---

# Getting Started with RAPIDS and Kubernetes

This guide will run through how to set up the RAPIDS Accelerator for Apache Spark in a Kubernetes cluster.
At the end of this guide, the reader will be able to run a sample Apache Spark application that runs
on NVIDIA GPUs in a Kubernetes cluster.

This is a quick start guide which uses default settings which may be different from your cluster.

Kubernetes requires a Docker image to run Spark.  Generally everything needed is in the Docker
image - Spark, the RAPIDS Accelerator for Spark jars, and the discovery script.  See this
[Dockerfile.cuda](Dockerfile.cuda) example.


## Prerequisites
    * Kubernetes cluster is up and running with NVIDIA GPU support
    * Docker is installed on a client machine
    * A Docker repository which is accessible by the Kubernetes cluster

These instructions do not cover how to setup a Kubernetes cluster.

Please refer to [Install Kubernetes](https://docs.nvidia.com/datacenter/cloud-native/kubernetes/install-k8s.html) on 
how to install a Kubernetes cluster with NVIDIA GPU support.

## Docker Image Preparation

On a client machine which has access to the Kubernetes cluster:

1. [Download Apache Spark](https://spark.apache.org/downloads.html).
   Supported versions of Spark are listed on the [RAPIDS Accelerator download page](../download.md).  Please note that only
   Scala version 2.12 is currently supported by the accelerator. 

   Note that you can download these into a local directory and untar the Spark `.tar.gz` as a directory named `spark`.

2. Download the [RAPIDS Accelerator for Spark jars](getting-started-on-prem.md#download-the-rapids-jars) and the
  [GPU discovery script](getting-started-on-prem.md#install-the-gpu-discovery-script).
  
   Put the 2 jars -- `rapids-4-spark_<version>.jar`, `cudf-<version>.jar`  and `getGpusResources.sh` in the same directory as `spark`.
   
   Note: If here you decide to put above 2 jars in the `spark/jars` directory which will be copied into 
   `/opt/spark/jars` directory in Docker image, then in the future you do not need to 
   specify `spark.driver.extraClassPath` or `spark.executor.extraClassPath` using `cluster` mode.
   This example just shows you a way to put customized jars or 3rd party jars.

3. Download the sample [Dockerfile.cuda](Dockerfile.cuda) in the same directory as `spark`.

   The sample Dockerfile.cuda will copy the `spark` directory's several sub-directories into `/opt/spark/` 
   along with the RAPIDS Accelerator jars and `getGpusResources.sh` into `/opt/sparkRapidsPlugin`
   inside the Docker image.
   
   You can modify the Dockerfile to copy your application into the docker image, i.e. `test.py`.
   
   Examine the Dockerfile.cuda file to ensure the file names are correct and modify if needed.
   
   Currently the directory in the local machine should look as below:
   ```shell 
   $ ls
   Dockerfile.cuda   cudf-<version>.jar   getGpusResources.sh   rapids-4-spark_<version>.jar   spark
   ```

4. Build the Docker image with a proper repository name and tag and push it to the repository
   ```shell 
   export IMAGE_NAME=xxx/yyy:tag
   docker build . -f Dockerfile.cuda -t $IMAGE_NAME
   docker push $IMAGE_NAME
   ```

## Running Spark Applications in the Kubernetes Cluster

### Submitting a Simple Test Job

This simple job will test if the RAPIDS plugin can be found.
`ClassNotFoundException` is a common error if the Spark driver can not 
find the RAPIDS Accelerator jar, resulting in an exception like this:
```
Exception in thread "main" java.lang.ClassNotFoundException: com.nvidia.spark.SQLPlugin
```

Here is an example job: 

```shell
export SPARK_HOME=~/spark
export IMAGE_NAME=xxx/yyy:tag
export K8SMASTER=k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port>
export SPARK_NAMESPACE=default
export SPARK_DRIVER_NAME=exampledriver

$SPARK_HOME/bin/spark-submit \
     --master $K8SMASTER \
     --deploy-mode cluster  \
     --name examplejob \
     --class org.apache.spark.examples.SparkPi \
     --conf spark.executor.instances=1 \
     --conf spark.executor.resource.gpu.amount=1 \
     --conf spark.executor.memory=4G \
     --conf spark.executor.cores=1 \
     --conf spark.task.cpus=1 \
     --conf spark.task.resource.gpu.amount=1 \
     --conf spark.rapids.memory.pinnedPool.size=2G \
     --conf spark.executor.memoryOverhead=3G \
     --conf spark.locality.wait=0s \
     --conf spark.sql.files.maxPartitionBytes=512m \
     --conf spark.sql.shuffle.partitions=10 \
     --conf spark.plugins=com.nvidia.spark.SQLPlugin \
     --conf spark.kubernetes.namespace=$SPARK_NAMESPACE  \
     --conf spark.kubernetes.driver.pod.name=$SPARK_DRIVER_NAME  \
     --conf spark.executor.resource.gpu.discoveryScript=/opt/sparkRapidsPlugin/getGpusResources.sh \
     --conf spark.executor.resource.gpu.vendor=nvidia.com \
     --conf spark.kubernetes.container.image=$IMAGE_NAME \
     --conf spark.executor.extraClassPath=/opt/sparkRapidsPlugin/rapids-4-spark_<version>.jar:/opt/sparkRapidsPlugin/cudf-<version>.jar   \
     --conf spark.driver.extraClassPath=/opt/sparkRapidsPlugin/rapids-4-spark_<version>.jar:/opt/sparkRapidsPlugin/cudf-<version>.jar   \
     --driver-memory 2G \
     local:///opt/spark/examples/jars/spark-examples_2.12-3.0.2.jar
```

   Note: `local://` means the jar file location is inside the Docker image.
   Since this is `cluster` mode, the Spark driver is running inside a pod in Kubernetes.
   The driver and executor pods can be seen when the job is running:
```shell
$ kubectl get pods
NAME                               READY   STATUS    RESTARTS   AGE
spark-pi-d11075782f399fd7-exec-1   1/1     Running   0          9s
exampledriver                      1/1     Running   0          15s
```

   To view the Spark driver log, use below command:
```shell
kubectl logs $SPARK_DRIVER_NAME
```

   To view the Spark driver UI when the job is running first expose the driver UI port:
```shell
kubectl port-forward $SPARK_DRIVER_NAME 4040:4040
```
   Then open a web browser to the Spark driver UI page on the exposed port:
```shell
http://localhost:4040
```

   To kill the Spark job:
```shell
$SPARK_HOME/bin/spark-submit --kill spark:$SPARK_DRIVER_NAME
```

   To delete the driver POD:
```shell
kubectl delete pod $SPARK_DRIVER_NAME
```

### Running an Interactive Spark Shell

If you need an interactive Spark shell with executor pods running inside the Kubernetes cluster:
```shell
$SPARK_HOME/bin/spark-shell \
     --master $K8SMASTER \
     --name mysparkshell \
     --deploy-mode client  \
     --conf spark.executor.instances=1 \
     --conf spark.executor.resource.gpu.amount=1 \
     --conf spark.executor.memory=4G \
     --conf spark.executor.cores=1 \
     --conf spark.task.cpus=1 \
     --conf spark.task.resource.gpu.amount=1 \
     --conf spark.rapids.memory.pinnedPool.size=2G \
     --conf spark.executor.memoryOverhead=3G \
     --conf spark.locality.wait=0s \
     --conf spark.sql.files.maxPartitionBytes=512m \
     --conf spark.sql.shuffle.partitions=10 \
     --conf spark.plugins=com.nvidia.spark.SQLPlugin \
     --conf spark.kubernetes.namespace=$SPARK_NAMESPACE  \
     --conf spark.executor.resource.gpu.discoveryScript=/opt/sparkRapidsPlugin/getGpusResources.sh \
     --conf spark.executor.resource.gpu.vendor=nvidia.com \
     --conf spark.kubernetes.container.image=$IMAGE_NAME \
     --conf spark.executor.extraClassPath=/opt/sparkRapidsPlugin/rapids-4-spark_<version>.jar:/opt/sparkRapidsPlugin/cudf-<version>.jar   \
     --driver-class-path=./cudf-<version>.jar:./rapids-4-spark_<version>.jar \
     --driver-memory 2G 
```

Only the `client` deploy mode should be used. If you specify the `cluster` deploy mode, you would see the following error:
```shell
Cluster deploy mode is not applicable to Spark shells.
```
Also notice that `--conf spark.driver.extraClassPath` was removed but `--driver-class-path` was added.
This is because now the driver is running on the client machine, so the jar paths should be local filesystem paths.

When running the shell you can see only the executor pods are running inside Kubernetes:
```
$ kubectl get pods
NAME                                     READY   STATUS    RESTARTS   AGE
mysparkshell-bfe52e782f44841c-exec-1     1/1     Running   0          11s
```

The following Scala code can be run in the Spark shell to test if the RAPIDS Accelerator is enabled.
```shell
val df = spark.sparkContext.parallelize(Seq(1)).toDF()
df.createOrReplaceTempView("df")
spark.sql("SELECT value FROM df WHERE value <>1").show
spark.sql("SELECT value FROM df WHERE value <>1").explain
:quit
```
The expected `explain` plan should contain the GPU related operators:
```shell
scala> spark.sql("SELECT value FROM df WHERE value <>1").explain
== Physical Plan ==
GpuColumnarToRow false
+- GpuFilter NOT (value#2 = 1)
   +- GpuRowToColumnar TargetSize(2147483647)
      +- *(1) SerializeFromObject [input[0, int, false] AS value#2]
         +- Scan[obj#1]
```

### Running PySpark in Client Mode

Of course, you can `COPY` the Python code in the Docker image when building it
and submit it using the `cluster` deploy mode as showin in the previous example pi job.

However if you do not want to re-build the Docker image each time and just want to submit the Python code
from the client machine, you can use the `client` deploy mode.

```shell
$SPARK_HOME/bin/spark-submit \
     --master $K8SMASTER \
     --deploy-mode client  \
     --name mypythonjob \
     --conf spark.executor.instances=1 \
     --conf spark.executor.resource.gpu.amount=1 \
     --conf spark.executor.memory=4G \
     --conf spark.executor.cores=1 \
     --conf spark.task.cpus=1 \
     --conf spark.task.resource.gpu.amount=1 \
     --conf spark.rapids.memory.pinnedPool.size=2G \
     --conf spark.executor.memoryOverhead=3G \
     --conf spark.locality.wait=0s \
     --conf spark.sql.files.maxPartitionBytes=512m \
     --conf spark.sql.shuffle.partitions=10 \
     --conf spark.plugins=com.nvidia.spark.SQLPlugin \
     --conf spark.kubernetes.namespace=$SPARK_NAMESPACE  \
     --conf spark.executor.resource.gpu.discoveryScript=/opt/sparkRapidsPlugin/getGpusResources.sh \
     --conf spark.executor.resource.gpu.vendor=nvidia.com \
     --conf spark.kubernetes.container.image=$IMAGE_NAME \
     --conf spark.executor.extraClassPath=/opt/sparkRapidsPlugin/rapids-4-spark_<version>.jar:/opt/sparkRapidsPlugin/cudf-<version>.jar   \
     --driver-memory 2G \
     --driver-class-path=./cudf-<version>.jar:./rapids-4-spark_<version>.jar \
     test.py
```

A sample `test.py` is as below:
```shell
from pyspark.sql import SQLContext
from pyspark import SparkConf
from pyspark import SparkContext
conf = SparkConf()
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
df=sqlContext.createDataFrame([1,2,3], "int").toDF("value")
df.createOrReplaceTempView("df")
sqlContext.sql("SELECT * FROM df WHERE value<>1").explain()
sqlContext.sql("SELECT * FROM df WHERE value<>1").show()
sc.stop()
```

## Running Spark Applications using Spark Operator

Using Spark Operator is another way to submit Spark Applications into a Kubernetes Cluster.

1. Locate the Spark Application jars/files in the docker image when preparing docker image.

For example, assume `/opt/sparkRapidsPlugin/test.py` is inside the docker image.

This is because currently only `cluster` deployment mode is supported by Spark Operator.

2. Create spark operator using `helm`.

Follow [Spark Operator quick start guide](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/master/docs/quick-start-guide.md)

3. Create a Spark Application YAML file

For example, create a file named `testpython-rapids.yaml` with the following contents:

```
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: testpython-rapids
  namespace: default
spec:
  sparkConf:
    "spark.ui.port": "4045"
    "spark.rapids.sql.concurrentGpuTasks": "1"
    "spark.executor.resource.gpu.amount": "1"
    "spark.task.resource.gpu.amount": "1"
    "spark.executor.memory": "1g"
    "spark.rapids.memory.pinnedPool.size": "2g"
    "spark.executor.memoryOverhead": "3g"
    "spark.locality.wait": "0s"
    "spark.sql.files.maxPartitionBytes": "512m"
    "spark.sql.shuffle.partitions": "10"
    "spark.plugins": "com.nvidia.spark.SQLPlugin"
    "spark.executor.resource.gpu.discoveryScript": "/opt/sparkRapidsPlugin/getGpusResources.sh"
    "spark.executor.resource.gpu.vendor": "nvidia.com"
    "spark.executor.extraClassPath": "/opt/sparkRapidsPlugin/rapids-4-spark.jar:/opt/sparkRapidsPlugin/cudf.jar"
    "spark.driver.extraClassPath": "/opt/sparkRapidsPlugin/rapids-4-spark.jar:/opt/sparkRapidsPlugin/cudf.jar"
  type: Python
  pythonVersion: 3
  mode: cluster
  image: "<IMAGE_NAME>"
  imagePullPolicy: Always
  mainApplicationFile: "local:///opt/sparkRapidsPlugin/test.py"
  sparkVersion: "3.1.1"
  restartPolicy:
    type: Never
  volumes:
    - name: "test-volume"
      hostPath:
        path: "/tmp"
        type: Directory
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "1024m"
    labels:
      version: 3.1.1
    serviceAccount: spark
    volumeMounts:
      - name: "test-volume"
        mountPath: "/tmp"
  executor:
    cores: 1
    instances: 1
    memory: "5000m"
    gpu:
      name: "nvidia.com/gpu"
      quantity: 1
    labels:
      version: 3.1.1
    volumeMounts:
      - name: "test-volume"
        mountPath: "/tmp"
```

4. Submit the Spark Application

```
sparkctl create testpython-rapids.yaml
```

Note: `sparkctl` can be built from the Spark Operator repo after [installing golang](https://golang.org/doc/install):
```
cd sparkctl
go build -o sparkctl
```

5. Check the driver log

```
sparkctl log testpython-rapids
```

6. Check the status of this Spark Application

```
sparkctl status testpython-rapids
```

7. Port forwarding when Spark driver is running

```
sparkctl forward testpython-rapids --local-port 1234 --remote-port 4045
```

Then open browser with `http://localhost:1234/` to check Spark UI.

8. Delete the Spark Application

```
sparkctl delete testpython-rapids
```

Please refer to [Running Spark on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html) for more information.
