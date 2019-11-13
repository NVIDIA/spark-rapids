# GCP Setup Instructions for Rapids Plugin For Apache Spark

This document contains a set of instructions on how to run a GCP Dataproc
cluster with Apache Spark 3.0 standalone mode for the Rapids Plugin.

Since Apache Spark 3.0 is not released yet, the instructions may change
at any time.

To start using GCP you will need:
* An active GCP account.
* GCloud SDK installed per [GCloud SDK
  Install](https://cloud.google.com/sdk/docs/quickstart-linux)
* A GCS bucket to upload initialization-action scripts.
  Instructions at: (https://cloud.google.com/storage/docs/creating-buckets)
* Private Google Access for the `default` network.
  Instructions at:[Enable Private Access]
  (https://cloud.google.com/vpc/docs/configure-private-google-access)
* A Google VPC NAT Gateway created for the `default` network. Follow
  instructions at: [GCP NAT Gateway]
  (https://cloud.google.com/vpc/docs/configure-private-google-access)
* A local download of spark distribution in tar.gz format.

Once you have `gcloud` and `gsutil` set up, set the parameters like number of
worker nodes and GPUs per node in `cluster-vars.env`. To use the cluster for
rapids-plugin, specify the paths to the Rapids Spark plugin jars
(rapids-4-spark-0.10-SNAPSHOT.jar,rapids-4-spark-tests-0.10-SNAPSHOT.jar) and
the cuDF jar(cudf-0.10-SNAPSHOT-cuda10.jar) as part of the `SHIPPED_JARS`
variable in the script.This field is left empty by default. The
specified jars will be copied to `$SPARK_HOME/jars/` on all the nodes. This is
needed since Spark standalone cannot initialize executors with these jars passed
to the `--jars` option. Then run the `startup.sh` script to create a new
cluster.

```
./startup.sh <name_of_the_cluster> <name_of_GCS_bucket_for_cluster_init> <local_path_to_spark.tar.gz>
```
By default, the script creates a cluster with five worker nodes and one master node.
Each of the six nodes have 2 GPUs each and a persistent SSD of size 1 TB.
The worker nodes are highmem, 32 CPU core systems. The master is a standard 32 core system.
All these values can be changed in the `cluster-vars.env` script and have ramifications on
the cost of the cluster.

To start Spark 3.0 standalone cluster run `start-spark-standalone.sh` as follows:
```
./start-spark-standalone.sh <cluster-name> <gcs-bucket-name> <name-of-spark-tgz>
```
SSH tunnels are needed in order to access webports for the Spark UIs.
The master node hosts the spark master process. The tunnel can be created as follows:
```
gcloud compute ssh MASTER-NODE --zone us-central1-a -- -L <LOCALPORT>:localhost:8080
```
You can check the status of the Spark cluster on the GCP Dataproc UI which is now
hosted at (http://localhost:<LOCALPORT>/). Other UIs are available through Google
Cloud's Dataproc web page.

The GCS connector is needed to access the data from a GCS bucket. This jar can be
created following the steps at : [GCS
Connector](https://github.com/GoogleCloudPlatform/bigdata-interop/tree/master/gcs).
Make sure to use the shaded version since it includes everything the spark-shell
needs to run.
The script picks up the version of the connector available in the etl bucket provided by default.

As an example, here is how to run a spark shell with the Rapids Plugin with the GCS file system connector:
```
$SPARK_HOME/bin/spark-shell --driver-memory=$DRIVER_MEMORY --master $MASTER \
 --conf "spark.executor.memory=$EXECUTOR_MEMORY" --conf "spark.executor.cores=$EXECUTOR_CORES" \
 --num-executors $NUM_EXECUTORS --conf "spark.plugins=ai.rapids.spark.RapidsSparkPlugin" \
 --conf spark.dynamicAllocation.enabled=false \
 --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
 --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
 --jars "${RAPIDS_PLUGIN_JAR_PATH},${CUDF_JAR_PATH},${RAPIDS_PLUGIN_TEST_JAR_PATH},${GCS_CONNECTOR_JAR_PATH}" \
 --conf spark.sql.extensions=ai.rapids.spark.Plugin
```
Create another ssh tunnel for the driver UI as shown below.
```
gcloud compute ssh MASTER-NODE --zone us-central1-a -- -L <LOCALPORT-DRIVER>:localhost:4040
```
The driver UI should now be available at (http://localhost:<LOCALPORT-DRIVER>).

Once the shell is launched successfully, the following scala code can be used as a reference on
how to run the plugin TPC-H benchmark for a query. TPC-H data can be generated using
`dbgen` utility and uploaded to
either GCS or HDFS.
```
import ai.rapids.sparkexamples.tpch._
object RunQ {
  def main(args: Array[String]): Unit = {
    val query = args(0)
    val parts = args(1)
    val input = args(2)
    var inputPath = <default path>
    if (input == "1tb") {
      inputPath = <path to 1 TB TPCH data>
    } else if(input == "100gb"){
      inputPath = <path to 100 GB TPCH data>
    } else if (input == "10gb") {
      inputPath = <path to 10GB TPCH data>
    }
    spark.conf.set("spark.rapids.sql.hasNans", false)
    spark.conf.set("spark.rapids.sql.incompatibleOps.enabled", true)
    spark.conf.set("spark.rapids.sql.variableFloatAgg.enabled", true)
    spark.conf.set("spark.rapids.sql.allowIncompatUTF8Strings", true)
    spark.conf.set("spark.rapids.sql.explain", true)
    spark.conf.set("spark.sql.shuffle.partitions", parts)
    spark.time(CSV.main(Array(inputPath,<output path>, query)))
}

  def runs(query: String, parts: Array[String], times: Integer, input: String) {
    parts.foreach { part =>
      for (i <- 0 to times-1) {
        RunQ.main(Array(query, part, input))
          System.out.println("Number of cores=" + spark.conf.get("spark.executor.cores"))
          System.out.println("Number of partitions="+ part)
      }
    }
  }
}
// runs Query 4 with 200 partitions once with 100GB data size
RunQ.runs("4",Array("200"),1,"100gb")
```

`stop-spark-standalone.sh` can be used to stop the Master and workers in spark standalone mode.
The arguments to it are the name of the cluster and the size of the cluster
