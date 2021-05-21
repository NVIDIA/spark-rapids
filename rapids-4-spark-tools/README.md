# Spark profiling tool

This is a profiling tool to parse and analyze Spark event logs. 
It generates information which can be used for debugging and profiling. Information such as Spark version, executor information, Properties and so on.
Works with both cpu and gpu generated event logs.

(The code is based on Apache Spark 3.1.1 source code, and tested using Spark 3.0.x and 3.1.1 event logs)

## How to compile and use with Spark
1. `mvn clean package`
2. Copy rapids-4-spark-tools-<version>.jar to $SPARK_HOME/jars/

`cp target/rapids-4-spark-tools-21.06.0-SNAPSHOT.jar $SPARK_HOME/jars/`

3.  In spark-shell:
For a single event log analysis:
```
org.apache.spark.sql.rapids.tool.profiling.ProfileMain.main(Array("/path/to/eventlog1"))
```

For multiple event logs comparison and analysis:
```
org.apache.spark.sql.rapids.tool.profiling.ProfileMain.main(Array("/path/to/eventlog1", "/path/to/eventlog2"))
```

## How to compile and use from command-line
1. `mvn clean package`
2. `cd $SPARK_HOME (Download Apache Spark if reequired)`
3. `./bin/spark-submit --class org.apache.spark.sql.rapids.tool.profiling.ProfileMain  <Spark-Rapids-Repo>/workload_profiling/target/rapids-4-spark-tools-<version>.jar /path/to/eventlog1`

## Options
```
$ ./bin/spark-submit --class org.apache.spark.sql.rapids.tool.profiling.ProfileMain  <Spark-Rapids-Repo>/workload_profiling/target/rapids-4-spark-tools-<version>.jar --help

Spark event log profiling tool

Example:

# Input 1 or more event logs from local path:
./bin/spark-submit --class com.nvidia.spark.rapids.tool.profiling.ProfileMain  <Spark-Rapids-Repo>/workload_profiling/target/rapids-4-spark-tools-<version>.jar /path/to/eventlog1 /path/to/eventlog2

# If event log is from S3:
export AWS_ACCESS_KEY_ID=xxx
export AWS_SECRET_ACCESS_KEY=xxx
./bin/spark-submit --class com.nvidia.spark.rapids.tool.profiling.ProfileMain  <Spark-Rapids-Repo>/workload_profiling/target/rapids-4-spark-tools-<version>.jar s3a://<BUCKET>/eventlog1 /path/to/eventlog2

# If event log is from hdfs:
./bin/spark-submit --class com.nvidia.spark.rapids.tool.profiling.ProfileMain  <Spark-Rapids-Repo>/workload_profiling/target/rapids-4-spark-tools-<version>.jar hdfs://<BUCKET>/eventlog1 /path/to/eventlog2

# Change output directory to /tmp. It outputs as "rapids_4_spark_tools_output.log" in the local directory if the output directory is not specified.
./bin/spark-submit --class com.nvidia.spark.rapids.tool.profiling.ProfileMain  <Spark-Rapids-Repo>/workload_profiling/target/rapids-4-spark-tools-<version>.jar -o /tmp /path/to/eventlog1

For usage see below:
  -o, --output-directory  <arg>   Output directory. Default is current directory
  -h, --help                      Show help message

 trailing arguments:
  eventlog (required)   Event log filenames(space separated). eg:
                        s3a://<BUCKET>/eventlog1 /path/to/eventlog2
```

## Functions
### A. Collect Information or Compare Information(if more than 1 eventlogs are as input)
- Print Application Information
- Print Executors information
- Print Rapids related parameters

**1. GPU run vs CPU run performance comparison or different runs with different parameters**

We can input multiple Spark event logs and this tool can compare enviroments, executors, Rapids related Spark parameters,

- Compare the durations/versions/gpuMode on or off:
```
[main] INFO  com.nvidia.spark.rapids.tool.profiling.ProfileMain$  - ### A. Compare Information Collected ###
[main] INFO  com.nvidia.spark.rapids.tool.profiling.ProfileMain$  - Compare Application Information:
[main] INFO  com.nvidia.spark.rapids.tool.profiling.ProfileMain$  -
+--------+-----------------------+-------------+-------------+--------+-----------+------------+-------+
|appIndex|appId                  |startTime    |endTime      |duration|durationStr|sparkVersion|gpuMode|
+--------+-----------------------+-------------+-------------+--------+-----------+------------+-------+
|1       |app-20210329165943-0103|1617037182848|1617037490515|307667  |5.1 min    |3.0.1       |false  |
|2       |app-20210329170243-0018|1617037362324|1617038578035|1215711 |20 min     |3.0.1       |true   |
+--------+-----------------------+-------------+-------------+--------+-----------+------------+-------+
```


- Compare Executor informations:
```
[main] INFO  com.nvidia.spark.rapids.tool.profiling.ProfileMain$  - Compare Executor Information:
[main] INFO  com.nvidia.spark.rapids.tool.profiling.ProfileMain$  -
+--------+----------+----------+-----------+------------+-------------+--------+--------+--------+------------+--------+--------+
|appIndex|executorID|totalCores|maxMem     |maxOnHeapMem|maxOffHeapMem|exec_cpu|exec_mem|exec_gpu|exec_offheap|task_cpu|task_gpu|
+--------+----------+----------+-----------+------------+-------------+--------+--------+--------+------------+--------+--------+
|1       |0         |4         |13984648396|13984648396 |0            |null    |null    |null    |null        |null    |null    |
|1       |1         |4         |13984648396|13984648396 |0            |null    |null    |null    |null        |null    |null    |
```


- Compare Rapids related Spark properties side-by-side:
```
[main] INFO  com.nvidia.spark.rapids.tool.profiling.ProfileMain$  - Compare Rapids Properties which are set explicitly:
[main] INFO  com.nvidia.spark.rapids.tool.profiling.ProfileMain$  -
+-------------------------------------------+----------+----------+
|key                                        |value_app1|value_app2|
+-------------------------------------------+----------+----------+
|spark.rapids.memory.pinnedPool.size        |null      |2g        |
|spark.rapids.sql.castFloatToDecimal.enabled|null      |true      |
|spark.rapids.sql.concurrentGpuTasks        |null      |2         |
|spark.rapids.sql.decimalType.enabled       |null      |true      |
|spark.rapids.sql.enabled                   |false     |true      |
|spark.rapids.sql.explain                   |null      |NOT_ON_GPU|
|spark.rapids.sql.hasNans                   |null      |FALSE     |
|spark.rapids.sql.incompatibleOps.enabled   |null      |true      |
|spark.rapids.sql.variableFloatAgg.enabled  |null      |TRUE      |
+-------------------------------------------+----------+----------+
```