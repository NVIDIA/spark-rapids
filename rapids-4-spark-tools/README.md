# Spark Qualification and Profiling tools

The qualification tool is used to rank a set of applications to determine if the RAPIDS Accelerator for Apache Spark 
is a good fit for those applications. Works with CPU generated event logs.

The profiling tool generates information which can be used for ranking applications debugging and profiling.
Information such as Spark version, executor information, properties and so on. Works with both CPU and GPU generated event logs.

(The code is based on Apache Spark 3.1.1 source code, and tested using Spark 3.0.x and 3.1.1 event logs)

## Prerequisites
- Spark 3.1.1 or newer installed
- Java 8 or above
- Complete Spark event log(s) from Spark 3.0 or above version.

Optional:
- maven installed 
  (only if you want to compile the jar yourself)
- A running Spark cluster 
  (only if you choose to run the job on a Spark cluster)
- hadoop-aws-<version>.jar and aws-java-sdk-<version>.jar 
  (only if any input event log is from S3)
  
## Download the jar or compile it
You do not need to compile the jar yourself because you can download it from maven repository directly.

Here are 2 options:
1. Download the jar file from maven repository
  
2. Compile the jar from github repo
```bash
git clone https://github.com/NVIDIA/spark-rapids.git
cd spark-rapids
mvn -pl .,rapids-4-spark-tools clean verify -DskipTests
```
The jar is generated in below directory :

`./rapids-4-spark-tools/target/rapids-4-spark-tools_2.12-<version>.jar`

## How to use this tool

This tool parses the spark event log(s) and creates an output report.
Acceptable input spark event log paths are local filesystem,hdfs,S3 or mixed.
It can be file path,directory path or mixed.

Below is an example input:
```
/path/to/localfilesystem/eventlog1 hdfs://path/to/directory/ s3a://<BUCKET>/to/directory/
```

If any input is a S3 file path or directory path, here 2 extra steps to access S3 in Spark:
1. Download the matched jars based on the Hadoop version:
   - hadoop-aws-<version>.jar
   - aws-java-sdk-<version>.jar 
     
Take Hadoop 2.7.4 for example, we can download and include below jars in the '--jars' option to spark-shell or spark-submit:
[hadoop-aws-2.7.4.jar](https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/2.7.4/hadoop-aws-2.7.4.jar) and 
[aws-java-sdk-1.7.4.jar](https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar)

2. In $SPARK_HOME/conf, create `hdfs-site.xml` with below AWS S3 keys inside:
```xml
<?xml version="1.0"?>
<configuration>
<property>
  <name>fs.s3a.access.key</name>
  <value>xxx</value>
</property>
<property>
  <name>fs.s3a.secret.key</name>
  <value>xxx</value>
</property>
</configuration>
```

## Qualification Tool

### Use from spark-shell
1. Include rapids-4-spark-tools_2.12-<version>.jar in the '--jars' option to spark-shell or spark-submit
2. After starting spark-shell:

For multiple event logs comparison and analysis:
```bash
com.nvidia.spark.rapids.tool.qualification.QualificationMain.main(Array("/path/to/eventlog1", "/path/to/eventlog2"))
```

### Use from spark-submit
```bash
$SPARK_HOME/bin/spark-submit --class com.nvidia.spark.rapids.tool.qualification.QualificationMain \
rapids-4-spark-tools_2.12-<version>.jar \
/path/to/eventlog1 /path/to/eventlog2
```

### Options
```bash
$SPARK_HOME/bin/spark-submit \
--class com.nvidia.spark.rapids.tool.qualification.QualificationMain \
rapids-4-spark-tools_2.12-<version>.jar \
--help

For usage see below:

  -i, --include-exec-cpu-percent   Include the executor CPU time percent. It
                                   will take longer with this option and you may
                                   want to limit the number of applications
                                   processed at once.
  -n, --num-output-rows  <arg>     Number of output rows for each Application.
                                   Default is 1000.
  -o, --output-directory  <arg>    Base output directory. Default is current
                                   directory for the default filesystem. The
                                   final output will go into a subdirectory
                                   called rapids_4_spark_qualification_output.
                                   It will overwrite any existing directory with
                                   the same name.
      --output-format  <arg>       Output format, supports csv and text. Default
                                   is csv. text output format creates a file
                                   named rapids_4_spark_qualification.log while
                                   csv will create a file using the standard
                                   Spark naming convention.
  -h, --help                       Show help message

 trailing arguments:
  eventlog (required)   Event log filenames(space separated) or directories
                        containing event logs. eg: s3a://<BUCKET>/eventlog1
                        /path/to/eventlog2
```

### Output
By default this outputs a csv file under sub-directory `./rapids_4_spark_qualification_output/` that contains 
the rankings of the applications.

The output location can be changed using the `--output-directory` option. Default is current directory.

The output format can be changed using the `--output-format` option. Default is csv. The other option is text.
  
The `--include-exec-cpu-percent` option can be used to include executor CPU time percent which is based on the aggregated task metrics:
`sum(executorCPUTime)/sum(executorRunTime)`. It can show us roughly how much time is spent on CPU.
Note: This option needs lot of memory for large amount of event logs as input. We suggest not enable it if the input has more than 10 event logs.

Run `--help` for more information.

### Functions

The qualification tool's algorithm is to calculate the dataframe SQL duration ratio = 
sum(SQL Dataframe Duration) / (application-duration)
and then rank all applications based on that ratio.

Each application(event log) could have multiple SQLs.
If a SQL's plan has Dataset API inside such as keyworkd `$Lambda` or `.apply`, this SQL is categorized as DataSet SQL, otherwise it is a Dataframe SQL.
`sum(SQL Dataframe Duration)` means the total duration for all Dataframe SQLs.

Note: duration is in milli-seconds.

It can also print the potential problems such as UDF.
  
Note: The tool won't cache all UDFs, and some of the UDFs can be handled with additional steps. 
Please refer to [supported_ops.md](../docs/supported_ops.md) for more details on UDF.

Sample output:
```
App Name,App ID,Rank,Potential Problems,SQL Dataframe Duration,App Duration
Spark shell,app-20210507105707-0001,78.03,"",810923,1039276
Spark shell,app-20210507103057-0000,75.87,"",316622,417307
```

## Profiling Tool

### Use from spark-shell
1. Include rapids-4-spark-tools_2.12-<version>.jar in the '--jars' option to spark-shell or spark-submit
2. After starting spark-shell:

For a single event log analysis:
```bash
com.nvidia.spark.rapids.tool.profiling.ProfileMain.main(Array("/path/to/eventlog1"))
```

For multiple event logs comparison and analysis:
```bash
com.nvidia.spark.rapids.tool.profiling.ProfileMain.main(Array("/path/to/eventlog1", "/path/to/eventlog2"))
```

### Use from spark-submit
```bash
$SPARK_HOME/bin/spark-submit --class com.nvidia.spark.rapids.tool.profiling.ProfileMain \
rapids-4-spark-tools_2.12-<version>.jar \
/path/to/eventlog1 /path/to/eventlog2
```

### Options
```bash
$SPARK_HOME/bin/spark-submit \
--class com.nvidia.spark.rapids.tool.profiling.ProfileMain \
rapids-4-spark-tools_2.12-<version>.jar \
--help

For usage see below:

  -c, --compare                   Compare Applications (Recommended to compare
                                  less than 10 applications). Default is false
  -g, --generate-dot              Generate query visualizations in DOT format.
                                  Default is false
  -n, --num-output-rows  <arg>    Number of output rows for each Application.
                                  Default is 1000
  -o, --output-directory  <arg>   Output directory. Default is current directory
  -h, --help                      Show help message

 trailing arguments:
  eventlog (required)   Event log filenames(space separated). eg:
                        s3a://<BUCKET>/eventlog1 /path/to/eventlog2
```

### Output
By default this outputs a log file named `rapids_4_spark_tools_output.log` that contains the rankings of the applications.

The output location can be changed using the `--output-directory` option. Default is current directory.

Run `--help` for more information.

### Functions
#### A. Collect Information or Compare Information(if more than 1 eventlogs are as input and option -c is specified)
- Print Application Information
- Print Executors information
- Print Rapids related parameters

**1. GPU run vs CPU run performance comparison or different runs with different parameters**

We can input multiple Spark event logs and this tool can compare environments, executors, Rapids related Spark parameters,

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


- Compare Executor information:
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
