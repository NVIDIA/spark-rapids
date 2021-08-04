---
layout: page
title: Qualification and Profiling tools
parent: Additional Functionality
nav_order: 7
---
# Spark Qualification and Profiling tools

The qualification tool analyzes applications to determine if the RAPIDS Accelerator for Apache Spark 
might be a good fit for those applications.

The profiling tool generates information which can be used for debugging and profiling applications.
The information contains the Spark version, executor details, properties, etc. This runs on either CPU or
GPU generated event logs.

This document covers below topics:

* [Prerequisites](#Prerequisites)
* [Download the tools jar or compile it](#Download-the-tools-jar-or-compile-it)
* [Qualification Tool](#Qualification-Tool)
    * [Qualification tool functions](#Qualification-tool-functions)
    * [Download the Spark 3 distribution](#Download-the-Spark-3-distribution)
    * [How to use qualification tool](#How-to-use-qualification-tool)
    * [Qualification tool options](#Qualification-tool-options)
    * [Qualification tool output](#Qualification-tool-output)
* [Profiling Tool](#Profiling-Tool)
    * [Profiling tool functions](#Profiling-tool-functions)
    * [Profiling tool metrics definitions](#Profiling-tool-metrics-definitions)
    * [How to use profiling tool](#How-to-use-profiling-tool)
    * [Profiling tool options](#Profiling-tool-options)
    * [Profiling tool output](#Profiling-tool-output)
      
## Prerequisites
- Spark 3.0.1 or newer, the Qualification tool just needs the Spark jars and the Profiling tool
  runs a Spark application so needs the Spark runtime.
- Java 8 or above
- Spark event log(s) from Spark 2.0 or above version.
  Supports both rolled and compressed event logs with `.lz4`, `.lzf`, `.snappy` and `.zstd` suffixes as
  well as Databricks-specific rolled and compressed(.gz) eventlogs.
  The tool does not support nested directories. Event log files or event log directories should be
  at the top level when specifying a directory.

Note: Spark event logs can be downloaded from Spark UI using a "Download" button on the right side,
or can be found in the location specified by `spark.eventLog.dir`. See the
[Apache Spark Monitoring](http://spark.apache.org/docs/latest/monitoring.html) documentation for
more information.

Optional:
- Maven installed 
  (only if you want to compile the jar yourself)
- hadoop-aws-<version>.jar and aws-java-sdk-<version>.jar 
  (only if any input event log is from S3)
  
## Download the tools jar or compile it
You do not need to compile the jar yourself because you can download it from the Maven repository directly.

Here are 2 options:
1. Download the jar file from [Maven repository](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark-tools_2.12/21.08.0/)

2. Compile the jar from github repo
```bash
git clone https://github.com/NVIDIA/spark-rapids.git
cd spark-rapids
mvn -pl .,tools clean verify -DskipTests
```
The jar is generated in below directory :

`./tools/target/rapids-4-spark-tools_2.12-<version>.jar`

If any input is a S3 file path or directory path, 2 extra steps are needed to access S3 in Spark:
1. Download the matched jars based on the Hadoop version:
   - `hadoop-aws-<version>.jar`
   - `aws-java-sdk-<version>.jar`
     
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
Please refer to this [doc](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html) on 
more options about integrating hadoop-aws module with S3.

## Qualification Tool

### Qualification tool functions

The qualification tool is used to look at a set of applications to determine if the RAPIDS Accelerator for Apache Spark
might be a good fit for those applications. The tool works by processing the CPU generated event logs from Spark.

This tool is intended to give the users a starting point and does not guarantee the applications it scores highest
will actually be accelerated the most. Currently it works by looking at the amount of time spent in tasks of SQL
Dataframe operations. The more total task time doing SQL Dataframe operations the higher the score is and the more
likely the plugin will be able to help accelerate that application. The tool also looks for read data formats and types
that the plugin doesn't support and if it finds any not supported it will take away from the score (based on the
total task time in SQL Dataframe operations).

Each application(event log) could have multiple SQL queries. If a SQL's plan has Dataset API inside such as keyword
 `$Lambda` or `.apply`, that SQL query is categorized as a DataSet SQL query, otherwise it is a Dataframe SQL query.

Note: the duration(s) reported are in milli-seconds.

There are 2 output files from running the tool. One is a summary text file printing in order the applications most
likely to be good candidates for the GPU to the ones least likely. It outputs the application ID, duration,
the SQL Dataframe duration and the SQL duration spent when we found SQL queries with potential problems. It also
outputs this same report to STDOUT.
The other file is a CSV file that contains more information and can be used for further post processing.

Note, potential problems are reported in the CSV file in a separate column, which is not included in the score. This
currently includes some UDFs and some decimal operations. The tool won't catch all UDFs, and some of the UDFs can be
handled with additional steps. Please refer to [supported_ops.md](../supported_ops.md) for more details on UDF.
For decimals, it tries to recognize decimal operations but it may not catch them all.

The CSV output also contains a `Executor CPU Time Percent` column that is not included in the score. This is an estimate
at how much time the tasks spent doing processing on the CPU vs waiting on IO. This is not always a good indicator
because sometimes you may be doing IO that is encrypted and the CPU has to do work to decrypt it, so the environment
you are running on needs to be taken into account.

`App Duration Estimated` is used to indicate if we had to estimate the application duration. If we
had to estimate it, it means the event log was missing the application finished event so we will use the last job
or sql execution time we find as the end time used to calculate the duration.

Note that SQL queries that contain failed jobs are not included.

Sample output in csv:
```
App Name,App ID,Score,Potential Problems,SQL DF Duration,SQL Dataframe Task Duration,App Duration,Executor CPU Time Percent,App Duration Estimated,SQL Duration with Potential Problems,SQL Ids with Failures,Read Score Percent,Read File Format Score,Unsupported Read File Formats and Types
job3,app-20210507174503-1704,4320658.0,"",9569,4320658,26171,35.34,false,0,"",20,100.0,""
job1,app-20210507174503-2538,19864.04,"",6760,21802,83728,71.3,false,0,"",20,55.56,"Parquet[decimal]"
```

Sample output in text:
```
===========================================================================
|                 App ID|App Duration|SQL DF Duration|Problematic Duration|
===========================================================================
|app-20210507174503-2538|       26171|           9569|                   0|
|app-20210507174503-1704|       83738|           6760|                   0|
```

### Download the Spark 3 distribution
The Qualification tool requires the Spark 3.x jars to be able to run. If you do not already have
Spark 3.x installed, you can download the Spark distribution to any machine and include the jars
in the classpath.

1. [Download Apache Spark 3.x](http://spark.apache.org/downloads.html) - Spark 3.1.1 for Apache Hadoop is recommended
2. Extract the Spark distribution into a local directory.
3. Either set `SPARK_HOME` to point to that directory or just put the path inside of the classpath
   `java -cp toolsJar:pathToSparkJars/*:...` when you run the qualification tool. See the
   [How to use qualification tool](#How-to-use-qualification-tool) section below.

### How to use qualification tool
This tool parses the Spark CPU event log(s) and creates an output report.
Acceptable input event log paths are files or directories containing spark events logs
in the local filesystem, HDFS, S3 or mixed. Note that if you are on an HDFS cluster
the default filesystem is likely HDFS for both the input and output so if you want to
point to the local filesystem be sure to include `file:` in the path

```bash
Usage: java -cp rapids-4-spark-tools_2.12-<version>.jar:$SPARK_HOME/jars/*
       com.nvidia.spark.rapids.tool.qualification.QualificationMain [options]
       <eventlogs | eventlog directories ...>
```

Example running on files in HDFS: (include $HADOOP_CONF_DIR in classpath)
```bash
java -cp ~/rapids-4-spark-tools_2.12-21.<version>.jar:$SPARK_HOME/jars/*:$HADOOP_CONF_DIR/ \
 com.nvidia.spark.rapids.tool.qualification.QualificationMain  /eventlogDir
```

### Qualification tool options

  Note: `--help` should be before the trailing event logs.
```bash
java -cp ~/rapids-4-spark-tools_2.12-21.<version>.jar:$SPARK_HOME/jars/*:$HADOOP_CONF_DIR/ \
 com.nvidia.spark.rapids.tool.qualification.QualificationMain --help

RAPIDS Accelerator for Apache Spark qualification tool

Usage: java -cp rapids-4-spark-tools_2.12-<version>.jar:$SPARK_HOME/jars/*
       com.nvidia.spark.rapids.tool.qualification.QualificationMain [options]
       <eventlogs | eventlog directories ...>

  -a, --application-name  <arg>     Filter event logs whose application name
                                    matches exactly or is a substring of input
                                    string. Regular expressions not
                                    supported. For filtering based on complement
                                    of application name, use ~APPLICATION_NAME.
                                    i.e Select all event logs except the ones
                                    which have application name as the input
                                    string.
  -f, --filter-criteria  <arg>      Filter newest or oldest N eventlogs based on
                                    application start timestamp, unique
                                    application name or filesystem timestamp.
                                    Filesystem based filtering happens before
                                    any application based filtering. For
                                    application based filtering, the order in
                                    which filters are applied is:
                                    application-name, start-app-time,
                                    filter-criteria. Application based
                                    filter-criteria are: 100-newest (for
                                    processing newest 100 event logs based on
                                    timestamp insidethe eventlog) i.e
                                    application start time)  100-oldest (for
                                    processing oldest 100 event logs based on
                                    timestamp insidethe eventlog) i.e
                                    application start time)
                                    100-newest-per-app-name (select at most 100
                                    newest log files for each unique application
                                    name) 100-oldest-per-app-name (select at
                                    most 100 oldest log files for each unique
                                    application name). Filesystem based filter
                                    criteria are: 100-newest-filesystem (for
                                    processing newest 100 event logs based on
                                    filesystem timestamp). 100-oldest-filesystem
                                    (for processing oldest 100 event logsbased
                                    on filesystem timestamp).
  -m, --match-event-logs  <arg>     Filter event logs whose filenames contain
                                    the input string. Filesystem based filtering
                                    happens before any application based
                                    filtering.
  -n, --num-output-rows  <arg>      Number of output rows in the summary report.
                                    Default is 1000.
      --num-threads  <arg>          Number of thread to use for parallel
                                    processing. The default is the number of
                                    cores on host divided by 4.
      --order  <arg>                Specify the sort order of the report. desc
                                    or asc, desc is the default. desc
                                    (descending) would report applications most
                                    likely to be accelerated at the top and asc
                                    (ascending) would show the least likely to
                                    be accelerated at the top.
  -o, --output-directory  <arg>     Base output directory. Default is current
                                    directory for the default filesystem. The
                                    final output will go into a subdirectory
                                    called rapids_4_spark_qualification_output.
                                    It will overwrite any existing directory
                                    with the same name.
  -r, --read-score-percent  <arg>   The percent the read format and datatypes
                                    apply to the score. Default is 20 percent.
      --report-read-schema          Whether to output the read formats and
                                    datatypes to the CSV file. This can be very
                                    long. Default is false.
  -s, --start-app-time  <arg>       Filter event logs whose application start
                                    occurred within the past specified time
                                    period. Valid time periods are
                                    min(minute),h(hours),d(days),w(weeks),m(months).
                                    If a period is not specified it defaults to
                                    days.
  -t, --timeout  <arg>              Maximum time in seconds to wait for the
                                    event logs to be processed. Default is 24
                                    hours (86400 seconds) and must be greater
                                    than 3 seconds. If it times out, it will
                                    report what it was able to process up until
                                    the timeout.
  -h, --help                        Show help message

 trailing arguments:
  eventlog (required)   Event log filenames(space separated) or directories
                        containing event logs. eg: s3a://<BUCKET>/eventlog1
                        /path/to/eventlog2
```

Example commands:
- Process the 10 newest logs, and only output the top 3 in the output:
```bash
java -cp ~/rapids-4-spark-tools_2.12-21.<version>.jar:$SPARK_HOME/jars/*:$HADOOP_CONF_DIR/ \
 com.nvidia.spark.rapids.tool.qualification.QualificationMain -f 10-newest -n 3 /eventlogDir
```
- Process last 100 days' logs:
```bash
java -cp ~/rapids-4-spark-tools_2.12-21.<version>.jar:$SPARK_HOME/jars/*:$HADOOP_CONF_DIR/ \
 com.nvidia.spark.rapids.tool.qualification.QualificationMain -s 100d /eventlogDir
```
- Process only the newest log with the same application name: 
```bash
java -cp ~/rapids-4-spark-tools_2.12-21.<version>.jar:$SPARK_HOME/jars/*:$HADOOP_CONF_DIR/ \
 com.nvidia.spark.rapids.tool.qualification.QualificationMain -f 1-newest-per-app-name /eventlogDir
```

### Qualification tool output
The summary report goes to STDOUT and by default it outputs 2 files under sub-directory
`./rapids_4_spark_qualification_output/` that contain the processed applications. The output will
go into your default filesystem, it supports local filesystem or HDFS.  Note that if you are on an
HDFS cluster the default filesystem is likely HDFS for both the input and output so if you want to
point to the local filesystem be sure to include `file:` in the path

The output location can be changed using the `--output-directory` option. Default is current directory.

It will output a text file with the name `rapids_4_spark_qualification_output.log` that is a summary report and
it will output a CSV file named `rapids_4_spark_qualification_output.csv` that has more data in it.

## Profiling Tool

The profiling tool generates information which can be used for debugging and profiling applications.
It will run a Spark application so requires Spark to be installed and setup. If you have a cluster already setup
you can run it on that, or you can simply run it in local mode as well. See the Apache Spark documentation
for [Downloading Apache Spark 3.x](http://spark.apache.org/downloads.html)

### Profiling tool functions

#### A. Collect Information or Compare Information(if more than 1 event logs are as input and option -c is specified)
- Application information
- Executors information
- Rapids related parameters
- Rapids Accelerator Jar and cuDF Jar
- Job, stage and SQL ID information (not in `compare` mode yet)
- SQL Plan Metrics
- Optionally : SQL Plan for each SQL query
- Optionally : Generates DOT graphs for each SQL query
- Optionally : Generates timeline graph for application

For example, GPU run vs CPU run performance comparison or different runs with different parameters.

We can input multiple Spark event logs and this tool can compare environments, executors, Rapids related Spark parameters,

- Compare the durations/versions/gpuMode on or off:
```
### A. Compare Information Collected ###
Compare Application Information:

+--------+-----------+-----------------------+-------------+-------------+--------+-----------+------------+-------------+
|appIndex|appName    |appId                  |startTime    |endTime      |duration|durationStr|sparkVersion|pluginEnabled|
+--------+-----------+-----------------------+-------------+-------------+--------+-----------+------------+-------------+
|1       |Spark shell|app-20210329165943-0103|1617037182848|1617037490515|307667  |5.1 min    |3.0.1       |false        |
|2       |Spark shell|app-20210329170243-0018|1617037362324|1617038578035|1215711 |20 min     |3.0.1       |true         |
+--------+-----------+-----------------------+-------------+-------------+--------+-----------+------------+-------------+
```

- Compare Executor information:
```
Compare Executor Information:
+--------+-----------------+------------+-------------+-----------+------------+-------------+--------------+------------------+---------------+-------+-------+
|appIndex|resourceProfileId|numExecutors|executorCores|maxMem     |maxOnHeapMem|maxOffHeapMem|executorMemory|numGpusPerExecutor|executorOffHeap|taskCpu|taskGpu|
+--------+-----------------+------------+-------------+-----------+------------+-------------+--------------+------------------+---------------+-------+-------+
|1       |0                |1           |4            |11264537395|11264537395 |0            |20480         |1                 |0              |1      |0.0    |
|1       |1                |2           |2            |3247335014 |3247335014  |0            |6144          |2                 |0              |2      |2.0    |
+--------+-----------------+------------+-------------+-----------+------------+-------------+-------------+--------------+------------------+---------------+-------+-------+
```

- Matching SQL IDs Across Applications:
```
Matching SQL IDs Across Applications:
+-----------------------+-----------------------+
|app-20210329165943-0103|app-20210329170243-0018|
+-----------------------+-----------------------+
|0                      |0                      |
|1                      |1                      |
|2                      |2                      |
|3                      |3                      |
|4                      |4                      |
+-----------------------+-----------------------+
```

There is one column per application. There is a row per SQL ID. The SQL IDs are matched
primarily on the structure of the SQL query run, and then on the order in which they were
run. Be aware that this is truly the structure of the query. Two queries that do similar
things, but on different data are likely to match as the same.  An effort is made to
also match between CPU plans and GPU plans so in most cases the same query run on the
CPU and on the GPU will match.

- Matching Stage IDs Across Applications:
```
Matching Stage IDs Across Applications:
+-----------------------+-----------------------+
|app-20210329165943-0103|app-20210329170243-0018|
+-----------------------+-----------------------+
|31                     |31                     |
|32                     |32                     |
|33                     |33                     |
|39                     |38                     |
|40                     |40                     |
|41                     |41                     |
+-----------------------+-----------------------+
```

There is one column per application. There is a row per stage ID. If a SQL query matches
between applications, see Matching SQL IDs Across Applications, then an attempt is made
to match stages within that application to each other.  This has the same issues with
stages when generating a dot graph.  This can be especially helpful when trying to compare
large queries and Spark happened to assign the stage IDs slightly differently, or in some
cases there are a different number of stages because of slight differences in the plan. This
is a best effort, and it is not guaranteed to match up all stages in a plan.

- Compare Rapids related Spark properties side-by-side:
```
Compare Rapids Properties which are set explicitly:
+-------------------------------------------+----------+----------+
|propertyName                               |appIndex_1|appIndex_2|
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
 
- List rapids-4-spark and cuDF jars based on classpath: 
```
Rapids Accelerator Jar and cuDF Jar:
/path/rapids-4-spark_2.12-0.5.0.jar
/path/cudf-0.19-cuda10-2.jar
```

- Job, stage and SQL ID information(not in `compare` mode yet):
```
+--------+-----+------------+-----+
|appIndex|jobID|stageIds    |sqlID|
+--------+-----+------------+-----+
|1       |0    |[0]         |null |
|1       |1    |[1, 2, 3, 4]|0    |
+--------+-----+------------+-----+
```

- SQL Plan Metrics for Application for each SQL plan node in each SQL:

These are also called accumulables in Spark.
```
SQL Plan Metrics for Application:
+--------+-----+------+-----------------------------------------------------------+-------------+-----------------------+-------------+----------+
|appIndex|sqlID|nodeID|nodeName                                                   |accumulatorId|name                   |max_value    |metricType|
+--------+-----+------+-----------------------------------------------------------+-------------+-----------------------+-------------+----------+
|1       |0    |1     |GpuColumnarExchange                                        |111          |output rows            |1111111111   |sum       |
|1       |0    |1     |GpuColumnarExchange                                        |112          |output columnar batches|222222       |sum       |
|1       |0    |1     |GpuColumnarExchange                                        |113          |data size              |333333333333 |size      |
|1       |0    |1     |GpuColumnarExchange                                        |114          |shuffle bytes written  |444444444444 |size      |
|1       |0    |1     |GpuColumnarExchange                                        |115          |shuffle records written|555555       |sum       |
|1       |0    |1     |GpuColumnarExchange                                        |116          |shuffle write time     |666666666666 |nsTiming  |
```

- Print SQL Plans (-p option):
Prints the SQL plan as a text string to a file prefixed with `planDescriptions-`.
For example if your application id is app-20210507103057-0000, then the
filename will be `planDescriptions-app-20210507103057-0000`

- Generate DOT graph for each SQL (-g option):
```
Generated DOT graphs for app app-20210507103057-0000 to /path/. in 17 second(s)
```
Once the DOT file is generated, you can install [graphviz](http://www.graphviz.org) to convert the DOT file 
as a graph in pdf format using below command:
```bash
dot -Tpdf ./app-20210507103057-0000-query-0/0.dot > app-20210507103057-0000.pdf
```

Or to svg using
```bash
dot -Tsvg ./app-20210507103057-0000-query-0/0.dot > app-20210507103057-0000.svg
```
The pdf or svg file has the SQL plan graph with metrics. The svg file will act a little
more like the Spark UI and include extra information for nodes when hovering over it with
a mouse.

As a part of this an effort is made to associate parts of the graph with the Spark stage it is a
part of. This is not 100% accurate. Some parts of the plan like `TakeOrderedAndProject` may
be a part of multiple stages and only one of the stages will be selected. `Exchanges` are purposely
left out of the sections associated with a stage because they cover at least 2 stages and possibly
more. In other cases we may not be able to determine what stage something was a part of. In those
cases we mark it as `UNKNOWN STAGE`. This is because we rely on metrics to link a node to a stage.
If a stage hs no metrics, like if the query crashed early, we cannot establish that link.

- Generate timeline for application (--generate-timeline option):

The output of this is an [svg](https://en.wikipedia.org/wiki/Scalable_Vector_Graphics) file
named `${APPLICATION_ID}-timeline.svg`.  Most web browsers can display this file.  It is a
timeline view similar Apache Spark's 
[event timeline](https://spark.apache.org/docs/latest/web-ui.html). 

This displays several data sections.

1) **Tasks** This shows all tasks in the application divided by executor.  Please note that this
   tries to pack the tasks in the graph. It does not represent actual scheduling on CPU cores.
   The tasks are labeled with the time it took for them to run, but there is no breakdown about
   different aspects of each task, like there is in Spark's timeline.
2) **STAGES** This shows the stages times reported by Spark. It starts with when the stage was 
   scheduled and ends when Spark considered the stage done.
3) **STAGE RANGES** This shows the time from the start of the first task to the end of the last
   task. Often a stage is scheduled, but there are not enough resources in the cluster to run it.
   This helps to show. How long it takes for a task to start running after it is scheduled, and in
   many cases how long it took to run all of the tasks in the stage. This is not always true because
   Spark can intermix tasks from different stages.
4) **JOBS** This shows the time range reported by Spark from when a job was scheduled to when it
   completed.
5) **SQL** This shows the time range reported by Spark from when a SQL statement was scheduled to
   when it completed.

Tasks and stages all are color coordinated to help know what tasks are associated with a given
stage. Jobs and SQL are not color coordinated.

#### B. Analysis
- Job + Stage level aggregated task metrics
- SQL level aggregated task metrics
- SQL duration, application during, if it contains a Dataset operation, potential problems, executor CPU time percent
- Shuffle Skew Check: (When task's Shuffle Read Size > 3 * Avg Stage-level size)

Below we will aggregate the task level metrics at different levels to do some analysis such as detecting possible shuffle skew.

- Job + Stage level aggregated task metrics:
```
### B. Analysis ###

Job + Stage level aggregated task metrics:
+--------+-------+--------+--------+--------------------+------------+------------+------------+------------+-------------------+------------------------------+---------------------------+-------------------+---------------------+-------------------+---------------------+-------------+----------------------+-----------------------+-------------------------+-----------------------+---------------------------+--------------+--------------------+-------------------------+---------------------+--------------------------+----------------------+----------------------------+---------------------+-------------------+---------------------+----------------+
|appIndex|ID     |numTasks|Duration|diskBytesSpilled_sum|duration_sum|duration_max|duration_min|duration_avg|executorCPUTime_sum|executorDeserializeCPUTime_sum|executorDeserializeTime_sum|executorRunTime_sum|gettingResultTime_sum|input_bytesRead_sum|input_recordsRead_sum|jvmGCTime_sum|memoryBytesSpilled_sum|output_bytesWritten_sum|output_recordsWritten_sum|peakExecutionMemory_max|resultSerializationTime_sum|resultSize_max|sr_fetchWaitTime_sum|sr_localBlocksFetched_sum|sr_localBytesRead_sum|sr_remoteBlocksFetched_sum|sr_remoteBytesRead_sum|sr_remoteBytesReadToDisk_sum|sr_totalBytesRead_sum|sw_bytesWritten_sum|sw_recordsWritten_sum|sw_writeTime_sum|
+--------+-------+--------+--------+--------------------+------------+------------+------------+------------+-------------------+------------------------------+---------------------------+-------------------+---------------------+-------------------+---------------------+-------------+----------------------+-----------------------+-------------------------+-----------------------+---------------------------+--------------+--------------------+-------------------------+---------------------+--------------------------+----------------------+----------------------------+---------------------+-------------------+---------------------+----------------+
|1       |job_0  |3333    |222222  |0                   |11111111    |111111      |111         |1111.1      |6666666            |55555                         |55555                      |55555555           |0                    |222222222222       |22222222222          |111111       |0                     |0                      |0                        |222222222              |1                          |11111         |11111               |99999                    |22222222222          |2222221                   |222222222222          |0                           |222222222222         |222222222222       |5555555              |444444          |
```
  

- SQL level aggregated task metrics:
```
SQL level aggregated task metrics:
+--------+------------------------------+-----+--------------------+--------+--------+---------------+---------------+----------------+--------------------+------------+------------+------------+------------+-------------------+------------------------------+---------------------------+-------------------+---------------------+-------------------+---------------------+-------------+----------------------+-----------------------+-------------------------+-----------------------+---------------------------+--------------+--------------------+-------------------------+---------------------+--------------------------+----------------------+----------------------------+---------------------+-------------------+---------------------+----------------+
|appIndex|appID                         |sqlID|description         |numTasks|Duration|executorCPUTime|executorRunTime|executorCPURatio|diskBytesSpilled_sum|duration_sum|duration_max|duration_min|duration_avg|executorCPUTime_sum|executorDeserializeCPUTime_sum|executorDeserializeTime_sum|executorRunTime_sum|gettingResultTime_sum|input_bytesRead_sum|input_recordsRead_sum|jvmGCTime_sum|memoryBytesSpilled_sum|output_bytesWritten_sum|output_recordsWritten_sum|peakExecutionMemory_max|resultSerializationTime_sum|resultSize_max|sr_fetchWaitTime_sum|sr_localBlocksFetched_sum|sr_localBytesRead_sum|sr_remoteBlocksFetched_sum|sr_remoteBytesRead_sum|sr_remoteBytesReadToDisk_sum|sr_totalBytesRead_sum|sw_bytesWritten_sum|sw_recordsWritten_sum|sw_writeTime_sum|
+--------+------------------------------+-----+--------------------+--------+--------+---------------+---------------+----------------+--------------------+------------+------------+------------+------------+-------------------+------------------------------+---------------------------+-------------------+---------------------+-------------------+---------------------+-------------+----------------------+-----------------------+-------------------------+-----------------------+---------------------------+--------------+--------------------+-------------------------+---------------------+--------------------------+----------------------+----------------------------+---------------------+-------------------+---------------------+----------------+
|1       |application_1111111111111_0001|0    |show at <console>:11|1111    |222222  |6666666        |55555555       |55.55           |0                   |13333333    |111111      |999         |3333.3      |6666666            |55555                         |66666                      |11111111           |0                    |111111111111       |11111111111          |111111       |0                     |0                      |0                        |888888888              |8                          |11111         |11111               |99999                    |11111111111          |2222222                   |222222222222          |0                           |222222222222         |444444444444       |5555555              |444444          |
```

- SQL duration, application during, if it contains a Dataset operation, potential problems, executor CPU time percent: 
```
SQL Duration and Executor CPU Time Percent
+--------+------------------------------+-----+------------+-------------------+------------+------------------+-------------------------+
|appIndex|App ID                        |sqlID|SQL Duration|Contains Dataset Op|App Duration|Potential Problems|Executor CPU Time Percent|
+--------+------------------------------+-----+------------+-------------------+------------+------------------+-------------------------+
|1       |application_1603128018386_7759|0    |11042       |false              |119990      |null              |68.48                    |
+--------+------------------------------+-----+------------+-------------------+------------+------------------+-------------------------+
```

- Shuffle Skew Check: 
```
Shuffle Skew Check: (When task's Shuffle Read Size > 3 * Avg Stage-level size)
+--------+-------+--------------+------+-------+---------------+--------------+-----------------+----------------+----------------+----------+----------------------------------------------------------------------------------------------------+
|appIndex|stageId|stageAttemptId|taskId|attempt|taskDurationSec|avgDurationSec|taskShuffleReadMB|avgShuffleReadMB|taskPeakMemoryMB|successful|reason                                                                                              |
+--------+-------+--------------+------+-------+---------------+--------------+-----------------+----------------+----------------+----------+----------------------------------------------------------------------------------------------------+
|1       |2      |0             |2222  |0      |111.11         |7.7           |2222.22          |111.11          |0.01            |false     |ExceptionFailure(ai.rapids.cudf.CudfException,cuDF failure at: /dddd/xxxxxxx/ccccc/bbbbbbbbb/aaaaaaa|
|1       |2      |0             |2224  |1      |222.22         |8.8           |3333.33          |111.11          |0.01            |false     |ExceptionFailure(ai.rapids.cudf.CudfException,cuDF failure at: /dddd/xxxxxxx/ccccc/bbbbbbbbb/aaaaaaa|
+--------+-------+--------------+------+-------+---------------+--------------+-----------------+----------------+----------------+----------+----------------------------------------------------------------------------------------------------+
```
#### C. Health Check
- List failed tasks, stages and jobs
- SQL Plan HealthCheck

Below are examples.
- Print failed tasks:
```
Failed tasks:
+--------+-------+--------------+------+-------+----------------------------------------------------------------------------------------------------+
|appIndex|stageId|stageAttemptId|taskId|attempt|failureReason                                                                              |
+--------+-------+--------------+------+-------+----------------------------------------------------------------------------------------------------+
|3       |4      |0             |2842  |0      |ExceptionFailure(ai.rapids.cudf.CudfException,cuDF failure at: /home/jenkins/agent/workspace/jenkins|
|3       |4      |0             |2858  |0      |TaskKilled(another attempt succeeded,List(AccumulableInfo(453,None,Some(22000),None,false,true,None)|
|3       |4      |0             |2884  |0      |TaskKilled(another attempt succeeded,List(AccumulableInfo(453,None,Some(21148),None,false,true,None)|
|3       |4      |0             |2908  |0      |TaskKilled(another attempt succeeded,List(AccumulableInfo(453,None,Some(20420),None,false,true,None)|
|3       |4      |0             |3410  |1      |ExceptionFailure(ai.rapids.cudf.CudfException,cuDF failure at: /home/jenkins/agent/workspace/jenkins|
|4       |1      |0             |1948  |1      |TaskKilled(another attempt succeeded,List(AccumulableInfo(290,None,Some(1107),None,false,true,None),|
+--------+-------+--------------+------+-------+----------------------------------------------------------------------------------------------------+
```

- Print failed stages:
```
Failed stages:
+--------+-------+---------+-------------------------------------+--------+---------------------------------------------------+
|appIndex|stageId|attemptId|name                                 |numTasks|failureReason                                      |
+--------+-------+---------+-------------------------------------+--------+---------------------------------------------------+
|3       |4      |0        |attachTree at Spark300Shims.scala:624|1000    |Job 0 cancelled as part of cancellation of all jobs|
+--------+-------+---------+-------------------------------------+--------+---------------------------------------------------+
```

- Print failed jobs:
```
Failed jobs:
+--------+-----+---------+------------------------------------------------------------------------+
|appIndex|jobID|jobResult|failureReason                                                           |
+--------+-----+---------+------------------------------------------------------------------------+
|3       |0    |JobFailed|java.lang.Exception: Job 0 cancelled as part of cancellation of all j...|
+--------+-----+---------+------------------------------------------------------------------------+
```

- SQL Plan HealthCheck:

  Prints possibly unsupported query plan nodes such as `$Lambda` key word means dataset API.
```
+--------+-----+------+--------+---------------------------------------------------------------------------------------------------+
|appIndex|sqlID|nodeID|nodeName|nodeDescription                                                                                    |
+--------+-----+------+--------+---------------------------------------------------------------------------------------------------+
|3       |1    |8     |Filter  |Filter $line21.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$Lambda$4578/0x00000008019f1840@4b63e04c.apply|
+--------+-----+------+--------+---------------------------------------------------------------------------------------------------+
```

### Profiling tool metrics definitions
All the metrics definitions can be found in the [executor task metrics doc](https://spark.apache.org/docs/latest/monitoring.html#executor-task-metrics) / [executor metrics doc](https://spark.apache.org/docs/latest/monitoring.html#executor-metrics) or the [SPARK webUI doc](https://spark.apache.org/docs/latest/web-ui.html#content).

### How to use profiling tool
This tool parses the Spark CPU or GPU event log(s) and creates an output report.
Acceptable input event log paths are files or directories containing spark events logs
in the local filesystem, HDFS, S3 or mixed. Note that if you are on an
HDFS cluster the default filesystem is likely HDFS for both the input and output so if you want to
point to the local filesystem be sure to include `file:` in the path

#### Use from spark-shell
1. Include `rapids-4-spark-tools_2.12-<version>.jar` in the '--jars' option to spark-shell or spark-submit
2. After starting spark-shell:

For a single event log analysis:
```bash
com.nvidia.spark.rapids.tool.profiling.ProfileMain.main(Array("/path/to/eventlog1"))
```

For multiple event logs comparison and analysis:
```bash
com.nvidia.spark.rapids.tool.profiling.ProfileMain.main(Array("/path/to/eventlog1", "/path/to/eventlog2"))
```

#### Use from spark-submit
```bash
$SPARK_HOME/bin/spark-submit --class com.nvidia.spark.rapids.tool.profiling.ProfileMain \
rapids-4-spark-tools_2.12-<version>.jar \
/path/to/eventlog1 /path/to/eventlog2 /directory/with/eventlogs
```

### Profiling tool options
  
  Note: `--help` should be before the trailing event logs.
```bash
$SPARK_HOME/bin/spark-submit \
--class com.nvidia.spark.rapids.tool.profiling.ProfileMain \
rapids-4-spark-tools_2.12-<version>.jar \
--help


For usage see below:

  -c, --compare                   Compare Applications (Recommended to compare
                                  less than 10 applications). Default is false
  -f, --filter-criteria  <arg>    Filter newest or oldest N eventlogs for
                                  processing.eg: 100-newest-filesystem (for
                                  processing newest 100 event logs). eg:
                                  100-oldest-filesystem (for processing oldest
                                  100 event logs)
  -g, --generate-dot              Generate query visualizations in DOT format.
                                  Default is false
      --generate-timeline         Write an SVG graph out for the full
                                  application timeline.
  -m, --match-event-logs  <arg>   Filter event logs whose filenames contain the
                                  input string
  -n, --num-output-rows  <arg>    Number of output rows for each Application.
                                  Default is 1000
  -o, --output-directory  <arg>   Base output directory. Default is current
                                  directory for the default filesystem. The
                                  final output will go into a subdirectory
                                  called rapids_4_spark_profile. It will
                                  overwrite any existing files with the same
                                  name.
  -p, --print-plans               Print the SQL plans to a file starting with
                                  'planDescriptions-'. Default is false
  -h, --help                      Show help message

 trailing arguments:
  eventlog (required)   Event log filenames(space separated) or directories
                        containing event logs. eg: s3a://<BUCKET>/eventlog1
                        /path/to/eventlog2
```

Example commands:
- Process 10 newest logs with filenames containing "local":
```bash
$SPARK_HOME/bin/spark-submit --class com.nvidia.spark.rapids.tool.profiling.ProfileMain \
rapids-4-spark-tools_2.12-<version>.jar \
-m "local" -f "10-newest-filesystem" \
/directory/with/eventlogs/
```
- Print SQL plans, generate dot files and also generate timeline(SVG graph):
```bash
$SPARK_HOME/bin/spark-submit --class com.nvidia.spark.rapids.tool.profiling.ProfileMain \
rapids-4-spark-tools_2.12-<version>.jar \
-p -g --generate-timeline \
/directory/with/eventlogs/
```

### Profiling tool output
By default this outputs a log file under sub-directory `./rapids_4_spark_profile` named
`rapids_4_spark_tools_output.log` that contains the processed applications. The output will go into your
default filesystem, it supports local filesystem or HDFS.  Note that if you are on an
HDFS cluster the default filesystem is likely HDFS for both the input and output so if you want to
point to the local filesystem be sure to include `file:` in the path
There are separate files that are generated under the same sub-directory when using the options to generate
query visualizations or printing the SQL plans.

The output location can be changed using the `--output-directory` option. Default is current directory.

There is a 100 characters limit for each output column. If the result of the column exceeds this limit, it is suffixed with ... for that column.

ResourceProfile ids are parsed for the event logs that are from Spark 3.1 or later. ResourceProfileId column is added in the output table for such event logs. 
A ResourceProfile allows the user to specify executor and task requirements for an RDD that will get applied during a stage. This allows the user to change the resource requirements between stages.
  
Note: We suggest you also save the output of the `spark-submit` or `spark-shell` to a log file for troubleshooting.

Run `--help` for more information.
