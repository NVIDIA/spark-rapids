---
layout: page
title: Profiling Tool
nav_order: 9
---
# Profiling Tool

The Profiling tool analyzes both CPU or GPU generated event logs and generates information 
which can be used for debugging and profiling Apache Spark applications.
The output information contains the Spark version, executor details, properties, etc.  
Starting with release _22.10_, the Profiling tool optionally provides optimized RAPIDS
configurations based on the worker's information (see [Auto-Tuner support](#auto-tuner-support)).

* TOC
{:toc}

## How to use the Profiling tool

### Prerequisites
- Java 8 or above, Spark 3.0.1+ jars
- Spark event log(s) from Spark 2.0 or above version. Supports both rolled and compressed event logs 
  with `.lz4`, `.lzf`, `.snappy` and `.zstd` suffixes as well as 
  Databricks-specific rolled and compressed(`.gz`) event logs.
- The tool does not support nested directories.
  Event log files or event log directories should be at the top level when specifying a directory.

Note: Spark event logs can be downloaded from Spark UI using a _Download_ button on the right side,
or can be found in the location specified by `spark.eventLog.dir`. See the
[Apache Spark Monitoring](http://spark.apache.org/docs/latest/monitoring.html) documentation for
more information.

### Step 1a: Download the tools jar
- Download the latest RAPIDS Accelerator for Apache Spark tools jar from [Maven repository](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark-tools_2.12/)

If you want to compile the jar, please refer to the instructions [here](./spark-qualification-tool.md#How-to-compile-the-tools-jar).

### Step 1b: Download the Apache Spark 3 distribution
The Profiling tool requires the Spark 3.x jars to be able to run but does not need an Apache Spark run time.
If you do not already have Spark 3.x installed, 
you can download the Spark distribution to any machine and include the jars in the classpath.
- [Download Apache Spark 3.x](http://spark.apache.org/downloads.html)

### Step 2 How to run the Profiling tool
The profiling tool parses the Spark CPU or GPU event log(s) and creates an output report.
If necessary, extract the Spark distribution into a local directory.  To run the tool, please note the following:
- Either set `SPARK_HOME` to point to that local directory or add it to the
classpath `java -cp toolsJar:pathToSparkJars/*:...` when you run the Profiling tool.
- Acceptable input event log paths are files or directories containing spark events logs
in the local filesystem, HDFS, S3 or mixed. 
- If you are processing a lot of event logs, then use combined or compare mode. Both these modes may need you to increase
the java heap size using `-Xmx` option.  For instance, to specify 30 GB heap size `java -Xmx30g`.

There are 3 modes of operation for the Profiling tool:
 1. Collection Mode: 
    Collection mode is the default mode when no other options are specified it simply collects information
    on each application individually and outputs a file per application
    
    ```bash
    Usage: java -cp rapids-4-spark-tools_2.12-<version>.jar:$SPARK_HOME/jars/*
           com.nvidia.spark.rapids.tool.profiling.ProfileMain [options]
           <eventlogs | eventlog directories ...>
    ```
    Note that this is the only mode that supports the _Auto-Tuner_ option described in more details
    in the [Auto-Tuner support](#auto-tuner-support) section.

 2. Combined Mode:
    Combined mode is collection mode but then combines all the applications 
    together and you get one file for all applications.
    
    ```bash
    Usage: java -cp rapids-4-spark-tools_2.12-<version>.jar:$SPARK_HOME/jars/*
           com.nvidia.spark.rapids.tool.profiling.ProfileMain --combined
           <eventlogs | eventlog directories ...>
    ```
 3. Compare Mode:
    Compare mode will combine all the applications information in the same tables into a single file 
    and also adds in tables to compare stages and sql ids across all of those applications.
    The Compare mode will use more memory if comparing lots of applications.
    
    ```bash
    Usage: java -cp rapids-4-spark-tools_2.12-<version>.jar:$SPARK_HOME/jars/*
           com.nvidia.spark.rapids.tool.profiling.ProfileMain --compare
           <eventlogs | eventlog directories ...>
    ```
    Note that if you are on an HDFS cluster the default filesystem is likely HDFS for both the input and output 
    so if you want to point to the local filesystem be sure to include `file:` in the path.
    
    Example running on files in HDFS: (include $HADOOP_CONF_DIR in classpath)
    
    ```bash
    java -cp ~/rapids-4-spark-tools_2.12-<version>.jar:$SPARK_HOME/jars/*:$HADOOP_CONF_DIR/ \
     com.nvidia.spark.rapids.tool.profiling.ProfileMain  /eventlogDir
    ```

Run `--help` for more information.

## Understanding Profiling tool detailed output and examples
The default output location is the current directory. 
The output location can be changed using the `--output-directory` option.
The output goes into a sub-directory named `rapids_4_spark_profile/` inside that output location.
- If running in normal collect mode, it processes event logs individually and outputs files for each application under
a directory named `rapids_4_spark_profile/{APPLICATION_ID}`. It creates a summary text file named `profile.log`.
- If running combine mode the output is put under a directory named `rapids_4_spark_profile/combined/` and creates a summary
text file named `rapids_4_spark_tools_combined.log`.
- If running compare mode the output is put under a directory named `rapids_4_spark_profile/compare/` and creates a summary
text file named `rapids_4_spark_tools_compare.log`.
The output will go into your default filesystem and the tool supports local filesystem or HDFS.

If you are on an HDFS cluster, then the default filesystem is likely HDFS for both the input and output
so if you want to point to the local filesystem be sure to include `file:` in the path.
There are separate files that are generated under the same sub-directory when using the options to generate query
visualizations or printing the SQL plans.
Optionally if the `--csv` option is specified then it creates a csv file for each table for each application in the
corresponding sub-directory.

Additional notes:
- There is a 100 characters limit for each output column. If the result of the column exceeds this limit, it is suffixed with
`...` for that column.
- ResourceProfile ids are parsed for the event logs that are from Spark 3.1 or later.  A ResourceProfile allows the user
to specify executor and task requirements for an RDD that will get applied during a stage.  This allows the user to change
the resource requirements between stages.

#### A. Collect Information or Compare Information(if more than 1 event logs are as input and option --compare is specified)
- Application information
- Application log path mapping
- Data Source information
- Executors information
- Job, stage and SQL ID information
- SQL to stage information
- Rapids related parameters
- Spark Properties
- Rapids Accelerator jar
- SQL Plan Metrics
- WholeStageCodeGen to node mappings (only applies to CPU plans)
- IO Metrics
- Compare Mode: Matching SQL IDs Across Applications
- Compare Mode: Matching Stage IDs Across Applications
- Optionally: SQL Plan for each SQL query
- Optionally: Generates DOT graphs for each SQL query
- Optionally: Generates timeline graph for application

For example, GPU run vs CPU run performance comparison or different runs with different parameters.

We can input multiple Spark event logs and this tool can compare environments, executors, Rapids related Spark parameters,

- Compare the durations/versions/gpuMode on or off:


- Application information
```
Application Information:

+--------+-----------+-----------------------+---------+-------------+-------------+--------+-----------+------------+-------------+
|appIndex|appName    |appId                  |sparkUser|startTime    |endTime      |duration|durationStr|sparkVersion|pluginEnabled|
+--------+-----------+-----------------------+---------+-------------+-------------+--------+-----------+------------+-------------+
|1       |Spark shell|app-20210329165943-0103|user1    |1617037182848|1617037490515|307667  |5.1 min    |3.0.1       |false        |
|2       |Spark shell|app-20210329170243-0018|user1    |1617037362324|1617038578035|1215711 |20 min     |3.0.1       |true         |
+--------+-----------+-----------------------+---------+-------------+-------------+--------+-----------+------------+-------------+
```

- Data Source information
The details of this output differ between using a Spark Data Source V1 and Data Source V2 reader. 
The Data Source V2 truncates the schema, so if you see `...`, then
the full schema is not available.

```
Data Source Information:
+--------+-----+-------+---------------------------------------------------------------------------------------------------------------------------+-----------------+---------------------------------------------------------------------------------------------+
|appIndex|sqlID|format |location                                                                                                                   |pushedFilters    |schema                                                                                       |
+--------+-----+-------+---------------------------------------------------------------------------------------------------------------------------+-----------------+---------------------------------------------------------------------------------------------+
|1       |0    |Text   |InMemoryFileIndex[file:/home/user1/workspace/spark-rapids-another/integration_tests/src/test/resources/trucks-comments.csv]|[]               |value:string                                                                                 |
|1       |1    |csv    |Location: InMemoryFileIndex[file:/home/user1/workspace/spark-rapids-another/integration_tests/src/test/re...               |PushedFilters: []|_c0:string                                                                                   |
|1       |2    |parquet|Location: InMemoryFileIndex[file:/home/user1/workspace/spark-rapids-another/lotscolumnsout]                                |PushedFilters: []|loan_id:bigint,monthly_reporting_period:string,servicer:string,interest_rate:double,curren...|
|1       |3    |parquet|Location: InMemoryFileIndex[file:/home/user1/workspace/spark-rapids-another/lotscolumnsout]                                |PushedFilters: []|loan_id:bigint,monthly_reporting_period:string,servicer:string,interest_rate:double,curren...|
|1       |4    |orc    |Location: InMemoryFileIndex[file:/home/user1/workspace/spark-rapids-another/logscolumsout.orc]                             |PushedFilters: []|loan_id:bigint,monthly_reporting_period:string,servicer:string,interest_rate:double,curren...|
|1       |5    |orc    |Location: InMemoryFileIndex[file:/home/user1/workspace/spark-rapids-another/logscolumsout.orc]                             |PushedFilters: []|loan_id:bigint,monthly_reporting_period:string,servicer:string,interest_rate:double,curren...|
|1       |6    |json   |Location: InMemoryFileIndex[file:/home/user1/workspace/spark-rapids-another/lotsofcolumnsout.json]                         |PushedFilters: []|adj_remaining_months_to_maturity:double,asset_recovery_costs:double,credit_enhancement_pro...|
|1       |7    |json   |Location: InMemoryFileIndex[file:/home/user1/workspace/spark-rapids-another/lotsofcolumnsout.json]                         |PushedFilters: []|adj_remaining_months_to_maturity:double,asset_recovery_costs:double,credit_enhancement_pro...|
|1       |8    |json   |Location: InMemoryFileIndex[file:/home/user1/workspace/spark-rapids-another/lotsofcolumnsout.json]                         |PushedFilters: []|adj_remaining_months_to_maturity:double,asset_recovery_costs:double,credit_enhancement_pro...|
|1       |9    |JDBC   |unknown                                                                                                                    |unknown          |                                                                                             |
+--------+-----+-------+---------------------------------------------------------------------------------------------------------------------------+-----------------+---------------------------------------------------------------------------------------------+
```

- Executor information:

```
Executor Information:
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

- SQL to Stage Information (sorted by stage duration)

Note that not all SQL nodes have a mapping to stage id so some nodes might be missing.

```
SQL to Stage Information:
+--------+-----+-----+-------+--------------+--------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|appIndex|sqlID|jobID|stageId|stageAttemptId|Stage Duration|SQL Nodes(IDs)                                                                                                                                                     |
+--------+-----+-----+-------+--------------+--------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|1       |0    |1    |1      |0             |8174          |Exchange(9),WholeStageCodegen (1)(10),Scan(13)                                                                                                                     |
|1       |0    |1    |2      |0             |8154          |Exchange(16),WholeStageCodegen (3)(17),Scan(20)                                                                                                                    |
|1       |0    |1    |3      |0             |2148          |Exchange(2),HashAggregate(4),SortMergeJoin(6),WholeStageCodegen (5)(3),Sort(8),WholeStageCodegen (2)(7),Exchange(9),Sort(15),WholeStageCodegen (4)(14),Exchange(16)|
|1       |0    |1    |4      |0             |126           |HashAggregate(1),WholeStageCodegen (6)(0),Exchange(2)                                                                                                              |
+--------+-----+-----+-------+--------------+--------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

- Compare Rapids related Spark properties side-by-side:

```
Compare Rapids Properties which are set explicitly:
+-------------------------------------------+----------+----------+
|propertyName                               |appIndex_1|appIndex_2|
+-------------------------------------------+----------+----------+
|spark.rapids.memory.pinnedPool.size        |null      |2g        |
|spark.rapids.sql.castFloatToDecimal.enabled|null      |true      |
|spark.rapids.sql.concurrentGpuTasks        |null      |2         |
|spark.rapids.sql.enabled                   |false     |true      |
|spark.rapids.sql.explain                   |null      |NOT_ON_GPU|
|spark.rapids.sql.incompatibleOps.enabled   |null      |true      |
+-------------------------------------------+----------+----------+
```
 
- List rapids-4-spark jars based on classpath:

```
Rapids Accelerator jar:
+--------+------------------------------------------------------------+
|appIndex|Rapids4Spark jars                                           |
+--------+------------------------------------------------------------+
|1       |spark://10.10.10.10:43445/jars/rapids-4-spark_2.12-0.5.0.jar|
|2       |spark://10.10.10.11:41319/jars/rapids-4-spark_2.12-0.5.0.jar|
+--------+------------------------------------------------------------+
```

- Job, stage and SQL ID information(not in `compare` mode yet):

```
Job Information:
+--------+-----+---------+-----+-------------+-------------+
|appIndex|jobID|stageIds |sqlID|startTime    |endTime      |
+--------+-----+---------+-----+-------------+-------------+
|1       |0    |[0]      |null |1622846402778|1622846410240|
|1       |1    |[1,2,3,4]|0    |1622846431114|1622846441591|
+--------+-----+---------+-----+-------------+-------------+
```

- SQL Plan Metrics for Application for each SQL plan node in each SQL:

These are also called accumulables in Spark.
Note that not all SQL nodes have a mapping to stage id.

```
SQL Plan Metrics for Application:
+--------+-----+------+-----------------------------------------------------------+-------------+-----------------------+-------------+----------+--------+
|appIndex|sqlID|nodeID|nodeName                                                   |accumulatorId|name                   |max_value    |metricType|stageIds|
+--------+-----+------+-----------------------------------------------------------+-------------+-----------------------+-------------+----------+--------+
|1       |0    |1     |GpuColumnarExchange                                        |111          |output rows            |1111111111   |sum       |4,3     |
|1       |0    |1     |GpuColumnarExchange                                        |112          |output columnar batches|222222       |sum       |4,3     |
|1       |0    |1     |GpuColumnarExchange                                        |113          |data size              |333333333333 |size      |4,3     |
|1       |0    |1     |GpuColumnarExchange                                        |114          |shuffle bytes written  |444444444444 |size      |4,3     | 
|1       |0    |1     |GpuColumnarExchange                                        |115          |shuffle records written|555555       |sum       |4,3     |
|1       |0    |1     |GpuColumnarExchange                                        |116          |shuffle write time     |666666666666 |nsTiming  |4,3     |
```

- WholeStageCodeGen to Node Mapping (only for CPU logs):

```
WholeStageCodeGen Mapping:
+--------+-----+------+---------------------+-------------------+------------+
|appIndex|sqlID|nodeID|SQL Node             |Child Node         |Child NodeID|
+--------+-----+------+---------------------+-------------------+------------+
|1       |0    |0     |WholeStageCodegen (6)|HashAggregate      |1           |
|1       |0    |3     |WholeStageCodegen (5)|HashAggregate      |4           |
|1       |0    |3     |WholeStageCodegen (5)|Project            |5           |
|1       |0    |3     |WholeStageCodegen (5)|SortMergeJoin      |6           |
|1       |0    |7     |WholeStageCodegen (2)|Sort               |8           |
```


#### B. Analysis
- Job + Stage level aggregated task metrics
- SQL level aggregated task metrics
- SQL duration, application during, if it contains Dataset or RDD operation, potential problems, executor CPU time percent
- Shuffle Skew Check: (When task's Shuffle Read Size > 3 * Avg Stage-level size)

Below we will aggregate the task level metrics at different levels 
to do some analysis such as detecting possible shuffle skew.

- Job + Stage level aggregated task metrics:

```
Job + Stage level aggregated task metrics:
+--------+-------+--------+--------+--------------------+------------+------------+------------+------------+-------------------+------------------------------+---------------------------+-------------------+-------------------+---------------------+-------------+----------------------+-----------------------+-------------------------+-----------------------+---------------------------+--------------+--------------------+-------------------------+---------------------+--------------------------+----------------------+----------------------------+---------------------+-------------------+---------------------+----------------+
|appIndex|ID     |numTasks|Duration|diskBytesSpilled_sum|duration_sum|duration_max|duration_min|duration_avg|executorCPUTime_sum|executorDeserializeCPUTime_sum|executorDeserializeTime_sum|executorRunTime_sum|input_bytesRead_sum|input_recordsRead_sum|jvmGCTime_sum|memoryBytesSpilled_sum|output_bytesWritten_sum|output_recordsWritten_sum|peakExecutionMemory_max|resultSerializationTime_sum|resultSize_max|sr_fetchWaitTime_sum|sr_localBlocksFetched_sum|sr_localBytesRead_sum|sr_remoteBlocksFetched_sum|sr_remoteBytesRead_sum|sr_remoteBytesReadToDisk_sum|sr_totalBytesRead_sum|sw_bytesWritten_sum|sw_recordsWritten_sum|sw_writeTime_sum|
+--------+-------+--------+--------+--------------------+------------+------------+------------+------------+-------------------+------------------------------+---------------------------+-------------------+-------------------+---------------------+-------------+----------------------+-----------------------+-------------------------+-----------------------+---------------------------+--------------+--------------------+-------------------------+---------------------+--------------------------+----------------------+----------------------------+---------------------+-------------------+---------------------+----------------+
|1       |job_0  |3333    |222222  |0                   |11111111    |111111      |111         |1111.1      |6666666            |55555                         |55555                      |55555555           |222222222222       |22222222222          |111111       |0                     |0                      |0                        |222222222              |1                          |11111         |11111               |99999                    |22222222222          |2222221                   |222222222222          |0                           |222222222222         |222222222222       |5555555              |444444          |
```

- SQL level aggregated task metrics:

```
SQL level aggregated task metrics:
+--------+------------------------------+-----+--------------------+--------+--------+---------------+---------------+----------------+--------------------+------------+------------+------------+------------+-------------------+------------------------------+---------------------------+-------------------+-------------------+---------------------+-------------+----------------------+-----------------------+-------------------------+-----------------------+---------------------------+--------------+--------------------+-------------------------+---------------------+--------------------------+----------------------+----------------------------+---------------------+-------------------+---------------------+----------------+
|appIndex|appID                         |sqlID|description         |numTasks|Duration|executorCPUTime|executorRunTime|executorCPURatio|diskBytesSpilled_sum|duration_sum|duration_max|duration_min|duration_avg|executorCPUTime_sum|executorDeserializeCPUTime_sum|executorDeserializeTime_sum|executorRunTime_sum|input_bytesRead_sum|input_recordsRead_sum|jvmGCTime_sum|memoryBytesSpilled_sum|output_bytesWritten_sum|output_recordsWritten_sum|peakExecutionMemory_max|resultSerializationTime_sum|resultSize_max|sr_fetchWaitTime_sum|sr_localBlocksFetched_sum|sr_localBytesRead_sum|sr_remoteBlocksFetched_sum|sr_remoteBytesRead_sum|sr_remoteBytesReadToDisk_sum|sr_totalBytesRead_sum|sw_bytesWritten_sum|sw_recordsWritten_sum|sw_writeTime_sum|
+--------+------------------------------+-----+--------------------+--------+--------+---------------+---------------+----------------+--------------------+------------+------------+------------+------------+-------------------+------------------------------+---------------------------+-------------------+-------------------+---------------------+-------------+----------------------+-----------------------+-------------------------+-----------------------+---------------------------+--------------+--------------------+-------------------------+---------------------+--------------------------+----------------------+----------------------------+---------------------+-------------------+---------------------+----------------+
|1       |application_1111111111111_0001|0    |show at <console>:11|1111    |222222  |6666666        |55555555       |55.55           |0                   |13333333    |111111      |999         |3333.3      |6666666            |55555                         |66666                      |11111111           |111111111111       |11111111111          |111111       |0                     |0                      |0                        |888888888              |8                          |11111         |11111               |99999                    |11111111111          |2222222                   |222222222222          |0                           |222222222222         |444444444444       |5555555              |444444          |
```

- SQL duration, application during, if it contains Dataset or RDD operation, potential problems, executor CPU time percent:

```
SQL Duration and Executor CPU Time Percent
+--------+-------------------+-----+------------+--------------------------+------------+---------------------------+-------------------------+
|appIndex|App ID             |sqlID|SQL Duration|Contains Dataset or RDD Op|App Duration|Potential Problems         |Executor CPU Time Percent|
+--------+-------------------+-----+------------+--------------------------+------------+---------------------------+-------------------------+
|1       |local-1626104300434|0    |1260        |false                     |131104      |NESTED COMPLEX TYPE        |92.65                    |
|1       |local-1626104300434|1    |259         |false                     |131104      |NESTED COMPLEX TYPE        |76.79                    |
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
- Removed BlockManagers and Executors
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

#### D. Recommended Configuration

The _Auto-Tuner_ output has 2 main sections:
1. _Spark Properties_: A list of Apache Spark configurations to tune the performance of the app.
   The list is the result of `diff` between the existing app configurations and the recommended
   ones. Therefore, a recommendation matches the existing app configuration, it will not show up in
   the list.
2. _Comments_: A list of messages to highlight properties that were missing in the app
   configurations, or the cause of failure to generate the recommendations.

**Examples**

- A succesful run with missing _softwareProperties_:
   ```
   Spark Properties:
   --conf spark.executor.cores=16
   --conf spark.executor.instances=8
   --conf spark.executor.memory=32768m
   --conf spark.executor.memoryOverhead=7372m
   --conf spark.rapids.memory.pinnedPool.size=4096m
   --conf spark.rapids.sql.concurrentGpuTasks=2
   --conf spark.sql.files.maxPartitionBytes=512m
   --conf spark.sql.shuffle.partitions=200
   --conf spark.task.resource.gpu.amount=0.0625

   Comments:
   - 'spark.executor.instances' was not set.
   - 'spark.executor.cores' was not set.
   - 'spark.task.resource.gpu.amount' was not set.
   - 'spark.rapids.sql.concurrentGpuTasks' was not set.
   - 'spark.executor.memory' was not set.
   - 'spark.rapids.memory.pinnedPool.size' was not set.
   - 'spark.executor.memoryOverhead' was not set.
   - 'spark.sql.files.maxPartitionBytes' was not set.
   - 'spark.sql.shuffle.partitions' was not set.
   - 'spark.sql.adaptive.enabled' should be enabled for better performance.
   ```

- A succesful run with defined _softwareProperties_. In this example, only
  two recommendations did not match the existing app app configurations:
  ```
  Spark Properties:
  --conf spark.executor.instances=8
  --conf spark.sql.shuffle.partitions=200

  Comments:
  - 'spark.sql.shuffle.partitions' was not set.
  ```

- Failing to load the worker info:
  ```
  Cannot recommend properties. See Comments.

  Comments:
  - java.io.FileNotFoundException: File worker-info.yaml does not exist
  - 'spark.executor.memory' should be set to at least 2GB/core.
  - 'spark.executor.instances' should be set to (gpuCount * numWorkers).
  - 'spark.task.resource.gpu.amount' should be set to Max(1, (numCores / gpuCount)).
  - 'spark.rapids.sql.concurrentGpuTasks' should be set to Max(4, (gpuMemory / 8G)).
  - 'spark.rapids.memory.pinnedPool.size' should be set to 2048m.
  - 'spark.sql.adaptive.enabled' should be enabled for better performance.
  ```

### Generating Visualizations

- Print SQL Plans (--print-plans option):
Prints the SQL plan as a text string to a file named `planDescriptions.log`.

- Generate DOT graph for each SQL (--generate-dot option):

```
Generated DOT graphs for app app-20210507103057-0000 to /path/. in 17 second(s)
```

A dot file will be generated for each query in the application.
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
If a stage has no metrics, like if the query crashed early, we cannot establish that link.

- Generate timeline for application (--generate-timeline option):

The output of this is an [svg](https://en.wikipedia.org/wiki/Scalable_Vector_Graphics) file
named `timeline.svg`.  Most web browsers can display this file.  It is a
timeline view similar to Apache Spark's
[event timeline](https://spark.apache.org/docs/latest/web-ui.html).

This displays several data sections.

1. **Tasks** This shows all tasks in the application divided by executor. Please note that this
   tries to pack the tasks in the graph. It does not represent actual scheduling on CPU cores.
   The tasks are labeled with the time it took for them to run. There is a breakdown of some metrics
   per task in the lower half of the task block with different colors used to designate different
   metrics.
   1. Yellow is the deserialization time for the task as reported by Spark. This works for both CPU
   and GPU tasks.
   2. White is the read time for a task. This is a combination of the "buffer time" GPU SQL metric
   and the shuffle read time as reported by Spark. The shuffle time works for both CPU and GPU
   tasks, but "buffer time" only is reported for GPU accelerated file reads.
   3. Red is the semaphore wait time. This is the amount of time a task spent waiting to get access
   to the GPU. When processing logs generated by versions of the spark rapids plugin prior to
   23.04 this would only show up on GPU tasks when DEBUG metrics are enabled. For logs generated
   with 23.04 and above it is always on. It does not apply to CPU tasks, as they don't go through
   the Semaphore.
   4. Green is the "op time" SQL metric along with a few other metrics that also indicate the amount
   of time the GPU was being used to process data. This is GPU specific.
   5. Blue is the write time for a task. This is the "write time" SQL metric used when writing out
   results as files using GPU acceleration, or it is the shuffle write time as reported by Spark.
   The shuffle metrics work for both CPU and GPU tasks, but the "write time" metrics is GPU specific.
   6. Anything else is time that is not accounted for by these metrics. Typically, this is time
   spent on the CPU, but could also include semaphore wait time as DEBUG metrics are not on by
   default.
2. **STAGES** This shows the stages times reported by Spark. It starts with when the stage was
   scheduled and ends when Spark considered the stage done.
3. **STAGE RANGES** This shows the time from the start of the first task to the end of the last
   task. Often a stage is scheduled, but there are not enough resources in the cluster to run it.
   This helps to show. How long it takes for a task to start running after it is scheduled, and in
   many cases how long it took to run all of the tasks in the stage. This is not always true because
   Spark can intermix tasks from different stages.
4. **JOBS** This shows the time range reported by Spark from when a job was scheduled to when it
   completed.
5. **SQL** This shows the time range reported by Spark from when a SQL statement was scheduled to
   when it completed.

Tasks and stages all are color coordinated to help know what tasks are associated with a given
stage. Jobs and SQL are not color coordinated.

## Profiling tool options
  
```
Profiling tool for the RAPIDS Accelerator and Apache Spark

Usage: java -cp rapids-4-spark-tools_2.12-<version>.jar:$SPARK_HOME/jars/*
       com.nvidia.spark.rapids.tool.profiling.ProfileMain [options]
       <eventlogs | eventlog directories ...>

  -a, --auto-tuner                Toggle AutoTuner module.
      --combined                  Collect mode but combine all applications into
                                  the same tables.
  -c, --compare                   Compare Applications (Note this may require
                                  more memory if comparing a large number of
                                  applications). Default is false.
      --csv                       Output each table to a CSV file as well
                                  creating the summary text file.
  -f, --filter-criteria  <arg>    Filter newest or oldest N eventlogs based on
                                  application start timestamp for processing.
                                  Filesystem based filtering happens before
                                  application based filtering (see start-app-time).
                                  eg: 100-newest-filesystem (for processing newest
                                  100 event logs). eg: 100-oldest-filesystem (for
                                  processing oldest 100 event logs).
  -g, --generate-dot              Generate query visualizations in DOT format.
                                  Default is false
      --generate-timeline         Write an SVG graph out for the full
                                  application timeline.
  -m, --match-event-logs  <arg>   Filter event logs whose filenames contain the
                                  input string
  -n, --num-output-rows  <arg>    Number of output rows for each Application.
                                  Default is 1000
      --num-threads  <arg>        Number of thread to use for parallel
                                  processing. The default is the number of cores
                                  on host divided by 4.
  -o, --output-directory  <arg>   Base output directory. Default is current
                                  directory for the default filesystem. The
                                  final output will go into a subdirectory
                                  called rapids_4_spark_profile. It will
                                  overwrite any existing files with the same
                                  name.
  -p, --print-plans               Print the SQL plans to a file named
                                  'planDescriptions.log'.
                                  Default is false.
  -s, --start-app-time  <arg>     Filter event logs whose application start
                                  occurred within the past specified time
                                  period. Valid time periods are
                                  min(minute),h(hours),d(days),w(weeks),m(months).
                                  If a period is not specified it defaults to
                                  days.
  -t, --timeout  <arg>            Maximum time in seconds to wait for the event
                                  logs to be processed. Default is 24 hours
                                  (86400 seconds) and must be greater than 3
                                  seconds. If it times out, it will report what
                                  it was able to process up until the timeout.
  -w, --worker-info  <arg>        File path containing the system information of
                                  a worker node. It is assumed that all workers
                                  are homogenous. It requires the AutoTuner to
                                  be enabled. Default is ./worker_info.yaml
  -h, --help                      Show help message

 trailing arguments:
  eventlog (required)   Event log filenames(space separated) or directories
                        containing event logs. eg: s3a://<BUCKET>/eventlog1
                        /path/to/eventlog2
```

### Auto-Tuner support

Starting with release _22.10_, the Profiling tool a new _Auto-Tuner_ that aims at optimizing
Apache Spark applications by recommending a set of configurations to tune the performance of
Rapids accelerator.  

Currently, the _Auto-Tuner_ calculates a set of configurations that impact the performance of Apache
Spark apps executing on GPU. Those calculations can leverage cluster information
(e.g. memory, cores, Spark default configurations) as well as information processed in the
application event logs.  Note that the tool also will recommend settings for the application assuming
that the job will be able to use all the cluster resources (CPU and GPU) when it is running.

The values loaded from the app logs have higher precedence than the default configs.  
Please refer to [Understanding the Profiling tool output](#d-recommended-configuration) for
more details on the output of the _Auto-Tuner_.

Note the following _Auto-Tuner_ limitations:
- It is currently only supported in the _Collection Mode_ (see [the 3 different modes](#step-2-how-to-run-the-profiling-tool)), and
- It is assumed that all the _worker_ nodes on the cluster are homogenous.

To run the _Auto-Tuner_, enable the `auto-tuner` flag and pass a valid `--worker-info <FILE_PATH>`.
The _Auto-Tuner_ needs to learn the system properties of the _worker_ nodes that run application
code in the cluster. The argument `FILE_PATH` can either be local or remote file (i.e., HDFS).   
A template of the worker information is shown below:

  ```
  system:
    numCores: 32
    memory: 212992MiB
    numWorkers: 5
  gpu:
    memory: 15109MiB
    count: 4
    name: T4
  softwareProperties:
    spark.driver.maxResultSize: 7680m
    spark.driver.memory: 15360m
    spark.executor.cores: '8'
    spark.executor.instances: '2'
    spark.executor.memory: 47222m
    spark.executorEnv.OPENBLAS_NUM_THREADS: '1'
    spark.scheduler.mode: FAIR
    spark.sql.cbo.enabled: 'true'
    spark.ui.port: '0'
    spark.yarn.am.memory: 640m
  ```


| Property           | Optional |                                                          If Missing                                                          |
|--------------------|:--------:|:----------------------------------------------------------------------------------------------------------------------------:|
| system.numCores    |    No    | _Auto-Tuner_ does not calculate recommendations                                                                              |
| system.memory      |    No    | _Auto-Tuner_ does not calculate any recommendations                                                                          |
| system.numWorkers  |    Yes   | Default: 1                                                                                                                   |
| gpu.name           |    Yes   | Default: T4 (Nvidia Tesla T4)                                                                                                |
| gpu.memory         |    Yes   | Default: 16G                                                                                                                 |
| softwareProperties |    Yes   | This section is optional. The _Auto-Tuner_ reads the configs within the logs of the Apache Spark apps with higher precedence |

## Profiling tool metrics definitions

All the metrics definitions can be found in the 
[executor task metrics doc](https://spark.apache.org/docs/latest/monitoring.html#executor-task-metrics) / 
[executor metrics doc](https://spark.apache.org/docs/latest/monitoring.html#executor-metrics) or 
the [SPARK webUI doc](https://spark.apache.org/docs/latest/web-ui.html#content).
