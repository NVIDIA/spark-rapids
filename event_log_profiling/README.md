# Spark event log profiling tool

This is a profiling tool to parse and analyze Spark event logs. 

(The code is based on Apache Spark 3.1.1 source code, and tested using Spark 3.0.x and 3.1.1 event logs already)

## How to compile and use with Spark
1. `mvn clean package`
2. Copy event_log_profiling_2.12-0.4.1.jar to $SPARK_HOME/jars/

`cp target/event_log_profiling_2.12-0.4.1.jar $SPARK_HOME/jars/`

3. Add below entries in $SPARK_HOME/conf/log4j.properties
```
log4j.logger.org.apache.spark.sql.rapids.tool.profiling=INFO, fileAppender
log4j.appender.fileAppender=org.apache.log4j.RollingFileAppender
log4j.appender.fileAppender.layout=org.apache.log4j.PatternLayout
log4j.appender.fileAppender.layout.ConversionPattern=[%t] %-5p %c %x - %m%n
log4j.appender.fileAppender.File=/tmp/event_log_profiling.log
```

4.  In spark-shell:
For a single event log analysis:
```
org.apache.spark.sql.rapids.tool.profiling.ProfileMain.main(Array("/path/to/eventlog1"))
```

For multiple event logs comparison and analysis:
```
org.apache.spark.sql.rapids.tool.profiling.ProfileMain.main(Array("/path/to/eventlog1", "/path/to/eventlog2"))
```

## How to compile and use from command-line
1. `mvn clean package -Pstandalone`
2. `java -jar target/event_log_profiling_2.12-0.4.1-jar-with-dependencies.jar /path/to/eventlog1 /path/to/eventlog2`

## Options
```
$ java -jar target/event_log_profiling_2.12-0.4.1-jar-with-dependencies.jar --help

Spark event log profiling tool

Example:

# Input 1 or more event logs from local path:
java -jar event_log_profiling_2.12-<version>-jar-with-dependencies.jar /path/to/eventlog1 /path/to/eventlog2

# If any event log is from S3:
export AWS_ACCESS_KEY_ID=xxx
export AWS_SECRET_ACCESS_KEY=xxx
java -jar event_log_profiling_2.12-<version>-jar-with-dependencies.jar s3a://<BUCKET>/eventlog1 /path/to/eventlog2

# Generate query visualizations in DOT format:
java -jar event_log_profiling_2.12-<version>-jar-with-dependencies.jar -g /path/to/eventlog1 /path/to/eventlog2

# Change output directory to /tmp
java -jar event_log_profiling_2.12-<version>-jar-with-dependencies.jar -o /tmp /path/to/eventlog1


For usage see below:

  -g, --generate-dot              Generate query visualizations in DOT format.
                                  Default is false
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
- Print rapids-4-spark and cuDF jars based on classpath
- Print SQL Plan Metrics(Accumulables) and optionally generate the dot files for each SQL(`-g` option).

To convert the dot file to PDF format with graphs, please install [graphviz](http://www.graphviz.org/documentation/) and run:

`dot -Tpdf myfile.dot > myfile.pdf`

### B. Healthcheck
- Check Spark Properties (currently only 1 check is implemented : spark.executor.resource.gpu.amount=1)
- List all failed tasks, stages and jobs.
- List removed Block Managers
- List removed Executors
- List *possible unsupported SQL Plan* 

(Currently only GpuColumnarToRow,GpuRowToColumnar, $Lambda$(Dataset API or UDF) will be highlighted.)

### C. Analysis
- SQL/Job/Stage level task-metrics aggregation
- ShuffleSkewCheck based on (When task's Shuffle Read Size > 3 * Avg Stage-level size)


## Design Doc
This tool parsed and analyzed most of the Spark Listeners – 21 types.

And then materialize them as cached Spark DataFrames.

All the analysis and healthcheck functions are done using Spark-SQL.

- SparkListenerLogStart
- SparkListenerResourceProfileAdded
- SparkListenerBlockManagerAdded
- SparkListenerBlockManagerRemoved
- SparkListenerEnvironmentUpdate
- SparkListenerTaskStart
- SparkListenerApplicationStart
- SparkListenerExecutorAdded
- SparkListenerExecutorRemoved
- SparkListenerSQLExecutionStart
- SparkListenerSQLExecutionEnd
- SparkListenerDriverAccumUpdates
- SparkListenerJobStart
- SparkListenerStageSubmitted
- SparkListenerTaskEnd
- SparkListenerStageCompleted
- SparkListenerJobEnd
- SparkListenerTaskGettingResult
- SparkListenerApplicationEnd
- SparkListenerSQLAdaptiveExecutionUpdate
- SparkListenerSQLAdaptiveSQLMetricUpdates

![Data Model](profiling_tool_data_model.jpg)

### Use cases showcase
**1. Shuffle Read Skew Detection**

This can help detect the Join Skew or Aggregation Skew which used to happen on Baidu queries.
```
[main] INFO  org.apache.spark.sql.rapids.tool.profiling.ProfileMain$  - Application application_xxx (index=1) Shuffle Skew Check:
[main] INFO  org.apache.spark.sql.rapids.tool.profiling.ProfileMain$  - (When task's Shuffle Read Size > 3 * Avg Stage-level size)
[main] INFO  org.apache.spark.sql.rapids.tool.profiling.ProfileMain$  -
+--------+-------+--------------+------+-------+---------------+--------------+-----------------+----------------+----------------+----------+----------------------------------------------------------------------------------------------------+
|appIndex|stageId|stageAttemptId|taskId|attempt|taskDurationSec|avgDurationSec|taskShuffleReadMB|avgShuffleReadMB|taskPeakMemoryMB|successful|endReason_first100char                                                                              |
+--------+-------+--------------+------+-------+---------------+--------------+-----------------+----------------+----------------+----------+----------------------------------------------------------------------------------------------------+
|1       |2      |0             |1590  |0      |8.83           |6.86          |1143.68          |228.44          |32.03           |true      |Success                                                                                             |
|1       |2      |3             |2995  |2      |5.09           |2.69          |794.83           |261.74          |0.03            |false     |TaskKilled(Stage cancelled,List(AccumulableInfo(xxx,None,Some(xxxx),None,false,true,None), Accumulab|
|1       |3      |0             |1830  |0      |15.19          |5.73          |1143.68          |231.99          |32.03           |true      |Success                                                                                             |
|1       |3      |1             |2252  |0      |6.19           |2.8           |1143.68          |321.77          |32.03           |true      |Success                                                                                             |
|1       |3      |1             |2269  |1      |5.03           |2.8           |1143.68          |321.77          |0.03            |false     |TaskKilled(another attempt succeeded,List(AccumulableInfo(xxx,None,Some(xxxx),None,false,true,None),|
|1       |3      |2             |2531  |1      |18.11          |2.49          |712.27           |234.69          |0.0             |false     |ExceptionFailure(java.lang.OutOfMemoryError,Could not allocate native memory: std::bad_alloc: RMM fa|
|1       |3      |3             |2958  |0      |6.77           |2.81          |1143.68          |251.83          |32.03           |true      |Success                                                                                             |
+--------+-------+--------------+------+-------+---------------+--------------+-----------------+----------------+----------------+----------+----------------------------------------------------------------------------------------------------+
```

**2. Failed Executors Check**

Azure reported NDS Q9 GPU run performance issue when comparing to CPU run.

This tool is used to list the failed executors/block managers which was the root cause.
```
[main] INFO  org.apache.spark.sql.rapids.tool.profiling.ProfileMain$  - Application app-xxx (index=2) removed Executors(s):
[main] INFO  org.apache.spark.sql.rapids.tool.profiling.ProfileMain$  -
+----------+-------------+----------------------------------------------------------------------------------------------------+
|executorID|time         |reason_first100char                                                                                 |
+----------+-------------+----------------------------------------------------------------------------------------------------+
|2         |1617037387190|Remote RPC client disassociated. Likely due to containers exceeding thresholds, or network issues. C|
|7         |1617037412432|Remote RPC client disassociated. Likely due to containers exceeding thresholds, or network issues. C|
```

**3. Possible Unsupported operators such as UDF/Dataset API**

```
[main] INFO  org.apache.spark.sql.rapids.tool.profiling.ProfileMain$  - SQL Plan HealthCheck:
[main] INFO  org.apache.spark.sql.rapids.tool.profiling.ProfileMain$  - Application app-20210422144630-0000 (index=1)
[main] INFO  org.apache.spark.sql.rapids.tool.profiling.ProfileMain$  - sqlID=1, NodeID=0, NodeName=GpuColumnarToRow,NodeDescription=GpuColumnarToRow false
[main] INFO  org.apache.spark.sql.rapids.tool.profiling.ProfileMain$  - sqlID=1, NodeID=6, NodeName=GpuRowToColumnar,NodeDescription=GpuRowToColumnar TargetSize(2147483647)
[main] INFO  org.apache.spark.sql.rapids.tool.profiling.ProfileMain$  - sqlID=1, NodeID=8, NodeName=Filter,NodeDescription=Filter $line21.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$Lambda$4578/0x00000008019f1840@4b63e04c.apply
[main] INFO  org.apache.spark.sql.rapids.tool.profiling.ProfileMain$  - sqlID=1, NodeID=9, NodeName=GpuColumnarToRow,NodeDescription=GpuColumnarToRow false
[main] INFO  org.apache.spark.sql.rapids.tool.profiling.ProfileMain$  - sqlID=0, NodeID=0, NodeName=GpuColumnarToRow,NodeDescription=GpuColumnarToRow false
```

**4. GPU run vs CPU run performance comparison or different runs with different parameters**

We can input multiple Spark event logs and this tool can compare enviroments, executors, Rapids related Spark paraneters,

and most importantly, ALL the 26 aggregarated task metrics side by side for Stage/Job/SQL levels.

- Compare the durations/versions/gpuMode on or off:
```
[main] INFO  org.apache.spark.sql.rapids.tool.profiling.ProfileMain$  - ### A. Compare Information Collected ###
[main] INFO  org.apache.spark.sql.rapids.tool.profiling.ProfileMain$  - Compare Application Information:
[main] INFO  org.apache.spark.sql.rapids.tool.profiling.ProfileMain$  -
+--------+-----------------------+-------------+-------------+--------+-----------+------------+-------+
|appIndex|appId                  |startTime    |endTime      |duration|durationStr|sparkVersion|gpuMode|
+--------+-----------------------+-------------+-------------+--------+-----------+------------+-------+
|1       |app-20210329165943-0103|1617037182848|1617037490515|307667  |5.1 min    |3.0.1       |false  |
|2       |app-20210329170243-0018|1617037362324|1617038578035|1215711 |20 min     |3.0.1       |true   |
+--------+-----------------------+-------------+-------------+--------+-----------+------------+-------+
```


- Compare Executor informations:
```
[main] INFO  org.apache.spark.sql.rapids.tool.profiling.ProfileMain$  - Compare Executor Information:
[main] INFO  org.apache.spark.sql.rapids.tool.profiling.ProfileMain$  -
+--------+----------+----------+-----------+------------+-------------+--------+--------+--------+------------+--------+--------+
|appIndex|executorID|totalCores|maxMem     |maxOnHeapMem|maxOffHeapMem|exec_cpu|exec_mem|exec_gpu|exec_offheap|task_cpu|task_gpu|
+--------+----------+----------+-----------+------------+-------------+--------+--------+--------+------------+--------+--------+
|1       |0         |4         |13984648396|13984648396 |0            |null    |null    |null    |null        |null    |null    |
|1       |1         |4         |13984648396|13984648396 |0            |null    |null    |null    |null        |null    |null    |
```


- Compare Rapids related Spark properties side-by-side:
```
[main] INFO  org.apache.spark.sql.rapids.tool.profiling.ProfileMain$  - Compare Rapids Properties which are set explicitly:
[main] INFO  org.apache.spark.sql.rapids.tool.profiling.ProfileMain$  -
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


- Calculate the internal accumulables for SQL Plan:
```
$  - SQL Plan Metrics for Application app-20210329165943-0103 (index=1)
[main] INFO  org.apache.spark.sql.rapids.tool.profiling.ProfileMain$  -
+-----+------+---------------------+-------------+-------------------------+------------+----------+
|sqlID|nodeID|nodeName             |accumulatorId|name                     |max_value   |metricType|
+-----+------+---------------------+-------------+-------------------------+------------+----------+
|24   |1     |WholeStageCodegen (1)|1247         |duration                 |70          |timing    |
|24   |3     |Filter               |1248         |number of output rows    |1           |sum       |
|24   |4     |ColumnarToRow        |1249         |number of output rows    |65          |sum       |
|24   |4     |ColumnarToRow        |1250         |number of input batches  |1           |sum       |
|24   |5     |Scan parquet         |941          |number of output rows    |65          |sum       |
|24   |5     |Scan parquet         |942          |number of files read     |1           |sum       |
|24   |5     |Scan parquet         |943          |metadata time            |0           |timing    |
|24   |5     |Scan parquet         |944          |size of files read       |2226        |size      |
|24   |5     |Scan parquet         |946          |scan time                |70          |timing    |
```


- SQL Level task-metrics comparison side-by-side for each SQL in each event log:

(This feature can replace the use of Spark-Measures since Spark-Measures needs to add a jar at runtime,
This tool only needs the event logs as input.)

Note: In metric name, sr= shuffle read, sw=shuffle write.
```
[main] INFO  org.apache.spark.sql.rapids.tool.profiling.ProfileMain$  - SQL level aggregated task metrics:
[main] INFO  org.apache.spark.sql.rapids.tool.profiling.ProfileMain$  -
+--------+------+--------+--------+--------------------+------------+------------+------------+------------+-------------------+------------------------------+---------------------------+-------------------+---------------------+-------------------+---------------------+-------------+----------------------+-----------------------+-------------------------+-----------------------+---------------------------+--------------+--------------------+-------------------------+---------------------+--------------------------+----------------------+----------------------------+---------------------+-------------------+---------------------+----------------+
|appIndex|ID    |numTasks|Duration|diskBytesSpilled_sum|duration_sum|duration_max|duration_min|duration_avg|executorCPUTime_sum|executorDeserializeCPUTime_sum|executorDeserializeTime_sum|executorRunTime_sum|gettingResultTime_sum|input_bytesRead_sum|input_recordsRead_sum|jvmGCTime_sum|memoryBytesSpilled_sum|output_bytesWritten_sum|output_recordsWritten_sum|peakExecutionMemory_max|resultSerializationTime_sum|resultSize_max|sr_fetchWaitTime_sum|sr_localBlocksFetched_sum|sr_localBytesRead_sum|sr_remoteBlocksFetched_sum|sr_remoteBytesRead_sum|sr_remoteBytesReadToDisk_sum|sr_totalBytesRead_sum|sw_bytesWritten_sum|sw_recordsWritten_sum|sw_writeTime_sum|
+--------+------+--------+--------+--------------------+------------+------------+------------+------------+-------------------+------------------------------+---------------------------+-------------------+---------------------+-------------------+---------------------+-------------+----------------------+-----------------------+-------------------------+-----------------------+---------------------------+--------------+--------------------+-------------------------+---------------------+--------------------------+----------------------+----------------------------+---------------------+-------------------+---------------------+----------------+
|1       |sql_24|2461    |140531  |0                   |7720969     |6659        |96          |3137.3      |4052816            |6148                          |16843                      |7691897            |0                    |133540369681       |43199498900          |39974        |0                     |0                      |0                        |0                      |10                         |2691          |1150                |193                      |12492                |2252                      |146426                |0                           |158918               |158918             |2445                 |246             |
|1       |sql_25|2461    |137806  |0                   |7627813     |7585        |81          |3099.5      |3852192            |3100                          |5334                       |7611574            |0                    |133540369681       |43199498900          |24291        |0                     |0                      |0                        |0                      |3                          |2648          |819                 |170                      |11065                |2275                      |147853                |0                           |158918               |158918             |2445                 |78              |
```



