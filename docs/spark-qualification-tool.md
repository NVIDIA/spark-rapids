---
layout: page
title: Spark Qualification tool
nav_order: 8
---

# Spark Qualification tool

The Qualification tool analyzes Spark events generated from CPU based Spark applications to determine 
if the RAPIDS Accelerator for Apache Spark might be a good fit for GPU acceleration.

This tool is intended to give the users a starting point and does not guarantee the
applications with the highest scores will actually be accelerated the most. Currently,
it reports by looking at the amount of time spent in tasks of SQL Dataframe operations.
This document covers below topics:

* TOC
{:toc}

## How to use the Qualification tool

The Qualification tool can be run in two different ways. One is to run it as a standalone tool on the
Spark event logs after the application(s) have run and other is to be integrated into a running Spark
application.

## Running the Qualification tool standalone on Spark event logs

### Prerequisites
- Java 8 or above, Spark 3.0.1+ jars.
- Spark event log(s) from Spark 2.0 or above version. Supports both rolled and compressed event logs 
  with `.lz4`, `.lzf`, `.snappy` and `.zstd` suffixes as well as Databricks-specific rolled and compressed(.gz) event logs.
- The tool does not support nested directories.
  Event log files or event log directories should be at the top level when specifying a directory.

Note: Spark event logs can be downloaded from Spark UI using a "Download" button on the right side,
or can be found in the location specified by `spark.eventLog.dir`. See the
[Apache Spark Monitoring](http://spark.apache.org/docs/latest/monitoring.html) documentation for
more information.

### Step 1 Download the tools jar and Apache Spark 3 Distribution
The Qualification tools require the Spark 3.x jars to be able to run but do not need an Apache Spark run time. 
If you do not already have Spark 3.x installed, you can download the Spark distribution to 
any machine and include the jars in the classpath.
- Download the jar file from [Maven repository](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark-tools_2.12/22.04.0/)
- [Download Apache Spark 3.x](http://spark.apache.org/downloads.html) - Spark 3.1.1 for Apache Hadoop is recommended

### Step 2 Run the Qualification tool
1. Event logs stored on a local machine:
    - Extract the Spark distribution into a local directory if necessary.
    - Either set SPARK_HOME to point to that directory or just put the path inside of the classpath
       `java -cp toolsJar:pathToSparkJars/*:...` when you run the Qualification tool.

    This tool parses the Spark CPU event log(s) and creates an output report. Acceptable inputs are either individual or 
    multiple event logs files or directories containing spark event logs in the local filesystem, HDFS, S3 or mixed.
    
    ```bash
    Usage: java -cp rapids-4-spark-tools_2.12-<version>.jar:$SPARK_HOME/jars/*
           com.nvidia.spark.rapids.tool.qualification.QualificationMain [options]
           <eventlogs | eventlog directories ...>
    ```

    ```bash
    Sample: java -cp rapids-4-spark-tools_2.12-<version>.jar:$SPARK_HOME/jars/*
           com.nvidia.spark.rapids.tool.qualification.QualificationMain /usr/logs/app-name1
    ```

2. Event logs stored on an on-premises HDFS cluster:

    Example running on files in HDFS: (include $HADOOP_CONF_DIR in classpath)
    
    ```bash
    Usage: java -cp ~/rapids-4-spark-tools_2.12-<version>.jar:$SPARK_HOME/jars/*:$HADOOP_CONF_DIR/ \
     com.nvidia.spark.rapids.tool.qualification.QualificationMain  /eventlogDir
    ```
    
    Note, on an HDFS cluster, the default filesystem is likely HDFS for both the input and output
    so if you want to point to the local filesystem be sure to include file: in the path.

### Qualification tool options
  Note: `--help` should be before the trailing event logs.

```bash
java -cp ~/rapids-4-spark-tools_2.12-<version>.jar:$SPARK_HOME/jars/*:$HADOOP_CONF_DIR/ \
 com.nvidia.spark.rapids.tool.qualification.QualificationMain --help

RAPIDS Accelerator for Apache Spark Qualification tool

Usage: java -cp rapids-4-spark-tools_2.12-<version>.jar:$SPARK_HOME/jars/*
       com.nvidia.spark.rapids.tool.qualification.QualificationMain [options]
       <eventlogs | eventlog directories ...>

  -a, --application-name  <arg>     Filter event logs by application name. The
                                    string specified can be a regular expression,
                                    substring, or exact match. For filtering based
                                    on complement of application name,
                                    use ~APPLICATION_NAME. i.e Select all event
                                    logs except the ones which have application
                                    name as the input string.
      --any                         Apply multiple event log filtering criteria and process
                                    only logs for which any condition is satisfied.
                                    Example: <Filter1> <Filter2> <Filter3> --any -> result
                                    is <Filter1> OR <Filter2> OR <Filter3>
      --all                         Apply multiple event log filtering criteria and process
                                    only logs for which all conditions are satisfied.
                                    Example: <Filter1> <Filter2> <Filter3> --all -> result
                                    is <Filter1> AND <Filter2> AND <Filter3>. Default is all=true.
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
      --spark-property <arg>        Filter applications based on certain Spark properties that were
                                    set during launch of the application. It can filter based
                                    on key:value pair or just based on keys. Multiple configs
                                    can be provided where the filtering is done if any of the
                                    config is present in the eventlog.
                                    filter on specific configuration(key:value):
                                    --spark-property=spark.eventLog.enabled:true
                                    filter all eventlogs which has config(key):
                                    --spark-property=spark.driver.port
                                    Multiple configs:
                                    --spark-property=spark.eventLog.enabled:true
                                    --spark-property=spark.driver.port.
  -t, --timeout  <arg>              Maximum time in seconds to wait for the
                                    event logs to be processed. Default is 24
                                    hours (86400 seconds) and must be greater
                                    than 3 seconds. If it times out, it will
                                    report what it was able to process up until
                                    the timeout.
  -u, --user-name <arg>             Applications which a particular user has submitted.
  -h, --help                        Show help message

 trailing arguments:
  eventlog (required)   Event log filenames(space separated) or directories
                        containing event logs. eg: s3a://<BUCKET>/eventlog1
                        /path/to/eventlog2
```

Example commands:
- Process the 10 newest logs, and only output the top 3 in the output:

```bash
java -cp ~/rapids-4-spark-tools_2.12-<version>.jar:$SPARK_HOME/jars/*:$HADOOP_CONF_DIR/ \
 com.nvidia.spark.rapids.tool.qualification.QualificationMain -f 10-newest -n 3 /eventlogDir
```

- Process last 100 days' logs:

```bash
java -cp ~/rapids-4-spark-tools_2.12-<version>.jar:$SPARK_HOME/jars/*:$HADOOP_CONF_DIR/ \
 com.nvidia.spark.rapids.tool.qualification.QualificationMain -s 100d /eventlogDir
```

- Process only the newest log with the same application name: 

```bash
java -cp ~/rapids-4-spark-tools_2.12-<version>.jar:$SPARK_HOME/jars/*:$HADOOP_CONF_DIR/ \
 com.nvidia.spark.rapids.tool.qualification.QualificationMain -f 1-newest-per-app-name /eventlogDir
```

Note: The “regular expression” used by -a option is based on
[java.util.regex.Pattern](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html).

### The Qualification tool Output
After the above command is executed, the summary report goes to STDOUT and by default it outputs 2 files 
under `./rapids_4_spark_qualification_output/` that contain the processed applications.
The output will go into your default filesystem and it supports both local filesystem and HDFS. 
Note that if you are on an HDFS cluster the default filesystem is likely HDFS for both the input and output.
If you want to point to the local filesystem be sure to include `file:` in the path

First output is a summary that is both printed on the STDOUT and saved as a text file. 
If there are more than one application that are currently analyzed, 
the applications that are reported on top are likely to be good candidates for the GPU 
when there is no problematic duration reported for those same applications.
The second output is a CSV file that contains more detailed information and can be used for further post processing.
For more information on the ordering, please refer to the section below that explains the “score” for each application.
See the [Understanding the Qualification tool Output](#understanding-the-qualification-tool-output) section
below for the description of output fields.

## Running the Qualification tool inside a running Spark application

### Prerequisites
- Java 8 or above, Spark 3.0.1+ 

### Download the tools jar
- Download the jar file from [Maven repository](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark-tools_2.12/22.04.0/)

### Modify your application code to call the api's

Currently only Scala api's are supported.
 
Create the `RunningQualicationApp`:
```
val qualApp = new com.nvidia.spark.rapids.tool.qualification.RunningQualificationApp()
```

Get the event listener from it and install it as a Spark listener:
```
val listener = qualApp.getEventListener
spark.sparkContext.addSparkListener(listener)
```

Run your queries and then get the summary or detailed output to see the results.

The summary output api:
```
/**
 * Get the summary report for qualification.
 * @param delimiter The delimiter separating fields of the summary report.
 * @param prettyPrint Whether to including the separate at start and end and
 *                    add spacing so the data rows align with column headings.
 * @return String of containing the summary report.
 */
getSummary(delimiter: String = "|", prettyPrint: Boolean = true): String
```

The detailed output api:
```
/**
 * Get the detailed report for qualification.
 * @param delimiter The delimiter separating fields of the summary report.
 * @param prettyPrint Whether to including the separate at start and end and
 *                    add spacing so the data rows align with column headings.
 * @return String of containing the detailed report.
 */
getDetailed(delimiter: String = "|", prettyPrint: Boolean = true, reportReadSchema: Boolean = false): String
```

Example:
```
// run your sql queries ...

// To get the summary output:
val summaryOutput = qualApp.getSummary()

// To get the detailed output:
val detailedOutput = qualApp.getDetailed()

// print the output somewhere for user to see
println(summaryOutput)
println(detailedOutput)
```

If you need to specify the tools jar as a maven dependency to compile the Spark application:
```
<dependency>
   <groupId>com.nvidia</groupId>
   <artifactId>rapids-4-spark-tools_2.12</artifactId>
   <version>${version}</version>
</dependency>
```

### Run the Spark application
- Run your Spark application and include the tools jar you downloaded with the spark '--jars' options and
view the output wherever you had it printed.

For example, if running the spark-shell:
```
$SPARK_HOME/bin/spark-shell --jars rapids-4-spark-tools_2.12-<version>.jar
```

## Understanding the Qualification tool Output
Its summary report outputs the following information:
1. Application ID 
2. Application duration
3. SQL/DF duration 
4. Problematic Duration, which indicates potential issues for acceleration.
   Some of the potential issues include User Defined Function (UDF) or any Dataset APIs.

Note: the duration(s) reported are in milli-seconds.
Sample output in text:
```
===========================================================================
|                 App ID|App Duration|SQL DF Duration|Problematic Duration|
===========================================================================
|app-20210507174503-2538|       26171|           9569|                   0|
|app-20210507174503-1704|       83738|           6760|                   0|
```

In the above example, two application event logs were analyzed. “app-20210507174503-2538” is rated higher 
than the “app-20210507174503-1704” because the score(in the csv output) for “app-20210507174503-2538”   
is higher than  “app-20210507174503-1704”. 
Here the `Problematic Duration` is zero but please keep in mind that we are only able to detect certain issues. 
This currently includes some UDFs and nested complex types.
The tool won't catch all UDFs, and some of the UDFs can be handled with additional steps.

Please refer to [supported_ops.md](./supported_ops.md) 
for more details on UDF.

The second output is a more detailed output.
Here is a sample output requesting csv style output:
```
App Name,App ID,Score,Potential Problems,SQL DF Duration,SQL Dataframe Task Duration,App Duration,Executor CPU Time Percent,App Duration Estimated,SQL Duration with Potential Problems,SQL Ids with Failures,Read Score Percent,Read File Format Score,Unsupported Read File Formats and Types,Unsupported Write Data Format,Complex Types,Nested Complex Types
job3,app-20210507174503-1704,4320658.0,"",9569,4320658,26171,35.34,false,0,"",20,100.0,"",JSON,array<struct<city:string;state:string>>;map<string;string>,array<struct<city:string;state:string>>
job1,app-20210507174503-2538,19864.04,"",6760,21802,83728,71.3,false,0,"",20,55.56,"Parquet[decimal]",JSON;CSV,"",""
```

As you can see on the above sample csv output, we have more columns than the STDOUT summary. 
Here is a brief description of each of column that is in the CSV:

1. App Name: Spark Application Name. 
2. App ID: Spark Application ID.
3. Score :  A score calculated based on SQL Dataframe Task Duration and gets negatively affected for any unsupported operators.
   Please refer to [Qualification tool score algorithm](#Qualification-tool-score-algorithm) for more details.
4. Potential Problems : Some UDFs and nested complex types.
5. SQL DF Duration: Time duration that includes only SQL/Dataframe queries.
6. SQL Dataframe Task Duration: Amount of time spent in tasks of SQL Dataframe operations.
7. App Duration: Total Application time.
8. Executor CPU Time Percent: This is an estimate at how much time the tasks spent doing processing on the CPU vs waiting on IO. 
   This is not always a good indicator because sometimes the IO that is encrypted and the CPU has to do work to decrypt it, 
   so the environment you are running on needs to be taken into account.
9. App Duration Estimated: True or False indicates if we had to estimate the application duration.
   If we had to estimate it, the value will be `True` and it means the event log was missing the application finished
   event so we will use the last job or sql execution time we find as the end time used to calculate the duration.
10. SQL Duration with Potential Problems : Time duration of any SQL/DF operations that contains 
    operations we consider potentially problematic. 
11. SQL Ids with Failures: SQL Ids of queries with failed jobs.
12. Read Score Percent: The value of the parameter `--read-score-percent` when the Qualification tool was run. Default is 20 percent. 
13. Read File Format Score: A score given based on whether the read file formats and types are supported.
14. Unsupported Read File Formats and Types: Looks at the Read Schema and
    reports the file formats along with types which may not be fully supported.
    Example: Parquet[decimal], JDBC[*]. Note that this is based on the current version of the plugin and
    future versions may add support for more file formats and types.
15. Unsupported Write Data Format: Reports the data format which we currently don’t support, i.e.
    if the result is written in JSON or CSV format.
16. Complex Types: Looks at the Read Schema and reports if there are any complex types(array, struct or maps) in the schema.
17. Nested Complex Types: Nested complex types are complex types which
    contain other complex types (Example: `array<struct<string,string>>`). 
    Note that it can read all the schemas for DataSource V1. The Data Source V2 truncates the schema,
    so if you see ..., then the full schema is not available.
    For such schemas we read until ... and report if there are any complex types and nested complex types in that.

Note that SQL queries that contain failed jobs are not included.

## Qualification tool score algorithm

In the Qualification tool’s output, all applications are ranked based on a “score”. 

The score is based on the total time spent in tasks of SQL Dataframe operations.
The tool also looks for read data formats and types that the plugin doesn't fully support and if it finds any,
it will take away from the score. The parameter to control this negative impact of the
score is  `-r, --read-score-percent` with the default value as 20(percent).
Each application(event log) could have multiple SQL queries. If a SQL's plan has a Dataset API or RDD call
inside of it, that SQL query is not categorized as a Dataframe SQL query. We are unable to determine how much
of that query is made up of Dataset or RDD calls so the entire query task time is not included in the score.

The idea behind this algorithm is that the longer the total task time doing SQL Dataframe operations
the higher the score is and the more likely the plugin will be able to help accelerate that application.

Note: The score does not guarantee there will be GPU acceleration and we are continuously improving
the score algorithm to qualify the best queries for GPU acceleration.

## How to compile the tools jar
Note: This step is optional.

```bash
git clone https://github.com/NVIDIA/spark-rapids.git
cd spark-rapids
mvn -Pdefault -pl .,tools clean verify -DskipTests
```

The jar is generated in below directory :

`./tools/target/rapids-4-spark-tools_2.12-<version>.jar`

If any input is a S3 file path or directory path, 2 extra steps are needed to access S3 in Spark:
1. Download the matched jars based on the Hadoop version:
   - `hadoop-aws-<version>.jar`
   - `aws-java-sdk-<version>.jar`
     
2. Take Hadoop 2.7.4 for example, we can download and include below jars in the '--jars' option to spark-shell or spark-submit:
   [hadoop-aws-2.7.4.jar](https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/2.7.4/hadoop-aws-2.7.4.jar) and 
   [aws-java-sdk-1.7.4.jar](https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar)

3. In $SPARK_HOME/conf, create `hdfs-site.xml` with below AWS S3 keys inside:

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
