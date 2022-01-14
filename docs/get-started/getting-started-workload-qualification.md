---
layout: page
title: Workload Qualification
nav_order: 8
parent: Getting-Started
---
# Getting Started on Spark workload qualification

The RAPIDS Accelerator for Apache Spark runs as many operations as possible on the GPU.  If there
are operators which do not yet run on GPU, they will seamlessly fallback to the CPU.  There may be
some performance overhead because of host memory to GPU memory transfer.  When converting an
existing Spark workload from CPU to GPU, it is recommended to do an analysis to understand if there
are any features (functions, expressions, data types, data formats) that do not yet run on the GPU.
Understanding this will help prioritize workloads that are best suited to the GPU.

Significant performance benefits can be gained even if all operations are not yet fully supported by
the GPU. It all depends on how critical the portion that is executing on the CPU is to the overall
performance of the query.

This article describes the tools we provide and how to do gap analysis and workload qualification.

## 1. Qualification and Profiling tool

### Requirements

- Spark event logs from Spark 2.x or 3.x
- Spark 3.0.1+ jars
- `rapids-4-spark-tools` jar

### How to use

If you have Spark event logs from prior runs of the applications on Spark 2.x or 3.x, you can use
the [Qualification tool](../spark-qualification-tool.md) and [Profiling
tool](../spark-profiling-tool.md) to analyze them.  The qualification tool outputs the score, rank
and some of the potentially not-supported features for each Spark application.  For example, the CSV
output can print `Unsupported Read File Formats and Types`, `Unsupported Write Data Format` and
`Potential Problems` which are the indication of some not-supported features.  Its output can help
you focus on the Spark applications which are best suited for the GPU.  

The profiling tool outputs SQL plan metrics and also prints out actual query plans to provide more
insights.  In the following example the profiling tool output for a specific Spark application shows
that it has a query with a large `HashAggregate` and `SortMergeJoin`. Those are indicators for a
good candidate application for the RAPIDS Accelerator.

```
+--------+-----+------+----------------------------------------------------+-------------+------------------------------------+-------------+----------+
|appIndex|sqlID|nodeID|nodeName                                            |accumulatorId|name                                |max_value    |metricType|
+--------+-----+------+----------------------------------------------------+-------------+------------------------------------+-------------+----------+
|1       |88   |8     |SortMergeJoin                                       |11111        |number of output rows               |500000000    |sum       |
|1       |88   |9     |HashAggregate                                       |22222        |number of output rows               |600000000    |sum       |
```

Since the two tools are only analyzing Spark event logs they do not have the detail that can be
captured from a running Spark job.  However it is very convenient because you can run the tools on
existing logs and do not need a GPU cluster to run the tools.

## 2. Get the Explain Output

This allows running queries on the CPU and the plugin will evaluate the queries as if it was
going to run on the GPU and tell you what would and wouldn't have been run on the GPU.
There are two ways to run this, one is running with the plugin set to explain only mode and
the other is to modify your existing Spark application code to call a function directly.

Please note that if using adaptive execution in Spark the explain output may not be perfect
as the plan could have changed along the way in a way that we wouldn't see by looking at just
the CPU plan.

### Requirements

- A Spark 3.x CPU cluster
- The `rapids-4-spark` and `cudf` [jars](../download.md)
- Ability to modify the existing Spark application code if using the function call directly

### Using the Configuration Flag for Explain Only Mode

Starting with version 22.02 of the RAPIDS Accelerator, the plugin can be run in explain only mode.
This mode allows you to run on a CPU cluster and can help us understand the potential GPU plan and
if there are any unsupported features. Basically it will log the output which is the same as
the driver logs with `spark.rapids.sql.explain=all`.

1. In `spark-shell`, add the `rapids-4-spark` and `cudf` jars into --jars option or put them in the
   Spark classpath and enable the configs `spark.rapids.sql.mode=explainOnly` and
   `spark.plugins=com.nvidia.spark.SQLPlugin`.

   For example:

   ```bash
   spark-shell --jars /PathTo/cudf-<version>.jar,/PathTo/rapids-4-spark_<version>.jar --conf spark.rapids.sql.mode=explainOnly --conf spark.plugins=com.nvidia.spark.SQLPlugin
   ```
2.  Enable optional RAPIDS Accelerator related parameters based on your setup.

   Enabling optional parameters may allow more operations to run on the GPU but please understand
   the meaning and risk of above parameters before enabling it. Please refer to the
   [configuration documentation](../configs.md) for details of RAPIDS Accelerator
   parameters.

   For example, if your jobs have `double`, `float` and `decimal` operators together with some Scala
   UDFs, you can set the following parameters:

  ```scala
   spark.conf.set("spark.rapids.sql.incompatibleOps.enabled", true)
   spark.conf.set("spark.rapids.sql.variableFloatAgg.enabled", true)
   spark.conf.set("spark.rapids.sql.decimalType.enabled", true)
   spark.conf.set("spark.rapids.sql.castFloatToDecimal.enabled",true)
   spark.conf.set("spark.rapids.sql.castDecimalToFloat.enabled",true)
   spark.conf.set("spark.rapids.sql.udfCompiler.enabled",true)
   ```

3. Run your query and check the driver logs for the explain output.

   Below are sample driver log messages starting with `!` which indicate the unsupported features in
   this version:

   ```
   ! <RowDataSourceScanExec> cannot run on GPU because GPU does not currently support the operator class org.apache.spark.sql.execution.RowDataSourceScanExec
   ```

This log can show you which operators (on what data type) can not run on GPU and the reason.
If it shows a specific RAPIDS Accelerator parameter which can be turned on to enable that feature,
you should first understand the risk and applicability of that parameter based on [configs
doc](../configs.md) and then enable that parameter and try the tool again.

Since its output is directly based on specific version of `rapids-4-spark` jar, the gap analysis is
pretty accurate.

### How to use the Function Call

Starting with version 21.12 of the RAPIDS Accelerator, a new function named
`explainPotentialGpuPlan` is added which can help us understand the potential GPU plan and if there
are any unsupported features on a CPU cluster.  Basically it can return output which is the same as
the driver logs with `spark.rapids.sql.explain=all`.

1. In `spark-shell`, add the `rapids-4-spark` and `cudf` jars into --jars option or put them in the
   Spark classpath.

   For example:

   ```bash
   spark-shell --jars /PathTo/cudf-<version>.jar,/PathTo/rapids-4-spark_<version>.jar
   ```

2. Test if the class can be successfully loaded or not.

   ```scala
   import com.nvidia.spark.rapids.ExplainPlan.explainPotentialGpuPlan
   ```

3. Enable optional RAPIDS Accelerator related parameters based on your setup.

   Enabling optional parameters may allow more operations to run on the GPU but please understand
   the meaning and risk of above parameters before enabling it. Please refer to [configs
   doc](../configs.md) for details of RAPIDS Accelerator parameters.
   
   For example, if your jobs have `double`, `float` and `decimal` operators together with some Scala
   UDFs, you can set the following parameters:
   
   ```scala
   spark.conf.set("spark.rapids.sql.incompatibleOps.enabled", true)
   spark.conf.set("spark.rapids.sql.variableFloatAgg.enabled", true)
   spark.conf.set("spark.rapids.sql.decimalType.enabled", true)
   spark.conf.set("spark.rapids.sql.castFloatToDecimal.enabled",true)
   spark.conf.set("spark.rapids.sql.castDecimalToFloat.enabled",true)
   spark.conf.set("spark.rapids.sql.udfCompiler.enabled",true)
   ```

4. Run the function `explainPotentialGpuPlan` on the query DataFrame.

   For example:

   ```scala
   val jdbcDF = spark.read.format("jdbc").
             option("driver", "com.mysql.jdbc.Driver").
             option("url", "jdbc:mysql://localhost:3306/hive?useSSL=false").
             option("dbtable", "TBLS").option("user", "xxx").
             option("password", "xxx").
             load()
   jdbcDF.createOrReplaceTempView("t")
   val mydf=spark.sql("select count(distinct TBL_ID) from t")
   
   val output=com.nvidia.spark.rapids.ExplainPlan.explainPotentialGpuPlan(mydf)
   println(output)
   ```

   Below are sample driver log messages starting with `!` which indicate the unsupported features in
   this version:
   
   ```
   ! <RowDataSourceScanExec> cannot run on GPU because GPU does not currently support the operator class org.apache.spark.sql.execution.RowDataSourceScanExec
   ```

This log can show you which operators (on what data type) can not run on GPU and the reason.
If it shows a specific RAPIDS Accelerator parameter which can be turned on to enable that feature,
you should first understand the risk and applicability of that parameter based on [configs
doc](../configs.md) and then enable that parameter and try the tool again.

Since its output is directly based on specific version of `rapids-4-spark` jar, the gap analysis is
pretty accurate.

## 3. Run Spark applications with Spark RAPIDS Accelerator on a GPU Spark Cluster

### Requirements

- A Spark 3.x GPU cluster
- The `rapids-4-spark` and `cudf` [jars](../download.md)

### How to use

Follow the getting-started guides to start a Spark 3+ GPU cluster and run the existing Spark
workloads on the GPU cluster with parameter `spark.rapids.sql.explain=all`.  The Spark driver log
should be collected to check the not-supported messages.  This is the most accurate way to do gap
analysis.

For example, the log lines starting with `!` is the so-called not-supported messages:
```
!Exec <GenerateExec> cannot run on GPU because not all expressions can be replaced
  ! <ReplicateRows> replicaterows(sum#99L, gender#76) cannot run on GPU because GPU does not currently support the operator ReplicateRows
```
The indentation indicates the parent and child relationship for those expressions.
If not all of the children expressions can run on GPU, the parent can not run on GPU either.
So above example shows the missing feature is `ReplicateRows` expression. So we filed a feature request 
[issue-4104](https://github.com/NVIDIA/spark-rapids/issues/4104) based on 21.12 version.
