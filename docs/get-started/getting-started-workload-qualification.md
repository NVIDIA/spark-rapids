---
layout: page
title: Workload Qualification
nav_order: 7
parent: Getting-Started
---
# Getting Started on Spark workload qualification

The RAPIDS Accelerator for Apache Spark does not support all the features which are supported in Apache Spark.
If you plan to convert existing Spark workload from CPU to GPU, it is highly recommended that you do gap analysis 
to figure out if there are any unsupported features such as functions, expressions, data types, data formats.
After that we will know which workload is more suitable to GPU, which is called "workload qualification" here.

If there are certain operators which can not run on GPU due to the current limitations, they will fallback to CPU mode.
As a result, it may incur some performance overhead because of host memory <=> GPU memory transfer. 

This article is to help you get familiar with the best practice and different tools we provide on how to do gap analysis 
and workload qualification.

## 1. Qualification and Profiling tool

### Requirement

- Access to Spark event logs from Spark 2.x or 3.x
- Spark 3.0.1+ jars
- `rapids-4-spark-tools` jar

### How to use

If you have Spark event logs from prior runs of the applications on Spark 2.x or 3.x, you can use the
[Qualification tool](../spark-qualification-tool.md) and [Profiling tool](../spark-profiling-tool.md) to analyze them.
The qualification tool outputs the score, rank and some of the potentially not-supported features for each Spark application.
Its output can help you focus on the top N Spark applications which are SQL heavy applications. 

The profiling tool outputs SQL plan metrics and also prints out actual query plans to provide more insights.
The following example profiling tool output for a specific Spark application shows that it has a query with a large 
`HashAggregate` and `SortMergeJoin`. Those are indicators for a good candidate for the RAPIDS Accelerator.

```
+--------+-----+------+----------------------------------------------------+-------------+------------------------------------+-------------+----------+
|appIndex|sqlID|nodeID|nodeName                                            |accumulatorId|name                                |max_value    |metricType|
+--------+-----+------+----------------------------------------------------+-------------+------------------------------------+-------------+----------+
|1       |88   |8     |SortMergeJoin                                       |11111        |number of output rows               |500000000    |sum       |
|1       |88   |9     |HashAggregate                                       |22222        |number of output rows               |600000000    |sum       |
```

Since the two tools are only analyzing Spark event logs, it can not provide very accurate gap analysis information.
However it is very convenient because you do not need a CPU or GPU Spark cluster to run them.

## 2. Function `explainPotentialGPUPlan` 

### Requirement

- A Spark 3.x CPU cluster
- A pair of `rapids-4-spark` and `cudf` jars

### How to use

Starting with version 21.12 of the RAPIDS Accelerator, a new function named `explainPotentialGPUPlan` is added which can help us understand 
the potential GPU plan and if there are any not-supported features on a CPU cluster.
Basically it can return output which is the same as the driver logs with `spark.rapids.sql.explain=all`.

1. In `spark-shell`, add the `rapids-4-spark` and `cudf` jars into --jars option or put them in Spark related classpath.

   For example:

   ```bash
   spark-shell --jars /PathTo/cudf-<version>.jar,/PathTo/rapids-4-spark_<version>.jar
   ```

2. Test if the class can be successfully loaded or not.

   ```scala
   import com.nvidia.spark.rapids.ExplainPlan.explainPotentialGPUPlan
   ```

3. Enable RAPIDS Accelerator related parameters to allow supporting existing features.
   
   For example, if your jobs have `double`/`float`/`decimal` operators together with some Scala UDFs, you can set 
   the following parameters:
   
   ```scala
   spark.conf.set("spark.rapids.sql.incompatibleOps.enabled", true)
   spark.conf.set("spark.rapids.sql.variableFloatAgg.enabled", true)
   spark.conf.set("spark.rapids.sql.decimalType.enabled", true)
   spark.conf.set("spark.rapids.sql.castFloatToDecimal.enabled",true)
   spark.conf.set("spark.rapids.sql.castDecimalToFloat.enabled",true)
   spark.conf.set("spark.rapids.sql.udfCompiler.enabled",true)
   ```
   
   Please refer to [config doc](../configs.md) for details of RAPIDS Accelerator parameters.
   Note: Please understand the meaning and risk of above parameters before enabling it. 

4. Run the function `explainPotentialGPUPlan` on the query DataFrame.

   For example:

   ```scala
   scala> val df_multi=spark.sql("SELECT value82*value63 FROM df2 union SELECT value82+value63 FROM df2")
   df_multi: org.apache.spark.sql.DataFrame = [(CAST(value82 AS DECIMAL(9,3)) * CAST(value63 AS DECIMAL(9,3))): decimal(15,5)]

   scala> val output=com.nvidia.spark.rapids.ExplainPlan.explainPotentialGPUPlan(df_multi)
   scala> println(output)
   ```

   Below are sample driver log messages starting with `!` which indicate the unsupported features in this version:
   
   ```
   !Exec <ProjectExec> cannot run on GPU because not all expressions can be replaced
     !Expression <Multiply> (promote_precision(cast(value82#30 as decimal(9,3))) * promote_precision(cast(value63#31 as decimal(9,3)))) cannot run on GPU because The actual output precision of the multiply ilarge to fit on the GPU DecimalType(19,6)
   ```

This log can show you which operators(on what data type) can not run on GPU and what is the reason.
If it shows a specific RAPIDS Accelerator parameter which can be turned on to enable that feature, you should firstly 
understand the risk and applicability of that parameter and then enable that parameter and try the tool again.

Since its output is directly based on specific version of `rapids-4-spark` jar, the gap analysis is pretty accurate.
But you need a Spark 3+ CPU cluster and is ok to modify the code to add this function.

## 3. Run Spark applications with Spark RAPIDS Accelerator on a GPU Spark Cluster

### Requirement

- A Spark 3.x GPU cluster
- A pair of `rapids-4-spark` and `cudf` jars

### How to use

Follow the getting-started guides to start a Spark 3+ GPU cluster and run the existing Spark workloads on the GPU 
cluster with parameter `spark.rapids.sql.explain=all`.
The Spark driver log should be collected to check the not-supported messages.
This is the most accurate way to do gap analysis.




