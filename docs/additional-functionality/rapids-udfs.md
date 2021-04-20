---
layout: page
title: RAPIDS Accelerated User-Defined Functions
parent: Additional Functionality
nav_order: 3
---
# RAPIDS Accelerated User-Defined Functions

This document describes how UDFs can provide a RAPIDS accelerated
implementation alongside the CPU implementation, enabling the
RAPIDS Accelerator to perform the user-defined operation on the GPU.

Note that there are other potential solutions to performing user-defined
operations on the GPU. See the
[Frequently Asked Questions entry](../FAQ.md#how-can-i-run-custom-expressionsudfs-on-the-gpu)
on UDFs for more details.

## UDF Obstacles To Query Acceleration

User-defined functions can perform almost arbitrary operations and thus are
very difficult to translate automatically into GPU operations. UDFs can
prevent potentially expensive portions of a query from being automatically
accelerated by the RAPIDS Accelerator due to the inability to perform the
custom operation on the GPU.

One possible solution is the UDF providing a GPU implementation compatible
with the RAPIDS Accelerator. This implementation can then be invoked by the
RAPIDS Accelerator when a corresponding query step using the UDF executes
on the GPU.

## Limitations of RAPIDS Accelerated UDFs

The RAPIDS Accelerator only supports RAPIDS accelerated forms of the
following UDF types:
- Scala UDFs implementing a `Function` interface and registered via `SparkSession.udf.register`
- Java UDFs implementing
  [one of the `org.apache.spark.sql.api.java.UDF` interfaces](https://github.com/apache/spark/tree/branch-3.0/sql/core/src/main/java/org/apache/spark/sql/api/java)
  and registered either via `SparkSession.udf.register` or
  `spark.udf.registerJavaFunction` in PySpark
- [Simple](https://github.com/apache/hive/blob/cb213d88304034393d68cc31a95be24f5aac62b6/ql/src/java/org/apache/hadoop/hive/ql/exec/UDF.java)
  or
  [Generic](https://github.com/apache/hive/blob/cb213d88304034393d68cc31a95be24f5aac62b6/ql/src/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDF.java)
  Hive UDFs

Other forms of Spark UDFs are not supported, such as:
- Scala or Java User-Defined Aggregate Functions
- Hive Aggregate Function (UDAF)
- Hive Tabular Function (UDTF)
- Lambda Functions

## Adding GPU Implementations to UDFs

For supported UDFs, the RAPIDS Accelerator will detect a GPU implementation
if the UDF class implements the
[RapidsUDF](../../sql-plugin/src/main/java/com/nvidia/spark/RapidsUDF.java)
interface. This interface requires implementing the following method:

```java
  ai.rapids.cudf.ColumnVector evaluateColumnar(ai.rapids.cudf.ColumnVector... args);
```

Unlike the CPU UDF which processes data one row at a time, the GPU version
processes a columnar batch of rows. This reduces invocation overhead and
enables parallel processing of the data by the GPU.

### Interpreting Inputs

The RAPIDS Accelerator will pass columnar forms of the same inputs for the
CPU version of the UDF. For example, if the CPU UDF expects two inputs, a
`String` and an `Integer`, then the `evaluateColumnar` method will be invoked
with an array of two cudf `ColumnVector` instances. The first instance will
be a column of type `STRING` and the second a column of type `INT32`. The two
columns will always have the same number of rows, but the UDF implementation
must not make any assumptions on the number of input rows.

#### Scalar Inputs

Passing scalar inputs to a RAPIDS accelerated UDF is supported with
limitations. The scalar value will be replicated into a full column before
being passed to `evaluateColumnar`. Therefore the UDF implementation cannot
easily detect the difference between a scalar input and a columnar input.

### Resource Management for Intermediate Results

GPU memory is a limited resource and can become exhausted when not managed
properly. The UDF is responsible for freeing any intermediate GPU results
computed during the processing of the UDF. The inputs to the UDF will be
closed by the RAPIDS Accelerator, so the UDF only needs to close any
intermediate data generated while producing the final result that is
returned.

### Generating Columnar Output

The `evaluateColumnar` method must return a `ColumnVector` of an appropriate
cudf type to match the result type of the original UDF. For example, if the
CPU UDF returns a `double` then `evaluateColumnar` must return a column of
type `FLOAT64`.

## RAPIDS Accelerated UDF Examples

Source code for examples of RAPIDS accelerated Hive UDFs is provided
in the [udf-examples](../../udf-examples) project.

### Spark Scala UDF Examples

- [URLDecode](../../udf-examples/src/main/scala/com/nvidia/spark/rapids/udf/scala/URLDecode.scala)
decodes URL-encoded strings using the
[Java APIs of RAPIDS cudf](https://docs.rapids.ai/api/cudf-java/stable)
- [URLEncode](../../udf-examples/src/main/scala/com/nvidia/spark/rapids/udf/scala/URLEncode.scala)
URL-encodes strings using the
[Java APIs of RAPIDS cudf](https://docs.rapids.ai/api/cudf-java/stable)

### Spark Java UDF Examples

- [URLDecode](../../udf-examples/src/main/java/com/nvidia/spark/rapids/udf/java/URLDecode.java)
decodes URL-encoded strings using the
[Java APIs of RAPIDS cudf](https://docs.rapids.ai/api/cudf-java/stable)
- [URLEncode](../../udf-examples/src/main/java/com/nvidia/spark/rapids/udf/java/URLEncode.java)
URL-encodes strings using the
[Java APIs of RAPIDS cudf](https://docs.rapids.ai/api/cudf-java/stable)

### Hive UDF Examples

- [URLDecode](../../udf-examples/src/main/java/com/nvidia/spark/rapids/udf/hive/URLDecode.java)
implements a Hive simple UDF using the
[Java APIs of RAPIDS cudf](https://docs.rapids.ai/api/cudf-java/stable)
to decode URL-encoded strings
- [URLEncode](../../udf-examples/src/main/java/com/nvidia/spark/rapids/udf/hive/URLEncode.java)
implements a Hive generic UDF using the
[Java APIs of RAPIDS cudf](https://docs.rapids.ai/api/cudf-java/stable)
to URL-encode strings
- [StringWordCount](../../udf-examples/src/main/java/com/nvidia/spark/rapids/udf/hive/StringWordCount.java)
implements a Hive simple UDF using
[native code](../../udf-examples/src/main/cpp/src) to count words in strings


## GPU Scheduling For Pandas UDF

---
**NOTE**

The _GPU Scheduling for Pandas UDF_ is an experimental feature, and may change at any point it time.

---

_GPU Scheduling for Pandas UDF_ is built on Apache Spark's [Pandas UDF(user defined
function)](https://spark.apache.org/docs/3.0.0/sql-pyspark-pandas-with-arrow.html#pandas-udfs-aka-vectorized-udfs),
and has two components:

- **Share GPU with JVM**: Let the Python process share JVM GPU. The Python process could run on the
  same GPU with JVM.

- **Increase Speed**: Make the data transport faster between JVM process and Python process.



To enable _GPU Scheduling for Pandas UDF_, you need to configure your spark job with extra settings.

1. Make sure GPU exclusive mode is disabled. Note that this will not work if you are using exclusive
   mode to assign GPUs under spark.
2. Currently the python files are packed into the spark rapids plugin jar.

    On Yarn, you need to add
    ```shell
    ...
    --py-files ${SPARK_RAPIDS_PLUGIN_JAR}
    ```


    On Standalone, you need to add
    ```shell
    ...
    --conf spark.executorEnv.PYTHONPATH=rapids-4-spark_2.12-0.5.0.jar \
    --py-files ${SPARK_RAPIDS_PLUGIN_JAR}
    ```

3. Enable GPU Scheduling for Pandas UDF.

    ```shell
    ...
    --conf spark.rapids.python.gpu.enabled=true \
    --conf spark.rapids.python.memory.gpu.pooling.enabled=false \
    --conf spark.rapids.sql.exec.ArrowEvalPythonExec=true \
    --conf spark.rapids.sql.exec.MapInPandasExec=true \
    --conf spark.rapids.sql.exec.FlatMapGroupsInPandasExec=true \
    --conf spark.rapids.sql.exec.AggregateInPandasExec=true \
    --conf spark.rapids.sql.exec.FlatMapCoGroupsInPandasExec=true \
    --conf spark.rapids.sql.exec.WindowInPandasExec=true
    ```

Please note the data transfer acceleration only supports scalar UDF and Scalar iterator UDF currently. 
You could choose the exec you need to enable.

### Other Configuration

Following configuration settings are also for _GPU Scheduling for Pandas UDF_
```
spark.rapids.python.concurrentPythonWorkers
spark.rapids.python.memory.gpu.allocFraction
spark.rapids.python.memory.gpu.maxAllocFraction
```

To find details on the above Python configuration settings, please see the [RAPIDS Accelerator for
Apache Spark Configuration Guide](../configs.md).
