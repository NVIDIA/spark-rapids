---
layout: page
title: RAPIDS-Accelerated User-Defined Functions
parent: Additional Functionality
nav_order: 3
---
# RAPIDS-Accelerated User-Defined Functions

This document describes how UDFs can provide a RAPIDS-accelerated
implementation alongside the CPU implementation, enabling the
RAPIDS Accelerator to perform the user-defined operation on the GPU.

Note that there are other potential solutions to performing user-defined
operations on the GPU. See the
[Frequently Asked Questions entry](FAQ.md#how-can-i-run-custom-expressionsudfs-on-the-gpu)
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

## Limitations of RAPIDS-Accelerated UDFs

The RAPIDS Accelerator only supports RAPIDS-accelerated forms of regular
Hive UDFs. Other forms of Spark UDFs are not supported, such as:
- Hive Aggregate Function (UDAF)
- Hive Tabular Function (UDTF)
- Lambda functions and others registered via `SparkSession.udf`
- Functions created with `org.apache.spark.sql.functions.udf`

## Adding GPU Implementations to Hive UDFs

As mentioned in the [Limitations](#limitations-of-rapids-accelerated-udfs)
section, the RAPIDS Accelerator only detects GPU implementations for Hive
regular UDFs. The Hive UDF can be either
[simple](https://github.com/apache/hive/blob/cb213d88304034393d68cc31a95be24f5aac62b6/ql/src/java/org/apache/hadoop/hive/ql/exec/UDF.java)
or
[generic](https://github.com/apache/hive/blob/cb213d88304034393d68cc31a95be24f5aac62b6/ql/src/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDF.java).

The RAPIDS Accelerator will detect a GPU implementation if the UDF class
implements the
[RapidsUDF](../sql-plugin/src/main/java/com/nvidia/spark/RapidsUDF.java)
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

Passing scalar inputs to a RAPIDS-accelerated UDF is supported with
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

### RAPIDS-Accelerated Hive UDF Examples

Source code for examples of RAPIDS-accelerated Hive UDFs is provided
in the [udf-examples](../udf-examples) project.

- [URLDecode](../udf-examples/src/main/java/com/nvidia/spark/rapids/udf/URLDecode.java)
implements a Hive simple UDF using the
[Java APIs of RAPIDS cudf](https://docs.rapids.ai/api/cudf-java/stable)
to decode URL-encoded strings
- [URLEncode](../udf-examples/src/main/java/com/nvidia/spark/rapids/udf/URLEncode.java)
implements a Hive generic UDF using the
[Java APIs of RAPIDS cudf](https://docs.rapids.ai/api/cudf-java/stable)
to URL-encode strings
- [StringWordCount](../udf-examples/src/main/java/com/nvidia/spark/rapids/udf/StringWordCount.java)
implements a Hive simple UDF using
[native code](../udf-examples/src/main/cpp/src) to count words in strings
