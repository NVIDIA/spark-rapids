# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Source fixtures for the JVM template integration tests.
These are filled into the templates, serving as a stand in
for what an agent would generate.
"""

# ---------------------------------------------------------------------------
# UDF source code
# ---------------------------------------------------------------------------

CPU_UDF_NAME = "IntegerMultiplyBy2UDF"

SCALA_UDF_SOURCE = """\
package com.udf

class IntegerMultiplyBy2UDF extends Function1[Integer, Integer] with Serializable {
  override def apply(value: Integer): Integer = {
    if (value == null) null else value * 2
  }
}
"""

JAVA_UDF_SOURCE = """\
package com.udf;

import org.apache.spark.sql.api.java.UDF1;

public class IntegerMultiplyBy2UDF implements UDF1<Integer, Integer> {
  @Override
  public Integer call(Integer value) {
    return value == null ? null : value * 2;
  }
}
"""

RAPIDS_UDF_NAME = "IntegerMultiplyBy2RapidsUDF"

SCALA_RAPIDS_UDF_SOURCE = """\
package com.udf

import ai.rapids.cudf._
import com.nvidia.spark.RapidsUDF
import Arm.withResource

class IntegerMultiplyBy2RapidsUDF extends Function1[Integer, Integer] with Serializable with RapidsUDF {
  override def apply(value: Integer): Integer = {
    if (value == null) null else value * 2
  }

  override def evaluateColumnar(numRows: Int, args: ColumnVector*): ColumnVector = {
    withResource(Scalar.fromInt(2)) { two =>
      args.head.mul(two)
    }
  }
}
"""

JAVA_RAPIDS_UDF_SOURCE = """\
package com.udf;

import org.apache.spark.sql.api.java.UDF1;
import ai.rapids.cudf.*;
import com.nvidia.spark.RapidsUDF;

public class IntegerMultiplyBy2RapidsUDF implements UDF1<Integer, Integer>, RapidsUDF {
  @Override
  public Integer call(Integer value) {
    return value == null ? null : value * 2;
  }

  @Override
  public ColumnVector evaluateColumnar(int numRows, ColumnVector... args) {
    try (Scalar two = Scalar.fromInt(2)) {
      return args[0].mul(two);
    }
  }
}
"""

NATIVE_UDF_NAME = "IntegerMultiplyBy2NativeRapidsUDF"

NATIVE_RAPIDS_UDF_SOURCE = """\
package com.udf;

import ai.rapids.cudf.ColumnVector;
import com.nvidia.spark.RapidsUDF;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.spark.sql.api.java.UDF1;

public class IntegerMultiplyBy2NativeRapidsUDF extends UDF
        implements UDF1<Integer, Integer>, RapidsUDF {
    @Override
    public Integer call(Integer value) {
        return value == null ? null : value * 2;
    }

    @Override
    public ColumnVector evaluateColumnar(int numRows, ColumnVector... args) {
        NativeUDFLoader.ensureLoaded();
        return new ColumnVector(integerMultiplyBy2(args[0].getNativeView()));
    }

    private static native long integerMultiplyBy2(long inputView);
}
"""

SQL_SOURCE = """\
SELECT *,
  value * 2 AS result
FROM test_table
"""

# ---------------------------------------------------------------------------
# Unit test methods
# ---------------------------------------------------------------------------

CREATE_TEST_DATA = """\
  def createTestData(spark: SparkSession): DataFrame = {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", IntegerType, nullable = true)
    ))
    val testData = Seq(
      Row(1, 123),
      Row(2, 0),
      Row(3, -5),
      Row(4, null)
    )
    spark.createDataFrame(spark.sparkContext.parallelize(testData), schema)
  }"""

EXECUTE_UDF = """\
  def executeUDF(spark: SparkSession, udfName: String, testDF: DataFrame): DataFrame = {
    testDF.createOrReplaceTempView("test_table")
    spark.sql(s"SELECT *, $udfName(value) AS result FROM test_table")
  }"""

VERIFY_UDF_RESULTS = """\
  def verifyUDFResults(resultDF: DataFrame, testDF: DataFrame): Unit = {
    val results = resultDF.collect().sortBy(_.getAs[Int]("id"))
    assert(results(0).getAs[Int]("result") === 246)
    assert(results(1).getAs[Int]("result") === 0)
    assert(results(2).getAs[Int]("result") === -10)
    assert(results(3).isNullAt(results(3).fieldIndex("result")))
  }"""

_SCALA_REGISTER_CALL = "spark.udf.register({name}, new com.udf.{cls}())"
_JAVA_REGISTER_CALL = "spark.udf.register({name}, new com.udf.{cls}(), org.apache.spark.sql.types.IntegerType)"


_REGISTER_METHOD = """\
  def {method}(spark: SparkSession, udfName: String): Unit = {{
    {register_call}
  }}"""


SCALA_REGISTER_UDF = _REGISTER_METHOD.format(
    method="registerUDF",
    register_call=_SCALA_REGISTER_CALL.format(name="udfName", cls=CPU_UDF_NAME),
)
JAVA_REGISTER_UDF = _REGISTER_METHOD.format(
    method="registerUDF",
    register_call=_JAVA_REGISTER_CALL.format(name="udfName", cls=CPU_UDF_NAME),
)

SCALA_REGISTER_RAPIDS_UDF = _REGISTER_METHOD.format(
    method="registerRapidsUDF",
    register_call=_SCALA_REGISTER_CALL.format(name="udfName", cls=RAPIDS_UDF_NAME),
)
JAVA_REGISTER_RAPIDS_UDF = _REGISTER_METHOD.format(
    method="registerRapidsUDF",
    register_call=_JAVA_REGISTER_CALL.format(name="udfName", cls=RAPIDS_UDF_NAME),
)
NATIVE_REGISTER_RAPIDS_UDF = _REGISTER_METHOD.format(
    method="registerRapidsUDF",
    register_call=_JAVA_REGISTER_CALL.format(name="udfName", cls=NATIVE_UDF_NAME),
)

# ---------------------------------------------------------------------------
# BenchUtils methods
# ---------------------------------------------------------------------------

BENCH_GENERATE = """\
  def generateSyntheticData(
      spark: SparkSession,
      numRows: Long,
      numPartitions: Int
  ): DataFrame = {
    val baseDF = spark.range(0, numRows, 1, numPartitions)
    baseDF.select(
      col("id"),
      (rand() * 1000).cast(IntegerType).alias("value")
    )
  }"""


_BENCH_EXECUTE_METHOD = """\
  def {method}(spark: SparkSession, df: DataFrame): DataFrame = {{
    df.createOrReplaceTempView("bench_table")
    {register}
    spark.sql("SELECT *, udf(value) AS result FROM bench_table")
  }}"""


BENCH_EXECUTE_SCALA_CPU = _BENCH_EXECUTE_METHOD.format(
    method="executeCpu",
    register=_SCALA_REGISTER_CALL.format(name='"udf"', cls=CPU_UDF_NAME),
)
BENCH_EXECUTE_JAVA_CPU = _BENCH_EXECUTE_METHOD.format(
    method="executeCpu",
    register=_JAVA_REGISTER_CALL.format(name='"udf"', cls=CPU_UDF_NAME),
)
BENCH_EXECUTE_SCALA_CUDF = _BENCH_EXECUTE_METHOD.format(
    method="executeGpu",
    register=_SCALA_REGISTER_CALL.format(name='"udf"', cls=RAPIDS_UDF_NAME),
)
BENCH_EXECUTE_JAVA_CUDF = _BENCH_EXECUTE_METHOD.format(
    method="executeGpu",
    register=_JAVA_REGISTER_CALL.format(name='"udf"', cls=RAPIDS_UDF_NAME),
)
BENCH_EXECUTE_CUDA = _BENCH_EXECUTE_METHOD.format(
    method="executeGpu",
    register=_JAVA_REGISTER_CALL.format(name='"udf"', cls=NATIVE_UDF_NAME),
)

BENCH_EXECUTE_SQL = """\
  def executeGpu(spark: SparkSession, df: DataFrame): DataFrame = {
    df.createOrReplaceTempView("bench_table")
    val sqlSource = scala.io.Source.fromFile("src/main/resources/integer_multiply_by_2.sql")
    val sqlContent = try sqlSource.mkString finally sqlSource.close()
    val benchSql = sqlContent.replace("test_table", "bench_table")
    spark.sql(benchSql)
  }"""

# ---------------------------------------------------------------------------
# MicroBenchRunner methods
# ---------------------------------------------------------------------------

MICRO_PREPARE_CPU = """\
  def prepareCpuData(
      hostColumns: Array[HostColumnVector],
      numRows: Int
  ): Array[AnyRef] = {
    val values = Array.tabulate(numRows) { i =>
      if (hostColumns(1).isNull(i)) null
      else Int.box(hostColumns(1).getInt(i))
    }
    Array[AnyRef](values)
  }"""


_MICRO_EXECUTE_CPU_METHOD = """\
  def executeCpu(data: Array[AnyRef], numRows: Int): Unit = {{
    val values = data(0).asInstanceOf[Array[Integer]]
    val udf = new com.udf.{cls}()
    var i = 0
    while (i < numRows) {{
      udf.{invoke}(values(i))
      i += 1
    }}
  }}"""

MICRO_EXECUTE_SCALA_CPU = _MICRO_EXECUTE_CPU_METHOD.format(
    cls=CPU_UDF_NAME,
    invoke="apply",
)
MICRO_EXECUTE_JAVA_CPU = _MICRO_EXECUTE_CPU_METHOD.format(
    cls=CPU_UDF_NAME,
    invoke="call",
)


_MICRO_EXECUTE_GPU_METHOD = """\
  def executeGpu(table: Table, numRows: Int): ColumnVector = {{
    val udf = new com.udf.{cls}()
    udf.evaluateColumnar(numRows, table.getColumn(1))
  }}"""

MICRO_EXECUTE_CUDF = _MICRO_EXECUTE_GPU_METHOD.format(cls=RAPIDS_UDF_NAME)
MICRO_EXECUTE_CUDA = _MICRO_EXECUTE_GPU_METHOD.format(cls=NATIVE_UDF_NAME)
