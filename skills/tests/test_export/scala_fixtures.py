# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Scala source code fixtures for integration tests.
"""

from .utils import replace_scala_todo_method

NAME = "scala"
REPLACE_TODO_FN = replace_scala_todo_method
TEST_SELECTOR_FLAG = "-Dsuites"

# ---------------------------------------------------------------------------
# UDF source code
# ---------------------------------------------------------------------------

UDF_SOURCE = """\
package com.udf

class IntegerMultiplyBy2UDF extends Function1[Integer, Integer] with Serializable {
  override def apply(value: Integer): Integer = {
    if (value == null) null else value * 2
  }
}
"""

RAPIDS_UDF_SOURCE = """\
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

SQL_SOURCE = """\
SELECT *,
  value * 2 AS result
FROM test_table
"""

# ---------------------------------------------------------------------------
# Unit test methods
# ---------------------------------------------------------------------------

UNIT_TEST_METHODS = {
    "createTestData": """\
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
  }""",
    "registerUDF": """\
  def registerUDF(spark: SparkSession, udfName: String): Unit = {
    spark.udf.register(udfName, new IntegerMultiplyBy2UDF())
  }""",
    "executeUDF": """\
  def executeUDF(spark: SparkSession, udfName: String, testDF: DataFrame): DataFrame = {
    testDF.createOrReplaceTempView("test_table")
    spark.sql(s"SELECT *, $udfName(value) AS result FROM test_table")
  }""",
    "verifyUDFResults": """\
  def verifyUDFResults(resultDF: DataFrame, testDF: DataFrame): Unit = {
    val results = resultDF.collect().sortBy(_.getAs[Int]("id"))
    assert(results(0).getAs[Int]("result") === 246)
    assert(results(1).getAs[Int]("result") === 0)
    assert(results(2).getAs[Int]("result") === -10)
    assert(results(3).isNullAt(results(3).fieldIndex("result")))
  }""",
}

RAPIDS_UDF_REGISTER = """\
  def registerRapidsUDF(spark: SparkSession, udfName: String): Unit = {
    spark.udf.register(udfName, new IntegerMultiplyBy2RapidsUDF())
  }"""

NATIVE_RAPIDS_UDF_REGISTER = """\
  def registerRapidsUDF(spark: SparkSession, udfName: String): Unit = {
    spark.udf.register(
      udfName,
      new IntegerMultiplyBy2NativeRapidsUDF(),
      org.apache.spark.sql.types.IntegerType)
  }"""

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

BENCH_CPU = """\
  def executeCpu(spark: SparkSession, df: DataFrame): DataFrame = {
    import com.udf.IntegerMultiplyBy2UDF
    df.createOrReplaceTempView("bench_table")
    spark.udf.register("integer_multiply_by_2", new IntegerMultiplyBy2UDF())
    spark.sql("SELECT *, integer_multiply_by_2(value) AS result FROM bench_table")
  }"""

BENCH_GPU_CUDF = """\
  def executeGpu(spark: SparkSession, df: DataFrame): DataFrame = {
    import com.udf.IntegerMultiplyBy2RapidsUDF
    df.createOrReplaceTempView("bench_table")
    spark.udf.register("integer_multiply_by_2_rapids", new IntegerMultiplyBy2RapidsUDF())
    spark.sql("SELECT *, integer_multiply_by_2_rapids(value) AS result FROM bench_table")
  }"""

BENCH_GPU_CUDA = """\
  def executeGpu(spark: SparkSession, df: DataFrame): DataFrame = {
    df.createOrReplaceTempView("bench_table")
    spark.udf.register(
      "integer_multiply_by_2_native",
      new com.udf.IntegerMultiplyBy2NativeRapidsUDF(),
      org.apache.spark.sql.types.IntegerType)
    spark.sql("SELECT *, integer_multiply_by_2_native(value) AS result FROM bench_table")
  }"""

BENCH_GPU_SQL = """\
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

MICRO_EXECUTE_CPU = """\
  def executeCpu(data: Array[AnyRef], numRows: Int): Unit = {
    val values = data(0).asInstanceOf[Array[Integer]]
    val udf = new com.udf.IntegerMultiplyBy2UDF()
    var i = 0
    while (i < numRows) {
      udf.apply(values(i))
      i += 1
    }
  }"""

MICRO_EXECUTE_GPU = """\
  def executeGpu(table: Table, numRows: Int): ColumnVector = {
    val udf = new com.udf.IntegerMultiplyBy2RapidsUDF()
    udf.evaluateColumnar(numRows, table.getColumn(1))
  }"""

MICRO_EXECUTE_GPU_CUDA = """\
  def executeGpu(table: Table, numRows: Int): ColumnVector = {
    val udf = new com.udf.IntegerMultiplyBy2NativeRapidsUDF()
    udf.evaluateColumnar(numRows, table.getColumn(1))
  }"""
