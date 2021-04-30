/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids

import java.io.File
import java.nio.file.Files

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class ProjectExprSuite extends SparkQueryCompareTestSuite {
  def forceHostColumnarToGpu(): SparkConf = {
    // turns off BatchScanExec, so we get a CPU BatchScanExec together with a HostColumnarToGpu
    new SparkConf().set("spark.rapids.sql.exec.BatchScanExec", "false")
  }

  test("rand is okay") {
    // We cannot test that the results are exactly equal because our random number
    // generator is not identical to spark, so just make sure it does not crash
    // and all of the numbers are in the proper range
    withGpuSparkSession(session => {
      val df = nullableFloatCsvDf(session)
      val data = df.select(col("floats"), rand().as("RANDOM")).collect()
      data.foreach(row => {
        val d = row.getDouble(1)
        assert(d < 1.0)
        assert(d >= 0.0)
      })
    }, conf = enableCsvConf())
  }

  testSparkResultsAreEqual("Test literal values in select", mixedFloatDf) {
    frame =>
      frame.select(col("floats"),
        lit(100), lit("hello, world!"),
        lit(BigDecimal(123456789L, 6)), lit(BigDecimal(0L)), lit(BigDecimal(1L, -3)),
        lit(BigDecimal(-2.12314e-8)),
        lit(Array(1, 2, 3, 4, 5)), lit(Array(1.2, 3.4, 5.6)),
        lit(Array("a", "b", null, "")),
        lit(Array(Array(1, 2), null, Array(3, 4))),
        lit(Array(Array(Array(1, 2), Array(2, 3), null), null)),
        new Column(Literal.create(Array(Row(1, "s1"), Row(2, "s2"), null),
          ArrayType(StructType(
            Array(StructField("id", IntegerType), StructField("name", StringType)))))),
        new Column(Literal.create(List(BigDecimal(123L, 2), BigDecimal(-1444L, 2)),
          ArrayType(DecimalType(10, 2)))))
  }

  testSparkResultsAreEqual("project time", frameFromParquet("timestamp-date-test.parquet"),
    conf = forceHostColumnarToGpu()) {
    frame => frame.select("time")
  }

  // test GpuRowToColumnarExec + GpuProjectExec + GpuColumnarToRowExec
  testSparkResultsAreEqual("project decimal with row source", mixedDf(_),
    conf = new SparkConf(), repart = 0) {
    frame => frame.select("decimals")
  }

  // test HostColumnarToGpu + GpuProjectExec + GpuColumnarToRowExec
  test("project decimal with columnar source") {
    val dir = Files.createTempDirectory("spark-rapids-test").toFile
    val path = new File(dir,
      s"HostColumnarToGpu-${System.currentTimeMillis()}.parquet").getAbsolutePath

    try {
      withCpuSparkSession(spark => mixedDf(spark).write.parquet(path), new SparkConf())

      val createDF = (ss: SparkSession) => ss.read.parquet(path)
      val fun = (df: DataFrame) => df.withColumn("dec", df("decimals")).select("dec")
      val conf = new SparkConf()
          .set("spark.rapids.sql.exec.FileSourceScanExec", "false")
          .set(RapidsConf.DECIMAL_TYPE_ENABLED.key, "true")
      val (fromCpu, fromGpu) = runOnCpuAndGpu(createDF, fun, conf, repart = 0)
      compareResults(false, 0.0, fromCpu, fromGpu)
    } finally {
      dir.delete()
    }
  }

  testSparkResultsAreEqual("getMapValue", frameFromParquet("map_of_strings.snappy.parquet")) {
    frame => frame.selectExpr("mapField['foo']")
  }
}
