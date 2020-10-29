/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tests

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DataTest {
  def noopUdf(spark: SparkSession, dataType: DataType): UserDefinedFunction = dataType match {
    case BooleanType => spark.udf.register("noop", (v: Boolean) => v)
    case ByteType => spark.udf.register("noop", (v: Byte) => v)
    case ShortType => spark.udf.register("noop", (v: Short) => v)
    case IntegerType => spark.udf.register("noop", (v: Int) => v)
    case LongType => spark.udf.register("noop", (v: Long) => v)
    case FloatType => spark.udf.register("noop", (v: Float) => v)
    case DoubleType => spark.udf.register("noop", (v: Double) => v)
    case DateType => spark.udf.register("noop", (v: Date) => v)
    case TimestampType => spark.udf.register("noop", (v: Timestamp) => v)
    case _ => throw new IllegalArgumentException(s"Data Type $dataType is not supported yet")
  }
  /**
   * The main method can be invoked by using spark-submit.
   */
  def main(args: Array[String]): Unit = {
    val dataPath = args(0)
    val outputPath = args(1)
    val numCols = if (args.length > 2) {
      args(2).toInt
    } else {
      100
    }
    val count = if (args.length > 3) {
      args(3).toInt
    } else {
      1
    }
    val spark = SparkSession.builder.appName("cpy-to-host-bench").getOrCreate()
//    spark.sparkContext.setLogLevel("DEBUG")
    spark.sparkContext.setLogLevel("WARN")
    val store_sales = spark.read.parquet(dataPath)
    // Sorry but for this test we need to drop all of the non-basic columns
    val fields = store_sales.schema
        .filter(_.dataType match {
          case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType |
               DoubleType | DateType | TimestampType => true
          case _ => false
        })
    val columns = fields.map(f => col(f.name))
    val noopUdf = DataTest.noopUdf(spark, fields.head.dataType)
    (0 until count).foreach { _ =>
      spark.time(store_sales
          .select(columns.slice(0, numCols): _*)
          .withColumn("_NOOP_", noopUdf(columns.head))
//          .withColumn("_NOOP_", columns.head)
          .write.mode("overwrite").parquet(outputPath))
    }
  }
}
