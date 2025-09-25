/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector, DType, GroupByAggregation, GroupByAggregationOnColumn, Scalar}
import com.nvidia.spark.{RapidsSimpleGroupByAggregation, RapidsUDAF, RapidsUDAFGroupByAggregation}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{functions, Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructField, StructType}

class ScalaAggregatorSuite extends SparkQueryCompareTestSuite {

  IGNORE_ORDER_testSparkResultsAreEqual(testName = "Groupby with ScalaAggregator Average",
      groupbyStringsIntsIntsFromCsv, repart = 7) { df =>
    // This is a basic smoke-test of the Scala UDAF framework, not an exhaustive test of
    // the specific UDAF implementation itself.
    df.createOrReplaceTempView("groupby_scala_average_udaf_test_table")
    df.sparkSession.udf.register("intAverage", functions.udaf(new IntAverageAggregator))
    df.sparkSession.sql(sqlText = """
      SELECT count(c1_int), intAverage(c1_int), max(c2_int), intAverage(c2_int)
      FROM groupby_scala_average_udaf_test_table
      GROUP BY key_str
    """)
  }

  IGNORE_ORDER_testSparkResultsAreEqual(testName = "Reduction with ScalaAggregator Average",
      groupbyStringsIntsIntsFromCsv, repart = 7) { df =>
    // This is a basic smoke-test of the Scala UDAF framework, not an exhaustive test of
    // the specific UDAF implementation itself.
    df.createOrReplaceTempView("reduction_scala_average_udaf_test_table")
    df.sparkSession.udf.register("intAverage", functions.udaf(new IntAverageAggregator))
    df.sparkSession.sql(sqlText = """
      SELECT intAverage(c1_int), count(c1_int), intAverage(c2_int), max(c2_int)
      FROM reduction_scala_average_udaf_test_table
    """)
  }

  private val emptyDfSchema = StructType(Seq(
    StructField("key_str", StringType, nullable = true),
    StructField("c1_int", IntegerType, nullable = true),
    StructField("c2_int", IntegerType, nullable = true))
  )

  IGNORE_ORDER_testSparkResultsAreEqual(
      testName = "Reduction with ScalaAggregator Average on empty dataset",
      ss => emptyRowsDf(ss, emptyDfSchema)) { df =>
    // This is a basic smoke-test of the Scala UDAF framework, not an exhaustive test of
    // the specific UDAF implementation itself.
    df.createOrReplaceTempView("reduction_scala_average_udaf_test_table")
    df.sparkSession.udf.register("intAverage", functions.udaf(new IntAverageAggregator))
    df.sparkSession.sql(sqlText =
      """
      SELECT intAverage(c1_int), count(c1_int), intAverage(c2_int), max(c2_int)
      FROM reduction_scala_average_udaf_test_table
    """)
  }

  Seq("partial", "final").foreach { replaceMode =>
    val fallType = if (replaceMode == "partial") "Gpu2Cpu" else "Cpu2Gpu"
    IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
      testName = s"Groupby with $fallType ScalaAggregator Average",
      groupbyStringsIntsIntsFromCsv,
      repart = 7,
      execsAllowedNonGpu = Seq("ObjectHashAggregateExec", "ProjectExec"),
      conf = new SparkConf().set("spark.rapids.sql.hashAgg.replaceMode", replaceMode)
    ) { df =>
      // This is a basic smoke-test of the Scala UDAF framework, not an exhaustive test of
      // the specific UDAF implementation itself.
      df.createOrReplaceTempView("groupby_scala_average_udaf_test_table")
      df.sparkSession.udf.register("intAverage", functions.udaf(new IntAverageAggregator))
      df.sparkSession.sql(sqlText =
        """
        SELECT count(c1_int), intAverage(c1_int), max(c2_int), intAverage(c2_int)
        FROM groupby_scala_average_udaf_test_table
        GROUP BY key_str
      """)
    }
  }
}

case class AverageBuffer(var sum: java.lang.Long, var count: Long)

class IntAverageAggregator extends Aggregator[Integer, AverageBuffer, Integer] with RapidsUDAF {

  // ===== CPU Spark Aggregator Implementation =====
  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  override def zero: AverageBuffer = AverageBuffer(null, 0L)

  // Combine two values to produce a new value. For performance, the function may
  // modify `buffer` and return it instead of constructing a new object
  override def reduce(buffer: AverageBuffer, data: Integer): AverageBuffer = {
    if (data != null) {
      buffer.sum += data
      buffer.count += 1
    }
    buffer
  }

  // Merge two intermediate values
  override def merge(b1: AverageBuffer, b2: AverageBuffer): AverageBuffer = {
    if (b2.sum != null) {
      b1.sum += b2.sum
    }
    b1.count += b2.count
    b1
  }

  // Transform the output of the reduction/aggregation
  override def finish(reduction: AverageBuffer): Integer = {
    // toInt is safe since no overflows here
    if (reduction.count == 0) null else (reduction.sum / reduction.count).toInt
  }

  // Specifies the Encoder for the intermediate value type
  override def bufferEncoder: Encoder[AverageBuffer] = Encoders.product
  // Specifies the Encoder for the final output value type
  override def outputEncoder: Encoder[Integer] = Encoders.INT

  // ===== GPU RapidsUDAF Implementation =====
  override def getDefaultValue: Array[Scalar] = {
    // Return default values for [sum, count] - these need to match the output of
    // "updateAggregation" and also ideally match the output of initialize in the
    // CPU version.
    // Make sure that if we get an exception we do not leak memory
    closeOnExcept(Scalar.fromNull(DType.INT64)) { nullScalar =>
      Array(
        nullScalar, // null sum (Long)
        Scalar.fromLong(0L) // 0 count (Long)
      )
    }
  }

  override def preProcess(numRows: Int, args: Array[ColumnVector]): Array[ColumnVector] = {
    require(args.length == 1)
    withResource(args.head) { intArg =>
      Array(intArg.castTo(DType.INT64)) // Cast int to long to avoid potential overflow
    }
  }

  override def postProcess(numRows: Int, args: Array[ColumnVector],
      outType: DataType): ColumnVector = {
    // Final step: divide sum by count to get average. Perform element-wise
    // division: sum / count.
    // Note that if the COUNT is 0 the SUM is null.
    // This is to close the input "args" to avoid GPU memory leak.
    val averageCol = withResource(args) { _ =>
      val sumCol = args(0)
      val countCol = args(1)
      sumCol.div(countCol)
    }
    withResource(averageCol) { averageCol =>
      // Cast to integers, no overflows here.
      averageCol.castTo(DType.INT32)
    }
  }

  override def aggBufferTypes(): Array[DataType] = Array(LongType, LongType)

  override def updateAggregation(): RapidsUDAFGroupByAggregation = {
    new RapidsSimpleGroupByAggregation() {
      // "preStep" uses default implementation (pass-through)

      override def reduce(numRows: Int, preStepData: Array[ColumnVector]): Array[Scalar] = {
        // For reduction (no group-by keys), compute SUM and COUNT directly
        val inputCol = preStepData(0)
        // Make sure that we don't leak if there is an exception
        closeOnExcept(inputCol.sum()) { sum =>
          val count = Scalar.fromLong(inputCol.getRowCount - inputCol.getNullCount)
          Array(sum, count)
        }
      }

      override def aggregate(inputIndices: Array[Int]): Array[GroupByAggregationOnColumn] = {
        // For group-by aggregation, create SUM and COUNT operations
        val colIndex = inputIndices(0)
        Array(
          GroupByAggregation.sum().onColumn(colIndex),
          GroupByAggregation.count().onColumn(colIndex)
        )
      }

      override def postStep(aggregatedData: Array[ColumnVector]): Array[ColumnVector] = {
        // cudf count() aggregate produces an integer column, so convert it to
        // Long to match the agg buffer type.
        require(aggregatedData.length == 2, "Expect two columns for postStep during update")
        withResource(aggregatedData) { _ =>
          Array(aggregatedData.head.incRefCount(), aggregatedData(1).castTo(DType.INT64))
        }
      }
    }
  }

  override def mergeAggregation(): RapidsUDAFGroupByAggregation = {
    new RapidsSimpleGroupByAggregation() {
      // "preStep" uses default implementation (pass-through)

      override def reduce(numRows: Int, preStepData: Array[ColumnVector]): Array[Scalar] = {
        // Merge by summing both sum and count columns
        val sumCol = preStepData(0)
        val countCol = preStepData(1)

        // Avoid leaks even if there is an exception when merging countCol
        closeOnExcept(sumCol.sum()) { mergedSum =>
          val mergedCount = countCol.sum()
          Array(mergedSum, mergedCount)
        }
      }

      override def aggregate(inputIndices: Array[Int]): Array[GroupByAggregationOnColumn] = {
        // Merge by summing both columns
        Array(
          GroupByAggregation.sum().onColumn(inputIndices(0)), // sum of sums
          GroupByAggregation.sum().onColumn(inputIndices(1)) // sum of counts
        )
      }

      // "postStep" uses default implementation (pass-through)
    }
  }
}