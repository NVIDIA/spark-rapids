/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructField, StructType}

@scala.annotation.nowarn("msg= is deprecated")
class ScalaUDAFSuite extends SparkQueryCompareTestSuite {

  IGNORE_ORDER_testSparkResultsAreEqual(testName = "Groupby with ScalaUDAF Average",
      groupbyStringsIntsIntsFromCsv) { df =>
    // This is a basic smoke-test of the Scala UDAF framework, not an exhaustive test of
    // the specific UDAF implementation itself.
    // "repartition(7)" is to avoid the Complete mode of the aggregate.
    df.repartition(7).createOrReplaceTempView("groupby_scala_average_udaf_test_table")
    df.sparkSession.udf.register("intAverage", new IntAverageUDAF)
    df.sparkSession.sql(sqlText = """
      SELECT count(c1_int), intAverage(c1_int), max(c2_int), count(c2_int),
             intAverage(c2_int), intAverage(c2_int + 1)
      FROM groupby_scala_average_udaf_test_table
      GROUP BY key_str
    """)
  }

  IGNORE_ORDER_testSparkResultsAreEqual(testName = "Reduction with ScalaUDAF Average",
      groupbyStringsIntsIntsFromCsv) { df =>
    // This is a basic smoke-test of the Scala UDAF framework, not an exhaustive test of
    // the specific UDAF implementation itself.
    // "repartition(7)" is to avoid the Complete mode of the aggregate.
    df.repartition(7).createOrReplaceTempView("reduction_scala_average_udaf_test_table")
    df.sparkSession.udf.register("intAverage", new IntAverageUDAF)
    df.sparkSession.sql(sqlText = """
      SELECT intAverage(c1_int), count(c1_int), max(c1_int), intAverage(c2_int),
             intAverage(c2_int + 1), max(c2_int)
      FROM reduction_scala_average_udaf_test_table
    """)
  }

  private val emptyDfSchema = StructType(Seq(
    StructField("key_str", StringType, nullable = true),
    StructField("c1_int", IntegerType, nullable = true),
    StructField("c2_int", IntegerType, nullable = true))
  )

  IGNORE_ORDER_testSparkResultsAreEqual(
    testName = "Reduction with ScalaUDAF Average on empty dataset",
    ss => emptyRowsDf(ss, emptyDfSchema)) { df =>
    // This is a basic smoke-test of the Scala UDAF framework, not an exhaustive test of
    // the specific UDAF implementation itself.
    df.createOrReplaceTempView("reduction_scala_average_udaf_test_table")
    df.sparkSession.udf.register("intAverage", new IntAverageUDAF)
    df.sparkSession.sql(sqlText =
      """
      SELECT intAverage(c1_int), count(c1_int), intAverage(c2_int), max(c2_int)
      FROM reduction_scala_average_udaf_test_table
    """)
  }
}

@scala.annotation.nowarn("msg= is deprecated")
class IntAverageUDAF extends UserDefinedAggregateFunction with RapidsUDAF {

  // ===== CPU Spark UDAF Implementation =====
  override def inputSchema: StructType = StructType(Seq(StructField("intValue", IntegerType)))

  override def bufferSchema: StructType = StructType(Seq(
    StructField("sum", LongType),
    StructField("count", LongType)
  ))

  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, null) // sum
    buffer.update(1, 0L) // count
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = if(buffer.isNullAt(0)) {
        input.getInt(0).toLong
      } else {
        buffer.getLong(0) + input.getInt(0)
      } // sum
      buffer(1) = buffer.getLong(1) + 1L // count
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (buffer1.isNullAt(0) && !buffer2.isNullAt(0)) {
      buffer1(0) = buffer2.getLong(0)
    } else if (!buffer1.isNullAt(0) && !buffer2.isNullAt(0)) {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0) // sum
    } else {
      // NOOP buffer2(0) is null so buffer1 holds the correct value already
    }
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1) // count
  }

  override def evaluate(buffer: Row): Any = {
    val count = buffer.getLong(1)
    // toInt is safe since no overflows here
    if (count == 0) null else (buffer.getLong(0) / count).toInt
  }

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

  override def getResult(numRows: Int, args: Array[ColumnVector],
      outType: DataType): ColumnVector = {
    // Final step: divide sum by count to get average. Perform element-wise
    // division: sum / count.
    // Note that if the COUNT is 0 the SUM is null.
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

  override def bufferTypes(): Array[DataType] = bufferSchema.map(_.dataType).toArray

  override def updateAggregation(): RapidsUDAFGroupByAggregation = {
    new RapidsSimpleGroupByAggregation() {
      override def preStep(numRows: Int, args: Array[ColumnVector]): Array[ColumnVector] = {
        withResource(args) { _ =>
          require(args.length == 1)
          // Cast the input to Long to avoid potential overflow
          Array(args.head.castTo(DType.INT64))
        }
      }

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

      override def postStep(numRows:Int, args: Array[ColumnVector]): Array[ColumnVector] = {
        // cudf count() aggregate produces an integer column, so convert it to Long
        // to match the agg buffer type.
        withResource(args) { _ =>
          require(args.length == 2,
            s"Expect 2 columns for update postStep, but got ${args.length}")
          // sum, count
          Array(args.head.incRefCount(), args(1).castTo(DType.INT64))
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