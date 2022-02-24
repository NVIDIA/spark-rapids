/*
 * Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector, DType, HostColumnVectorCore, Table}
import com.nvidia.spark.rapids.shims.v2.SparkShimImpl
import java.io.File
import org.scalatest.Assertions

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.vectorized.ColumnarBatch

/** A collection of utility methods useful in tests. */
object TestUtils extends Assertions with Arm {
  // Need to set a legacy config to allow clearing the active session
  private val clearSessionConf = {
    val conf = new SQLConf
    conf.setConfString("spark.sql.legacy.allowModifyActiveSession", "true")
    conf
  }

  def getTempDir(basename: String): File = new File(
    System.getProperty("test.build.data", System.getProperty("java.io.tmpdir", "/tmp")),
    basename)

  /** Compare the equality of two tables */
  def compareTables(expected: Table, actual: Table): Unit = {
    assertResult(expected.getRowCount)(actual.getRowCount)
    assertResult(expected.getNumberOfColumns)(actual.getNumberOfColumns)
    (0 until expected.getNumberOfColumns).foreach { i =>
      compareColumns(expected.getColumn(i), actual.getColumn(i))
    }
  }

  /** Compare the equality of two [[ColumnarBatch]] instances */
  def compareBatches(expected: ColumnarBatch, actual: ColumnarBatch): Unit = {
    assertResult(expected.numRows)(actual.numRows)
    assertResult(expected.numCols)(actual.numCols)
    (0 until expected.numCols).foreach { i =>
      compareColumns(expected.column(i).asInstanceOf[GpuColumnVector].getBase,
        actual.column(i).asInstanceOf[GpuColumnVector].getBase)
    }
  }

  /** Recursively check if the predicate matches in the given plan */
  def findOperator(plan: SparkPlan, predicate: SparkPlan => Boolean): Option[SparkPlan] = {
    SparkShimImpl.findOperators(plan, predicate).headOption
  }

  /** Return final executed plan */
  def getFinalPlan(plan: SparkPlan): SparkPlan = {
    plan match {
      case a: AdaptiveSparkPlanExec =>
        a.executedPlan
      case _ => plan
    }
  }

  /** Compare the equality of two `ColumnVector` instances */
  def compareColumns(expected: ColumnVector, actual: ColumnVector): Unit = {
    assertResult(expected.getType)(actual.getType)
    assertResult(expected.getRowCount)(actual.getRowCount)
    withResource(expected.copyToHost()) { e =>
      withResource(actual.copyToHost()) { a =>
        compareColumns(e, a)
      }
    }
  }

  def compareColumns(e: HostColumnVectorCore, a: HostColumnVectorCore): Unit = {
    assertResult(e.getType)(a.getType)
    assertResult(e.getRowCount)(a.getRowCount)
    assertResult(e.getNumChildren)(a.getNumChildren)
    (0L until e.getRowCount).foreach { i =>
      assertResult(e.isNull(i))(a.isNull(i))
      if (!e.isNull(i)) {
        e.getType match {
          case DType.BOOL8 => assertResult(e.getBoolean(i))(a.getBoolean(i))
          case DType.INT8 => assertResult(e.getByte(i))(a.getByte(i))
          case DType.INT16 => assertResult(e.getShort(i))(a.getShort(i))
          case DType.INT32 => assertResult(e.getInt(i))(a.getInt(i))
          case DType.INT64 => assertResult(e.getLong(i))(a.getLong(i))
          case DType.FLOAT32 => assertResult(e.getFloat(i))(a.getFloat(i))
          case DType.FLOAT64 => assertResult(e.getDouble(i))(a.getDouble(i))
          case DType.STRING => assertResult(e.getJavaString(i))(a.getJavaString(i))
          case dt if dt.isDecimalType && dt.isBackedByLong =>
            assertResult(e.getBigDecimal(i))(a.getBigDecimal(i))
          case DType.LIST | DType.STRUCT =>
            (0 until e.getNumChildren).foreach { childIdx =>
              val eChild = e.getChildColumnView(childIdx)
              val aChild = a.getChildColumnView(childIdx)
              compareColumns(eChild, aChild)
            }
          case _ => throw new UnsupportedOperationException("not implemented yet")
        }
      }
    }
  }

  def withGpuSparkSession(conf: SparkConf)(f: SparkSession => Unit): Unit = {
    TrampolineUtil.cleanupAnyExistingSession()
    val spark = SparkSession.builder()
        .master("local[1]")
        .config(conf)
        .config(RapidsConf.SQL_ENABLED.key, "true")
        .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
        .appName(classOf[GpuPartitioningSuite].getSimpleName)
        .getOrCreate()
    try {
      f(spark)
    } finally {
      spark.stop()
      SQLConf.withExistingConf(clearSessionConf) {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }
  }
}
