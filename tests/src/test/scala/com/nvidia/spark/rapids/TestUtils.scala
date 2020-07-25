/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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
import java.nio.ByteBuffer

import ai.rapids.cudf.{BufferType, ColumnVector, DType, HostColumnVector, Table}
import org.scalatest.Assertions

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.vectorized.ColumnarBatch

/** A collection of utility methods useful in tests. */
object TestUtils extends Assertions with Arm {
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

  /** Compre the equality of two `ColumnVector` instances */
  def compareColumns(expected: ColumnVector, actual: ColumnVector): Unit = {
    assertResult(expected.getType)(actual.getType)
    assertResult(expected.getRowCount)(actual.getRowCount)
    withResource(expected.copyToHost()) { e =>
      withResource(actual.copyToHost()) { a =>
        (0L until expected.getRowCount).foreach { i =>
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
              case _ => throw new UnsupportedOperationException("not implemented yet")
            }
          }
        }
      }
    }
  }

  def withGpuSparkSession(conf: SparkConf)(f: SparkSession => Unit): Unit = {
    SparkSession.getActiveSession.foreach(_.close())
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
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      GpuShuffleEnv.setRapidsShuffleManagerInitialized(false, GpuShuffleEnv.RAPIDS_SHUFFLE_CLASS)
    }
  }
}
