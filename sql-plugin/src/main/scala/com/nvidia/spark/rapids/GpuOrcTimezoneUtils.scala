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

import ai.rapids.cudf.{ColumnView, DType, Scalar, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq
import java.time.{LocalDateTime, ZoneId}
import java.util.Optional
import scala.collection.mutable.ArrayBuffer

object GpuOrcTimezoneUtils {

  /**
   * Get the offset in microseconds for 2025-01-01 between JVM timezone and UTC timezone.
   * @param jvmTz the JVM timezone to calculate the offset for
   * @return the offset in microseconds
   *         between the JVM timezone and UTC timezone for 2025-01-01
   *         This is used to rebase the timestamp columns in the input table.
   */
  private def getOffsetForJanuaryFirst2015(jvmTz: ZoneId): Long = {
    val t1 = LocalDateTime.of(2015, 1, 1, 0, 0, 0).atZone(jvmTz).toInstant.getEpochSecond
    val t2 = LocalDateTime.of(2015, 1, 1, 0, 0, 0).atZone(ZoneId.of("UTC")).toInstant.getEpochSecond
    val diffMicros: Long = (t2 - t1) * 1000000L // convert seconds to microseconds
    diffMicros
  }

  /**
   * Recursively rebase the timestamp columns in the input column view to the target timezone.
   * It handles nested types: list and struct.
   * The rebase logic is simple: just subtract the offset in microseconds between the
   * target timezone and UTC timezone.
   * For more details about the rebase logic, please refer to:
   * https://github.com/apache/orc/blob/rel/release-1.9.1/
   * java/core/src/java/org/apache/orc/impl/TreeReaderFactory.java#L1157
   * `TimestampTreeReader.getBaseTimestamp` generates the base timestamp with JVM default timezone.
   * `threadLocalDateFormat.get().setTimeZone(writerTimeZone);`
   * The above writerTimeZone is not the timezone in the ORC file stripe footer,
   * it is the default JVM timezone.
   * `TimestampTreeReader.readTimestamp` applies the diff:
   *   `long millis = (data.next() + base_timestamp)`
   * Note: the input timestamp columns are read as in the UTC timezone.
   *
   */
  private def rebaseTimestampRecursively(
                                          col: ColumnView,
                                          toZoneId: ZoneId,
                                          toClose: ArrayBuffer[ColumnView],
                                          diffMicros: Long): ColumnView = {

    // Util function to add a view to the buffer "toClose".
    val addToClose = (v: ColumnView) => {
      toClose += v
      v
    }

    val dType = col.getType
    if (dType.hasTimeResolution) {
      assert(dType == DType.TIMESTAMP_MICROSECONDS,
        s"Only TIMESTAMP_MICROSECONDS is supported, but got $dType")

      // 1. timestamp type, rebase timestamp column
      withResource(col.bitCastTo(DType.INT64)) { longs =>
        withResource(Scalar.fromLong(diffMicros)) { offsetScalar =>
          withResource(longs.sub(offsetScalar)) { rebased =>
            rebased.castTo(DType.TIMESTAMP_MICROSECONDS)
          }
        }
      }
    } else if (dType == DType.LIST) {
      // 2. nest list type
      val child = addToClose(col.getChildColumnView(0))
      val newChild = rebaseTimestampRecursively(child, toZoneId, toClose, diffMicros)
      if (newChild != child) {
        col.replaceListChild(addToClose(newChild))
      } else {
        col
      }
    } else if (dType == DType.STRUCT) {
      // 3. nest struct type
      val newViews = (0 until col.getNumChildren).safeMap { i =>
        val child = addToClose(col.getChildColumnView(i))
        val newChild = rebaseTimestampRecursively(child, toZoneId, toClose, diffMicros)
        if (newChild != child) {
          addToClose(newChild)
        }
        newChild
      }
      val opNullCount = Optional.of(col.getNullCount.asInstanceOf[java.lang.Long])
      new ColumnView(col.getType, col.getRowCount, opNullCount, col.getValid,
        col.getOffsets, newViews.toArray)
    } else {
      // 4. other types, no need to rebase
      col
    }
  }

  /**
   * Rebase the timestamp columns in the input table to the system default timezone.
   * If the system's default timezone is UTC, it returns the input table as it is.
   *
   * @param input the input table, it will be closed after returning
   * @return a new table with rebased timestamp columns
   */
  def rebaseTimeZone(input: Table): Table = {
    val toZoneId = ZoneId.systemDefault()

    if (toZoneId == ZoneId.of("UTC")) {
      // UTC timezone, no need to rebase
      return input
    }

    // get the offset in microseconds for 2015-01-01 between JVM timezone and UTC timezone
    val diffMicros = getOffsetForJanuaryFirst2015(toZoneId)

    withResource(input) { _ =>
      val newColumns = (0 until input.getNumberOfColumns).safeMap { colIdx =>
        val col = input.getColumn(colIdx)
        withResource(new ArrayBuffer[ColumnView]) { toClose =>
          val rebased = rebaseTimestampRecursively(col, toZoneId, toClose, diffMicros)
          if (col == rebased) {
            // no change
            col.incRefCount()
          } else {
            // rebased, copy the new column
            toClose += rebased
            rebased.copyToColumnVector()
          }
        }
      }

      withResource(newColumns) { _ =>
        new Table(newColumns: _*)
      }
    }
  }
}
