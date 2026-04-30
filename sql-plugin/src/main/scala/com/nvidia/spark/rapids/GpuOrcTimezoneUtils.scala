/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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
import com.nvidia.spark.rapids.jni.GpuTimeZoneDB
import java.time.{LocalDateTime, ZoneId}
import java.util.Optional
import scala.collection.mutable.ArrayBuffer

object GpuOrcTimezoneUtils {

  /**
   * Returns the fixed ORC base-timestamp offset, in microseconds, between the JVM timezone and
   * UTC at 2015-01-01 00:00:00.
   *
   * ORC's same-timezone read path derives a base timestamp from the reader/JVM timezone using
   * this reference instant, so the Scala fast path mirrors that behavior with the same fixed
   * offset.
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

      val asLong = addToClose(col.bitCastTo(DType.INT64))
      withResource(Scalar.fromLong(diffMicros)) { diffScalar =>
        val shifted = addToClose(asLong.sub(diffScalar))
        // bitCastTo returns a view into shifted's memory; shifted must stay alive via toClose
        shifted.bitCastTo(DType.TIMESTAMP_MICROSECONDS)
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
   * Applies the reader/JVM timezone base-offset rebase used by ORC's same-timezone path.
   *
   * The input table has already been decoded as if timestamp values were UTC
   * (`ignoreTimezoneInStripeFooter`). When the caller decides that no writer-to-reader
   * timezone conversion is needed, this helper applies the fixed reader-timezone offset derived
   * from ORC's base-timestamp logic and recursively rebases timestamp columns, including nested
   * lists and structs.
   *
   * If the reader timezone is UTC, the input table is returned unchanged.
   *
   * @param input input table; ownership is transferred to this method
   * @return a new table with timestamp columns rebased using the reader timezone base offset
   */
  private def rebaseWithReaderTimezoneBaseOffset(input: Table): Table = {
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

  /**
   * Rebase ORC timestamps considering writer and reader timezones.
   *
   * When the writer timezone from the ORC stripe footer differs from the reader timezone
   * (JVM default), uses the JNI kernel `GpuTimeZoneDB.convertOrcTimezones` to perform
   * timezone conversion following ORC's `SerializationUtils.convertBetweenTimezones`.
   *
   * When the writer timezone is empty or the caller chooses the same-timezone fast path, falls
   * back to the reader-timezone base-offset rebase via
   * `rebaseWithReaderTimezoneBaseOffset`.
   *
   * @param input the input table (timestamps read as UTC via ignoreTimezoneInStripeFooter)
   * @param writerTimezone the writer timezone from the ORC stripe footer
   * @return table with rebased timestamp columns; input is closed
   */
  def rebaseOrcTimestamps(input: Table, writerTimezone: String): Table = {
    val readerTz = ZoneId.systemDefault().getId
    val writerTz = if (writerTimezone.isEmpty) readerTz else writerTimezone

    if (hasSameTimezoneRules(writerTz, readerTz)) {
      rebaseWithReaderTimezoneBaseOffset(input)
    } else {
      rebaseWithWriterTimezone(input, writerTz, readerTz)
    }
  }

  private def hasSameTimezoneRules(tz1: String, tz2: String): Boolean = {
    val zone1 = java.util.TimeZone.getTimeZone(tz1)
    val zone2 = java.util.TimeZone.getTimeZone(tz2)
    zone1.hasSameRules(zone2)
  }

  /**
   * Rebase timestamps when writer and reader timezones differ.
   *
   * cuDF reads ORC timestamps with `ignoreTimezoneInStripeFooter`, so the base_timestamp
   * is computed in UTC. ORC Java computes base_timestamp in the *writer* timezone, so the
   * millis passed to `convertBetweenTimezones` already encode the writer TZ base offset.
   *
   * To match ORC Java, we first apply the writer TZ base offset (same logic as the
   * same-TZ path but using the writer TZ instead of the reader TZ), then apply the
   * cross-TZ delta via the JNI `convertOrcTimezones` kernel.
   */
  private def rebaseWithWriterTimezone(
      input: Table, writerTz: String, readerTz: String): Table = {
    val writerZoneId = ZoneId.of(writerTz)
    val writerBaseOffsetMicros = getOffsetForJanuaryFirst2015(writerZoneId)

    withResource(input) { _ =>
      withResource(GpuTimeZoneDB.buildOrcTimezoneContext(writerTz, readerTz)) { tzCtx =>
        val newColumns = (0 until input.getNumberOfColumns).safeMap { colIdx =>
          val col = input.getColumn(colIdx)
          val dType = col.getType
          if (dType.hasTimeResolution) {
            GpuTimeZoneDB.convertOrcTimezones(col, writerBaseOffsetMicros, tzCtx)
          } else if (dType == DType.LIST || dType == DType.STRUCT) {
            withResource(new ArrayBuffer[ColumnView]) { toClose =>
              val rebased = rebaseNestedWithWriterTimezone(
                col, tzCtx, writerBaseOffsetMicros, toClose)
              if (rebased eq col) {
                col.incRefCount()
              } else {
                toClose += rebased
                rebased.copyToColumnVector()
              }
            }
          } else {
            col.incRefCount()
          }
        }
        withResource(newColumns) { _ =>
          new Table(newColumns: _*)
        }
      }
    }
  }

  private def rebaseNestedWithWriterTimezone(
      col: ColumnView,
      tzCtx: GpuTimeZoneDB.OrcTimezoneContext,
      writerBaseOffsetMicros: Long,
      toClose: ArrayBuffer[ColumnView]): ColumnView = {
    val addToClose = (v: ColumnView) => { toClose += v; v }
    val dType = col.getType

    if (dType.hasTimeResolution) {
      GpuTimeZoneDB.convertOrcTimezones(col, writerBaseOffsetMicros, tzCtx)
    } else if (dType == DType.LIST) {
      val child = addToClose(col.getChildColumnView(0))
      val newChild = rebaseNestedWithWriterTimezone(
        child, tzCtx, writerBaseOffsetMicros, toClose)
      if (newChild ne child) {
        col.replaceListChild(addToClose(newChild))
      } else {
        col
      }
    } else if (dType == DType.STRUCT) {
      val newViews = (0 until col.getNumChildren).safeMap { i =>
        val child = addToClose(col.getChildColumnView(i))
        val newChild = rebaseNestedWithWriterTimezone(
          child, tzCtx, writerBaseOffsetMicros, toClose)
        if (newChild ne child) addToClose(newChild)
        newChild
      }
      val opNullCount = Optional.of(col.getNullCount.asInstanceOf[java.lang.Long])
      new ColumnView(col.getType, col.getRowCount, opNullCount, col.getValid,
        col.getOffsets, newViews.toArray)
    } else {
      col
    }
  }
}
