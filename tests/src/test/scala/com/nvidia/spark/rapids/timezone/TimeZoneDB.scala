/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.timezone

import java.time.ZoneId

import ai.rapids.cudf.{ColumnVector, DType, HostColumnVector}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.util.DateTimeUtils

object TimeZoneDB {

  def cacheDatabase(): Unit = {}

  /**
   * Interpret a timestamp as a time in the given time zone,
   * and renders that time as a timestamp in UTC
   *
   */
  def fromTimestampToUtcTimestamp(
      inputVector: ColumnVector,
      currentTimeZone: ZoneId): ColumnVector = {
    assert(inputVector.getType == DType.TIMESTAMP_MICROSECONDS)
    val zoneStr = currentTimeZone.getId
    val rowCount = inputVector.getRowCount.toInt
    withResource(inputVector.copyToHost()) { input =>
      withResource(HostColumnVector.builder(DType.TIMESTAMP_MICROSECONDS, rowCount)) { builder =>
        var currRow = 0
        while (currRow < rowCount) {
          val origin = input.getLong(currRow)
          // Spark implementation
          val dist = DateTimeUtils.toUTCTime(origin, zoneStr)
          builder.append(dist)
          currRow += 1
        }
        withResource(builder.build()) { b =>
          b.copyToDevice()
        }
      }
    }
  }

  /**
   * Renders a timestamp in UTC time zone as a timestamp in `desiredTimeZone` time zone
   *
   * @param inputVector     UTC timestamp
   * @param desiredTimeZone desired time zone
   * @return timestamp in the `desiredTimeZone`
   */
  def fromUtcTimestampToTimestamp(
      inputVector: ColumnVector,
      desiredTimeZone: ZoneId): ColumnVector = {
    assert(inputVector.getType == DType.TIMESTAMP_MICROSECONDS)
    val zoneStr = desiredTimeZone.getId
    val rowCount = inputVector.getRowCount.toInt
    withResource(inputVector.copyToHost()) { input =>
      withResource(HostColumnVector.builder(DType.TIMESTAMP_MICROSECONDS, rowCount)) { builder =>
        var currRow = 0
        while (currRow < rowCount) {
          val origin = input.getLong(currRow)
          // Spark implementation
          val dist = DateTimeUtils.fromUTCTime(origin, zoneStr)
          builder.append(dist)
          currRow += 1
        }
        withResource(builder.build()) { b =>
          b.copyToDevice()
        }
      }
    }
  }

  /**
   * Converts timestamp to date since 1970-01-01 at the given zone ID.
   *
   * @return
   */
  def fromTimestampToDate(inputVector: ColumnVector, currentTimeZone: ZoneId): ColumnVector = {
    assert(inputVector.getType == DType.TIMESTAMP_MICROSECONDS)
    val rowCount = inputVector.getRowCount.toInt
    withResource(inputVector.copyToHost()) { input =>
      withResource(HostColumnVector.builder(DType.TIMESTAMP_DAYS, rowCount)) { builder =>
        var currRow = 0
        while (currRow < rowCount) {
          val origin = input.getLong(currRow)
          // Spark implementation
          val dist = DateTimeUtils.microsToDays(origin, currentTimeZone)
          builder.append(dist)
          currRow += 1
        }
        withResource(builder.build()) { b =>
          b.copyToDevice()
        }
      }
    }
  }

  /**
   * Converts date at the given zone ID to microseconds since 1970-01-01 00:00:00Z.
   *
   * @param inputVector     UTC timestamp
   * @param desiredTimeZone desired time zone
   * @return timestamp in the `desiredTimeZone`
   */
  def fromDateToTimestamp(inputVector: ColumnVector, desiredTimeZone: ZoneId): ColumnVector = {
    assert(inputVector.getType == DType.TIMESTAMP_DAYS)
    val rowCount = inputVector.getRowCount.toInt
    withResource(inputVector.copyToHost()) { input =>
      withResource(HostColumnVector.builder(DType.INT64, rowCount)) { builder =>
        var currRow = 0
        while (currRow < rowCount) {
          val origin = input.getInt(currRow)
          // Spark implementation
          val dist = DateTimeUtils.daysToMicros(origin, desiredTimeZone)
          builder.append(dist)
          currRow += 1
        }
        withResource(builder.build()) { b =>
          b.copyToDevice()
        }
      }
    }
  }
}
