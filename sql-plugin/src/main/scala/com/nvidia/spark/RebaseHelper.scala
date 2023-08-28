/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

package com.nvidia.spark

import ai.rapids.cudf.{ColumnView, DType, Scalar}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.SparkShimImpl

import org.apache.spark.sql.catalyst.util.RebaseDateTime
import org.apache.spark.sql.rapids.execution.TrampolineUtil

object RebaseHelper {
  private[this] def isDateRebaseNeeded(column: ColumnView, startDay: Int): Boolean = {
    val dtype = column.getType
    if (dtype == DType.TIMESTAMP_DAYS) {
      val hasBad = withResource(Scalar.timestampDaysFromInt(startDay)) {column.lessThan}
      val anyBad = withResource(hasBad) {_.any()}
      withResource(anyBad) { _ => anyBad.isValid && anyBad.getBoolean }
    } else if (dtype == DType.LIST) {
      withResource(column.getChildColumnView(0)) { child =>
        isDateRebaseNeeded(child, startDay)
      }
    } else if(dtype == DType.STRUCT) {
      for (i <- 0 until column.getNumChildren) {
        withResource(column.getChildColumnView(i)) { child =>
          if(isDateRebaseNeeded(child, startDay)) {
            return true
          }
        }
      }
      // if we get here then none of the children needed a rebase and will return false below
    }
   false // default for everything else
  }

  private[this] def isTimeRebaseNeeded(column: ColumnView, startTs: Long): Boolean = {
    val dtype = column.getType
    if (dtype.hasTimeResolution) {
      require(dtype == DType.TIMESTAMP_MICROSECONDS)
      withResource(
        Scalar.timestampFromLong(DType.TIMESTAMP_MICROSECONDS, startTs)) { minGood =>
        withResource(column.lessThan(minGood)) { hasBad =>
          withResource(hasBad.any()) { a =>
            a.isValid && a.getBoolean
          }
        }
      }
    } else if (dtype == DType.LIST) {
      withResource(column.getChildColumnView(0)) { child =>
        isTimeRebaseNeeded(child, startTs)
      }
    } else if (dtype == DType.STRUCT) {
      for (i <- 0 until column.getNumChildren) {
        withResource(column.getChildColumnView(i)) { child =>
          if (isTimeRebaseNeeded(child, startTs)) {
            return true
          }
        }
      }
      // if we get here then none of the children needed a rebase and will return false below
    }
    false // default for everything else
  }

  def isDateRebaseNeededInRead(column: ColumnView): Boolean =
    isDateRebaseNeeded(column, RebaseDateTime.lastSwitchJulianDay)

  def isTimeRebaseNeededInRead(column: ColumnView): Boolean =
    isTimeRebaseNeeded(column, RebaseDateTime.lastSwitchJulianTs)

  def isDateRebaseNeededInWrite(column: ColumnView): Boolean =
    isDateRebaseNeeded(column, RebaseDateTime.lastSwitchGregorianDay)

  def isTimeRebaseNeededInWrite(column: ColumnView): Boolean =
    isTimeRebaseNeeded(column, RebaseDateTime.lastSwitchGregorianTs)

  def newRebaseExceptionInRead(format: String): Exception = {
    val config = if (format == "Parquet") {
      SparkShimImpl.parquetRebaseReadKey
    } else if (format == "Avro") {
      SparkShimImpl.avroRebaseReadKey
    } else {
      throw new IllegalStateException("unrecognized format " + format)
    }
    TrampolineUtil.makeSparkUpgradeException("3.0",
      "reading dates before 1582-10-15 or timestamps before " +
      s"1900-01-01T00:00:00Z from $format files can be ambiguous, as the files may be written by " +
      "Spark 2.x or legacy versions of Hive, which uses a legacy hybrid calendar that is " +
      "different from Spark 3.0+'s Proleptic Gregorian calendar. See more details in " +
      s"SPARK-31404. The RAPIDS Accelerator does not support reading these 'LEGACY' files. To do " +
      s"so you should disable $format support in the RAPIDS Accelerator " +
      s"or set $config to 'CORRECTED' to read the datetime values as it is.",
      null)
  }
}
