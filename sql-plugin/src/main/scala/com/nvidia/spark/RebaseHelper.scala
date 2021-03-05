/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector, DType, Scalar}
import com.nvidia.spark.rapids.{Arm, ShimLoader}

import org.apache.spark.sql.catalyst.util.RebaseDateTime
import org.apache.spark.sql.rapids.execution.TrampolineUtil

object RebaseHelper extends Arm {
  private[this] def isDateTimeRebaseNeeded(column: ColumnVector,
                                                startDay: Int,
                                                startTs: Long): Boolean = {
    // TODO update this for nested column checks
    //  https://github.com/NVIDIA/spark-rapids/issues/1126
    val dtype = column.getType
    if (dtype == DType.TIMESTAMP_DAYS) {
      withResource(Scalar.timestampDaysFromInt(startDay)) { minGood =>
        withResource(column.lessThan(minGood)) { hasBad =>
          withResource(hasBad.any()) { a =>
            a.getBoolean
          }
        }
      }
    } else if (dtype.isTimestampType) {
      // TODO - https://github.com/NVIDIA/spark-rapids/issues/1130 to properly handle
      // TIMESTAMP_MILLIS, for use require so we fail if that happens
      require(dtype == DType.TIMESTAMP_MICROSECONDS)
      withResource(
        Scalar.timestampFromLong(DType.TIMESTAMP_MICROSECONDS, startTs)) { minGood =>
        withResource(column.lessThan(minGood)) { hasBad =>
          withResource(hasBad.any()) { a =>
            a.getBoolean
          }
        }
      }
    } else {
      false
    }
  }

  def isDateTimeRebaseNeededWrite(column: ColumnVector): Boolean =
    isDateTimeRebaseNeeded(column,
      RebaseDateTime.lastSwitchGregorianDay,
      RebaseDateTime.lastSwitchGregorianTs)

  def isDateTimeRebaseNeededRead(column: ColumnVector): Boolean =
    isDateTimeRebaseNeeded(column,
      RebaseDateTime.lastSwitchJulianDay,
      RebaseDateTime.lastSwitchJulianTs)

  def newRebaseExceptionInRead(format: String): Exception = {
    val config = if (format == "Parquet") {
      ShimLoader.getSparkShims.parquetRebaseReadKey
    } else if (format == "Avro") {
      ShimLoader.getSparkShims.avroRebaseReadKey
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
