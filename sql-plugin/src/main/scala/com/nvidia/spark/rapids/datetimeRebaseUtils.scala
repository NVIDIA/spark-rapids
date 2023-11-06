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

package com.nvidia.spark.rapids

import ai.rapids.cudf.{ColumnVector, DType, Scalar}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.SparkShimImpl

import org.apache.spark.sql.catalyst.util.RebaseDateTime
import org.apache.spark.sql.rapids.execution.TrampolineUtil

/**
 * Mirror of Spark's LegacyBehaviorPolicy.
 * <p>
 * This is to provides a stable reference to other Java code in our codebase and also mitigate
 * from Spark's breaking change that will cause issues with our code that uses Spark's
 * LegacyBehaviorPolicy.
 */
sealed abstract class DateTimeRebaseMode(val value: String) extends Serializable

object DateTimeRebaseMode {
  def fromName(name: String): DateTimeRebaseMode = name match {
    case DateTimeRebaseException.value => DateTimeRebaseException
    case DateTimeRebaseLegacy.value => DateTimeRebaseLegacy
    case DateTimeRebaseCorrected.value => DateTimeRebaseCorrected
    case _ => throw new IllegalArgumentException(
      DateTimeRebaseUtils.invalidRebaseModeMessage(name))
  }
}

/**
 * Mirror of Spark's LegacyBehaviorPolicy.EXCEPTION.
 */
case object DateTimeRebaseException extends DateTimeRebaseMode("EXCEPTION")

/**
 * Mirror of Spark's LegacyBehaviorPolicy.LEGACY.
 */
case object DateTimeRebaseLegacy extends DateTimeRebaseMode("LEGACY")

/**
 * Mirror of Spark's LegacyBehaviorPolicy.CORRECTED.
 */
case object DateTimeRebaseCorrected extends DateTimeRebaseMode("CORRECTED")

object DateTimeRebaseUtils {
  def invalidRebaseModeMessage(name: String): String =
    s"Invalid datetime rebase mode: $name (must be either 'EXCEPTION', 'LEGACY', or 'CORRECTED')"

  // Copied from Spark
  private val SPARK_VERSION_METADATA_KEY = "org.apache.spark.version"
  private val SPARK_LEGACY_DATETIME_METADATA_KEY = "org.apache.spark.legacyDateTime"
  private val SPARK_LEGACY_INT96_METADATA_KEY = "org.apache.spark.legacyINT96"
  private val SPARK_TIMEZONE_METADATA_KEY = "org.apache.spark.timeZone"

  private def rebaseModeFromFileMeta(lookupFileMeta: String => String,
      modeByConfig: String,
      minVersion: String,
      metadataKey: String): DateTimeRebaseMode = {

    // If there is no version, we return the mode specified by the config.
    val mode = Option(lookupFileMeta(SPARK_VERSION_METADATA_KEY)).map { version =>
      // Files written by Spark 2.4 and earlier follow the legacy hybrid calendar and we need to
      // rebase the datetime values.
      // Files written by `minVersion` and latter may also need the rebase if they were written
      // with the "LEGACY" rebase mode.
      if (version < minVersion || lookupFileMeta(metadataKey) != null) {
        DateTimeRebaseLegacy
      } else {
        DateTimeRebaseCorrected
      }
    }.getOrElse(DateTimeRebaseMode.fromName(modeByConfig))

    // Check the timezone of the file if the mode is LEGACY.
    if (mode == DateTimeRebaseLegacy) {
      val fileTimeZone = lookupFileMeta(SPARK_TIMEZONE_METADATA_KEY)
      if (fileTimeZone != null && fileTimeZone != "UTC") {
        throw new UnsupportedOperationException(
          "LEGACY datetime rebase mode is only supported for files written in UTC timezone. " +
            s"Actual file timezone: $fileTimeZone")
      }
    }

    mode
  }

  def datetimeRebaseMode(lookupFileMeta: String => String,
      modeByConfig: String): DateTimeRebaseMode = {
    rebaseModeFromFileMeta(lookupFileMeta, modeByConfig, "3.0.0",
      SPARK_LEGACY_DATETIME_METADATA_KEY)
  }

  def int96RebaseMode(lookupFileMeta: String => String,
      modeByConfig: String): DateTimeRebaseMode = {
    rebaseModeFromFileMeta(lookupFileMeta, modeByConfig, "3.1.0",
      SPARK_LEGACY_INT96_METADATA_KEY)
  }

  private[this] def isDateRebaseNeeded(column: ColumnVector,
      startDay: Int): Boolean = {
    // TODO update this for nested column checks
    //  https://github.com/NVIDIA/spark-rapids/issues/1126
    val dtype = column.getType
    if (dtype == DType.TIMESTAMP_DAYS) {
      val hasBad = withResource(Scalar.timestampDaysFromInt(startDay)) {
        column.lessThan
      }
      val anyBad = withResource(hasBad) {
        _.any()
      }
      withResource(anyBad) { _ =>
        anyBad.isValid && anyBad.getBoolean
      }
    } else {
      false
    }
  }

  private[this] def isTimeRebaseNeeded(column: ColumnVector,
      startTs: Long): Boolean = {
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
    } else {
      false
    }
  }

  def isDateRebaseNeededInRead(column: ColumnVector): Boolean =
    isDateRebaseNeeded(column, RebaseDateTime.lastSwitchJulianDay)

  def isTimeRebaseNeededInRead(column: ColumnVector): Boolean =
    isTimeRebaseNeeded(column, RebaseDateTime.lastSwitchJulianTs)

  def isDateRebaseNeededInWrite(column: ColumnVector): Boolean =
    isDateRebaseNeeded(column, RebaseDateTime.lastSwitchGregorianDay)

  def isTimeRebaseNeededInWrite(column: ColumnVector): Boolean =
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
