/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.shims.GpuIntervalUtils
import org.scalatest.FunSuite

import org.apache.spark.sql.catalyst.util.DateTimeConstants.{MICROS_PER_DAY, MICROS_PER_SECOND}
import org.apache.spark.sql.types.{DayTimeIntervalType => DT}

class GpuIntervalUtilsTest extends FunSuite {

  test("testToDayTimeIntervalString 1") {
    val testData = Array(
      (0L, "INTERVAL '0 00:00:00' DAY TO SECOND"),
      (1L, "INTERVAL '0 00:00:00.000001' DAY TO SECOND"),
      (1000L, "INTERVAL '0 00:00:00.001' DAY TO SECOND"),
      (Long.MinValue, "INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND"),
      (-1L, "INTERVAL '-0 00:00:00.000001' DAY TO SECOND"),
      (-123 * MICROS_PER_DAY - 3 * MICROS_PER_SECOND, "INTERVAL '-123 00:00:03' DAY TO SECOND")
    )

    val micros = ColumnVector.fromLongs(testData.map(e => e._1): _*)

    // cast dt to string
    val actual = GpuIntervalUtils.toDayTimeIntervalString(micros, DT.DAY, DT.SECOND)
    val expected = ColumnVector.fromStrings(testData.map(e => e._2): _*)

    // assert
    CudfTestHelper.assertColumnsAreEqual(expected, actual)
  }

  test("testToDayTimeIntervalString 2") {
    val testData = Array(
      (1790552632L * 3600L * 1000000L + 5, "INTERVAL '1790552632' HOUR"),
      (-2350919938L * 3600L * 1000000L - 100 , "INTERVAL '-2350919938' HOUR"),
      (0L, "INTERVAL '00' HOUR")
    )

    val micros = ColumnVector.fromLongs(testData.map(e => e._1): _*)

    // cast dt to string
    val actual = GpuIntervalUtils.toDayTimeIntervalString(micros, DT.HOUR, DT.HOUR)
    val expected = ColumnVector.fromStrings(testData.map(e => e._2): _*)

    // assert
    CudfTestHelper.assertColumnsAreEqual(expected, actual)
  }

  test("testToDayTimeIntervalString 3") {
    val testData = Array(
      (5L * 1000000L, "INTERVAL '05' SECOND"),
      (-2350919938L * 1000000L, "INTERVAL '-2350919938' SECOND"),
      (19938L * 1000000L, "INTERVAL '19938' SECOND"),
      (0L, "INTERVAL '00' SECOND"),
      (Long.MinValue, "INTERVAL '-9223372036854.775808' SECOND"),
      (Long.MaxValue, "INTERVAL '9223372036854.775807' SECOND")
    )

    val micros = ColumnVector.fromLongs(testData.map(e => e._1): _*)

    // cast dt to string
    val actual = GpuIntervalUtils.toDayTimeIntervalString(micros, DT.SECOND, DT.SECOND)
    val expected = ColumnVector.fromStrings(testData.map(e => e._2): _*)

    // assert
    CudfTestHelper.assertColumnsAreEqual(expected, actual)
  }
}
