/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

// spark-distros:321cdh:
package com.nvidia.spark.rapids.shims.spark321cdh

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.SparkShimImpl
import org.scalatest.FunSuite
import org.apache.spark.sql.types.{DayTimeIntervalType, YearMonthIntervalType}

class Spark321cdhShimsSuite extends FunSuite {
  test("spark shims version") {
    assert(SparkShimImpl.getSparkShimVersion === ClouderaShimVersion(3, 2, 1, "3.2.7171000"))
  }

  test("shuffle manager class") {
    assert(ShimLoader.getRapidsShuffleManagerClass ===
      classOf[com.nvidia.spark.rapids.spark321cdh.RapidsShuffleManager].getCanonicalName)
  }

  test("TypeSig321cdh") {
    val check = TypeSig.DAYTIME + TypeSig.YEARMONTH
    assert(check.isSupportedByPlugin(DayTimeIntervalType()) == true)
    assert(check.isSupportedByPlugin(YearMonthIntervalType()) == true)
  }

}
