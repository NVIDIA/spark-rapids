/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "332db"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "350"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import org.apache.spark.SparkUpgradeException

object SparkUpgradeExceptionShims {

  def newSparkUpgradeException(
      version: String,
      message: String,
      cause: Throwable): SparkUpgradeException = {
    new SparkUpgradeException(
      "INCONSISTENT_BEHAVIOR_CROSS_VERSION",
      Map(version -> message),
      cause)
  }

  // Used in tests to compare the class seen in an exception to
  // `SparkUpgradeException` which is private in Spark
  def getSparkUpgradeExceptionClass: Class[_] = {
    classOf[SparkUpgradeException]
  }
}
