/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import org.apache.spark.sql.JsonFunctionsSuite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

class RapidsJsonFunctionsSuite extends JsonFunctionsSuite with RapidsSQLTestsTrait {
  override def beforeAll(): Unit = {
    super.beforeAll()
    SQLConf.get.setConfString("spark.rapids.sql.expression.JsonTuple", "true")
    SQLConf.get.setConfString("spark.rapids.sql.expression.GetJsonObject", "true")
    SQLConf.get.setConfString("spark.rapids.sql.expression.JsonToStructs", "true")
    SQLConf.get.setConfString("spark.rapids.sql.expression.StructsToJson", "true")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SQLConf.get.unsetConf("spark.rapids.sql.expression.JsonTuple")
    SQLConf.get.unsetConf("spark.rapids.sql.expression.GetJsonObject")
    SQLConf.get.unsetConf("spark.rapids.sql.expression.JsonToStructs")
    SQLConf.get.unsetConf("spark.rapids.sql.expression.StructsToJson")
  }
}
