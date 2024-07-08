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

import java.util.TimeZone

import org.apache.spark.sql.catalyst.expressions.JsonExpressionsSuite
import org.apache.spark.sql.rapids.utils.{RapidsJsonConfTrait, RapidsTestsTrait}

class RapidsJsonExpressionsSuite
    extends JsonExpressionsSuite with RapidsTestsTrait with RapidsJsonConfTrait {

  override def beforeAll(): Unit = {
      super.beforeAll()
      // Set timezone to UTC to avoid fallback, so that tests run on GPU to detect bugs
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  }

  override def afterAll(): Unit = {
    TimeZone.setDefault(originalTimeZone)
    super.afterAll()
  }


  test("from_json - invalid data in rapids") {
    val jsonData = """{"a" 1}"""
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    checkEvaluation(
      JsonToStructs(schema, Map.empty, Literal(jsonData), UTC_OPT),
      InternalRow(null)
    )

    val exception = intercept[TestFailedException] {
      checkEvaluation(
        JsonToStructs(schema, Map("mode" -> FailFastMode.name), Literal(jsonData), UTC_OPT),
        InternalRow(null)
      )
    }.getCause
    assert(exception.isInstanceOf[SparkException])
    assert(exception.getMessage.contains(
      "Malformed records are detected in record parsing. Parse Mode: FAILFAST"))
  }
}
