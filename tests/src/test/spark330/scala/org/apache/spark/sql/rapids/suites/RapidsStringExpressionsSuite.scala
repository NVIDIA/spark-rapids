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

import org.apache.spark.sql.catalyst.expressions.{Literal, StringExpressionsSuite, SubstringIndex}
import org.apache.spark.sql.rapids.utils.RapidsTestsTrait
import org.apache.spark.sql.types.StringType

class RapidsStringExpressionsSuite extends StringExpressionsSuite with RapidsTestsTrait {
  test("string substring_index function in rapids") {
    checkEvaluation(
      SubstringIndex(Literal("www.apache.org"), Literal("."), Literal(3)), "www.apache.org")
    checkEvaluation(
      SubstringIndex(Literal("www.apache.org"), Literal("."), Literal(2)), "www.apache")
    checkEvaluation(
      SubstringIndex(Literal("www.apache.org"), Literal("."), Literal(1)), "www")
    checkEvaluation(
      SubstringIndex(Literal("www.apache.org"), Literal("."), Literal(0)), "")
    checkEvaluation(
      SubstringIndex(Literal("www.apache.org"), Literal("."), Literal(-3)), "www.apache.org")
    checkEvaluation(
      SubstringIndex(Literal("www.apache.org"), Literal("."), Literal(-2)), "apache.org")
    checkEvaluation(
      SubstringIndex(Literal("www.apache.org"), Literal("."), Literal(-1)), "org")
    checkEvaluation(
      SubstringIndex(Literal(""), Literal("."), Literal(-2)), "")
    checkEvaluation(
      SubstringIndex(Literal.create(null, StringType), Literal("."), Literal(-2)), null)
    checkEvaluation(SubstringIndex(
      Literal("www.apache.org"), Literal.create(null, StringType), Literal(-2)), null)
    // non ascii chars
    // scalastyle:off
    checkEvaluation(
      SubstringIndex(Literal("大千世界大千世界"), Literal( "千"), Literal(2)), "大千世界大")
    // scalastyle:on
    checkEvaluation(
      SubstringIndex(Literal("www||apache||org"), Literal( "||"), Literal(2)), "www||apache")
  }
}