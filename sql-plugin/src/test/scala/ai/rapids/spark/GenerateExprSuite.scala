/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package ai.rapids.spark

import org.apache.spark.sql.functions._

class GenerateExprSuite extends SparkQueryCompareTestSuite {
  IGNORE_ORDER_testSparkResultsAreEqual("posexplode(array(floats, more_floats))", mixedFloatDf) {
    frame => frame.select(posexplode(array(col("floats"), col("more_floats") + 100)))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("posexplode(array(strings))", doubleStringsDf) {
    frame => frame.select(posexplode(array(col("more_doubles"), col("doubles"))))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("posexplode(array(longs))", biggerLongsDf) {
    frame => frame.select(posexplode(array(col("longs"), col("more_longs"))))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("posexplode(array(strings), strings)", doubleStringsDf) {
    frame => frame.select(posexplode(array(col("more_doubles"), lit("10"))), col("doubles"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("posexplode(array(lit), strings)", doubleStringsDf) {
    frame => frame.select(posexplode(lit(Array("1", "2", "3"))), col("more_doubles"), 
      col("doubles"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("explode(array(floats, more_floats))", mixedFloatDf) {
    frame => frame.select(explode(array(col("floats"), col("more_floats") + 100)))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("explode(array(strings))", doubleStringsDf) {
    frame => frame.select(explode(array(col("more_doubles"), col("doubles"))))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("explode(array(longs))", biggerLongsDf) {
    frame => frame.select(explode(array(col("longs"), col("more_longs"))))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("explode(array(strings), strings)", doubleStringsDf) {
    frame => frame.select(explode(array(col("more_doubles"), lit("10"))), col("doubles"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("explode(array(lit), strings)", doubleStringsDf) {
    frame => frame.select(explode(lit(Array("1", "2", "3"))), col("more_doubles"), col("doubles"))
  }
}
