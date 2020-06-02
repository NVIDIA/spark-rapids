/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

import org.apache.spark.sql.functions.col

class UnionExprSuite extends SparkQueryCompareTestSuite {

  testSparkResultsAreEqual("Test union doubles", doubleDf) {
    frame => frame.union(frame)
  }

  testSparkResultsAreEqual("Test unionAll doubles", doubleDf) {
    frame => frame.unionAll(frame)
  }

  testSparkResultsAreEqual("Test unionByName doubles", doubleDf) {
    frame => frame.unionByName(frame.select(col("more_doubles"), col("doubles")))
  }
}
