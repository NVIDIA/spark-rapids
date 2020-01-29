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

class LimitExecSuite extends SparkQueryCompareTestSuite {

  testSparkResultsAreEqual("limit more than rows", intCsvDf) {
      frame => frame.limit(10).repartition(2)
  }

  testSparkResultsAreEqual("limit less than rows equal to batchSize", intCsvDf,
    conf = makeBatched(1)) {
    frame => frame.limit(1).repartition(2)
  }

  testSparkResultsAreEqual("limit less than rows applied with batches pending", intCsvDf,
    conf = makeBatched(3)) {
    frame => frame.limit(2).repartition(2)
  }
}