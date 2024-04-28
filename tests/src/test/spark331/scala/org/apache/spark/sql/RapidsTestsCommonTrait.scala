/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*** spark-rapids-shim-json-lines
{"spark": "331"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql

import com.nvidia.spark.rapids.TestStats
import org.scalactic.source.Position
import org.scalatest.{Args, Status, Tag}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.RapidsTestConstants.RAPIDS_TEST
import org.apache.spark.sql.catalyst.expressions.ExpressionEvalHelper

trait RapidsTestsCommonTrait
  extends SparkFunSuite
  with ExpressionEvalHelper
  with RapidsTestsBaseTrait {

  override def runTest(testName: String, args: Args): Status = {
    TestStats.suiteTestNumber += 1
    TestStats.offloadRapids = true
    TestStats.startCase(testName)
    val status = super.runTest(testName, args)
    if (TestStats.offloadRapids) {
      TestStats.offloadRapidsTestNumber += 1
      print("'" + testName + "'" + " offload to rapids\n")
    } else {
      // you can find the keyword 'Validation failed for' in function doValidate() in log
      // to get the fallback reason
      print("'" + testName + "'" + " NOT use rapids\n")
      TestStats.addFallBackCase()
    }

    TestStats.endCase(status.succeeds());
    status
  }

  protected def testRapids(testName: String, testTag: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    test(RAPIDS_TEST + testName, testTag: _*)(testFun)
  }
  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    if (shouldRun(testName)) {
      super.test(testName, testTags: _*)(testFun)
    } else {
      super.ignore(testName, testTags: _*)(testFun)
    }
  }
}
