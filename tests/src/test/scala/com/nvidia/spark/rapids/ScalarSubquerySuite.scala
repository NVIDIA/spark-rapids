/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.GpuScalarSubquery

class ScalarSubquerySuite extends SparkQueryCompareTestSuite {

  private def checkExecPlan(plan: SparkPlan): Unit = {
    if (!plan.conf.getAllConfs(RapidsConf.SQL_ENABLED.key).toBoolean) return
    plan.find(_.expressions.exists(e => e.find(_.isInstanceOf[GpuScalarSubquery]).nonEmpty)) match {
      case Some(plan) =>
        val subqueryExec = plan.expressions.collectFirst {
          case e if e.find(_.isInstanceOf[GpuScalarSubquery]).nonEmpty =>
            e.find(_.isInstanceOf[GpuScalarSubquery]).get
        }.get.asInstanceOf[GpuScalarSubquery].plan
      case None =>
        throw new AssertionError("Could not find GpuScalarSubquery expression among plans")
    }
  }

  testSparkResultsAreEqual("Uncorrelated Scalar Subquery", longsFromCSVDf,
    repart = 0) {
    frame => {
      frame.createOrReplaceTempView("table")
      val ret = frame.sparkSession.sql(
        "SELECT longs, (SELECT max(more_longs) FROM table) FROM table")
      checkExecPlan(ret.queryExecution.executedPlan)
      ret
    }
  }
}
