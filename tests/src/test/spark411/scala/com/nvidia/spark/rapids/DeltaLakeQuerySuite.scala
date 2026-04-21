/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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
{"spark": "411"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.delta.{DeltaProvider, NoDeltaProvider}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.{ExecutionPlanCaptureCallback, GpuFileSourceScanExec}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims._

class DeltaLakeQuerySuiteSpark411 extends SparkQueryCompareTestSuite {
  private def deltaConf: SparkConf = new SparkConf()
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

  private def withGpuDeltaSparkSession[U](f: SparkSession => U): U = {
    TrampolineUtil.cleanupAnyExistingSession()
    val spark = getBuilder()
      .master("local[1]")
      .config(deltaConf)
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .config("spark.sql.queryExecutionListeners",
        "org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback")
      .config(RapidsConf.SQL_ENABLED.key, "true")
      .config(RapidsConf.EXPLAIN.key, "ALL")
      .appName("Spark Rapids Delta 411 smoke tests")
      .getOrCreate()
    try {
      f(spark)
    } finally {
      spark.stop()
      TrampolineUtil.cleanupAnyExistingSession()
    }
  }

  test("delta provider resolves to a real implementation on spark 411") {
    val provider = DeltaProvider()
    assert(provider ne NoDeltaProvider)
    assert(provider.getClass.getName.contains("Delta41xProvider"))
  }

  test("delta read and write execute on spark 411") {
    withTempPath { path =>
      withGpuDeltaSparkSession { spark =>
        import spark.implicits._

        val input = Seq((0L, "zero"), (1L, "one"), (2L, "two")).toDF("id", "value")

        ExecutionPlanCaptureCallback.startCapture()
        input.write.format("delta").mode("overwrite").save(path.getAbsolutePath)
        val writePlans = ExecutionPlanCaptureCallback.getResultsWithTimeout()
        assert(writePlans.nonEmpty, "expected a captured write plan")
        ExecutionPlanCaptureCallback.assertContains(writePlans.head, "GpuRapidsDeltaWrite")

        val readDf = spark.read.format("delta").load(path.getAbsolutePath)
          .orderBy("id")
        val readPlan = readDf.queryExecution.executedPlan
        val gpuScan = TestUtils.findOperator(readPlan, _.isInstanceOf[GpuFileSourceScanExec])
        assert(gpuScan.isDefined, s"expected a GPU file scan in plan: $readPlan")

        val result = readDf.as[(Long, String)]
          .collect()
          .toSeq

        assertResult(Seq((0L, "zero"), (1L, "one"), (2L, "two")))(result)
      }
    }
  }
}
