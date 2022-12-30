/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.FileUtils.withTempPath
import org.apache.spark.sql.rapids.GpuFileSourceScanExec

class GpuFileScanPrunePartitionSuite extends SparkQueryCompareTestSuite with Arm {

  private def testGpuFileScanOutput(
      func: DataFrame => DataFrame,
      exPartitionCols: String*): Unit = {
    withTempPath { file =>
      // Generate partitioned files.
      withCpuSparkSession(spark => {
        import spark.implicits._
        Seq((1, 11, "s1"), (2, 22, "s2"), (3, 33, "s3"), (4, 44, "s4"), (5, 55, "s5"))
          .toDF("a", "b", "c").write.partitionBy("b", "c").parquet(file.getCanonicalPath)
      })

      val confs = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", "parquet")
      // Run the input function
      val df = withGpuSparkSession(spark => func(spark.read.parquet(file.toString)), confs)

      // check the output of GpuFileSourceScan
      val plans = df.queryExecution.executedPlan.collect {
        case plan: GpuFileSourceScanExec =>
          // all the excluded partition columns should not exist in the output
          exPartitionCols.foreach(colName =>
            assert(!plan.output.exists(a => a.name == colName),
              s"Partition column '$colName' is excluded, but found in the output of FileScan")
          )
          plan
      }
      assert(plans.nonEmpty, "GpuFileSourceScan is not found")
    }
  }

  test("Prune Partition Columns when GpuProject(GpuFileSourceScan)") {
    testGpuFileScanOutput(_.groupBy("b").max("a"), "c")
    testGpuFileScanOutput(_.select("a"), "b", "c")
    testGpuFileScanOutput(_.select("a", "c"), "b")
  }

  test("Prune Partition Columns when GpuProject(GpuFilter(GpuFileSourceScan))") {
    testGpuFileScanOutput(_.select("a", "c").filter("a != 1"), "b")
    testGpuFileScanOutput(_.select("a").filter("a != 1"), "b", "c")
  }
}
