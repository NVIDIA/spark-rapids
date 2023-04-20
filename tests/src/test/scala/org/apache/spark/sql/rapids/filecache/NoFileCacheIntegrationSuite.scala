/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.filecache

import com.nvidia.spark.rapids.SparkQueryCompareTestSuite
import com.nvidia.spark.rapids.shims.GpuBatchScanExec

import org.apache.spark.SparkConf
import org.apache.spark.sql.rapids.GpuFileSourceScanExec

class NoFileCacheIntegrationSuite extends SparkQueryCompareTestSuite {
  private val FILE_SPLITS_PARQUET = "file-splits.parquet"

  def isFileCacheEnabled(conf: SparkConf): Boolean = {
    conf.getBoolean("spark.rapids.filecache.enabled", false)
  }

  test("no filecache no metrics v1") {
    val conf = new SparkConf(false)
        .set("spark.rapids.filecache.enabled", "false")
        .set("spark.sql.sources.useV1SourceList", "parquet")
    withGpuSparkSession({ spark =>
      assume(!isFileCacheEnabled(spark.sparkContext.conf))
      val df = frameFromParquet(FILE_SPLITS_PARQUET)(spark)
      df.collect()
      val gpuScan = df.queryExecution.executedPlan.find(_.isInstanceOf[GpuFileSourceScanExec])
      assert(gpuScan.isDefined)
      assert(!gpuScan.get.metrics.keys.exists(_.startsWith("filecache")))
    }, conf)
  }

  test("no filecache no metrics v2") {
    val conf = new SparkConf(false)
        .set("spark.rapids.filecache.enabled", "false")
        .set("spark.sql.sources.useV1SourceList", "")
    withGpuSparkSession({ spark =>
      assume(!isFileCacheEnabled(spark.sparkContext.conf))
      val df = frameFromParquet(FILE_SPLITS_PARQUET)(spark)
      df.collect()
      val gpuScan = df.queryExecution.executedPlan.find(_.isInstanceOf[GpuBatchScanExec])
      assert(gpuScan.isDefined)
      assert(!gpuScan.get.metrics.keys.exists(_.startsWith("filecache")))
    }, conf)
  }
}
