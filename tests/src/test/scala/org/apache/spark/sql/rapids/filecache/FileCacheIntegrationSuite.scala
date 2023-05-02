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
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.rapids.GpuFileSourceScanExec

class FileCacheIntegrationSuite extends SparkQueryCompareTestSuite {
  import com.nvidia.spark.rapids.GpuMetric._

  private val FILE_SPLITS_PARQUET = "file-splits.parquet"
  private val MAP_OF_STRINGS_PARQUET = "map_of_strings.snappy.parquet"
  private val SCHEMA_CAN_PRUNE_ORC = "schema-can-prune.orc"
  private val SCHEMA_CANT_PRUNE_ORC = "schema-cant-prune.orc"

  // File cache only supported on Spark 3.2+
  assumeSpark320orLater

  def isFileCacheEnabled(conf: SparkConf): Boolean = {
    conf.getBoolean("spark.rapids.filecache.enabled", false)
  }

  test("filecache metrics v1 Parquet") {
    val conf = new SparkConf(false)
        .set("spark.rapids.filecache.enabled", "true")
        .set("spark.sql.sources.useV1SourceList", "parquet")
    withGpuSparkSession({ spark =>
      assume(isFileCacheEnabled(spark.sparkContext.conf))
      var df = frameFromParquet(FILE_SPLITS_PARQUET)(spark)
      df.collect()
      var gpuScan = df.queryExecution.executedPlan.find(_.isInstanceOf[GpuFileSourceScanExec])
      assert(gpuScan.isDefined)
      checkMetricsFullMiss(gpuScan.get.metrics)
      df = frameFromParquet(FILE_SPLITS_PARQUET)(spark)
      df.collect()
      gpuScan = df.queryExecution.executedPlan.find(_.isInstanceOf[GpuFileSourceScanExec])
      assert(gpuScan.isDefined)
      checkMetricsFullHit(gpuScan.get.metrics)
    }, conf)
  }

  test("filecache metrics v2 Parquet") {
    val conf = new SparkConf(false)
        .set("spark.rapids.filecache.enabled", "true")
        .set("spark.sql.sources.useV1SourceList", "")
    withGpuSparkSession({ spark =>
      assume(isFileCacheEnabled(spark.sparkContext.conf))
      var df = frameFromParquet(MAP_OF_STRINGS_PARQUET)(spark)
      df.collect()
      var gpuScan = df.queryExecution.executedPlan.find(_.isInstanceOf[GpuBatchScanExec])
      assert(gpuScan.isDefined)
      checkMetricsFullMiss(gpuScan.get.metrics)
      df = frameFromParquet(MAP_OF_STRINGS_PARQUET)(spark)
      df.collect()
      gpuScan = df.queryExecution.executedPlan.find(_.isInstanceOf[GpuBatchScanExec])
      assert(gpuScan.isDefined)
      checkMetricsFullHit(gpuScan.get.metrics)
    }, conf)
  }

  test("filecache metrics v1 ORC") {
    val conf = new SparkConf(false)
        .set("spark.rapids.filecache.enabled", "true")
        .set("spark.sql.sources.useV1SourceList", "orc")
    withGpuSparkSession({ spark =>
      assume(isFileCacheEnabled(spark.sparkContext.conf))
      val df = frameFromOrc(SCHEMA_CAN_PRUNE_ORC)(spark)
      df.collect()
      val gpuScan = df.queryExecution.executedPlan.find(_.isInstanceOf[GpuFileSourceScanExec])
      assert(gpuScan.isDefined)
      // no metrics for ORC yet
      assert(!gpuScan.get.metrics.keys.exists(_.startsWith("filecache")))
    }, conf)
  }

  test("filecache metrics v2 ORC") {
    val conf = new SparkConf(false)
        .set("spark.rapids.filecache.enabled", "true")
        .set("spark.sql.sources.useV1SourceList", "")
    withGpuSparkSession({ spark =>
      assume(isFileCacheEnabled(spark.sparkContext.conf))
      val df = frameFromOrc(SCHEMA_CANT_PRUNE_ORC)(spark)
      df.collect()
      val gpuScan = df.queryExecution.executedPlan.find(_.isInstanceOf[GpuBatchScanExec])
      assert(gpuScan.isDefined)
      // no metrics for ORC yet
      assert(!gpuScan.get.metrics.keys.exists(_.startsWith("filecache")))
    }, conf)
  }

  private def checkMetricsFullMiss(metrics: Map[String, SQLMetric]): Unit = {
    assertResult(0)(metrics(FILECACHE_FOOTER_HITS).value)
    assertResult(0)(metrics(FILECACHE_FOOTER_HITS_SIZE).value)
    assertResult(1)(metrics(FILECACHE_FOOTER_MISSES).value)
    assert(metrics(FILECACHE_FOOTER_MISSES_SIZE).value > 0)
    assertResult(0)(metrics(FILECACHE_DATA_RANGE_HITS).value)
    assertResult(0)(metrics(FILECACHE_DATA_RANGE_HITS_SIZE).value)
    assert(metrics(FILECACHE_DATA_RANGE_MISSES).value > 0)
    assert(metrics(FILECACHE_DATA_RANGE_MISSES_SIZE).value > 0)
    assert(metrics.contains(FILECACHE_FOOTER_READ_TIME))
    assert(metrics.contains(FILECACHE_DATA_RANGE_READ_TIME))
  }

  private def checkMetricsFullHit(metrics: Map[String, SQLMetric]): Unit = {
    assertResult(1)(metrics(FILECACHE_FOOTER_HITS).value)
    assert(metrics(FILECACHE_FOOTER_HITS_SIZE).value > 0)
    assertResult(0)(metrics(FILECACHE_FOOTER_MISSES).value)
    assertResult(0)(metrics(FILECACHE_FOOTER_MISSES_SIZE).value)
    assert(metrics(FILECACHE_DATA_RANGE_HITS).value > 0)
    assert(metrics(FILECACHE_DATA_RANGE_HITS_SIZE).value > 0)
    assertResult(0)(metrics(FILECACHE_DATA_RANGE_MISSES).value)
    assertResult(0)(metrics(FILECACHE_DATA_RANGE_MISSES_SIZE).value)
    assert(metrics.contains(FILECACHE_FOOTER_READ_TIME))
    assert(metrics.contains(FILECACHE_DATA_RANGE_READ_TIME))
  }
}
