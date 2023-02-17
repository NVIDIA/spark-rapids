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

package com.nvidia.spark.rapids

import ai.rapids.cudf.{Rmm, RmmAllocationMode, RmmEventHandler}
import com.nvidia.spark.rapids.jni.RmmSpark
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.rapids.execution.GpuHashJoin

class JoinsRetrySuite
    extends SparkQueryCompareTestSuite with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    if (Rmm.isInitialized) {
      Rmm.shutdown()
    }

    Rmm.initialize(RmmAllocationMode.CUDA_DEFAULT, null, 512 * 1024 * 1024)
    val deviceStorage = new RapidsDeviceMemoryStore()
    val catalog = new RapidsBufferCatalog(deviceStorage)
    RapidsBufferCatalog.setCatalog(catalog)
    val baseEventHandler = new BaseRmmEventHandler()
    RmmSpark.setEventHandler(baseEventHandler)
    RmmSpark.associateCurrentThreadWithTask(1)
    GpuHashJoin.minBatchSize = 10
  }

  override def afterEach(): Unit = {
    GpuHashJoin.doSplitOOM = false
    GpuHashJoin.retryOOMCount = 0
    RapidsBufferCatalog.close()
    if (Rmm.isInitialized) {
      Rmm.shutdown()
    }
  }

  lazy val shuffledJoinConf = new SparkConf()
      .set("spark.sql.autoBroadcastJoinThreshold", "160")
      .set("spark.sql.join.preferSortMergeJoin", "false")
      .set("spark.sql.shuffle.partitions", "2") // hack to try and work around bug in cudf

  def testWithGatherCreateRetry(
      numRetries: Integer,
      doSplit: Boolean,
      dfA: SparkSession => DataFrame,
      dfB: SparkSession => DataFrame,
      conf: SparkConf = new SparkConf()
      ) (fun: (DataFrame, DataFrame) => DataFrame): Unit = {
    GpuHashJoin.retryOOMCount = numRetries
    GpuHashJoin.doSplitOOM = doSplit
    val gpuResult = withGpuSparkSession(spark => {
      val df1 = dfA(spark)
      val df2 = dfB(spark)
      fun(df1, df2).collect()
    }, conf)
    assert(GpuHashJoin.retryOOMCount == 0)
    // Validate correct results.
    val cpuResult = withCpuSparkSession(spark => {
      val df1 = dfA(spark)
      val df2 = dfB(spark)
      fun(df1, df2).collect()
    }, conf)
    compareResults(true, 0.0, cpuResult, gpuResult)
  }

  def testWithGatherNextRetry(
      numRetries: Integer,
      splitCount: Integer,
      dfA: SparkSession => DataFrame,
      dfB: SparkSession => DataFrame,
      conf: SparkConf = new SparkConf()
      ) (fun: (DataFrame, DataFrame) => DataFrame): Unit = {
    GpuHashJoin.retryGatherCount = numRetries
    GpuHashJoin.splitGatherCount = splitCount
    val gpuResult = withGpuSparkSession(spark => {
      val df1 = dfA(spark)
      val df2 = dfB(spark)
      fun(df1, df2).collect()
    }, conf)
    assert(GpuHashJoin.retryGatherCount == 0)
    // Validate correct results.
    val cpuResult = withCpuSparkSession(spark => {
      val df1 = dfA(spark)
      val df2 = dfB(spark)
      fun(df1, df2).collect()
    }, conf)
    compareResults(true, 0.0, cpuResult, gpuResult)
  }

  test("Test hash join with gather create retries") {
    testWithGatherCreateRetry(numRetries = 3, doSplit = false,
      longsDf, biggerLongsDf, conf = shuffledJoinConf) {
      (A, B) => A.join(B, A("longs") === B("longs"))
    }
  }
  test("Test hash join with gather create split") {
    testWithGatherCreateRetry(numRetries = 0, doSplit = true,
      longsDf, biggerLongsDf, conf = shuffledJoinConf) {
      (A, B) => A.join(B, A("longs") === B("longs"))
    }
  }
  test("Test hash join with gather create retries and split") {
    testWithGatherCreateRetry(numRetries = 2, doSplit = true,
      longsDf, biggerLongsDf, conf = shuffledJoinConf) {
      (A, B) => A.join(B, A("longs") === B("longs"))
    }
  }
  test("Test hash semi join with gather create retries") {
    testWithGatherCreateRetry(numRetries = 2, doSplit = false,
      longsDf, biggerLongsDf, conf = shuffledJoinConf) {
      (A, B) => A.join(B, A("longs") === B("longs"), "LeftSemi")
    }
  }
  test("Test hash anti join with gather create retries") {
    testWithGatherCreateRetry(numRetries = 2, doSplit = false,
      longsDf, biggerLongsDf, conf = shuffledJoinConf) {
      (A, B) => A.join(B, A("longs") === B("longs"), "LeftAnti")
    }
  }
  test("Test hash right join with gather create retries and split") {
    testWithGatherCreateRetry(numRetries = 2, doSplit = true,
      longsDf, biggerLongsDf, conf = shuffledJoinConf) {
      (A, B) => A.join(B, A("longs") === B("longs"), "Right")
    }
  }
  test("Test hash full join with gather create retries and split") {
    testWithGatherCreateRetry(numRetries = 2, doSplit = true,
      longsDf, biggerLongsDf, conf = shuffledJoinConf) {
      (A, B) => A.join(B, A("longs") === B("longs"), "FullOuter")
    }
  }
  test("Test hash join with gather next retries") {
    testWithGatherNextRetry(numRetries = 3, splitCount = 0,
      longsDf, biggerLongsDf, conf = shuffledJoinConf) {
      (A, B) => A.join(B, A("longs") === B("longs"))
    }
  }
  test("Test hash join with gather next split") {
    testWithGatherNextRetry(numRetries = 0, splitCount = 1,
      longsDf, biggerLongsDf, conf = shuffledJoinConf) {
      (A, B) => A.join(B, A("longs") === B("longs"))
    }
  }
  test("Test hash semi join with gather next retries") {
    testWithGatherNextRetry(numRetries = 3, splitCount = 0,
      longsDf, biggerLongsDf, conf = shuffledJoinConf) {
      (A, B) => A.join(B, A("longs") === B("longs"), "LeftSemi")
    }
  }
  test("Test hash anti join with gather next retries") {
    testWithGatherNextRetry(numRetries = 1, splitCount = 0,
      longsDf, biggerLongsDf, conf = shuffledJoinConf) {
      (A, B) => A.join(B, A("longs") === B("longs"), "LeftAnti")
    }
  }
  test("Test hash right join with gather next retries and split") {
    testWithGatherNextRetry(numRetries = 2, splitCount = 1,
      longsDf, biggerLongsDf, conf = shuffledJoinConf) {
      (A, B) => A.join(B, A("longs") === B("longs"), "Right")
    }
  }
  test("Test hash full join with gather next retries and split") {
    testWithGatherNextRetry(numRetries = 2, splitCount = 1,
      longsDf, biggerLongsDf, conf = shuffledJoinConf) {
      (A, B) => A.join(B, A("longs") === B("longs"), "FullOuter")
    }
  }

  class BaseRmmEventHandler extends RmmEventHandler {
    override def getAllocThresholds: Array[Long] = null
    override def getDeallocThresholds: Array[Long] = null
    override def onAllocThreshold(l: Long): Unit = {}
    override def onDeallocThreshold(l: Long): Unit = {}

    override def onAllocFailure(size: Long, retryCount: Int): Boolean = {
      false
    }
  }
}
