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
package com.nvidia.spark.rapids

import org.scalatest.{BeforeAndAfterAll, Suites}

/**
 * Runs all of the test suites with Adaptive Query Execution enabled.
 */
class AdaptiveQueryExecutionSuite extends Suites (
    new AnsiCastOpSuite,
    new CastOpSuite,
    new CsvScanSuite,
    new ExpandExecSuite,
    new GpuBatchUtilsSuite,
    new GpuCoalesceBatchesSuite,
    new HashAggregatesSuite,
    new HashSortOptimizeSuite,
    new LimitExecSuite,
    new OrcScanSuite,
    new ParquetScanSuite,
    new ParquetWriterSuite,
    new ProjectExprSuite,
    new SortExecSuite,
    new StringFallbackSuite,
    new WindowFunctionSuite)
  with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = true
  }

  override protected def afterAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = false
  }
}

