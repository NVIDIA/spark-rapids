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

import scala.collection.immutable

import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, Suite}

/**
 * Runs all of the test suites with Adaptive Query Execution enabled.
 */
class AdaptiveQueryExecutionSuite extends Suite
  with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = true
  }

  override def nestedSuites: immutable.IndexedSeq[Suite] = {
    // we need to enable AQE before registering the tests so
    // that the test names are correct
    SparkSessionHolder.adaptiveQueryEnabled = true
    try {
      Seq(new AnsiCastOpSuiteAdaptive,
        new CastOpSuiteAdaptive,
        new CsvScanSuiteAdaptive,
        new ExpandExecSuiteAdaptive,
        new GpuBatchUtilsSuiteAdaptive,
        new GpuCoalesceBatchesSuiteAdaptive,
        new HashAggregatesSuiteAdaptive,
        new HashSortOptimizeSuiteAdaptive,
        new LimitExecSuiteAdaptive,
        new OrcScanSuiteAdaptive,
        new ParquetScanSuiteAdaptive,
        new ParquetWriterSuiteAdaptive,
        new ProjectExprSuiteAdaptive,
        new SortExecSuiteAdaptive,
        new StringFallbackSuiteAdaptive,
        new WindowFunctionSuiteAdaptive).toIndexedSeq
    } finally {
      SparkSessionHolder.adaptiveQueryEnabled = false
    }
  }

  override protected def afterAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = false
  }
}

// we need the AQE suites to have unique names so that they don't overwrite
// surefire results from the original suites
@DoNotDiscover class AnsiCastOpSuiteAdaptive extends AnsiCastOpSuite
@DoNotDiscover class CastOpSuiteAdaptive extends CastOpSuite
@DoNotDiscover class CsvScanSuiteAdaptive extends CsvScanSuite
@DoNotDiscover class ExpandExecSuiteAdaptive extends ExpandExecSuite
@DoNotDiscover class GpuBatchUtilsSuiteAdaptive extends GpuBatchUtilsSuite
@DoNotDiscover class GpuCoalesceBatchesSuiteAdaptive extends GpuCoalesceBatchesSuite
@DoNotDiscover class HashAggregatesSuiteAdaptive extends HashAggregatesSuite
@DoNotDiscover class HashSortOptimizeSuiteAdaptive extends HashSortOptimizeSuite
@DoNotDiscover class LimitExecSuiteAdaptive extends LimitExecSuite
@DoNotDiscover class OrcScanSuiteAdaptive extends OrcScanSuite
@DoNotDiscover class ParquetScanSuiteAdaptive extends ParquetScanSuite
@DoNotDiscover class ParquetWriterSuiteAdaptive extends ParquetWriterSuite
@DoNotDiscover class ProjectExprSuiteAdaptive extends ProjectExprSuite
@DoNotDiscover class SortExecSuiteAdaptive extends SortExecSuite
@DoNotDiscover class StringFallbackSuiteAdaptive extends StringFallbackSuite
@DoNotDiscover class WindowFunctionSuiteAdaptive extends WindowFunctionSuite