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

import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, Suites}

/**
 * Runs all of the test suites with Adaptive Query Execution enabled.
 */
class AdaptiveQueryExecutionSuite extends Suites(
  new AnsiCastOpSuiteAdaptive,
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
  new WindowFunctionSuiteAdaptive)

// we need the AQE suites to have unique names so that they don't overwrite
// surefire results from the original suites
@DoNotDiscover class AnsiCastOpSuiteAdaptive extends AnsiCastOpSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = true
  }
  override  def afterAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = false
  }
}
@DoNotDiscover class CastOpSuiteAdaptive extends CastOpSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = true
  }
  override  def afterAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = false
  }
}
@DoNotDiscover class CsvScanSuiteAdaptive extends CsvScanSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = true
  }
  override  def afterAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = false
  }
}
@DoNotDiscover class ExpandExecSuiteAdaptive extends ExpandExecSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = true
  }
  override  def afterAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = false
  }
}
@DoNotDiscover class GpuBatchUtilsSuiteAdaptive extends GpuBatchUtilsSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = true
  }
  override  def afterAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = false
  }
}
@DoNotDiscover class GpuCoalesceBatchesSuiteAdaptive extends GpuCoalesceBatchesSuite
    with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = true
  }
  override  def afterAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = false
  }
}
@DoNotDiscover class HashAggregatesSuiteAdaptive extends HashAggregatesSuite
    with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = true
  }
  override  def afterAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = false
  }
}
@DoNotDiscover class HashSortOptimizeSuiteAdaptive extends HashSortOptimizeSuite
    with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    super.beforeAll()
    SparkSessionHolder.adaptiveQueryEnabled = true
  }
  override  def afterAll(): Unit = {
    super.afterAll()
    SparkSessionHolder.adaptiveQueryEnabled = false
  }
}
@DoNotDiscover class LimitExecSuiteAdaptive extends LimitExecSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = true
  }
  override  def afterAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = false
  }
}
@DoNotDiscover class OrcScanSuiteAdaptive extends OrcScanSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = true
  }
  override  def afterAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = false
  }
}
@DoNotDiscover class ParquetScanSuiteAdaptive extends ParquetScanSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = true
  }
  override  def afterAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = false
  }
}
@DoNotDiscover class ParquetWriterSuiteAdaptive extends ParquetWriterSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = true
  }
  override  def afterAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = false
  }
}
@DoNotDiscover class ProjectExprSuiteAdaptive extends ProjectExprSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = true
  }
  override  def afterAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = false
  }
}
@DoNotDiscover class SortExecSuiteAdaptive extends SortExecSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = true
  }
  override  def afterAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = false
  }
}
@DoNotDiscover class StringFallbackSuiteAdaptive extends StringFallbackSuite
    with BeforeAndAfterAll {
override def beforeAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = true
  }
  override  def afterAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = false
  }
}
@DoNotDiscover class WindowFunctionSuiteAdaptive extends WindowFunctionSuite
    with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = true
  }
  override  def afterAll(): Unit = {
    SparkSessionHolder.adaptiveQueryEnabled = false
  }
}