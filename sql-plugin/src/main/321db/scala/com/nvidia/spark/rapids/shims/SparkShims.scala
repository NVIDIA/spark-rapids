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

package com.nvidia.spark.rapids.shims

import com.databricks.sql.execution.window.RunningWindowFunctionExec
import com.databricks.sql.optimizer.PlanDynamicPruningFilters
import com.nvidia.spark.rapids._
import org.apache.hadoop.fs.FileStatus

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.python.WindowInPandasExec
import org.apache.spark.sql.execution.window.WindowExecBase
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.rapids.execution.{GpuBroadcastExchangeExec, GpuSubqueryBroadcastExec}
import org.apache.spark.sql.rapids.execution.shims.{GpuSubqueryBroadcastMeta, ReuseGpuBroadcastExchangeAndSubquery}
import org.apache.spark.sql.rapids.shims.GpuFileScanRDD
import org.apache.spark.sql.types._

object SparkShimImpl extends Spark321PlusDBShims with Spark320until340Shims {

}

// Fallback to the default definition of `deterministic`
trait GpuDeterministicFirstLastCollectShim extends Expression
