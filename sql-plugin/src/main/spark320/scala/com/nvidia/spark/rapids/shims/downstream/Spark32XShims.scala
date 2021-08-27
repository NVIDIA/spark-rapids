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

package com.nvidia.spark.rapids.shims.downstream

import com.nvidia.spark.rapids.{ExecChecks, ExecRule, GpuDataSourceRDD, GpuExec, GpuOverrides, SparkPlanMeta, SparkShims, TypeSig}
import com.nvidia.spark.rapids.GpuOverrides.exec
import org.apache.hadoop.fs.FileStatus

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, ShuffledHashJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.{GpuBroadcastExchangeExecBase, GpuCustomShuffleReaderExec}

/**
* Shim base class that can be compiled with every supported 3.2.x
*/
trait Spark32XShims extends SparkShims {
  override def parquetRebaseReadKey: String =
    SQLConf.PARQUET_REBASE_MODE_IN_READ.key
  override def parquetRebaseWriteKey: String =
    SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key
  override def avroRebaseReadKey: String =
    SQLConf.AVRO_REBASE_MODE_IN_READ.key
  override def avroRebaseWriteKey: String =
    SQLConf.AVRO_REBASE_MODE_IN_WRITE.key
  override def parquetRebaseRead(conf: SQLConf): String =
    conf.getConf(SQLConf.PARQUET_REBASE_MODE_IN_READ)
  override def parquetRebaseWrite(conf: SQLConf): String =
    conf.getConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE)

  override def aqeShuffleReaderExec: ExecRule[_ <: SparkPlan] = exec[AQEShuffleReadExec](
    "A wrapper of shuffle query stage",
    ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_64 + TypeSig.ARRAY +
        TypeSig.STRUCT + TypeSig.MAP).nested(), TypeSig.all),
    (exec, conf, p, r) =>
      new SparkPlanMeta[AQEShuffleReadExec](exec, conf, p, r) {
        override def tagPlanForGpu(): Unit = {
          if (!exec.child.supportsColumnar) {
            willNotWorkOnGpu(
              "Unable to replace CustomShuffleReader due to child not being columnar")
          }
        }

        override def convertToGpu(): GpuExec = {
          GpuCustomShuffleReaderExec(childPlans.head.convertIfNeeded(),
            exec.partitionSpecs)
        }
      })
}
