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

package com.nvidia.spark.rapids.shims.spark320

import com.nvidia.spark.rapids.{GpuBatchScanExecBase, GpuDataSourceRDD}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanExecBase

// Added to shims since LeafExecNode changed hierarchy in Spark 3.2
case class GpuBatchScanExec(
    output: Seq[AttributeReference],
    @transient scan: Scan) extends DataSourceV2ScanExecBase with GpuBatchScanExecBase {

  override lazy val partitions: Seq[InputPartition] = batch.planInputPartitions()

  override lazy val readerFactory: PartitionReaderFactory = batch.createReaderFactory()

  override lazy val inputRDD: RDD[InternalRow] = {
    new GpuDataSourceRDD(sparkContext, partitions, readerFactory)
  }

  override def doCanonicalize(): GpuBatchScanExec = {
    this.copy(output = output.map(QueryPlan.normalizeExpressions(_, output)))
  }
}
