/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{NvtxColor, NvtxRange}

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Pull back any data on the GPU to the host so the host can access it.
 */
case class GpuBringBackToHost(child: SparkPlan) extends UnaryExecNode with GpuExec {

  override def output: Seq[Attribute] = child.output
  override def supportsColumnar: Boolean = true

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // Both GPU and CPU code expects this to close the incoming batch.
    AutoCloseColumnBatchIterator.map[ColumnarBatch](child.executeColumnar(), b => {
      val range = new NvtxRange("BringBackToHost", NvtxColor.RED)
      try {
        val hostColumns = (0 until b.numCols()).map(
          i => b.column(i).asInstanceOf[GpuColumnVector].copyToHost())
        new ColumnarBatch(hostColumns.toArray)
      } finally {
        range.close()
        b.close()
        // Leaving the GPU for a while
        GpuSemaphore.releaseIfNecessary(TaskContext.get())
      }
    })
  }
}
