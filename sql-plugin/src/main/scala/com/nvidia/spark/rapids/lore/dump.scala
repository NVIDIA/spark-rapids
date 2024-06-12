import com.nvidia.spark.rapids.GpuExec
import com.nvidia.spark.rapids.lore.LoreOutputInfo
import org.apache.hadoop.fs.Path

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch

/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

case class GpuLoreDumpExec(child: SparkPlan, loreOutputInfo: LoreOutputInfo) extends UnaryExecNode
  with GpuExec {
  override def output: Seq[Attribute] = child.output

  override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException("GpuLoreDumpExec does not support row mode")
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = ???

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    GpuLoreDumpExec(newChild, loreOutputInfo)

  def dumpNextBatch(batch: ColumnarBatch) = {

  }
}

object GpuLoreDumpExec {
  def dumpNextBatch(batchId: Long, batch: ColumnarBatch, rootPath: Path) = {

  }
}

