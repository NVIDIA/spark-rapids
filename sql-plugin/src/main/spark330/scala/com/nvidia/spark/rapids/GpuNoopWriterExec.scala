/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A trait for GPU commands that write to a no-op data source.
 * The data is consumed and discarded.
 */
trait GpuNoopWriterExec extends V2CommandExec with GpuExec {
  override def output: Seq[Attribute] = Nil

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar().map { batch =>
      batch.close()
      new ColumnarBatch(Array.empty, 0)
    }
  }

  override def run(): Seq[InternalRow] = {
    child match {
      case g: GpuExec => g.executeColumnar().foreach(_.close())
      case _ => child.execute().foreach(_ => ())
    }
    Nil
  }
}

case class GpuOverwriteByExpressionExec(
    child: SparkPlan) extends GpuNoopWriterExec

case class GpuAppendDataExec(
    child: SparkPlan) extends GpuNoopWriterExec
