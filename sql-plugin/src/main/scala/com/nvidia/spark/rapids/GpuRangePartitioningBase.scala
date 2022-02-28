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

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.shims.v2.ShimExpression

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A GPU accelerated `org.apache.spark.sql.catalyst.plans.physical.Partitioning` that partitions
 * sortable records by range into roughly equal ranges. The ranges are determined by sampling
 * the content of the RDD passed in.
 *
 * @note The actual number of partitions created might not be the same
 * as the `numPartitions` parameter, in the case where the number of sampled records is less than
 * the value of `partitions`.
 *
 * The GpuRangePartitioner is where all of the processing actually happens.
 */
abstract class GpuRangePartitioningBase(
    gpuOrdering: Seq[SortOrder],
    numPartitions: Int) extends GpuExpression with ShimExpression with GpuPartitioning {
  
  override def children: Seq[SortOrder] = gpuOrdering
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def columnarEval(batch: ColumnarBatch): Any =
    throw new IllegalStateException("This cannot be executed")
}
