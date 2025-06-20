/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332cdh"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{GpuExpression, GpuPartitioning}

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, OrderedDistribution}
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
case class GpuRangePartitioning(
    gpuOrdering: Seq[SortOrder],
    numPartitions: Int) extends GpuExpression with ShimExpression with GpuPartitioning {

  override def children: Seq[SortOrder] = gpuOrdering
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType
  override def satisfies0(required: Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case OrderedDistribution(requiredOrdering) =>
          // If `ordering` is a prefix of `requiredOrdering`:
          //   Let's say `ordering` is [a, b] and `requiredOrdering` is [a, b, c]. According to the
          //   RangePartitioning definition, any [a, b] in a previous partition must be smaller
          //   than any [a, b] in the following partition. This also means any [a, b, c] in a
          //   previous partition must be smaller than any [a, b, c] in the following partition.
          //   Thus `RangePartitioning(a, b)` satisfies `OrderedDistribution(a, b, c)`.
          //
          // If `requiredOrdering` is a prefix of `ordering`:
          //   Let's say `ordering` is [a, b, c] and `requiredOrdering` is [a, b]. According to the
          //   RangePartitioning definition, any [a, b, c] in a previous partition must be smaller
          //   than any [a, b, c] in the following partition. If there is a [a1, b1] from a
          //   previous partition which is larger than a [a2, b2] from the following partition,
          //   then there must be a [a1, b1 c1] larger than [a2, b2, c2], which violates
          //   RangePartitioning definition. So it's guaranteed that, any [a, b] in a previous
          //   partition must not be greater(i.e. smaller or equal to) than any [a, b] in the
          //   following partition. Thus `RangePartitioning(a, b, c)` satisfies
          //   `OrderedDistribution(a, b)`.
          val minSize = Seq(requiredOrdering.size, gpuOrdering.size).min
          requiredOrdering.take(minSize) == gpuOrdering.take(minSize)
        case c @ ClusteredDistribution(requiredClustering, requireAllClusterKeys, _) =>
          val expressions = gpuOrdering.map(_.child)
          if (requireAllClusterKeys) {
            c.areAllClusterKeysMatched(expressions)
          } else {
            expressions.forall(x => requiredClustering.exists(_.semanticEquals(x)))
          }
        case _ => false
      }
    }
  }

  override def columnarEvalAny(batch: ColumnarBatch): Any =
    throw new IllegalStateException("This cannot be executed")
}
