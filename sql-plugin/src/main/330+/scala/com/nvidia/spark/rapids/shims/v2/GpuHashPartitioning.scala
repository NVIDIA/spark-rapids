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

package com.nvidia.spark.rapids.shims.v2

import com.nvidia.spark.rapids.GpuHashPartitioningBase

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution}

case class GpuHashPartitioning(expressions: Seq[Expression], numPartitions: Int)
  extends GpuHashPartitioningBase(expressions, numPartitions) {

  override def satisfies0(required: Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case ClusteredDistribution(requiredClustering, _) =>
          expressions.forall(x => requiredClustering.exists(_.semanticEquals(x)))
        case _ => false
      }
    }
  }

 // override def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec =
 //   GpuHashShuffleSpec(this, distribution)
}
/*
/** We have to create the GPU version because it has the partitioning object inside. */
case class GpuHashShuffleSpec(
    partitioning: GpuHashPartitioning,
    distribution: ClusteredDistribution) extends ShuffleSpec {
  lazy val hashKeyPositions: Seq[mutable.BitSet] =
    createHashKeyPositions(distribution.clustering, partitioning.expressions)

  override def isCompatibleWith(other: ShuffleSpec): Boolean = other match {
    case SinglePartitionShuffleSpec => partitioning.numPartitions == 1
    case otherHashSpec @ GpuHashShuffleSpec(otherPartitioning, otherDistribution) =>
      // we need to check:
      //  1. both distributions have the same number of clustering expressions
      //  2. both partitioning have the same number of partitions
      //  3. both partitioning have the same number of expressions
      //  4. each pair of expression from both has overlapping positions in their
      //     corresponding distributions.
      distribution.clustering.length == otherDistribution.clustering.length &&
        partitioning.numPartitions == otherPartitioning.numPartitions &&
        partitioning.expressions.length == otherPartitioning.expressions.length && {
        val otherHashKeyPositions = otherHashSpec.hashKeyPositions
        hashKeyPositions.zip(otherHashKeyPositions).forall { case (left, right) =>
          left.intersect(right).nonEmpty
        }
      }
    case ShuffleSpecCollection(specs) => specs.exists(isCompatibleWith)
    case _ => false
  }

  override def canCreatePartitioning: Boolean = true

  override def createPartitioning(clustering: Seq[Expression]): Partitioning = {
    val exprs = hashKeyPositions.map(v => clustering(v.head))
    GpuHashPartitioning(exprs, partitioning.numPartitions)
  }

  override def numPartitions: Int = partitioning.numPartitions

  /**
   * Returns a sequence where each element is a set of positions of the key in `hashKeys` to its
   * positions in `requiredClusterKeys`. For instance, if `requiredClusterKeys` is [a, b, b] and
   * `hashKeys` is [a, b], the result will be [(0), (1, 2)].
   */
  private def createHashKeyPositions(
      requiredClusterKeys: Seq[Expression],
      hashKeys: Seq[Expression]): Seq[mutable.BitSet] = {
    val distKeyToPos = mutable.Map.empty[Expression, mutable.BitSet]
    requiredClusterKeys.zipWithIndex.foreach { case (distKey, distKeyPos) =>
      distKeyToPos.getOrElseUpdate(distKey.canonicalized, mutable.BitSet.empty).add(distKeyPos)
    }

    hashKeys.map(k => distKeyToPos(k.canonicalized))
  }
}
*/