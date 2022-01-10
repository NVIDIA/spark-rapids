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

package org.apache.spark.sql.rapids.execution

import scala.collection.AbstractIterator
import scala.concurrent.Future

import com.nvidia.spark.rapids._

import org.apache.spark.{MapOutputStatistics, ShuffleDependency}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute}
import org.apache.spark.sql.catalyst.plans.physical.RoundRobinPartitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.execution.metric._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.MutablePair

class GpuShuffleMeta(
    shuffle: ShuffleExchangeExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[ShuffleExchangeExec](shuffle, conf, parent, rule) {
  // Some kinds of Partitioning are a type of expression, but Partitioning itself is not
  // so don't let them leak through as expressions
  override val childExprs: scala.Seq[ExprMeta[_]] = Seq.empty
  override val childParts: scala.Seq[PartMeta[_]] =
    Seq(GpuOverrides.wrapPart(shuffle.outputPartitioning, conf, Some(this)))

  // Propagate possible type conversions on the output attributes of map-side plans to
  // reduce-side counterparts. We can pass through the outputs of child because Shuffle will
  // not change the data schema. And we need to pass through because Shuffle itself and
  // reduce-side plans may failed to pass the type check for tagging CPU data types rather
  // than their GPU counterparts.
  //
  // Taking AggregateExec with TypedImperativeAggregate function as example:
  //    Assume I have a query: SELECT a, COLLECT_LIST(b) FROM table GROUP BY a, which physical plan
  //    looks like:
  //    ObjectHashAggregate(keys=[a#10], functions=[collect_list(b#11, 0, 0)],
  //                        output=[a#10, collect_list(b)#17])
  //      +- Exchange hashpartitioning(a#10, 200), true, [id=#13]
  //        +- ObjectHashAggregate(keys=[a#10], functions=[partial_collect_list(b#11, 0, 0)],
  //                               output=[a#10, buf#21])
  //          +- LocalTableScan [a#10, b#11]
  //
  //    We will override the data type of buf#21 in GpuNoHashAggregateMeta. Otherwise, the partial
  //    Aggregate will fall back to CPU because buf#21 produce a GPU-unsupported type: BinaryType.
  //    Just like the partial Aggregate, the ShuffleExchange will also fall back to CPU unless we
  //    apply the same type overriding as its child plan: the partial Aggregate.
  override protected val useOutputAttributesOfChild: Boolean = true

  // For transparent plan like ShuffleExchange, the accessibility of runtime data transition is
  // depended on the next non-transparent plan. So, we need to trace back.
  override val availableRuntimeDataTransition: Boolean =
    childPlans.head.availableRuntimeDataTransition

  override def tagPlanForGpu(): Unit = {

    shuffle.outputPartitioning match {
      case _: RoundRobinPartitioning
        if shuffle.sqlContext.sparkSession.sessionState.conf
            .sortBeforeRepartition =>
        val orderableTypes = GpuOverrides.pluginSupportedOrderableSig + TypeSig.DECIMAL_128
        shuffle.output.map(_.dataType)
            .filterNot(orderableTypes.isSupportedByPlugin)
            .foreach { dataType =>
              willNotWorkOnGpu(s"round-robin partitioning cannot sort $dataType to run " +
                  s"this on the GPU set ${SQLConf.SORT_BEFORE_REPARTITION.key} to false")
            }
      case _ =>
    }

  }

}

