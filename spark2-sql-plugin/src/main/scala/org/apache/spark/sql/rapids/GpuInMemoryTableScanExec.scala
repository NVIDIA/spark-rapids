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

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.{DataFromReplacementRule, ExecChecks, RapidsConf, RapidsMeta, SparkPlanMeta}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

class InMemoryTableScanMeta(
    imts: InMemoryTableScanExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
    extends SparkPlanMeta[InMemoryTableScanExec](imts, conf, parent, rule) {

  override def tagPlanForGpu(): Unit = {
    def stringifyTypeAttributeMap(groupedByType: Map[DataType, Set[String]]): String = {
      groupedByType.map { case (dataType, nameSet) =>
        dataType + " " + nameSet.mkString("[", ", ", "]")
      }.mkString(", ")
    }

    val supportedTypeSig = rule.getChecks.get.asInstanceOf[ExecChecks]
    val unsupportedTypes: Map[DataType, Set[String]] = imts.relation.output
        .filterNot(attr => supportedTypeSig.check.isSupportedByPlugin(attr.dataType))
        .groupBy(_.dataType)
        .mapValues(_.map(_.name).toSet)

    val msgFormat = "unsupported data types in output: %s"
    if (unsupportedTypes.nonEmpty) {
      willNotWorkOnGpu(msgFormat.format(stringifyTypeAttributeMap(unsupportedTypes)))
    }
    // user won't actually enable it so assume they could
    /*
    if (!imts.relation.cacheBuilder.serializer
        .isInstanceOf[com.nvidia.spark.ParquetCachedBatchSerializer]) {
      willNotWorkOnGpu("ParquetCachedBatchSerializer is not being used")
      if (SQLConf.get.getConf(StaticSQLConf.SPARK_CACHE_SERIALIZER)
          .equals("com.nvidia.spark.ParquetCachedBatchSerializer")) {
        throw new IllegalStateException("Cache serializer failed to load! " +
            "Something went wrong while loading ParquetCachedBatchSerializer class")
      }
    }
    */
  }
}
