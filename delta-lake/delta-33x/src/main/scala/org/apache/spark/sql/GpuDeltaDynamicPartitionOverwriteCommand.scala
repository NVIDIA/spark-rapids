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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan, V2WriteCommand}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.delta.DeltaOptions
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.delta.rapids.{GpuDeltaLog, GpuWriteIntoDelta}
import org.apache.spark.sql.execution.command.RunnableCommand

case class GpuDeltaDynamicPartitionOverwriteCommand(
    gpuDeltaLog: GpuDeltaLog,
    table: NamedRelation,
    deltaTable: DeltaTableV2,
    query: LogicalPlan,
    writeOptions: Map[String, String],
    isByName: Boolean,
    analyzedQuery: Option[LogicalPlan] = None) extends RunnableCommand with V2WriteCommand {

  override def child: LogicalPlan = query

  override def withNewQuery(newQuery: LogicalPlan): V2WriteCommand = {
    copy(query = newQuery)
  }

  override def withNewTable(newTable: NamedRelation): V2WriteCommand = {
    copy(table = newTable)
  }

  override def storeAnalyzedQuery(): Command = copy(analyzedQuery = Some(query))

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    copy(query = newChild)
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaOptions = new DeltaOptions(
      CaseInsensitiveMap[String](
        deltaTable.options ++
          writeOptions ++
          Seq(DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION ->
            DeltaOptions.PARTITION_OVERWRITE_MODE_DYNAMIC)),
      sparkSession.sessionState.conf)

    GpuWriteIntoDelta(
      gpuDeltaLog,
      WriteIntoDelta(
        gpuDeltaLog.deltaLog,
        SaveMode.Overwrite,
        deltaOptions,
        partitionColumns = Nil,
        deltaTable.deltaLog.unsafeVolatileSnapshot.metadata.configuration,
        Dataset.ofRows(sparkSession, query),
        deltaTable.catalogTable
      )
    ).run(sparkSession)
  }
}
