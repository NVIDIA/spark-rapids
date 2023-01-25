/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package org.apache.spark.sql.hive.rapids.shims

import com.nvidia.spark.rapids.shims.GpuCreateHiveTableAsSelectBase
import com.nvidia.spark.rapids.{DataFromReplacementRule, DataWritingCommandMeta, GpuDataWritingCommand, GpuOverrides, RapidsConf, RapidsMeta}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}
import org.apache.spark.sql.rapids.execution.TrampolineUtil

final class GpuCreateHiveTableAsSelectCommandMeta(cmd: CreateHiveTableAsSelectCommand,
                                                  conf: RapidsConf,
                                                  parent: Option[RapidsMeta[_,_,_]],
                                                  rule: DataFromReplacementRule)
  extends DataWritingCommandMeta[CreateHiveTableAsSelectCommand](cmd, conf, parent, rule) {

    var cpuWritingCommand: DataWritingCommand = null
    override def tagSelfForGpu(): Unit = {

      willNotWorkOnGpu("CALEB: GCHTASCM: CHTAS not implemented yet!") // TODO: Remove.

      val spark = SparkSession.active
      val tableDesc = cmd.tableDesc // For the *new* table.

      if (tableDesc.partitionColumnNames.nonEmpty) {
        willNotWorkOnGpu("partitioned writes are not supported")
      }

      if (tableDesc.bucketSpec.isDefined) {
        willNotWorkOnGpu("bucketing is not supported")
      }

      val catalog = spark.sessionState.catalog
      val tableExists = catalog.tableExists(tableDesc.identifier)
      val writingCommand = cmd.getWritingCommand(catalog, tableDesc, tableExists) match {
        case insertToHiveTable: InsertIntoHiveTable => Some(insertToHiveTable)
        case c =>
          willNotWorkOnGpu("Unsupported write command " + c)
          None
      }

      val createTableMeta = this

      if (writingCommand.isDefined) {
        val insertMeta = new GpuInsertIntoHiveTableMeta(
          writingCommand.get,
          conf,
          None,
          GpuOverrides.dataWriteCmds(writingCommand.get.getClass))

        insertMeta.tagForGpu()
        if (!insertMeta.canThisBeReplaced) {
          insertMeta.getCannotBeReplacedReasons.get.foreach( reason => {
            willNotWorkOnGpu(reason)
          })
        }
      }
    }
    override def convertToGpu(): GpuDataWritingCommand = null
}

case class GpuCreateHiveTableAsSelectCommand(
    tableDesc: CatalogTable,
    query: LogicalPlan,
    outputColumnNames: Seq[String],
    mode: SaveMode,
    cpuCmd: CreateHiveTableAsSelectCommand) extends GpuCreateHiveTableAsSelectBase {
  override def getWritingCommand(
      catalog: SessionCatalog,
      tableDesc: CatalogTable,
      tableExists: Boolean): GpuDataWritingCommand = {
    // Leverage the existing support for InsertIntoHadoopFsRelationCommand to do the write
    cpuCmd.getWritingCommand(catalog, tableDesc, tableExists) match {
      case c: InsertIntoHiveTable =>
        val rapidsConf = new RapidsConf(conf)
        val rule = GpuOverrides.dataWriteCmds(c.getClass)
        val meta = new GpuInsertIntoHiveTableMeta(c, rapidsConf, None, rule)
        meta.tagForGpu()
        if (!meta.canThisBeReplaced) {
          throw new IllegalStateException("Unable to convert writing command: " +
              meta.explain(all = false))
        }
        meta.convertToGpu()
      case c =>
        throw new UnsupportedOperationException(s"Unsupported write command: $c")
    }
  }

  override def writingCommandClassName: String =
    TrampolineUtil.getSimpleName(classOf[GpuInsertIntoHiveTable])

  // Do not support partitioned or bucketed writes
  override def requireSingleBatch: Boolean = false
}