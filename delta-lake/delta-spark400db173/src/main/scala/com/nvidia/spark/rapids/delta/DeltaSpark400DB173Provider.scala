/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Portions of this class have been taken from DeltaTableV2 class
 * in the Delta Lake project at https://github.com/delta-io/delta.
 *
 * Copyright (2021) The Delta Lake Project Authors.
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

package com.nvidia.spark.rapids.delta

import com.databricks.sql.transaction.tahoe.{DeltaConfigs, DeltaOptions}
import com.databricks.sql.transaction.tahoe.commands.WriteIntoDeltaEdge
import com.databricks.sql.transaction.tahoe.coordinatedcommits.{
  CatalogOwnedTableUtils,
  CoordinatedCommitsUtils
}
import com.databricks.sql.transaction.tahoe.rapids.{
  GpuDeltaCatalog,
  GpuDeltaLog,
  GpuDeltaV1Write,
  GpuWriteIntoDelta
}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.delta.shims.DeltaLogShim

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.TableSpec
import org.apache.spark.sql.connector.write.V1Write
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{
  AtomicCreateTableAsSelectExec,
  AtomicReplaceTableAsSelectExec
}
import org.apache.spark.sql.execution.datasources.v2.rapids.{
  GpuAtomicCreateTableAsSelectExec,
  GpuAtomicReplaceTableAsSelectExec
}
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims
import org.apache.spark.sql.sources.InsertableRelation

object DeltaSpark400DB173Provider extends DatabricksDeltaProviderBase {

  override protected def toGpuWrite(
     writeConfig: DeltaWriteV1Config,
     rapidsConf: RapidsConf): V1Write = new GpuDeltaV1Write {
    override def toInsertableRelation(): InsertableRelation = {
      new InsertableRelation {
        override def insert(data: DataFrame, overwrite: Boolean): Unit = {
          val session = data.sparkSession
          val deltaLog = writeConfig.deltaLog

          // The V1 write adapter does not expose transaction metadata configuration.
          // Seed WriteIntoDeltaEdge from the current snapshot, matching existing DB shims.
          val cpuWrite = WriteIntoDeltaEdge(
            deltaLog,
            if (writeConfig.forceOverwrite) SaveMode.Overwrite else SaveMode.Append,
            new DeltaOptions(writeConfig.options.toMap, session.sessionState.conf),
            Nil,
            DeltaLogShim.getMetadata(deltaLog).configuration,
            data)
          val gpuWrite = GpuWriteIntoDelta(new GpuDeltaLog(deltaLog, rapidsConf), cpuWrite)
          gpuWrite.run(session)

          // Match InsertInto behavior by refreshing cached plans that refer to this relation,
          // including the relation itself when it is cached.
          val classic = TrampolineConnectShims.getActiveSession
          classic.sharedState.cacheManager.recacheByPlan(
            classic, LogicalRelation(deltaLog.createRelation()))
        }
      }
    }
  }

  override def tagForGpu(
      cpuExec: AtomicCreateTableAsSelectExec,
      meta: AtomicCreateTableAsSelectExecMeta): Unit = {
    meta.willNotWorkOnGpu(
      "Delta CTAS is not yet supported on GPU for DB-17.3")
    tagDB173UnsupportedTableSpec(meta, cpuExec.tableSpec, cpuExec.session)
    super.tagForGpu(cpuExec, meta)
  }

  override def tagForGpu(
      cpuExec: AtomicReplaceTableAsSelectExec,
      meta: AtomicReplaceTableAsSelectExecMeta): Unit = {
    meta.willNotWorkOnGpu(
      "Delta RTAS is not yet supported on GPU for DB-17.3")
    tagDB173UnsupportedTableSpec(meta, cpuExec.tableSpec, cpuExec.session)
    super.tagForGpu(cpuExec, meta)
  }

  // CTAS/RTAS are disabled for this first DB-17.3 Delta PR because CreateDeltaTableCommand
  // gained semantics this shim does not port yet. Keep these feature-specific tags so the
  // explain output names the concrete feature involved, and so the checks are already in
  // place when create-table support is re-enabled incrementally. Each fall-back is tracked
  // separately:
  //   - row filter / column mask             -> https://github.com/NVIDIA/spark-rapids/issues/14601
  //   - catalog-owned / coordinated commits  -> https://github.com/NVIDIA/spark-rapids/issues/14601
  //   - liquid clustering / auto TTL         -> https://github.com/NVIDIA/spark-rapids/issues/14599
  private def tagDB173UnsupportedTableSpec(
      meta: RapidsMeta[_, _, _],
      tableSpec: TableSpec,
      spark: SparkSession): Unit = {
    if (tableSpec.rowFilter.isDefined) {
      meta.willNotWorkOnGpu("Delta CTAS/RTAS with a row filter is not supported on GPU")
    }
    if (tableSpec.columnMasks.isDefined) {
      meta.willNotWorkOnGpu("Delta CTAS/RTAS with column masks is not supported on GPU")
    }
    if (tableSpec.clusterSpec.isDefined) {
      meta.willNotWorkOnGpu("Delta CTAS/RTAS with liquid clustering is not supported on GPU")
    }
    if (tableSpec.autoTTLSpec.isDefined) {
      meta.willNotWorkOnGpu("Delta CTAS/RTAS with auto-TTL is not supported on GPU")
    }
    val hasCatalogOwnedConfig =
      CatalogOwnedTableUtils.shouldEnableCatalogOwned(spark, tableSpec.properties, false)
    val hasDefaultCatalogOwnedConfig = CatalogOwnedTableUtils.defaultCatalogOwnedEnabled(spark)
    if (hasCatalogOwnedConfig || hasDefaultCatalogOwnedConfig) {
      meta.willNotWorkOnGpu(
        "Delta CTAS/RTAS for a catalog-owned table is not supported on GPU")
    }
    val ccKeys = CoordinatedCommitsUtils.TABLE_PROPERTY_KEYS.toSet
    val hasExplicitCCConfig =
      tableSpec.properties.keysIterator.exists(key => containsKeyIgnoreCase(ccKeys, key))
    val hasDefaultCCConfig =
      CoordinatedCommitsUtils.getDefaultCCConfigurations(spark, true).nonEmpty
    if (hasExplicitCCConfig || hasDefaultCCConfig) {
      meta.willNotWorkOnGpu(
        "Delta CTAS/RTAS configuring coordinated commits is not supported on GPU")
    }
    // DB-17.3 enables DV at create time when either the table property is set explicitly
    // or the session's default-property conf is set. The CPU CreateDeltaTableCommand calls
    // DeletionVectorEnablementHelper.shouldEnable + enableInMetadata; the GPU command does
    // not, so a table that the user expects to have DV enabled would be created without it.
    // Auto-enable triggers are not covered here (they depend on table-shape heuristics) -
    // tracked alongside the broader DB-17.3 create-command port in
    // https://github.com/NVIDIA/spark-rapids/issues/14601.
    val dvConf = DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION
    val dvKeys = (dvConf.key +: dvConf.alternateKeys).toSet
    val dvExplicitlyEnabled = tableSpec.properties.collectFirst {
      case (key, value) if containsKeyIgnoreCase(dvKeys, key) => value
    }.exists(trueOrInvalidBoolean)
    val dvEnabledByDefaultConf =
      spark.conf.getOption(dvConf.defaultTablePropertyKey).exists(trueOrInvalidBoolean)
    if (dvExplicitlyEnabled || dvEnabledByDefaultConf) {
      meta.willNotWorkOnGpu(
        "Delta CTAS/RTAS that creates a table with Deletion Vectors enabled is not " +
          "supported on GPU")
    }
  }

  private def containsKeyIgnoreCase(keys: Set[String], key: String): Boolean =
    keys.exists(_.equalsIgnoreCase(key))

  private def trueOrInvalidBoolean(value: String): Boolean =
    !value.trim.equalsIgnoreCase("false")

  override def convertToGpu(
      cpuExec: AtomicCreateTableAsSelectExec,
      meta: AtomicCreateTableAsSelectExecMeta): GpuExec = {
    GpuAtomicCreateTableAsSelectExec(
      cpuExec.output,
      new GpuDeltaCatalog(cpuExec.catalog, meta.conf),
      cpuExec.ident,
      cpuExec.partitioning,
      cpuExec.query,
      cpuExec.tableSpec,
      cpuExec.writeOptions,
      cpuExec.ifNotExists)
  }

  override def convertToGpu(
      cpuExec: AtomicReplaceTableAsSelectExec,
      meta: AtomicReplaceTableAsSelectExecMeta): GpuExec = {
    GpuAtomicReplaceTableAsSelectExec(
      cpuExec.output,
      new GpuDeltaCatalog(cpuExec.catalog, meta.conf),
      cpuExec.ident,
      cpuExec.partitioning,
      cpuExec.query,
      cpuExec.tableSpec,
      cpuExec.writeOptions,
      cpuExec.orCreate,
      cpuExec.invalidateCache)
  }
}
