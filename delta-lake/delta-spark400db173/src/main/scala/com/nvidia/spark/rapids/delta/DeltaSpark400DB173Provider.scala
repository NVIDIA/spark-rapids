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

          // The V1 write adapter does not carry DB-17.3 transaction metadata options.
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
    tagDB173UnsupportedTableSpec(meta, cpuExec.tableSpec, cpuExec.session)
    super.tagForGpu(cpuExec, meta)
    if (meta.canThisBeReplaced) {
      meta.willNotWorkOnGpu(
        "Delta CTAS is not yet supported on GPU for DB-17.3")
    }
  }

  override def tagForGpu(
      cpuExec: AtomicReplaceTableAsSelectExec,
      meta: AtomicReplaceTableAsSelectExecMeta): Unit = {
    tagDB173UnsupportedTableSpec(meta, cpuExec.tableSpec, cpuExec.session)
    super.tagForGpu(cpuExec, meta)
    if (meta.canThisBeReplaced) {
      meta.willNotWorkOnGpu(
        "Delta RTAS is not yet supported on GPU for DB-17.3")
    }
  }

  // Keep CTAS/RTAS on CPU for DB-17.3 until GpuCreateDeltaTableCommand preserves the new
  // table-creation semantics. The feature-specific tags below make explain output name the
  // exact unsupported Delta feature instead of only the broad CTAS/RTAS fallback:
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
    // DB-17.3 can enable deletion vectors during table creation from either an explicit table
    // property or the session default-property conf. The CPU CreateDeltaTableCommand updates
    // metadata for that case; the GPU command does not yet, so keep the plan on CPU rather than
    // creating a table with DV unexpectedly disabled. Heuristic auto-enable cases are covered by
    // the broader create-command port tracked in
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
}
