/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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

import java.lang.reflect.Field

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable
import scala.util.Try

import com.nvidia.spark.rapids._
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.connector.catalog.{StagingTableCatalog, SupportsWrite}
import org.apache.spark.sql.connector.write.V1Write
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions, DeltaParquetFileFormat}
import org.apache.spark.sql.delta.catalog.{DeltaCatalog, DeltaTableV2}
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.delta.rapids.{DeltaRuntimeShim, GpuDeltaLog, GpuWriteIntoDelta}
import org.apache.spark.sql.delta.sources.{DeltaDataSource, DeltaSourceUtils}
import org.apache.spark.sql.execution.datasources.{FileFormat, LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExecV1, AtomicCreateTableAsSelectExec, AtomicReplaceTableAsSelectExec, OverwriteByExpressionExecV1}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.ExternalSource
import org.apache.spark.sql.rapids.execution.UnshimmedTrampolineUtil
import org.apache.spark.sql.sources.{CreatableRelationProvider, InsertableRelation}

/**
 * Implements the DeltaProvider interface for open source delta.io Delta Lake.
 */
abstract class DeltaIOProvider extends DeltaProviderImplBase {
  override def getCreatableRelationRules: Map[Class[_ <: CreatableRelationProvider],
      CreatableRelationProviderRule[_ <: CreatableRelationProvider]] = {
    Seq(
      ExternalSource.toCreatableRelationProviderRule[DeltaDataSource](
        "Write to Delta Lake table",
        (a, conf, p, r) => {
          require(p.isDefined, "Must provide parent meta")
          new DeltaCreatableRelationProviderMeta(a, conf, p, r)
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[CreatableRelationProvider]), r)).toMap
  }

  override def isSupportedFormat(format: Class[_ <: FileFormat]): Boolean = {
    format == classOf[DeltaParquetFileFormat]
  }

  override def isSupportedWrite(write: Class[_ <: SupportsWrite]): Boolean = {
    write == classOf[DeltaTableV2]
  }

  override def isSupportedCatalog(catalogClass: Class[_ <: StagingTableCatalog]): Boolean = {
    catalogClass == classOf[DeltaCatalog]
  }

  override def tagForGpu(
      cpuExec: AtomicCreateTableAsSelectExec,
      meta: AtomicCreateTableAsSelectExecMeta): Unit = {
    require(isSupportedCatalog(cpuExec.catalog.getClass))
    if (!meta.conf.isDeltaWriteEnabled) {
      meta.willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
    }
    checkDeltaProvider(meta, cpuExec.properties, cpuExec.conf)
    RapidsDeltaUtils.tagForDeltaWrite(meta, cpuExec.query.schema, None,
      cpuExec.writeOptions.asCaseSensitiveMap().asScala.toMap, cpuExec.session)
  }

  override def tagForGpu(
      cpuExec: AtomicReplaceTableAsSelectExec,
      meta: AtomicReplaceTableAsSelectExecMeta): Unit = {
    require(isSupportedCatalog(cpuExec.catalog.getClass))
    if (!meta.conf.isDeltaWriteEnabled) {
      meta.willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
    }
    checkDeltaProvider(meta, cpuExec.properties, cpuExec.conf)
    RapidsDeltaUtils.tagForDeltaWrite(meta, cpuExec.query.schema, None,
      cpuExec.writeOptions.asCaseSensitiveMap().asScala.toMap, cpuExec.session)
  }

  private case class DeltaWriteV1Config(
      deltaLog: DeltaLog,
      forceOverwrite: Boolean,
      options: mutable.HashMap[String, String])

  private def extractWriteV1Config(
      meta: RapidsMeta[_, _, _],
      deltaLog: DeltaLog,
      write: V1Write): Option[DeltaWriteV1Config] = {
    // The V1Write instance on the CPU is an anonymous class that contains a private
    // WriteIntoDeltaBuilder class, the latter of which contains details on the type of write
    // being performed. In order to translate the write to the GPU, we need to examine this
    // state via reflection.
    def getField(cls: Class[_], fieldName: String): Option[Field] = {
      try {
        val field = cls.getDeclaredField(fieldName)
        field.setAccessible(true)
        Some(field)
      } catch {
        case _: NoSuchFieldException => None
      }
    }
    val writeClass = write.getClass
    getField(writeClass, "$outer").map { outerField =>
      val outerObj = outerField.get(write)
      val outerClass = outerObj.getClass
      if (outerClass.getName == "org.apache.spark.sql.delta.catalog.WriteIntoDeltaBuilder") {
        val forceOverwrite = getField(outerClass,
          "org$apache$spark$sql$delta$catalog$WriteIntoDeltaBuilder$$forceOverwrite").map { f =>
          f.getBoolean(outerObj)
        }
        val options = getField(outerClass,
          "org$apache$spark$sql$delta$catalog$WriteIntoDeltaBuilder$$options").map { f =>
          f.get(outerObj).asInstanceOf[mutable.HashMap[String, String]]
        }
        if (forceOverwrite.isDefined && options.isDefined) {
          Some(DeltaWriteV1Config(deltaLog, forceOverwrite.get, options.get))
        } else {
          meta.willNotWorkOnGpu(s"write class has unsupported outer class $outerClass")
          None
        }
      } else {
        meta.willNotWorkOnGpu(s"write class has unsupported outer class $outerClass")
        None
      }
    }.getOrElse {
      meta.willNotWorkOnGpu(s"write class $writeClass is not supported")
      None
    }
  }

  override def tagForGpu(
      cpuExec: AppendDataExecV1,
      meta: AppendDataExecV1Meta): Unit = {
    if (!meta.conf.isDeltaWriteEnabled) {
      meta.willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
    }
    val deltaTable = cpuExec.table.asInstanceOf[DeltaTableV2]
    val tablePath = if (deltaTable.catalogTable.isDefined) {
      new Path(deltaTable.catalogTable.get.location)
    } else {
      DeltaDataSource.parsePathIdentifier(cpuExec.session, deltaTable.path.toString,
        deltaTable.options)._1
    }
    val deltaLog = DeltaLog.forTable(cpuExec.session, tablePath, deltaTable.options)
    RapidsDeltaUtils.tagForDeltaWrite(meta, cpuExec.plan.schema, Some(deltaLog),
      deltaTable.options, cpuExec.session)
    extractWriteV1Config(meta, deltaLog, cpuExec.write).foreach { writeConfig =>
      meta.setCustomTaggingData(writeConfig)
    }
  }

  override def convertToGpu(
      cpuExec: AppendDataExecV1,
      meta: AppendDataExecV1Meta): GpuExec = {
    val writeConfig = meta.getCustomTaggingData match {
      case Some(c: DeltaWriteV1Config) => c
      case _ => throw new IllegalStateException("Missing Delta write config from tagging pass")
    }
    val gpuWrite = toGpuWrite(writeConfig, meta.conf)
    GpuAppendDataExecV1(cpuExec.table, cpuExec.plan, cpuExec.refreshCache, gpuWrite)
  }

  override def tagForGpu(
      cpuExec: OverwriteByExpressionExecV1,
      meta: OverwriteByExpressionExecV1Meta): Unit = {
    if (!meta.conf.isDeltaWriteEnabled) {
      meta.willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
    }
    val deltaTable = cpuExec.table.asInstanceOf[DeltaTableV2]
    val tablePath = if (deltaTable.catalogTable.isDefined) {
      new Path(deltaTable.catalogTable.get.location)
    } else {
      DeltaDataSource.parsePathIdentifier(cpuExec.session, deltaTable.path.toString,
        deltaTable.options)._1
    }
    val deltaLog = DeltaLog.forTable(cpuExec.session, tablePath, deltaTable.options)
    RapidsDeltaUtils.tagForDeltaWrite(meta, cpuExec.plan.schema, Some(deltaLog),
      deltaTable.options, cpuExec.session)
    extractWriteV1Config(meta, deltaLog, cpuExec.write).foreach { writeConfig =>
      meta.setCustomTaggingData(writeConfig)
    }
  }

  override def convertToGpu(
      cpuExec: OverwriteByExpressionExecV1,
      meta: OverwriteByExpressionExecV1Meta): GpuExec = {
    val writeConfig = meta.getCustomTaggingData match {
      case Some(c: DeltaWriteV1Config) => c
      case _ => throw new IllegalStateException("Missing Delta write config from tagging pass")
    }
    val gpuWrite = toGpuWrite(writeConfig, meta.conf)
    GpuOverwriteByExpressionExecV1(cpuExec.table, cpuExec.plan, cpuExec.refreshCache, gpuWrite)
  }

  private def checkDeltaProvider(
      meta: RapidsMeta[_, _, _],
      properties: Map[String, String],
      conf: SQLConf): Unit = {
    val provider = properties.getOrElse("provider", conf.getConf(SQLConf.DEFAULT_DATA_SOURCE_NAME))
    if (!DeltaSourceUtils.isDeltaDataSourceName(provider)) {
      meta.willNotWorkOnGpu(s"table provider '$provider' is not a Delta Lake provider")
    }
  }

  private def toGpuWrite(
      writeConfig: DeltaWriteV1Config,
      rapidsConf: RapidsConf): V1Write = new V1Write {
    override def toInsertableRelation(): InsertableRelation = {
      new InsertableRelation {
        override def insert(data: DataFrame, overwrite: Boolean): Unit = {
          val session = data.sparkSession
          val deltaLog = writeConfig.deltaLog

          // TODO: Get the config from WriteIntoDelta's txn.
          val cpuWrite = WriteIntoDelta(
            deltaLog,
            if (writeConfig.forceOverwrite) SaveMode.Overwrite else SaveMode.Append,
            new DeltaOptions(writeConfig.options.toMap, session.sessionState.conf),
            Nil,
            DeltaRuntimeShim.unsafeVolatileSnapshotFromLog(deltaLog).metadata.configuration,
            data)
          val gpuWrite = GpuWriteIntoDelta(new GpuDeltaLog(deltaLog, rapidsConf), cpuWrite)
          gpuWrite.run(session)

          // TODO: Push this to Apache Spark
          // Re-cache all cached plans(including this relation itself, if it's cached) that refer
          // to this data source relation. This is the behavior for InsertInto
          session.sharedState.cacheManager.recacheByPlan(
            session, LogicalRelation(deltaLog.createRelation()))
        }
      }
    }
  }
}

class DeltaCreatableRelationProviderMeta(
    source: DeltaDataSource,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends CreatableRelationProviderMeta[DeltaDataSource](source, conf, parent, rule) {
  require(parent.isDefined, "Must provide parent meta")
  private val saveCmd = parent.get.wrapped match {
    case s: SaveIntoDataSourceCommand => s
    case s =>
      throw new IllegalStateException(s"Expected SaveIntoDataSourceCommand, found ${s.getClass}")
  }

  override def tagSelfForGpu(): Unit = {
    if (!conf.isDeltaWriteEnabled) {
      willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
          s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
    }
    val path = saveCmd.options.get("path")
    if (path.isDefined) {
      val deltaLog = DeltaLog.forTable(SparkSession.active, path.get, saveCmd.options)
      RapidsDeltaUtils.tagForDeltaWrite(this, saveCmd.query.schema, Some(deltaLog),
        saveCmd.options, SparkSession.active)
    } else {
      willNotWorkOnGpu("no path specified for Delta Lake table")
    }
  }

  override def convertToGpu(): GpuCreatableRelationProvider = new GpuDeltaDataSource(conf)
}

/**
 * Implements the Delta Probe interface for probing the Delta Lake provider for
 * open source delta.io Delta Lake.
 * @note This is instantiated via reflection from ShimLoader.
 */
class DeltaProbeImpl extends DeltaProbe {
  override def getDeltaProvider: DeltaProvider = {
    val cpuClassName = "org.apache.spark.sql.delta.sources.DeltaDataSource"
    val hasDeltaJar = UnshimmedTrampolineUtil.classIsLoadable(cpuClassName) &&
        Try(ShimReflectionUtils.loadClass(cpuClassName)).isSuccess
    if (hasDeltaJar) {
      DeltaRuntimeShim.getDeltaProvider
    } else {
      NoDeltaProvider
    }
  }
}
