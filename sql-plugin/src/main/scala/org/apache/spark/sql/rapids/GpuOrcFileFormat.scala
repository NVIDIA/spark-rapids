/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

import java.time.ZoneId

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.OrcShims
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.orc.OrcConf
import org.apache.orc.OrcConf._
import org.apache.orc.mapred.OrcStruct

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.orc.{OrcFileFormat, OrcOptions, OrcUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types._

object GpuOrcFileFormat extends Logging {
  // The classname used when Spark is configured to use the Hive implementation for ORC.
  // Spark is not always compiled with Hive support so we cannot import from Spark jars directly.
  private val HIVE_IMPL_CLASS = "org.apache.spark.sql.hive.orc.OrcFileFormat"

  def isSparkOrcFormat(cls: Class[_ <: FileFormat]): Boolean = {
    cls == classOf[OrcFileFormat] || cls.getCanonicalName.equals(HIVE_IMPL_CLASS)
  }

  def tagGpuSupport(meta: RapidsMeta[_, _, _],
                    spark: SparkSession,
                    options: Map[String, String],
                    schema: StructType): Option[GpuOrcFileFormat] = {

    if (!meta.conf.isOrcEnabled) {
      meta.willNotWorkOnGpu("ORC input and output has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_ORC} to true")
    }

    if (!meta.conf.isOrcWriteEnabled) {
      meta.willNotWorkOnGpu("ORC output has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_ORC_WRITE} to true")
    }

    val keyProviderPath = options.getOrElse("hadoop.security.key.provider.path", "")
    val keyProvider = options.getOrElse("orc.key.provider", "")
    val encrypt = options.getOrElse("orc.encrypt", "")
    val mask = options.getOrElse("orc.mask", "")

    if (keyProvider.nonEmpty || keyProviderPath.nonEmpty || encrypt.nonEmpty || mask.nonEmpty) {
      meta.willNotWorkOnGpu("Encryption is not yet supported on GPU. If encrypted ORC " +
          "writes are not required unset the \"hadoop.security.key.provider.path\" and " +
          "\"orc.key.provider\" and \"orc.encrypt\" and \"orc.mask\"")
    }

    // Check if bloom filter is enabled. If yes, then disable GPU.
    // Refer to https://orc.apache.org/docs/spark-config.html for the description of ORC configs.
    val bloomFilterColumns = options.getOrElse("orc.bloom.filter.columns", "")
    if (bloomFilterColumns.nonEmpty) {
      meta.willNotWorkOnGpu("Bloom filter write for ORC is not yet supported on GPU. " +
        "If bloom filter is not required, unset \"orc.bloom.filter.columns\"")
    }

    // For date type, timezone needs to be checked also. This is because JVM timezone and UTC
    // timezone offset is considered when getting [[java.sql.date]] from
    // [[org.apache.spark.sql.execution.datasources.DaysWritable]] object
    // which is a subclass of [[org.apache.hadoop.hive.serde2.io.DateWritable]].
    val types = schema.map(_.dataType).toSet
    val hasBools = schema.exists { field =>
      TrampolineUtil.dataTypeExistsRecursively(field.dataType, t =>
        t.isInstanceOf[BooleanType])
    }

    if (types.exists(GpuOverrides.isOrContainsDateOrTimestamp(_))) {
      if (!GpuOverrides.isUTCTimezone()) {
        meta.willNotWorkOnGpu("Only UTC timezone is supported for ORC. " +
          s"Current timezone settings: (JVM : ${ZoneId.systemDefault()}, " +
          s"session: ${SQLConf.get.sessionLocalTimeZone}). ")
      }
    }

    if (hasBools && !meta.conf.isOrcBoolTypeEnabled) {
      meta.willNotWorkOnGpu("Nullable Booleans can not work in certain cases with ORC writer." +
        "See https://github.com/rapidsai/cudf/issues/6763")
    }
    FileFormatChecks.tag(meta, schema, OrcFormatType, WriteFileOp)

    val sqlConf = spark.sessionState.conf

    val parameters = CaseInsensitiveMap(options)

    case class ConfDataForTagging(orcConf: OrcConf, defaultValue: Any, message: String)

    def tagIfOrcOrHiveConfNotSupported(params: ConfDataForTagging): Unit = {
      val conf = params.orcConf
      val defaultValue = params.defaultValue
      val message = params.message
      val confValue = parameters.get(conf.getAttribute)
        .orElse(parameters.get(conf.getHiveConfName))
      if (confValue.isDefined && confValue.get != defaultValue) {
        logInfo(message)
      }
    }

    val orcOptions = new OrcOptions(options, sqlConf)
    orcOptions.compressionCodec match {
      case "NONE" | "SNAPPY" | "ZSTD" =>
      case c => meta.willNotWorkOnGpu(s"compression codec $c is not supported")
    }

    // hard coding the default value as it could change in future
    val supportedConf = Map(
      STRIPE_SIZE.ordinal() ->
        ConfDataForTagging(STRIPE_SIZE, 67108864L, "only 64MB stripe size is supported"),
      BUFFER_SIZE.ordinal() ->
        ConfDataForTagging(BUFFER_SIZE, 262144, "only 256KB block size is supported"),
      ROW_INDEX_STRIDE.ordinal() ->
        ConfDataForTagging(ROW_INDEX_STRIDE, 10000, "only 10,000 row index stride is supported"),
      BLOCK_PADDING.ordinal() ->
        ConfDataForTagging(BLOCK_PADDING, true, "Block padding isn't supported"))

    OrcConf.values().foreach(conf => {
      if (supportedConf.contains(conf.ordinal())) {
        tagIfOrcOrHiveConfNotSupported(supportedConf(conf.ordinal()))
      } else {
        if ((conf.getHiveConfName != null && parameters.contains(conf.getHiveConfName))
              || parameters.contains(conf.getAttribute)) {
          // these configurations are implementation specific and don't apply to cudf
          // The user has set them so we can't run on GPU
          logInfo(s"${conf.name()} is unsupported configuration")
        }
      }
    })

    if (meta.canThisBeReplaced) {
      Some(new GpuOrcFileFormat)
    } else {
      None
    }
  }
}

class GpuOrcFileFormat extends ColumnarFileFormat with Logging {
  /**
   * Prepares a write job and returns an `ColumnarOutputWriterFactory`.  Client side job
   * preparation can be put here.  For example, user defined output committer can be configured here
   * by setting the output committer class in the conf of spark.sql.sources.outputCommitterClass.
   */
  override def prepareWrite(sparkSession: SparkSession,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): ColumnarOutputWriterFactory = {

    val sqlConf = sparkSession.sessionState.conf
    val orcOptions = new OrcOptions(options, sqlConf)

    val conf = job.getConfiguration

    conf.set(MAPRED_OUTPUT_SCHEMA.getAttribute, OrcShims.getOrcSchemaString(dataSchema))

    conf.set(COMPRESS.getAttribute, orcOptions.compressionCodec)

    conf.asInstanceOf[JobConf]
      .setOutputFormat(classOf[org.apache.orc.mapred.OrcOutputFormat[OrcStruct]])

    val asyncOutputWriteEnabled = RapidsConf.ENABLE_ASYNC_OUTPUT_WRITE.get(sqlConf)
    // holdGpuBetweenBatches is on by default if asyncOutputWriteEnabled is on
    val holdGpuBetweenBatches = RapidsConf.ASYNC_QUERY_OUTPUT_WRITE_HOLD_GPU_IN_TASK.get(sqlConf)
      .getOrElse(asyncOutputWriteEnabled)
    val orcStripeSizeRows = RapidsConf.TEST_ORC_STRIPE_SIZE_ROWS.get(sqlConf)

    new ColumnarOutputWriterFactory {
      override def newInstance(path: String,
                               dataSchema: StructType,
                               context: TaskAttemptContext,
          statsTrackers: Seq[ColumnarWriteTaskStatsTracker],
                               debugOutputPath: Option[String]): ColumnarOutputWriter = {
        new GpuOrcWriter(path, dataSchema, context, statsTrackers, debugOutputPath,
          holdGpuBetweenBatches, orcStripeSizeRows, asyncOutputWriteEnabled)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        val compressionExtension: String = {
          val name = context.getConfiguration.get(COMPRESS.getAttribute)
          OrcUtils.extensionsForCompressionCodecNames.getOrElse(name, "")
        }

        compressionExtension + ".orc"
      }

      override def partitionFlushSize(context: TaskAttemptContext): Long = {
        context.getConfiguration.getLong("orc.stripe.size", 64L * 1024L * 1024L) // 64M
      }
    }
  }
}

class GpuOrcWriter(
    override val path: String,
    dataSchema: StructType,
    context: TaskAttemptContext,
    statsTrackers: Seq[ColumnarWriteTaskStatsTracker],
    debugOutputPath: Option[String],
    holdGpuBetweenBatches: Boolean,
    orcStripeSizeRows: Option[Integer],
    useAsyncWrite: Boolean)
  extends ColumnarOutputWriter(context, dataSchema, "ORC", true, statsTrackers, debugOutputPath,
    holdGpuBetweenBatches, useAsyncWrite) {

  override val tableWriter: TableWriter = {
    val builder = SchemaUtils
      .writerOptionsFromSchema(ORCWriterOptions.builder(), dataSchema, nullable = false)
      .withCompressionType(CompressionType.valueOf(OrcConf.COMPRESS.getString(conf)))
    orcStripeSizeRows.foreach { ss =>
      builder.withStripeSizeRows(ss)
    }
    Table.writeORCChunked(builder.build(), this)
  }
}
