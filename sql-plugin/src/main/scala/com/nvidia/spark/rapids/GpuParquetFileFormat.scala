/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import ai.rapids.cudf._
import com.nvidia.spark.RebaseHelper
import org.apache.hadoop.mapreduce.{Job, OutputCommitter, TaskAttemptContext}
import org.apache.parquet.hadoop.{ParquetOutputCommitter, ParquetOutputFormat}
import org.apache.parquet.hadoop.ParquetOutputFormat.JobSummaryLevel
import org.apache.parquet.hadoop.codec.CodecConfig
import org.apache.parquet.hadoop.util.ContextUtil

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.execution.datasources.parquet.{ParquetOptions, ParquetWriteSupport}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.ParquetOutputTimestampType
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.{DateType, StructType, TimestampType}

object GpuParquetFileFormat {
  def tagGpuSupport(
      meta: RapidsMeta[_, _, _],
      spark: SparkSession,
      options: Map[String, String],
      schema: StructType): Option[GpuParquetFileFormat] = {
    val sqlConf = spark.sessionState.conf
    val parquetOptions = new ParquetOptions(options, sqlConf)

    if (!meta.conf.isParquetEnabled) {
      meta.willNotWorkOnGpu("Parquet input and output has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_PARQUET} to true")
    }

    if (!meta.conf.isParquetWriteEnabled) {
      meta.willNotWorkOnGpu("Parquet output has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_PARQUET_WRITE} to true")
    }

    parseCompressionType(parquetOptions.compressionCodecClassName)
      .getOrElse(meta.willNotWorkOnGpu(
        s"compression codec ${parquetOptions.compressionCodecClassName} is not supported"))

    if (sqlConf.writeLegacyParquetFormat) {
      meta.willNotWorkOnGpu(s"Spark legacy format is not supported")
    }

    val schemaHasTimestamps = schema.exists { field =>
      TrampolineUtil.dataTypeExistsRecursively(field.dataType, _.isInstanceOf[TimestampType])
    }
    if (schemaHasTimestamps) {
      // TODO: Could support TIMESTAMP_MILLIS by performing cast on all timestamp input columns
      sqlConf.parquetOutputTimestampType match {
        case ParquetOutputTimestampType.TIMESTAMP_MICROS =>
        case t => meta.willNotWorkOnGpu(s"Output timestamp type $t is not supported")
      }
    }

    val schemaHasDates = schema.exists { field =>
      TrampolineUtil.dataTypeExistsRecursively(field.dataType, _.isInstanceOf[DateType])
    }

    sqlConf.getConf(SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_WRITE) match {
      case "EXCEPTION" => //Good
      case "CORRECTED" => //Good
      case "LEGACY" =>
        if (schemaHasDates || schemaHasTimestamps) {
          meta.willNotWorkOnGpu("LEGACY rebase mode for dates and timestamps is not supported")
        }
      case other =>
        meta.willNotWorkOnGpu(s"$other is not a supported rebase mode")
    }

    if (meta.canThisBeReplaced) {
      Some(new GpuParquetFileFormat)
    } else {
      None
    }
  }

  def parseCompressionType(compressionType: String): Option[CompressionType] = {
    compressionType match {
      case "NONE" | "UNCOMPRESSED" => Some(CompressionType.NONE)
      case "SNAPPY" => Some(CompressionType.SNAPPY)
      case _ => None
    }
  }
}

class GpuParquetFileFormat extends ColumnarFileFormat with Logging {
  /**
   * Prepares a write job and returns an [[ColumnarOutputWriterFactory]].  Client side job
   * preparation can be put here.  For example, user defined output committer can be configured
   * here by setting the output committer class in the conf of
   * spark.sql.sources.outputCommitterClass.
   */
  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): ColumnarOutputWriterFactory = {
    val parquetOptions = new ParquetOptions(options, sparkSession.sessionState.conf)

    val conf = ContextUtil.getConfiguration(job)

    val dateTimeRebaseException =
      "EXCEPTION".equals(conf.get(SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_WRITE.key))

    val committerClass =
      conf.getClass(
        SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key,
        classOf[ParquetOutputCommitter],
        classOf[OutputCommitter])

    if (conf.get(SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key) == null) {
      logInfo("Using default output committer for Parquet: " +
          classOf[ParquetOutputCommitter].getCanonicalName)
    } else {
      logInfo("Using user defined output committer for Parquet: " +
        committerClass.getCanonicalName)
    }

    conf.setClass(
      SQLConf.OUTPUT_COMMITTER_CLASS.key,
      committerClass,
      classOf[OutputCommitter])

    // We're not really using `ParquetOutputFormat[Row]` for writing data here.  The reason why
    // we set it here is to setup the output committer class to `ParquetOutputCommitter`, which is
    // bundled with `ParquetOutputFormat[Row]`.
    job.setOutputFormatClass(classOf[ParquetOutputFormat[Row]])

    ParquetOutputFormat.setWriteSupportClass(job, classOf[ParquetWriteSupport])

    // This metadata is useful for keeping UDTs like Vector/Matrix.
    ParquetWriteSupport.setSchema(dataSchema, conf)

    if (sparkSession.sessionState.conf.writeLegacyParquetFormat) {
      throw new UnsupportedOperationException("Spark legacy output format not supported")
    }
    // Sets flags for `ParquetWriteSupport`, which converts Catalyst schema to Parquet
    // schema and writes actual rows to Parquet files.
    conf.set(
      SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key,
      sparkSession.sessionState.conf.writeLegacyParquetFormat.toString)

    val outputTimestampType = sparkSession.sessionState.conf.parquetOutputTimestampType
    if (outputTimestampType != ParquetOutputTimestampType.TIMESTAMP_MICROS) {
      val hasTimestamps = dataSchema.exists { field =>
        TrampolineUtil.dataTypeExistsRecursively(field.dataType, _.isInstanceOf[TimestampType])
      }
      if (hasTimestamps) {
        throw new UnsupportedOperationException(
          s"Unsupported output timestamp type: $outputTimestampType")
      }
    }
    conf.set(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key, outputTimestampType.toString)

    // Sets compression scheme
    conf.set(ParquetOutputFormat.COMPRESSION, parquetOptions.compressionCodecClassName)

    val compressionType =
      GpuParquetFileFormat.parseCompressionType(parquetOptions.compressionCodecClassName)
        .getOrElse(
          throw new UnsupportedOperationException(
            s"compression codec ${parquetOptions.compressionCodecClassName} is not supported"))

    // SPARK-15719: Disables writing Parquet summary files by default.
    if (conf.get(ParquetOutputFormat.JOB_SUMMARY_LEVEL) == null
        && conf.get(ParquetOutputFormat.ENABLE_JOB_SUMMARY) == null) {
      conf.setEnum(ParquetOutputFormat.JOB_SUMMARY_LEVEL, JobSummaryLevel.NONE)
    }

    if (ParquetOutputFormat.getJobSummaryLevel(conf) != JobSummaryLevel.NONE
        && !classOf[ParquetOutputCommitter].isAssignableFrom(committerClass)) {
      // output summary is requested, but the class is not a Parquet Committer
      logWarning(s"Committer $committerClass is not a ParquetOutputCommitter and cannot" +
          s" create job summaries. " +
          s"Set Parquet option ${ParquetOutputFormat.JOB_SUMMARY_LEVEL} to NONE.")
    }

    new ColumnarOutputWriterFactory {
        override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): ColumnarOutputWriter = {
        new GpuParquetWriter(path, dataSchema, compressionType, dateTimeRebaseException, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        CodecConfig.from(context).getCodec.getExtension + ".parquet"
      }
    }
  }
}

class GpuParquetWriter(
    path: String,
    dataSchema: StructType,
    compressionType: CompressionType,
    dateTimeRebaseException: Boolean,
    context: TaskAttemptContext)
  extends ColumnarOutputWriter(path, context, dataSchema, "Parquet") {

  override def scanTableBeforeWrite(table: Table): Unit = {
    if (dateTimeRebaseException) {
      (0 until table.getNumberOfColumns).foreach { i =>
        if (RebaseHelper.isDateTimeRebaseNeededWrite(table.getColumn(i))) {
          throw DataSourceUtils.newRebaseExceptionInWrite("Parquet")
        }
      }
    }
  }

  override val tableWriter: TableWriter = {
    val writeContext = new ParquetWriteSupport().init(conf)
    val builder = ParquetWriterOptions.builder()
      .withMetadata(writeContext.getExtraMetaData)
      .withCompressionType(compressionType)
    dataSchema.foreach(entry => {
      if (entry.nullable) {
        builder.withColumnNames(entry.name)
      } else {
        builder.withNotNullableColumnNames(entry.name)
      }
    })
    val options = builder.build()
    Table.writeParquetChunked(options, this)
  }
}
