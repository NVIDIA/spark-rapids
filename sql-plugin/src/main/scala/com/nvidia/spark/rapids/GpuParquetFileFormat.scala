/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
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
import ai.rapids.cudf.ParquetColumnWriterOptions._
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
import org.apache.spark.sql.rapids.ColumnarWriteTaskStatsTracker
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.{ArrayType, DataTypes, DateType, Decimal, DecimalType, StructField, StructType, TimestampType}
import org.apache.spark.sql.vectorized.ColumnarBatch

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

    FileFormatChecks.tag(meta, schema, ParquetFormatType, WriteFileOp)

    parseCompressionType(parquetOptions.compressionCodecClassName)
      .getOrElse(meta.willNotWorkOnGpu(
        s"compression codec ${parquetOptions.compressionCodecClassName} is not supported"))

    if (sqlConf.writeLegacyParquetFormat) {
      meta.willNotWorkOnGpu("Spark legacy format is not supported")
    }

    if (!meta.conf.isParquetInt96WriteEnabled && sqlConf.parquetOutputTimestampType ==
      ParquetOutputTimestampType.INT96) {
      meta.willNotWorkOnGpu(s"Writing INT96 is disabled, if you want to enable it turn it on by " +
        s"setting the ${RapidsConf.ENABLE_PARQUET_INT96_WRITE} to true. NOTE: check " +
        "out the compatibility.md to know about the limitations associated with INT96 writer")
    }

    val schemaHasTimestamps = schema.exists { field =>
      TrampolineUtil.dataTypeExistsRecursively(field.dataType, _.isInstanceOf[TimestampType])
    }
    if (schemaHasTimestamps) {
      if(!isOutputTimestampTypeSupported(sqlConf.parquetOutputTimestampType)) {
        meta.willNotWorkOnGpu(s"Output timestamp type " +
          s"${sqlConf.parquetOutputTimestampType} is not supported")
      }
    }

    val schemaHasDates = schema.exists { field =>
      TrampolineUtil.dataTypeExistsRecursively(field.dataType, _.isInstanceOf[DateType])
    }

    ShimLoader.getSparkShims.parquetRebaseWrite(sqlConf) match {
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

  def parquetWriterOptionsFromSchema[T <: NestedBuilder[_, _], V <: ParquetColumnWriterOptions]
  (builder: ParquetColumnWriterOptions.NestedBuilder[T, V],
   schema: StructType, writeInt96: Boolean): T = {
    // TODO once https://github.com/rapidsai/cudf/issues/7654 is fixed go back to actually
    // setting if the output is nullable or not everywhere we have hard-coded nullable=true
    schema.foreach(field =>
      field.dataType match {
        case dt: DecimalType =>
          builder.withDecimalColumn(field.name, dt.precision, true)
        case TimestampType =>
          builder.withTimestampColumn(field.name,
            writeInt96, true)
        case s: StructType =>
          builder.withStructColumn(
            parquetWriterOptionsFromSchema(structBuilder(field.name), s, writeInt96).build())
        case a: ArrayType =>
          builder.withListColumn(
            parquetWriterOptionsFromSchema(listBuilder(field.name),
              StructType(Array(StructField(field.name, a.elementType, true))), writeInt96)
              .build())
        case _ =>
          builder.withColumns(true, field.name)
      }
    )
    builder.asInstanceOf[T]
  }

  def parseCompressionType(compressionType: String): Option[CompressionType] = {
    compressionType match {
      case "NONE" | "UNCOMPRESSED" => Some(CompressionType.NONE)
      case "SNAPPY" => Some(CompressionType.SNAPPY)
      case _ => None
    }
  }

  def isOutputTimestampTypeSupported(
     outputTimestampType: ParquetOutputTimestampType.Value): Boolean = {
    outputTimestampType match {
      case ParquetOutputTimestampType.TIMESTAMP_MICROS |
           ParquetOutputTimestampType.TIMESTAMP_MILLIS |
           ParquetOutputTimestampType.INT96 => true
      case _ => false
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

    val dateTimeRebaseException = "EXCEPTION".equals(
        sparkSession.sqlContext.getConf(ShimLoader.getSparkShims.parquetRebaseWriteKey))

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
    if(!GpuParquetFileFormat.isOutputTimestampTypeSupported(outputTimestampType)) {
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

  val outputTimestampType = conf.get(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key)

  override def scanTableBeforeWrite(table: Table): Unit = {
    if (dateTimeRebaseException) {
      (0 until table.getNumberOfColumns).foreach { i =>
        if (RebaseHelper.isDateTimeRebaseNeededWrite(table.getColumn(i))) {
          throw DataSourceUtils.newRebaseExceptionInWrite("Parquet")
        }
      }
    }
  }

  /**
   * Persists a columnar batch. Invoked on the executor side. When writing to dynamically
   * partitioned tables, dynamic partition columns are not included in columns to be written.
   * NOTE: It is the writer's responsibility to close the batch.
   */
  override def write(batch: ColumnarBatch,
     statsTrackers: Seq[ColumnarWriteTaskStatsTracker]): Unit = {
    val outputMillis = outputTimestampType == ParquetOutputTimestampType.TIMESTAMP_MILLIS.toString
    val newBatch =
      new ColumnarBatch(GpuColumnVector.extractColumns(batch).map {
        cv => {
          cv.dataType() match {
            case DataTypes.TimestampType if outputMillis =>
              new GpuColumnVector(DataTypes.TimestampType, withResource(cv.getBase()) { v =>
                v.castTo(DType.TIMESTAMP_MILLISECONDS)
              })
            case DataTypes.TimestampType
              if outputTimestampType == ParquetOutputTimestampType.INT96.toString =>
              withResource(Scalar.fromLong(Long.MaxValue / 1000)) { upper =>
                withResource(Scalar.fromLong(Long.MinValue / 1000)) { lower =>
                  withResource(cv.getBase().bitCastTo(DType.INT64)) { int64 =>
                    withResource(int64.greaterOrEqualTo(upper)) { a =>
                      withResource(int64.lessOrEqualTo(lower)) { b =>
                        withResource(a.or(b)) { aOrB =>
                          withResource(aOrB.any()) { any =>
                            if (any.getBoolean()) {
                              // its the writer's responsibility to close the batch
                              batch.close()
                              throw new IllegalArgumentException("INT96 column contains one " +
                                "or more values that can overflow and will result in data " +
                                "corruption. Please set " +
                                "`spark.rapids.sql.format.parquet.writer.int96.enabled` to false" +
                                " so we can fallback on CPU for writing parquet but still take " +
                                "advantage of parquet read on the GPU.")
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              cv
            case d: DecimalType if d.precision <= Decimal.MAX_INT_DIGITS =>
              // There is a bug in Spark that causes a problem if we write Decimals with
              // precision < 10 as Decimal64.
              // https://issues.apache.org/jira/browse/SPARK-34167
              new GpuColumnVector(d, withResource(cv.getBase()) { v =>
                v.castTo(DType.create(DType.DTypeEnum.DECIMAL32, -d.scale))
              })
            case _ => cv
          }
        }
      })

    super.write(newBatch, statsTrackers)
  }

  override val tableWriter: TableWriter = {
    val writeContext = new ParquetWriteSupport().init(conf)
    val builder = GpuParquetFileFormat
      .parquetWriterOptionsFromSchema(ParquetWriterOptions.builder(), dataSchema,
        ParquetOutputTimestampType.INT96 == SQLConf.get.parquetOutputTimestampType)
      .withMetadata(writeContext.getExtraMetaData)
      .withCompressionType(compressionType)
    Table.writeParquetChunked(builder.build(), this)
  }
}
