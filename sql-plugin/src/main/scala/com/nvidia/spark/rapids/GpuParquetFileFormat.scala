/*
 * Copyright (c) 2019-2023, NVIDIA CORPORATION.
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
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingArray
import com.nvidia.spark.rapids.shims.{ParquetFieldIdShims, ParquetTimestampNTZShims, SparkShimImpl}
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
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuParquetFileFormat {
  def tagGpuSupport(
      meta: RapidsMeta[_, _, _],
      spark: SparkSession,
      options: Map[String, String],
      schema: StructType): Option[GpuParquetFileFormat] = {

    val sqlConf = spark.sessionState.conf

    val parquetOptions = new ParquetOptions(options, sqlConf)

    // lookup encryption keys in the options, then Hadoop conf, then Spark runtime conf
    def lookupEncryptionConfig(key: String): String = {
      options.getOrElse(key, {
        val hadoopConf = spark.sparkContext.hadoopConfiguration.get(key, "")
        if (hadoopConf.nonEmpty) {
          hadoopConf
        } else {
          spark.conf.get(key, "")
        }
      })
    }

    val columnEncryption = lookupEncryptionConfig("parquet.encryption.column.keys")
    val footerEncryption = lookupEncryptionConfig("parquet.encryption.footer.key")

    if (columnEncryption.nonEmpty || footerEncryption.nonEmpty) {
      meta.willNotWorkOnGpu("Encryption is not yet supported on GPU. If encrypted Parquet " +
          "writes are not required unset the \"parquet.encryption.column.keys\" and " +
          "\"parquet.encryption.footer.key\" in Parquet options")
    }

    if (!meta.conf.isParquetEnabled) {
      meta.willNotWorkOnGpu("Parquet input and output has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_PARQUET} to true")
    }

    if (!meta.conf.isParquetWriteEnabled) {
      meta.willNotWorkOnGpu("Parquet output has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_PARQUET_WRITE} to true")
    }

    // Check if bloom filter is enabled for any columns. If yes, then disable GPU write.
    // For Parquet tables, bloom filters are enabled for column `col` by setting
    // `parquet.bloom.filter.enabled#col` to `true` in `options` or table properties.
    // Refer to https://spark.apache.org/docs/3.2.0/sql-data-sources-load-save-functions.html
    // for further details.
    options.foreach {
      case (key, _) if key.startsWith("parquet.bloom.filter.enabled#") =>
        meta.willNotWorkOnGpu(s"Bloom filter write for Parquet is not yet supported on GPU. " +
          s"If bloom filter is not required, unset $key")
      case _ =>
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


    SparkShimImpl.int96ParquetRebaseWrite(sqlConf) match {
      case "EXCEPTION" =>
      case "CORRECTED" =>
      case "LEGACY" =>
        if (schemaHasTimestamps) {
          meta.willNotWorkOnGpu("LEGACY rebase mode for int96 timestamps is not supported")
        }
      case other =>
        meta.willNotWorkOnGpu(s"$other is not a supported rebase mode for int96")
    }

    SparkShimImpl.parquetRebaseWrite(sqlConf) match {
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
      case "ZSTD" => Some(CompressionType.ZSTD)
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
  @scala.annotation.nowarn(
    "msg=value ENABLE_JOB_SUMMARY in class ParquetOutputFormat is deprecated"
  )
  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): ColumnarOutputWriterFactory = {
    val sqlConf = sparkSession.sessionState.conf
    val parquetOptions = new ParquetOptions(options, sqlConf)

    val conf = ContextUtil.getConfiguration(job)

    val outputTimestampType = sqlConf.parquetOutputTimestampType
    val dateTimeRebaseException = "EXCEPTION".equals(
      sparkSession.sqlContext.getConf(SparkShimImpl.parquetRebaseWriteKey))
    val timestampRebaseException =
      outputTimestampType.equals(ParquetOutputTimestampType.INT96) &&
          "EXCEPTION".equals(sparkSession.sqlContext
              .getConf(SparkShimImpl.int96ParquetRebaseWriteKey)) ||
          !outputTimestampType.equals(ParquetOutputTimestampType.INT96) && dateTimeRebaseException

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

    if (sqlConf.writeLegacyParquetFormat) {
      throw new UnsupportedOperationException("Spark legacy output format not supported")
    }
    // Sets flags for `ParquetWriteSupport`, which converts Catalyst schema to Parquet
    // schema and writes actual rows to Parquet files.
    conf.set(
      SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key,
      sqlConf.writeLegacyParquetFormat.toString)

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

    ParquetFieldIdShims.setupParquetFieldIdWriteConfig(conf, sqlConf)
    val parquetFieldIdWriteEnabled = ParquetFieldIdShims.getParquetIdWriteEnabled(sqlConf)

    ParquetTimestampNTZShims.setupTimestampNTZConfig(conf, sqlConf)

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
        new GpuParquetWriter(path, dataSchema, compressionType, dateTimeRebaseException,
          timestampRebaseException, context, parquetFieldIdWriteEnabled)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        CodecConfig.from(context).getCodec.getExtension + ".parquet"
      }
    }
  }
}

class GpuParquetWriter(
    override val path: String,
    dataSchema: StructType,
    compressionType: CompressionType,
    dateRebaseException: Boolean,
    timestampRebaseException: Boolean,
    context: TaskAttemptContext,
    parquetFieldIdEnabled: Boolean)
  extends ColumnarOutputWriter(context, dataSchema, "Parquet", true) {

  val outputTimestampType = conf.get(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key)

  override def scanTableBeforeWrite(table: Table): Unit = {
    (0 until table.getNumberOfColumns).foreach { i =>
      val col = table.getColumn(i)
      // if col is a day
      if (dateRebaseException && RebaseHelper.isDateRebaseNeededInWrite(col)) {
        throw DataSourceUtils.newRebaseExceptionInWrite("Parquet")
      }
      // if col is a time
      else if (timestampRebaseException && RebaseHelper.isTimeRebaseNeededInWrite(col)) {
        throw DataSourceUtils.newRebaseExceptionInWrite("Parquet")
      }
    }
  }

  override def transform(batch: ColumnarBatch): Option[ColumnarBatch] = {
    val transformedCols = GpuColumnVector.extractColumns(batch).safeMap { cv =>
      new GpuColumnVector(cv.dataType, deepTransformColumn(cv.getBase, cv.dataType))
        .asInstanceOf[org.apache.spark.sql.vectorized.ColumnVector]
    }
    Some(new ColumnarBatch(transformedCols))
  }

  private def deepTransformColumn(cv: ColumnVector, dt: DataType): ColumnVector = {
    ColumnCastUtil.deepTransform(cv, Some(dt)) {
      // Timestamp types are checked and transformed for all nested columns.
      // Note that cudf's `isTimestampType` returns `true` for `TIMESTAMP_DAYS`, which is not
      // included in Spark's `TimestampType`.
      case (cv, _) if cv.getType.isTimestampType && cv.getType != DType.TIMESTAMP_DAYS =>
        val typeMillis = ParquetOutputTimestampType.TIMESTAMP_MILLIS.toString
        val typeInt96 = ParquetOutputTimestampType.INT96.toString

        outputTimestampType match {
          case `typeMillis` if cv.getType != DType.TIMESTAMP_MILLISECONDS =>
            cv.castTo(DType.TIMESTAMP_MILLISECONDS)

          case `typeInt96` =>
            val inRange = withResource(Scalar.fromLong(Long.MaxValue / 1000)) { upper =>
              withResource(Scalar.fromLong(Long.MinValue / 1000)) { lower =>
                withResource(cv.bitCastTo(DType.INT64)) { int64 =>
                  withResource(int64.greaterOrEqualTo(upper)) { a =>
                    withResource(int64.lessOrEqualTo(lower)) { b =>
                      a.or(b)
                    }
                  }
                }
              }
            }
            val anyInRange = withResource(inRange)(_.any())
            withResource(anyInRange) { _ =>
              require(!(anyInRange.isValid && anyInRange.getBoolean),
                // Its the writer's responsibility to close the input batch when this
                // exception is thrown.
                "INT96 column contains one " +
                "or more values that can overflow and will result in data " +
                "corruption. Please set " +
                "`spark.rapids.sql.format.parquet.writer.int96.enabled` to false " +
                "so we can fallback on CPU for writing parquet but still take " +
                "advantage of parquet read on the GPU.")
            }
            cv.copyToColumnVector() /* the input is unchanged */

          // Here the value of `outputTimestampType` should be `TIMESTAMP_MICROS`
          case _ => cv.copyToColumnVector() /* the input is unchanged */
        }

      // Decimal types are checked and transformed only for the top level column because we don't
      // have access to Spark's data type of the nested column.
      case (cv, Some(d: DecimalType)) =>
        // There is a bug in Spark that causes a problem if we write Decimals with
        // precision < 10 as Decimal64.
        // https://issues.apache.org/jira/browse/SPARK-34167
        if (d.precision <= Decimal.MAX_INT_DIGITS) {
          cv.castTo(DType.create(DType.DTypeEnum.DECIMAL32, -d.scale))
        } else if (d.precision <= Decimal.MAX_LONG_DIGITS) {
          cv.castTo(DType.create(DType.DTypeEnum.DECIMAL64, -d.scale))
        } else {
          // Here, decimal should be in DECIMAL128 so the input will be unchanged.
          cv.copyToColumnVector()
        }
    }
  }

  override val tableWriter: TableWriter = {
    val writeContext = new ParquetWriteSupport().init(conf)
    val builder = SchemaUtils
      .writerOptionsFromSchema(ParquetWriterOptions.builder(), dataSchema,
        ParquetOutputTimestampType.INT96 == SQLConf.get.parquetOutputTimestampType,
        parquetFieldIdEnabled)
      .withMetadata(writeContext.getExtraMetaData)
      .withCompressionType(compressionType)
    Table.writeParquetChunked(builder.build(), this)
  }
}
