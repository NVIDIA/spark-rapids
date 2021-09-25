/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
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
import org.apache.spark.sql.types.StructType

object GpuOrcFileFormat extends Logging {
  // The classname used when Spark is configured to use the Hive implementation for ORC.
  // Spark is not always compiled with Hive support so we cannot import from Spark jars directly.
  private val HIVE_IMPL_CLASS = "org.apache.spark.sql.hive.orc.OrcFileFormat"

  def isSparkOrcFormat(format: FileFormat): Boolean = format match {
    case _: OrcFileFormat => true
    case f if f.getClass.getCanonicalName.equals(HIVE_IMPL_CLASS) => true
    case _ => false
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
      case "NONE" | "SNAPPY" =>
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

    val orcOptions = new OrcOptions(options, sparkSession.sessionState.conf)

    val conf = job.getConfiguration

    conf.set(MAPRED_OUTPUT_SCHEMA.getAttribute, OrcFileFormat.getQuotedSchemaString(dataSchema))

    conf.set(COMPRESS.getAttribute, orcOptions.compressionCodec)

    conf.asInstanceOf[JobConf]
      .setOutputFormat(classOf[org.apache.orc.mapred.OrcOutputFormat[OrcStruct]])

    new ColumnarOutputWriterFactory {
      override def newInstance(path: String,
                               dataSchema: StructType,
                               context: TaskAttemptContext): ColumnarOutputWriter = {
        new GpuOrcWriter(path, dataSchema, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        val compressionExtension: String = {
          val name = context.getConfiguration.get(COMPRESS.getAttribute)
          OrcUtils.extensionsForCompressionCodecNames.getOrElse(name, "")
        }

        compressionExtension + ".orc"
      }
    }
  }
}

class GpuOrcWriter(path: String,
                   dataSchema: StructType,
                   context: TaskAttemptContext)
  extends ColumnarOutputWriter(path, context, dataSchema, "ORC") {

  override val tableWriter: TableWriter = {
    val builder= ORCWriterOptions.builder()
      .withCompressionType(CompressionType.valueOf(OrcConf.COMPRESS.getString(conf)))

    dataSchema.foreach(entry => {
      if (entry.nullable) {
        builder.withColumnNames(entry.name)
      } else {
        builder.withNotNullableColumnNames(entry.name)
      }
    })

    val options = builder.build()
    Table.writeORCChunked(options, this)
  }
}
