/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

import java.io.{File, IOException}

import ai.rapids.cudf.{CompressionType, NvtxColor, NvtxRange, ORCWriterOptions}
import ai.rapids.spark._
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.orc.OrcConf
import org.apache.orc.OrcConf._
import org.apache.orc.mapred.OrcStruct
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.orc.{OrcFileFormat, OrcOptions}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuOrcFileFormat {
  def tagGpuSupport(
      meta: RapidsMeta[_, _, _],
      spark: SparkSession,
      options: Map[String, String]): Option[GpuOrcFileFormat] = {
    val sqlConf = spark.sessionState.conf

    val parameters = CaseInsensitiveMap(options)

    def tagIfOrcOrHiveConfNotSupported(params: (OrcConf, Any, String)) = {
      val conf = params._1
      val defaultValue = params._2
      val message = params._3
      val confValue = parameters.get(conf.getAttribute)
        .orElse(parameters.get(conf.getHiveConfName))
      if (confValue.isDefined && confValue.get != defaultValue) {
        meta.willNotWorkOnGpu(message)
      }
    }

    val orcOptions = new OrcOptions(options, sqlConf)
    orcOptions.compressionCodec match {
      case "NONE" | "SNAPPY" =>
      case c => meta.willNotWorkOnGpu(s"compression codec $c is not supported")
    }

    // hard coding the default value as it could change in future
    val supportedConf = Map(STRIPE_SIZE.ordinal() -> ((STRIPE_SIZE, 67108864L, "only 64MB stripe size is supported")),
      BLOCK_SIZE.ordinal() -> ((BLOCK_SIZE, 268435456L, "only 256KB block size is supported")),
      ROW_INDEX_STRIDE.ordinal() -> ((ROW_INDEX_STRIDE, 10000, "only 10,000 row index stride is supported")),
      BLOCK_PADDING.ordinal() -> ((BLOCK_PADDING, true, "Block padding isn't supported")))

    OrcConf.values().foreach(conf => {
      if (supportedConf.contains(conf.ordinal())) {;
        tagIfOrcOrHiveConfNotSupported(supportedConf(conf.ordinal()))
      } else {
        if (conf.getHiveConfName != null && parameters.contains(conf.getHiveConfName) || parameters.contains(conf.getAttribute)) {
          // these configurations are implementation specific and don't apply to cudf
          // The user has set them so we can't run on GPU
          meta.willNotWorkOnGpu(s"${conf.name()} is unsupported configuration")
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
   * Prepares a write job and returns an [[ColumnarOutputWriterFactory]].  Client side job
   * preparation can be put here.  For example, user defined output committer can be configured here
   * by setting the output committer class in the conf of spark.sql.sources.outputCommitterClass.
   */
  override def prepareWrite(
      sparkSession: SparkSession,
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
        override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): ColumnarOutputWriter = {
        new GpuOrcWriter(path, dataSchema, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        orcOptions.compressionCodec + ".orc"
      }
    }
  }
}

class GpuOrcWriter(
                    path: String,
                    dataSchema: StructType,
                    context: TaskAttemptContext) extends ColumnarOutputWriter {
  /**
   * Closes the [[ColumnarOutputWriter]]. Invoked on the executor side after all columnar batches
   * are persisted, before the task output is committed.
   */
  override def close(): Unit = {}

  // write a batch and return the time spent on the GPU
  override def writeBatch(batch: ColumnarBatch): Long = {
    var needToCloseBatch = true
    var tempFile: Option[File] = None
    try {
      val conf = context.getConfiguration
      val options = ORCWriterOptions.builder()
        .withCompressionType(CompressionType.valueOf(OrcConf.COMPRESS.getString(conf)))
        .withColumnNames(dataSchema.map(_.name): _*)
        .build()
      tempFile = Some(File.createTempFile("gpu", ".orc"))
      tempFile.get.getParentFile.mkdirs()
      val startTimestamp = System.nanoTime
      val nvtxRange = new NvtxRange("GPU Orc write", NvtxColor.BLUE)
      try {
        val table = GpuColumnVector.from(batch)
        try {
          table.writeORC(options, tempFile.get)
        } finally {
          table.close()
        }
      } finally {
        nvtxRange.close()
      }

      // Batch is no longer needed, write process from here does not use GPU.
      batch.close()
      needToCloseBatch = false
      GpuSemaphore.releaseIfNecessary(TaskContext.get)
      val gpuTime = System.nanoTime - startTimestamp

      val hadoopPath = new Path(path)
      if (!FileUtil.copy(tempFile.get, hadoopPath.getFileSystem(conf), hadoopPath, false, conf)) {
        throw new IOException(s"Failed to copy data to $hadoopPath")
      }

      gpuTime
    } finally {
      if (needToCloseBatch) {
        batch.close()
      }
      tempFile.foreach(_.delete())
    }
  }
}
