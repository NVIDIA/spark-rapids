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

package ai.rapids.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * A FileFormat that allows reading ORC files with the GPU.
 */
class GpuReadOrcFileFormat extends OrcFileFormat {
  override def supportBatch(sparkSession: SparkSession, dataSchema: StructType): Boolean = true

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val sqlConf = sparkSession.sessionState.conf
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val factory = GpuOrcPartitionReaderFactory(
      sqlConf,
      broadcastedHadoopConf,
      dataSchema,
      requiredSchema,
      partitionSchema,
      filters.toArray,
      new RapidsConf(sqlConf),
      PartitionReaderIterator.buildScanMetrics(sparkSession.sparkContext))
    PartitionReaderIterator.buildReader(factory)
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job, options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory =
    throw new IllegalStateException(s"${this.getClass.getCanonicalName} should not be writing")
}

object GpuReadOrcFileFormat {
  def tagSupport(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    val fsse = meta.wrapped
    GpuOrcScan.tagSupport(
      fsse.sqlContext.sparkSession,
      fsse.requiredSchema,
      meta
    )
  }
}
