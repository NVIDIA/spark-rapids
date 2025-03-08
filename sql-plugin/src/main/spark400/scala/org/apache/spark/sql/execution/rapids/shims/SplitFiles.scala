/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.rapids.shims

import com.nvidia.spark.rapids.shims.PartitionedFileUtilsShim
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.{CompressionCodecFactory, SplittableCompressionCodec}

import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession

trait SplitFiles {
  def splitFiles(sparkSession: SparkSession,
      hadoopConf: Configuration,
      selectedPartitions: Array[PartitionDirectory],
      maxSplitBytes: Long): Seq[PartitionedFile] = {

    def canBeSplit(filePath: Path, hadoopConf: Configuration): Boolean = {
      val codec = new CompressionCodecFactory(hadoopConf).getCodec(filePath)
      codec == null || codec.isInstanceOf[SplittableCompressionCodec]
    }

    selectedPartitions.flatMap { partition =>
      partition.files.flatMap { f =>
        PartitionedFileUtilsShim.splitFiles(
          f,
          isSplitable = canBeSplit(f.getPath, hadoopConf),
          maxSplitBytes,
          partition.values
        )
      }.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    }
  }

  def splitFiles(
      selectedPartitions: Array[PartitionDirectory],
      relation: HadoopFsRelation,
      maxSplitBytes: Long): Array[PartitionedFile] = {

    selectedPartitions.flatMap { partition =>
      partition.files.flatMap { file =>
        val filePath = file.getPath
        val isSplitable = relation.fileFormat.isSplitable(
          relation.sparkSession, relation.options, filePath)
        PartitionedFileUtilsShim.splitFiles(
          file = file,
          isSplitable = isSplitable,
          maxSplitBytes = maxSplitBytes,
          partitionValues = partition.values
        )
      }
    }.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
  }
} 