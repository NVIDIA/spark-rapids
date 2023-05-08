/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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
{"spark": "311"}
{"spark": "312"}
{"spark": "313"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import java.nio.ByteBuffer

import com.nvidia.spark.rapids.{GpuMetric, HostMemoryOutputStream, NoopMetric}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.common.io.DiskRangeList
import org.apache.orc.{CompressionCodec, DataReader, OrcFile, OrcProto, StripeInformation, TypeDescription}
import org.apache.orc.impl.{BufferChunk, DataReaderProperties, OrcIndex, RecordReaderUtils}

/**
 * File cache is not supported for Spark 3.1.x so this is a thin wrapper
 * around the ORC DataReader.
 */
class GpuOrcDataReader(
    val props: DataReaderProperties,
    val conf: Configuration,
    val metrics: Map[String, GpuMetric]) extends DataReader {
  private var dataReader = RecordReaderUtils.createDefaultDataReader(props)

  override def open(): Unit = dataReader.open()

  override def readRowIndex(
      stripe: StripeInformation,
      fileSchema: TypeDescription,
      footer: OrcProto.StripeFooter,
      ignoreNonUtf8BloomFilter: Boolean,
      included: Array[Boolean],
      indexes: Array[OrcProto.RowIndex],
      sargColumns: Array[Boolean],
      version: OrcFile.WriterVersion,
      bloomFilterKinds: Array[OrcProto.Stream.Kind],
      bloomFilterIndices: Array[OrcProto.BloomFilterIndex]): OrcIndex = {
    dataReader.readRowIndex(stripe, fileSchema, footer, ignoreNonUtf8BloomFilter, included,
      indexes, sargColumns, version, bloomFilterKinds, bloomFilterIndices)
  }

  override def readStripeFooter(stripe: StripeInformation): OrcProto.StripeFooter = {
    dataReader.readStripeFooter(stripe)
  }

  override def readFileData(
      range: DiskRangeList,
      baseOffset: Long,
      forceDirect: Boolean): DiskRangeList = {
    dataReader.readFileData(range, baseOffset, forceDirect)
  }

  override def isTrackingDiskRanges: Boolean = dataReader.isTrackingDiskRanges

  override def releaseBuffer(buffer: ByteBuffer): Unit = dataReader.releaseBuffer(buffer)

  override def getCompressionCodec: CompressionCodec = dataReader.getCompressionCodec

  override def close(): Unit = {
    if (dataReader != null) {
      dataReader.close()
      dataReader = null
    }
  }

 def copyFileDataToHostStream(out: HostMemoryOutputStream, ranges: DiskRangeList): Unit = {
    val bufferChunks = dataReader.readFileData(ranges, 0, false)
    metrics.getOrElse(GpuMetric.WRITE_BUFFER_TIME, NoopMetric).ns {
      var current = bufferChunks
      while (current != null) {
        out.write(current.getData)
        if (dataReader.isTrackingDiskRanges && current.isInstanceOf[BufferChunk]) {
          dataReader.releaseBuffer(current.getData)
        }
        current = current.next
      }
    }
  }
}

object GpuOrcDataReader {
  // File cache is not being used, so we want to coalesce read ranges
  val shouldMergeDiskRanges: Boolean = true
}
