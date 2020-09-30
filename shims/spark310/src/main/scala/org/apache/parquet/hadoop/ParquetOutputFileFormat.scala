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

package org.apache.parquet.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.OutputFile

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport

class ParquetOutputFileFormat {

  private var memoryManager: MemoryManager = null
  val writeSupport = new ParquetWriteSupport().asInstanceOf[WriteSupport[InternalRow]]
  val DEFAULT_MEMORY_POOL_RATIO: Float = 0.95f
  val DEFAULT_MIN_MEMORY_ALLOCATION: Long = 1 * 1024 * 1024 // 1MB

  def getRecordWriter(output: OutputFile, conf: Configuration): RecordWriter[Void, InternalRow] = {
    import ParquetOutputFormat._

    val blockSize = getLongBlockSize(conf)
    val maxPaddingSize =
      conf.getInt(MAX_PADDING_BYTES, ParquetWriter.MAX_PADDING_SIZE_DEFAULT)
    val validating = getValidation(conf)

    val init = writeSupport.init(conf)
    val w = new ParquetFileWriter(output, init.getSchema,
      Mode.CREATE, blockSize, maxPaddingSize)
    w.start()

    val maxLoad = conf.getFloat(MEMORY_POOL_RATIO, DEFAULT_MEMORY_POOL_RATIO)
    val minAllocation = conf.getLong(MIN_MEMORY_ALLOCATION, DEFAULT_MIN_MEMORY_ALLOCATION)

    classOf[ParquetOutputFormat[InternalRow]].synchronized {
      memoryManager = new MemoryManager(maxLoad, minAllocation)
    }

    val writerVersion =
      ParquetProperties.WriterVersion.fromString(conf.get(ParquetOutputFormat.WRITER_VERSION,
        ParquetProperties.WriterVersion.PARQUET_1_0.toString))

    val codecFactory = new CodecFactory(conf, getPageSize(conf))

    // we should be using this constructor but its package-private. I am getting a RTE thrown even
    // though this class is in the same package as ParquetRecordWriter.scala
    //    new ParquetRecordWriter[InternalRow](w, writeSupport, init.getSchema,
    //      init.getExtraMetaData, blockSize, CompressionCodecName.SNAPPY, validating,
    //      props, memoryManager, conf)

    new ParquetRecordWriter[InternalRow](w, writeSupport, init.getSchema, init.getExtraMetaData,
      blockSize, getPageSize(conf), codecFactory.getCompressor(CompressionCodecName.UNCOMPRESSED),
      getDictionaryPageSize(conf), getEnableDictionary(conf), validating, writerVersion,
      memoryManager)
  }
}
