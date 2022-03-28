/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.OrcOutputStripe
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.common.io.DiskRangeList
import org.apache.orc.{CompressionCodec, CompressionKind, DataReader, OrcConf, OrcFile, OrcProto, PhysicalWriter, Reader, StripeInformation, TypeDescription}
import org.apache.orc.impl.{BufferChunk, BufferChunkList, DataReaderProperties, InStream, OrcCodecPool, OutStream, ReaderImpl, SchemaEvolution}
import org.apache.orc.impl.RecordReaderImpl.SargApplier
import org.apache.orc.impl.reader.StripePlanner
import org.apache.orc.impl.writer.StreamOptions

trait OrcShims320untilAllBase {

  // the ORC Reader in non-CDH Spark is closeable
  def withReader[T <: Reader, V](r: T)(block: T => V): V = {
    try {
      block(r)
    } finally {
      r.safeClose()
    }
  }

  // the ORC Reader in non-CDH Spark is closeable
  def closeReader(reader: Reader): Unit = {
    if(reader != null) {
      reader.close()
    }
  }

  // read data to buffer
  def readFileData(dataReader: DataReader, inputDataRanges: DiskRangeList): DiskRangeList = {

    // convert DiskRangeList to BufferChunkList
    val chuckList = new BufferChunkList
    var curr = inputDataRanges
    while (curr != null) {
      chuckList.add(new BufferChunk(curr.getOffset, curr.getLength))
      curr = curr.next
    }

    // BufferChunk is subclass of DiskRangeList
    dataReader.readFileData(chuckList, false).get()
  }

  // create reader properties builder
  def newDataReaderPropertiesBuilder(compressionSize: Int,
      compressionKind: CompressionKind, typeCount: Int): DataReaderProperties.Builder = {
    val compression = new InStream.StreamOptions()
      .withBufferSize(compressionSize).withCodec(OrcCodecPool.getCodec(compressionKind))
    DataReaderProperties.builder().withCompression(compression)
  }

  // create ORC out stream
  def newOrcOutStream(name: String, bufferSize: Int, codec: CompressionCodec,
      receiver: PhysicalWriter.OutputReceiver): OutStream = {
    val options = new StreamOptions(bufferSize)
    if (codec != null) {
      options.withCodec(codec, codec.getDefaultOptions)
    }
    new OutStream(name, options, receiver)
  }

  // filter stripes by pushing down filter
  def filterStripes(
      stripes: Seq[StripeInformation],
      conf: Configuration,
      orcReader: Reader,
      dataReader: DataReader,
      gen: (StripeInformation, OrcProto.StripeFooter, Array[Int], Array[Int]) => OrcOutputStripe,
      evolution: SchemaEvolution,
      sargApp: SargApplier,
      sargColumns: Array[Boolean],
      ignoreNonUtf8BloomFilter: Boolean,
      writerVersion: OrcFile.WriterVersion,
      fileIncluded: Array[Boolean],
      columnMapping: Array[Int],
      idMapping: Array[Int]): ArrayBuffer[OrcOutputStripe] = {

    val orcReaderImpl = orcReader.asInstanceOf[ReaderImpl]
    val maxDiskRangeChunkLimit = OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getInt(conf)
    val planner = new StripePlanner(evolution.getFileSchema, orcReaderImpl.getEncryption(),
      dataReader, writerVersion, ignoreNonUtf8BloomFilter, maxDiskRangeChunkLimit)

    val result = new ArrayBuffer[OrcOutputStripe](stripes.length)
    stripes.foreach { stripe =>
      val stripeFooter = dataReader.readStripeFooter(stripe)
      val needStripe = if (sargApp != null) {
        val orcIndex = planner.parseStripe(stripe, fileIncluded).readRowIndex(sargColumns, null)
        val rowGroups = sargApp.pickRowGroups(stripe, orcIndex.getRowGroupIndex,
          orcIndex.getBloomFilterKinds, stripeFooter.getColumnsList, orcIndex.getBloomFilterIndex,
          true)
        rowGroups != SargApplier.READ_NO_RGS
      } else {
        true
      }

      if (needStripe) {
        result.append(gen(stripe, stripeFooter, columnMapping, idMapping))
      }
    }
    result

  }

  /**
   * Compare if the two TypeDescriptions are equal by ignoring attribute
   */
  def typeDescriptionEqual(lhs: TypeDescription, rhs: TypeDescription): Boolean = {
    lhs.equals(rhs, false)
  }

  // forcePositionalEvolution is available from Spark-3.2.
  def forcePositionalEvolution(conf: Configuration): Boolean = {
    OrcConf.FORCE_POSITIONAL_EVOLUTION.getBoolean(conf)
  }
}
