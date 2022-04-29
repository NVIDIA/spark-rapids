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

import com.nvidia.spark.rapids.OrcOutputStripe
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.common.io.DiskRangeList
import org.apache.orc.impl.RecordReaderImpl.SargApplier
import org.apache.orc.impl.{DataReaderProperties, OutStream, SchemaEvolution}
import org.apache.orc._
import org.apache.spark.sql.execution.datasources.orc.OrcUtils
import org.apache.spark.sql.types.DataType

import scala.collection.mutable.ArrayBuffer

trait OrcShims311until320Base {

  // read data to buffer
  def readFileData(dataReader: DataReader, inputDataRanges: DiskRangeList): DiskRangeList = {
    dataReader.readFileData(inputDataRanges, 0, false)
  }

  // create reader properties builder
  def newDataReaderPropertiesBuilder(compressionSize: Int,
      compressionKind: CompressionKind, typeCount: Int): DataReaderProperties.Builder = {
    DataReaderProperties.builder()
      .withBufferSize(compressionSize)
      .withCompression(compressionKind)
      .withTypeCount(typeCount)
  }

  // create ORC out stream
  def newOrcOutStream(name: String, bufferSize: Int, codec: CompressionCodec,
      receiver: PhysicalWriter.OutputReceiver): OutStream = {
    new OutStream(name, bufferSize, codec, receiver)
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
    val result = new ArrayBuffer[OrcOutputStripe](stripes.length)
    stripes.foreach { stripe =>
      val stripeFooter = dataReader.readStripeFooter(stripe)
      val needStripe = if (sargApp != null) {
        // An ORC schema is a single struct type describing the schema fields
        val orcFileSchema = evolution.getFileType(0)
        val orcIndex = dataReader.readRowIndex(stripe, orcFileSchema, stripeFooter,
          ignoreNonUtf8BloomFilter, fileIncluded, null, sargColumns,
          writerVersion, null, null)
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
    lhs.equals(rhs)
  }

  // forcePositionalEvolution is available from Spark-3.2. So setting this as false.
  def forcePositionalEvolution(conf:Configuration): Boolean = {
    false
  }

  // orcTypeDescriptionString is renamed to getOrcSchemaString from 3.3+
  def getOrcSchemaString(dt: DataType): String = {
    OrcUtils.orcTypeDescriptionString(dt)
  }

}
