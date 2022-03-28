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

import org.apache.spark.sql.execution.datasources.orc.OrcUtils
import org.apache.spark.sql.types.DataType

// 320+ ORC shims
object OrcShims extends OrcShims320untilAllBase {

  // orcTypeDescriptionString is renamed to getOrcSchemaString from 3.3+
  def getOrcSchemaString(dt: DataType): String = {
    OrcUtils.orcTypeDescriptionString(dt)
  }

}
