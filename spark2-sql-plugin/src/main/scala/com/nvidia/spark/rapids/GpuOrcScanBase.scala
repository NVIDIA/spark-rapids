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

package com.nvidia.spark.rapids

import java.io.DataOutputStream
import java.net.URI
import java.nio.ByteBuffer
import java.nio.channels.{Channels, WritableByteChannel}
import java.util
import java.util.Locale
import java.util.concurrent.{Callable, ThreadPoolExecutor}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}
import scala.collection.mutable
import scala.language.implicitConversions
import scala.math.max

import com.google.protobuf.CodedOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.io.DiskRangeList
import org.apache.orc.{CompressionKind, DataReader, OrcConf, OrcFile, OrcProto, PhysicalWriter, Reader, StripeInformation, TypeDescription}
import org.apache.orc.impl._
import org.apache.orc.impl.RecordReaderImpl.SargApplier
import org.apache.orc.mapred.OrcInputFormat

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.execution.TrampolineUtil
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{ArrayType, DataType, DecimalType, MapType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

object GpuOrcScanBase {
  def tagSupport(
      sparkSession: SparkSession,
      schema: StructType,
      meta: RapidsMeta[_, _]): Unit = {
    if (!meta.conf.isOrcEnabled) {
      meta.willNotWorkOnGpu("ORC input and output has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_ORC} to true")
    }

    if (!meta.conf.isOrcReadEnabled) {
      meta.willNotWorkOnGpu("ORC input has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_ORC_READ} to true")
    }

    FileFormatChecks.tag(meta, schema, OrcFormatType, ReadFileOp)

    if (sparkSession.conf
      .getOption("spark.sql.orc.mergeSchema").exists(_.toBoolean)) {
      meta.willNotWorkOnGpu("mergeSchema and schema evolution is not supported yet")
    }
  }
}

