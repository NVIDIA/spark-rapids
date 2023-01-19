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

package org.apache.spark.sql.hive.rapids.shims

import ai.rapids.cudf.{CSVWriterOptions, QuoteStyle, Table, TableWriter}
import com.google.common.base.Charsets
import com.nvidia.spark.rapids.{ColumnarFileFormat, ColumnarOutputWriter, ColumnarOutputWriterFactory, FileFormatChecks, HiveDelimitedTextFormatType, WriteFileOp}
import java.nio.charset.Charset
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.rapids.GpuHiveTextFileUtils._
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, MapType, StructField, StructType}

object GpuHiveTextFileFormat extends Logging {

  def isSupportedType(dataType: DataType): Boolean = dataType match {
    case ArrayType(_,_) => false
    case StructType(_)  => false
    case MapType(_,_,_) => false
    case BinaryType     => false
    case _              => true
  }

  def hasUnsupportedType(column: StructField): Boolean = !isSupportedType(column.dataType)

  def tagGpuSupport(meta: GpuInsertIntoHiveTableMeta)
    : Option[ColumnarFileFormat] = {

    val insertCommand = meta.wrapped
    val storage  = insertCommand.table.storage
    if (storage.outputFormat.getOrElse("") != textOutputFormat) {
      meta.willNotWorkOnGpu(s"unsupported output-format found: ${storage.outputFormat}, " +
        s"only $textOutputFormat is currently supported")
    }
    if (storage.serde.getOrElse("") != lazySimpleSerDe) {
      meta.willNotWorkOnGpu(s"unsupported serde found: ${storage.serde}, " +
        s"only $lazySimpleSerDe is currently supported")
    }

    val serializationFormat = storage.properties.getOrElse(serializationKey, "")
    if (serializationFormat != ctrlASeparatedFormat) {
      meta.willNotWorkOnGpu(s"unsupported serialization format found: " +
        s"$serializationFormat, " +
        s"only \'^A\' separated text output (i.e. serialization.format=1) " +
        s"is currently supported")
    }

    val lineTerminator = storage.properties.getOrElse(lineDelimiterKey, newLine)
    if (lineTerminator != newLine) {
      meta.willNotWorkOnGpu(s"unsupported line terminator found: " +
        s"$lineTerminator, " +
        s"only newline (\'\\n\') separated text output is currently supported")
    }

    if (!storage.properties.getOrElse(escapeDelimiterKey, "").equals("")) {
      meta.willNotWorkOnGpu("escapes are not currently supported")
      // "serialization.escape.crlf" matters only if escapeDelimiterKey is set
    }

    val charset = Charset.forName(
      storage.properties.getOrElse("serialization.encoding", "UTF-8"))
    if (!(charset.equals(Charsets.US_ASCII) || charset.equals(Charsets.UTF_8))) {
      meta.willNotWorkOnGpu("only UTF-8 and ASCII are supported as the charset")
    }

    if (insertCommand.table.bucketSpec.isDefined) {
      meta.willNotWorkOnGpu("bucketed tables are not supported")
    }

    FileFormatChecks.tag(meta,
                         insertCommand.table.schema,
                         HiveDelimitedTextFormatType,
                         WriteFileOp)

    Some(new GpuHiveTextFileFormat())
  }
}

class GpuHiveTextFileFormat extends ColumnarFileFormat with Logging {

  override def supportDataType(dataType: DataType): Boolean =
    GpuHiveTextFileFormat.isSupportedType(dataType)

  override def prepareWrite(sparkSession: SparkSession,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): ColumnarOutputWriterFactory = {
    new ColumnarOutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String = ".txt"

      override def newInstance(path: String,
                               dataSchema: StructType,
                               context: TaskAttemptContext): ColumnarOutputWriter = {
        new GpuHiveTextWriter(path, dataSchema, context)
      }
    }
  }
}

class GpuHiveTextWriter(override val path: String,
                        dataSchema: StructType,
                        context: TaskAttemptContext)
  extends ColumnarOutputWriter(context, dataSchema, "HiveText") {
  override val tableWriter: TableWriter = {
    val writeOptions = CSVWriterOptions.builder()
      .withFieldDelimiter('\u0001')
      .withRowDelimiter("\n")
      .withIncludeHeader(false)
      .withTrueValue("true")
      .withFalseValue("false")
      .withNullValue("\\N")
      .withQuoteStyle(QuoteStyle.NONE)

    Table.getCSVBufferWriter(writeOptions.build, this)
  }
}