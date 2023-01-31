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

import ai.rapids.cudf.{ColumnVector, CSVWriterOptions, DType, HostBufferConsumer, QuoteStyle, Scalar, Table, TableWriter => CudfTableWriter}
import com.google.common.base.Charsets
import com.nvidia.spark.rapids.{ColumnarFileFormat, ColumnarOutputWriter, ColumnarOutputWriterFactory, FileFormatChecks, HiveDelimitedTextFormatType, RapidsConf, WriteFileOp}
import java.nio.charset.Charset
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.rapids.GpuHiveTextFileUtils._
import org.apache.spark.sql.types.{DataType, StructType}

object GpuHiveTextFileFormat extends Logging {

  private def checkIfEnabled(meta: GpuInsertIntoHiveTableMeta): Unit = {
    if (!meta.conf.isHiveDelimitedTextEnabled) {
      meta.willNotWorkOnGpu("Hive text I/O has been disabled. To enable this, " +
        s"set ${RapidsConf.ENABLE_HIVE_TEXT} to true")
    }
    if (!meta.conf.isHiveDelimitedTextWriteEnabled) {
      meta.willNotWorkOnGpu("writing Hive delimited text tables has been disabled, " +
        s"to enable this, set ${RapidsConf.ENABLE_HIVE_TEXT_WRITE} to true")
    }
  }

  def tagGpuSupport(meta: GpuInsertIntoHiveTableMeta)
  : Option[ColumnarFileFormat] = {
    checkIfEnabled(meta)

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

    val serializationFormat = storage.properties.getOrElse(serializationKey, "1")
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
    if (!charset.equals(Charsets.UTF_8)) {
      meta.willNotWorkOnGpu("only UTF-8 is supported as the charset")
    }

    if (insertCommand.table.bucketSpec.isDefined) {
      meta.willNotWorkOnGpu("bucketed tables are not supported")
    }

    if (insertCommand.conf.getConfString("hive.exec.compress.output", "false") == "true") {
      meta.willNotWorkOnGpu("compressed output is not supported, " +
        "set hive.exec.compress.output to false to enable writing Hive text via GPU")
    }

    FileFormatChecks.tag(meta,
                         insertCommand.table.schema,
                         HiveDelimitedTextFormatType,
                         WriteFileOp)

    Some(new GpuHiveTextFileFormat())
  }
}

class GpuHiveTextFileFormat extends ColumnarFileFormat with Logging {

  override def supportDataType(dataType: DataType): Boolean = isSupportedType(dataType)

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

  // This CSV writer reformats timestamps. By default, the CUDF CSV writer
  // writes timestamps in the following format:
  //   "2020-09-16T22:32:01.123456Z"
  // Such a timestamp is incompatible with Hive's LazySimpleSerDe format:
  //   "uuuu-MM-dd HH:mm:ss[.SSS...]"
  // (Specifically, the `T` between `dd` and `HH`, and the `Z` at the end.)
  class TimestampReformattingCSVWriter(writeOptions: CSVWriterOptions,
                                       bufferConsumer: HostBufferConsumer)
    extends CudfTableWriter {

    val underlying: CudfTableWriter = Table.getCSVBufferWriter(writeOptions, bufferConsumer)

    private def convertTimestampToString(col: ColumnVector): ColumnVector = {

      val asStrings = col.asStrings() // TODO: Replace with col.asStrings(withFormat).
      val nullsReplaced = withResource(Scalar.fromString("\\N")) { nullString =>
        withResource(asStrings) { _ =>
          asStrings.replaceNulls(nullString)
        }
      }
      val removeT = withResource(Scalar.fromString("T")) { T =>
        withResource(Scalar.fromString(" ")) { space =>
          withResource(nullsReplaced) { _ =>
            nullsReplaced.stringReplace(T, space)
          }
        }
      }
      val removeZ = withResource(Scalar.fromString("Z")) { Z =>
        withResource(Scalar.fromString("")) { empty =>
          withResource(removeT) { _ =>
            removeT.stringReplace(Z, empty)
          }
        }
      }
      removeZ
    }

    override def write(table: Table): Unit = {
      val columns = for (i <- 0 until table.getNumberOfColumns) yield {
        val col = table.getColumn(i)
        col.getType match {
          case DType.TIMESTAMP_DAYS =>         convertTimestampToString(col)
          case DType.TIMESTAMP_SECONDS =>      convertTimestampToString(col)
          case DType.TIMESTAMP_MILLISECONDS => convertTimestampToString(col)
          case DType.TIMESTAMP_MICROSECONDS => convertTimestampToString(col)
          case DType.TIMESTAMP_NANOSECONDS =>  convertTimestampToString(col)
          case _ => col.incRefCount()
        }
      }

      withResource(new Table(columns: _*)) { t =>
        underlying.write(t)
      }

      columns.foreach(_.close)
    }

    override def close(): Unit = {
      underlying.close()
    }
  }

  override val tableWriter: CudfTableWriter = {
    val writeOptions = CSVWriterOptions.builder()
      .withFieldDelimiter('\u0001')
      .withRowDelimiter("\n")
      .withIncludeHeader(false)
      .withTrueValue("true")
      .withFalseValue("false")
      .withNullValue("\\N")
      .withQuoteStyle(QuoteStyle.NONE)

    new TimestampReformattingCSVWriter(writeOptions = writeOptions.build,
                                            bufferConsumer = this)
  }
}

