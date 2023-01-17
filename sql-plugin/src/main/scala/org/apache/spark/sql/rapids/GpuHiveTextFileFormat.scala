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

package org.apache.spark.sql.rapids

import ai.rapids.cudf.TableWriter
import com.nvidia.spark.rapids.{ColumnarFileFormat, ColumnarOutputWriter, ColumnarOutputWriterFactory, InsertIntoHadoopFsRelationCommandMeta}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
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

  def tagGpuSupport(meta: InsertIntoHadoopFsRelationCommandMeta,
                    insertToFileCommand: InsertIntoHadoopFsRelationCommand)
    : Option[ColumnarFileFormat] = {

    meta.willNotWorkOnGpu("CALEB: Hive output is not supported yet")

    // TODO: Figure out why this doesn't work.
    // FileFormatChecks.tag(meta, insertToFileCommand.schema, CsvFormatType, WriteFileOp)

    // Workaround for FileFormatChecks.tag() dropping the ball.
    println(s"CALEB: insertToFileCommand.query.schema: ${insertToFileCommand.query.schema}.")
    insertToFileCommand.query.schema.foreach( field =>
      if (hasUnsupportedType(field)) {
        meta.willNotWorkOnGpu(s"column ${field.name} has type ${field.dataType}, " +
          s"unsupported for writing in ${insertToFileCommand.fileFormat} file format")
      }
    )

    // TODO: Check output format. Only Hive delimited text is supported.

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
      override def getFileExtension(context: TaskAttemptContext): String = "txt"

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
    val writeOptions = ai.rapids.cudf.CSVWriterOptions.builder()
      .withFieldDelimiter('\u0001')
      .withRowDelimiter("\n")
      .withIncludeHeader(false)
      .withTrueValue("true")
      .withFalseValue("false")
      .withNullValue("\\N")
      // .withQuoteStyle(QuoteStyle.NONE) // TODO: Enable after available in spark-rapids-jni.

    ai.rapids.cudf.Table.getCSVBufferWriter(writeOptions.build, this)
  }
}