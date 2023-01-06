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

import com.nvidia.spark.rapids.{ColumnarFileFormat, CsvFormatType, FileFormatChecks, InsertIntoHadoopFsRelationCommandMeta, WriteFileOp}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.types.{ArrayType, BinaryType, IntegerType, LongType, MapType, StructField, StructType}

object GpuHiveTextFileFormat extends Logging {

  def hasUnsupportedType(column: StructField): Boolean = {
    column.dataType match {
      case IntegerType => true // CALEB: Remove.
      case LongType => true // CALEB: Remove.
      case ArrayType(_,_) => true
      case StructType(_)  => true
      case MapType(_,_,_) => true
      case BinaryType     => true
      case _              => false
    }
  }

  def tagGpuSupport(meta: InsertIntoHadoopFsRelationCommandMeta,
                    insertToFileCommand: InsertIntoHadoopFsRelationCommand)
    : Option[ColumnarFileFormat] = {

    meta.willNotWorkOnGpu("CALEB: Hive output is not supported yet")

    // TODO: Figure out why this doesn't work.
    FileFormatChecks.tag(meta, insertToFileCommand.schema, CsvFormatType, WriteFileOp)

    // Workaround for FileFormatChecks.tag() dropping the ball.
    println(s"CALEB: insertToFileCommand.query.schema: ${insertToFileCommand.query.schema}.")
    insertToFileCommand.query.schema.foreach( field =>
      if (hasUnsupportedType(field)) {
        meta.willNotWorkOnGpu(s"column ${field.name} has type ${field.dataType}, " +
          s"unsupported for writing in ${insertToFileCommand.fileFormat} file format")
      }
    )
    None
  }

}