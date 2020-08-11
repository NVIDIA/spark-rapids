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

package org.apache.spark.sql.execution.datasources.v2.rapids

import java.io.IOException

import org.apache.parquet.io.ParquetDecodingException

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.InputFileBlockHolder
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.{PartitionedFile, SchemaColumnConvertNotSupportedException}
import org.apache.spark.sql.internal.SQLConf

/**
 * Equivalent of PartitionedFileReader in Spark but instead has multiple files.
 */
case class MultiplePartitionedFileReader[T](
    files: Array[PartitionedFile],
    reader: PartitionReader[T]) extends PartitionReader[T] {
  override def next(): Boolean = reader.next()

  override def get(): T = reader.get()

  override def close(): Unit = reader.close()

  override def toString: String = files.mkString(",")
}

/**
 * Similar to FilePartitionReader in Spark but instead has multiple files.
 */
class MultiFilePartitionReader[T](reader: MultiplePartitionedFileReader[T])
  extends PartitionReader[T] with Logging {

  private var currentReader = reader
  private val sqlConf = SQLConf.get
  private def ignoreMissingFiles = sqlConf.ignoreMissingFiles
  private def ignoreCorruptFiles = sqlConf.ignoreCorruptFiles

  override def next(): Boolean = {
    // there is a check for InputFileName expression that shouldn't allow user toget
    // input file name when the small file optimization is enabled, just set it to
    // empty string here to indicate its not available.
    InputFileBlockHolder.set("", 0, -1)

    if (currentReader == null) {
      return false
    }

    // In PartitionReader.next(), the current reader proceeds to next record.
    // It might throw RuntimeException/IOException and Spark should handle these exceptions.
    val hasNext = try {
      currentReader != null && currentReader.next()
    } catch {
      case e: SchemaColumnConvertNotSupportedException =>
        val message = "Parquet column cannot be converted in " +
          s"file ${currentReader.files}. Column: ${e.getColumn}, " +
          s"Expected: ${e.getLogicalType}, Found: ${e.getPhysicalType}"
        throw new QueryExecutionException(message, e)
      case e: ParquetDecodingException =>
        if (e.getMessage.contains("Can not read value at")) {
          val message = "Encounter error while reading parquet files. " +
            "One possible cause: Parquet column cannot be converted in the " +
            "corresponding files. Details: "
          throw new QueryExecutionException(message, e)
        }
        throw e
      case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
        logWarning(
          s"Skipped the rest of the content in the corrupted file: $currentReader", e)
        false
    }
    if (hasNext) {
      true
    } else {
      close()
      currentReader = null
      false
    }
  }

  override def get(): T = reader.get()

  override def close(): Unit = {
    if (currentReader != null) {
      currentReader.close()
      currentReader = null
    }
    InputFileBlockHolder.unset()
  }
}
