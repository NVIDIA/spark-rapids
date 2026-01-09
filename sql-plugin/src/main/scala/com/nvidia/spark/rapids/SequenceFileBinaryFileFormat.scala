/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{DataOutputBuffer, SequenceFile}
import org.apache.hadoop.mapreduce.Job
import org.slf4j.LoggerFactory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

/**
 * A Spark SQL file format that reads Hadoop SequenceFiles and returns raw bytes for key/value.
 *
 * The schema is always:
 *  - key: BinaryType
 *  - value: BinaryType
 *
 * This format is intended to support protobuf payloads stored as raw bytes in the SequenceFile
 * record value bytes.
 */
class SequenceFileBinaryFileFormat extends FileFormat with DataSourceRegister with Serializable {
  import SequenceFileBinaryFileFormat._

  override def shortName(): String = SHORT_NAME

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = Some(dataSchema)

  // TODO: Fix split boundary handling to enable multi-partition reads
  // Currently disabled to ensure correct record counts
  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = false

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    // Hadoop Configuration is not serializable; Spark will serialize the returned reader function.
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    (partFile: PartitionedFile) => {
      val filePathStr = partFile.filePath.toString
      val path = new Path(new URI(filePathStr))
      val conf = new Configuration(broadcastedHadoopConf.value.value)
      val reader =
        try {
          new SequenceFile.Reader(conf, SequenceFile.Reader.file(path))
        } catch {
          case e: Exception =>
            val msg = s"Failed to open SequenceFile reader for $path"
            LoggerFactory.getLogger(classOf[SequenceFileBinaryFileFormat]).error(msg, e)
            throw e
        }

      // For the initial version, we explicitly fail fast on compressed SequenceFiles.
      // (Record- and block-compressed files can be added later.)
      if (reader.isCompressed || reader.isBlockCompressed) {
        val msg = s"$SHORT_NAME does not support compressed SequenceFiles " +
          s"(isCompressed=${reader.isCompressed}, " +
          s"isBlockCompressed=${reader.isBlockCompressed}), " +
          s"file=$path, keyClass=${reader.getKeyClassName}, " +
          s"valueClass=${reader.getValueClassName}"
        LoggerFactory.getLogger(classOf[SequenceFileBinaryFileFormat]).error(msg)
        reader.close()
        throw new UnsupportedOperationException(msg)
      }

      val start = partFile.start
      val end = start + partFile.length
      
      // Debug logging
      val log = LoggerFactory.getLogger(classOf[SequenceFileBinaryFileFormat])
      log.info(s"[DEBUG] Split: start=$start, end=$end, length=${partFile.length}, file=$path")
      
      if (start > 0) {
        // sync(position) jumps to the first sync point AFTER position.
        // If position is exactly at a sync point, it skips to the NEXT one.
        // Use sync(start - 1) to ensure we don't miss records at the split boundary.
        reader.sync(start - 1)
        log.info(s"[DEBUG] After sync(${start - 1}): position=${reader.getPosition}")
      }

      val reqFields = requiredSchema.fields
      val reqLen = reqFields.length
      val partLen = partitionSchema.length
      val totalLen = reqLen + partLen
      val outputSchema = StructType(requiredSchema.fields ++ partitionSchema.fields)

      val wantKey = requiredSchema.fieldNames.exists(_.equalsIgnoreCase(KEY_FIELD))
      val wantValue = requiredSchema.fieldNames.exists(_.equalsIgnoreCase(VALUE_FIELD))

      val keyBuf = new DataOutputBuffer()
      val valueBytes = reader.createValueBytes()
      val valueOut = new DataOutputBuffer()
      val valueDos = new DataOutputStream(valueOut)

      new Iterator[InternalRow] {
        private[this] val unsafeProj = UnsafeProjection.create(outputSchema)
        private[this] var nextRow: InternalRow = _
        private[this] var prepared = false
        private[this] var done = false

        override def hasNext: Boolean = {
          if (!prepared && !done) {
            prepared = true
            keyBuf.reset()
            // Check position BEFORE reading the next record.
            // If current position >= end, this record belongs to the next split.
            if (reader.getPosition >= end) {
              done = true
              close()
            } else if (reader.nextRaw(keyBuf, valueBytes) >= 0) {
              nextRow = buildRow()
            } else {
              done = true
              close()
            }
          }
          !done
        }

        override def next(): InternalRow = {
          if (!hasNext) {
            throw new NoSuchElementException("End of stream")
          }
          prepared = false
          val ret = nextRow
          nextRow = null
          ret
        }

        private def buildRow(): InternalRow = {
          val row = new GenericInternalRow(totalLen)
          var i = 0
          while (i < reqLen) {
            val name = reqFields(i).name
            if (name.equalsIgnoreCase(KEY_FIELD)) {
              if (wantKey) {
                val keyLen = keyBuf.getLength
                row.update(i, util.Arrays.copyOf(keyBuf.getData, keyLen))
              } else {
                row.setNullAt(i)
              }
            } else if (name.equalsIgnoreCase(VALUE_FIELD)) {
              if (wantValue) {
                valueOut.reset()
                valueBytes.writeUncompressedBytes(valueDos)
                val valueLen = valueOut.getLength
                row.update(i, util.Arrays.copyOf(valueOut.getData, valueLen))
              } else {
                row.setNullAt(i)
              }
            } else {
              // Unknown column requested
              row.setNullAt(i)
            }
            i += 1
          }

          // Append partition values (if any)
          var p = 0
          while (p < partLen) {
            val dt = partitionSchema.fields(p).dataType
            row.update(reqLen + p, partFile.partitionValues.get(p, dt))
            p += 1
          }
          // Spark expects UnsafeRow for downstream serialization.
          unsafeProj.apply(row).copy()
        }

        private def close(): Unit = {
          reader.close()
        }
      }
    }
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    throw new UnsupportedOperationException(
      s"${this.getClass.getCanonicalName} does not support writing")
  }
}

object SequenceFileBinaryFileFormat {
  final val SHORT_NAME: String = "sequencefilebinary"
  final val KEY_FIELD: String = "key"
  final val VALUE_FIELD: String = "value"

  final val dataSchema: StructType = StructType(Seq(
    StructField(KEY_FIELD, BinaryType, nullable = true),
    StructField(VALUE_FIELD, BinaryType, nullable = true)
  ))
}


