/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

import java.io.IOException

import ai.rapids.cudf.{DType, HostMemoryBuffer, Table}
import com.nvidia.spark.rapids.Arm.withResource
// ProtobufOptions and ProtobufUtils are in the same package
import com.nvidia.spark.rapids.shims.ShimFilePartitionReaderFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

/**
 * Schema definition for a protobuf field.
 * Maps a protobuf field number to a Spark data type.
 *
 * @param name Field name (will become column name)
 * @param fieldNumber Protobuf field number/tag
 * @param dataType Spark SQL data type
 */
case class ProtobufFieldSchema(
    name: String,
    fieldNumber: Int,
    dataType: DataType)

/**
 * Options for reading Protobuf files.
 *
 * @param schema Schema definition for protobuf fields
 * @param isHadoopSequenceFile Whether the file is in Hadoop SequenceFile format
 */
case class ProtobufReadOptions(
    schema: Seq[ProtobufFieldSchema],
    isHadoopSequenceFile: Boolean = true)

/**
 * GPU-accelerated Protobuf reader utility methods.
 */
object GpuProtobufScan extends Logging {

  /**
   * Convert Spark data type to cudf DType.
   */
  def sparkTypeToCudfDType(dataType: DataType): DType = {
    dataType match {
      case LongType => DType.INT64
      case IntegerType => DType.INT32
      case StringType => DType.STRING
      case DoubleType => DType.FLOAT64
      case FloatType => DType.FLOAT32
      case BooleanType => DType.BOOL8
      case ByteType => DType.INT8
      case ShortType => DType.INT16
      case _ => throw new IllegalArgumentException(
        s"Unsupported data type for protobuf: $dataType")
    }
  }

  /**
   * Tag GPU support for protobuf reading.
   */
  def tagSupport(meta: RapidsMeta[_, _, _], schema: StructType): Unit = {
    if (!meta.conf.isProtobufEnabled) {
      meta.willNotWorkOnGpu("Protobuf input has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_PROTOBUF} to true")
    }

    if (!meta.conf.isProtobufReadEnabled) {
      meta.willNotWorkOnGpu("Protobuf input has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_PROTOBUF_READ} to true")
    }

    // Check for unsupported data types
    schema.fields.foreach { field =>
      field.dataType match {
        case LongType | IntegerType | StringType | DoubleType |
             FloatType | BooleanType | ByteType | ShortType =>
        // Supported types
        case ArrayType(_, _) =>
          meta.willNotWorkOnGpu(s"Array type is not yet supported for protobuf field ${field.name}")
        case MapType(_, _, _) =>
          meta.willNotWorkOnGpu(s"Map type is not yet supported for protobuf field ${field.name}")
        case StructType(_) =>
          val msg = s"Nested struct is not yet supported for protobuf field ${field.name}"
          meta.willNotWorkOnGpu(msg)
        case dt =>
          meta.willNotWorkOnGpu(s"Data type $dt is not supported for protobuf field ${field.name}")
      }
    }
  }

  /**
   * Read protobuf data from a file and return a GPU table.
   *
   * @param filePath Path to the protobuf file
   * @param schema Schema definition for the protobuf fields
   * @param options Reading options
   * @param conf Hadoop configuration
   * @return GPU Table containing the parsed data
   */
  def readProtobufFile(
      filePath: Path,
      schema: Seq[ProtobufFieldSchema],
      options: ProtobufReadOptions,
      conf: Configuration): Table = {
    val fs = filePath.getFileSystem(conf)
    val fileStatus = fs.getFileStatus(filePath)
    val fileLength = fileStatus.getLen

    // Read file content into host memory
    withResource(HostMemoryBuffer.allocate(fileLength)) { hostBuffer =>
      withResource(fs.open(filePath)) { inputStream =>
        val buffer = new Array[Byte](8 * 1024 * 1024) // 8MB buffer
        var offset = 0L
        var bytesRead = inputStream.read(buffer)
        while (bytesRead > 0) {
          hostBuffer.setBytes(offset, buffer, 0, bytesRead)
          offset += bytesRead
          bytesRead = inputStream.read(buffer)
        }
      }

      // Build protobuf options
      val protobufOptionsBuilder = ProtobufOptions.builder()
      schema.foreach { field =>
        val dtype = sparkTypeToCudfDType(field.dataType)
        protobufOptionsBuilder.withField(field.name, field.fieldNumber, dtype)
      }
      protobufOptionsBuilder.withHadoopSequenceFile(options.isHadoopSequenceFile)

      // Read using spark-rapids-jni ProtobufUtils
      ProtobufUtils.readProtobuf(protobufOptionsBuilder.build(), hostBuffer, 0, fileLength)
    }
  }

  /**
   * Read protobuf data from a byte array.
   *
   * @param data Raw protobuf data
   * @param schema Schema definition
   * @param options Reading options
   * @return GPU Table containing the parsed data
   */
  def readProtobufBytes(
      data: Array[Byte],
      schema: Seq[ProtobufFieldSchema],
      options: ProtobufReadOptions): Table = {
    val protobufOptionsBuilder = ProtobufOptions.builder()
    schema.foreach { field =>
      val dtype = sparkTypeToCudfDType(field.dataType)
      protobufOptionsBuilder.withField(field.name, field.fieldNumber, dtype)
    }
    protobufOptionsBuilder.withHadoopSequenceFile(options.isHadoopSequenceFile)

    ProtobufUtils.readProtobuf(protobufOptionsBuilder.build(), data)
  }
}

/**
 * GPU-accelerated partition reader for Protobuf files.
 *
 * @param conf Hadoop configuration
 * @param partFile Partitioned file to read
 * @param readDataSchema Schema to read
 * @param protoSchema Protobuf field schema
 * @param options Reading options
 * @param metrics Execution metrics
 */
class GpuProtobufPartitionReader(
    conf: Configuration,
    partFile: PartitionedFile,
    readDataSchema: StructType,
    protoSchema: Seq[ProtobufFieldSchema],
    options: ProtobufReadOptions,
    metrics: Map[String, GpuMetric]) extends PartitionReader[ColumnarBatch] with Logging {

  private var batch: Option[ColumnarBatch] = None
  private var isDone = false

  override def next(): Boolean = {
    if (isDone) {
      false
    } else {
      batch = readBatch()
      isDone = true
      batch.isDefined
    }
  }

  override def get(): ColumnarBatch = {
    batch.getOrElse {
      throw new NoSuchElementException("No batch available")
    }
  }

  override def close(): Unit = {
    batch.foreach(_.close())
    batch = None
  }

  private def readBatch(): Option[ColumnarBatch] = {
    val readDataMetric = metrics.get(GpuMetric.READ_FS_TIME)
    val readStartTime = System.nanoTime()

    val path = new Path(partFile.filePath.toString())
    val fs = path.getFileSystem(conf)

    try {
      // Calculate actual read range
      val (readOffset, readLength) = if (partFile.start == 0 && partFile.length < 0) {
        // Read entire file
        (0L, fs.getFileStatus(path).getLen)
      } else {
        (partFile.start, partFile.length)
      }

      // For protobuf files, we typically read the entire file
      // because message boundaries don't align with byte boundaries
      val actualLength = if (readLength < 0) {
        fs.getFileStatus(path).getLen - readOffset
      } else {
        readLength
      }

      withResource(HostMemoryBuffer.allocate(actualLength)) { hostBuffer =>
        withResource(fs.open(path)) { inputStream =>
          inputStream.seek(readOffset)
          val buffer = new Array[Byte](8 * 1024 * 1024) // 8MB buffer
          var offset = 0L
          var remaining = actualLength
          while (remaining > 0) {
            val toRead = math.min(buffer.length, remaining.toInt)
            val bytesRead = inputStream.read(buffer, 0, toRead)
            if (bytesRead < 0) {
              throw new IOException(s"Unexpected end of file at offset ${readOffset + offset}")
            }
            hostBuffer.setBytes(offset, buffer, 0, bytesRead)
            offset += bytesRead
            remaining -= bytesRead
          }
        }

        readDataMetric.foreach(_ += System.nanoTime() - readStartTime)

        val decodeStartTime = System.nanoTime()

        // Build protobuf options
        val protobufOptionsBuilder = ProtobufOptions.builder()
        protoSchema.foreach { field =>
          val dtype = GpuProtobufScan.sparkTypeToCudfDType(field.dataType)
          protobufOptionsBuilder.withField(field.name, field.fieldNumber, dtype)
        }
        protobufOptionsBuilder.withHadoopSequenceFile(options.isHadoopSequenceFile)

        // Read and convert to columnar batch using ProtobufUtils
        val protoOpts = protobufOptionsBuilder.build()
        val table = ProtobufUtils.readProtobuf(protoOpts, hostBuffer, 0, actualLength)
        
        val decodeMetric = metrics.get(GpuMetric.GPU_DECODE_TIME)
        decodeMetric.foreach(_ += System.nanoTime() - decodeStartTime)

        if (table.getRowCount == 0) {
          table.close()
          None
        } else {
          withResource(table) { t =>
            Some(GpuColumnVector.from(t, readDataSchema.fields.map(_.dataType).toArray))
          }
        }
      }
    } catch {
      case e: Exception =>
        logError(s"Error reading protobuf file: ${path}", e)
        throw e
    }
  }
}

/**
 * Factory for creating GpuProtobufPartitionReader instances.
 *
 * @param sqlConf SQL configuration
 * @param broadcastedConf Broadcasted Hadoop configuration
 * @param readDataSchema Schema to read
 * @param protoSchema Protobuf field schema
 * @param protoOptions Reading options
 * @param metrics Execution metrics
 */
case class GpuProtobufPartitionReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    readDataSchema: StructType,
    protoSchema: Seq[ProtobufFieldSchema],
    protoOptions: ProtobufReadOptions,
    metrics: Map[String, GpuMetric],
    @transient params: Map[String, String])
  extends ShimFilePartitionReaderFactory(params) {

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("Row-based reading is not supported for GPU Protobuf reader")
  }

  override def buildColumnarReader(partFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    new GpuProtobufPartitionReader(
      conf, partFile, readDataSchema, protoSchema, protoOptions, metrics)
  }
}

/**
 * Utility object for reading Protobuf data in Spark.
 * Provides a high-level API for GPU-accelerated protobuf reading.
 */
object ProtobufDataReader extends Logging {
  
  /**
   * Read protobuf files into a DataFrame.
   *
   * @param spark SparkSession
   * @param path Path to protobuf files
   * @param schema Protobuf field schema definition
   * @param isHadoopSequenceFile Whether files are in Hadoop SequenceFile format
   * @return DataFrame containing the protobuf data
   */
  def read(
      spark: SparkSession,
      path: String,
      schema: Seq[ProtobufFieldSchema],
      isHadoopSequenceFile: Boolean = true): org.apache.spark.sql.DataFrame = {
    
    val sparkSchema = StructType(schema.map { field =>
      StructField(field.name, field.dataType, nullable = true)
    })
    
    val conf = spark.sparkContext.hadoopConfiguration
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(conf)
    val options = ProtobufReadOptions(schema, isHadoopSequenceFile)
    
    // Read all files and union results
    val files = if (fs.getFileStatus(hadoopPath).isDirectory) {
      fs.listStatus(hadoopPath).filter(_.isFile).map(_.getPath)
    } else {
      Array(hadoopPath)
    }
    
    val tables = files.flatMap { filePath =>
      try {
        Some(GpuProtobufScan.readProtobufFile(filePath, schema, options, conf))
      } catch {
        case e: Exception =>
          logWarning(s"Failed to read protobuf file: $filePath", e)
          None
      }
    }
    
    if (tables.isEmpty) {
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], sparkSchema)
    } else {
      // Convert tables to DataFrame and union
      val dfs = tables.map { table =>
        withResource(table) { t =>
          val batch = GpuColumnVector.from(t, sparkSchema.fields.map(_.dataType).toArray)
          withResource(batch) { b =>
            // Convert GPU batch to DataFrame
            val rows = (0 until b.numRows()).map { i =>
              Row.fromSeq(sparkSchema.fields.indices.map { j =>
                val col = b.column(j)
                if (col.isNullAt(i)) null
                else sparkSchema.fields(j).dataType match {
                  case LongType => col.getLong(i)
                  case IntegerType => col.getInt(i)
                  case StringType => col.getUTF8String(i).toString
                  case DoubleType => col.getDouble(i)
                  case FloatType => col.getFloat(i)
                  case BooleanType => col.getBoolean(i)
                  case ByteType => col.getByte(i)
                  case ShortType => col.getShort(i)
                  case _ => null
                }
              })
            }
            spark.createDataFrame(spark.sparkContext.parallelize(rows.toSeq), sparkSchema)
          }
        }
      }
      
      dfs.reduceOption(_ union _).getOrElse(
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], sparkSchema)
      )
    }
  }
}

