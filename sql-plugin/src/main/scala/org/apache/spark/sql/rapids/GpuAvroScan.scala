package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.{Arm, ColumnarPartitionReaderWithPartitionValues, FilePartitionReaderBase, GpuMetric, PartitionReaderWithBytesRead, RapidsConf, RapidsMeta}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.AvroOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

object GpuAvroScan {

  def tagSupport(
    sparkSession: SparkSession,
    dataSchema: StructType,
    readSchema: StructType,
    options: Map[String, String],
    meta: RapidsMeta[_, _, _]): Unit = {
  }

}

case class GpuAvroPartitionReaderFactory(
  sqlConf: SQLConf,
  broadcastedConf: Broadcast[SerializableConfiguration],
  dataSchema: StructType,
  readDataSchema: StructType,
  partitionSchema: StructType,
  parsedOptions: AvroOptions,
  @transient rapidsConf: RapidsConf,
  metrics: Map[String, GpuMetric]) extends FilePartitionReaderFactory {

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("ROW BASED PARSING IS NOT SUPPORTED ON THE GPU...")
  }

  override def buildColumnarReader(partFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    val reader = new PartitionReaderWithBytesRead(new AvroPartitionReader(conf, partFile,
      dataSchema, readDataSchema, metrics))
    ColumnarPartitionReaderWithPartitionValues.newReader(partFile, reader, partitionSchema)
  }
}

case class AvroBlockRange(start: Long, length: Long)

/**
 * A tool to filter Avro blocks
 *
 * @param sqlConf         SQLConf
 * @param broadcastedConf the Hadoop configuration
 */
private case class GpuAvroFileFilterHandler(
  @transient sqlConf: SQLConf,
  broadcastedConf: Broadcast[SerializableConfiguration]) extends Arm {

  def filterBlocks(partFile: PartitionedFile): BufferedIterator[AvroBlockRange] = {
    Iterator.empty.buffered
  }

}

class AvroPartitionReader(
  conf: Configuration,
  partFile: PartitionedFile,
  dataSchema: StructType,
  readDataSchema: StructType,
  execMetrics: Map[String, GpuMetric]) extends FilePartitionReaderBase(conf, execMetrics) {

  override def next(): Boolean = false
}
