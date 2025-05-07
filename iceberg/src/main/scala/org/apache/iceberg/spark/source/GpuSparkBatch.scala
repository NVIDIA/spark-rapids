package org.apache.iceberg.spark.source

import java.util.Objects

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}

class GpuSparkBatch(
    val cpuBatch: SparkBatch,
    val parentScan: GpuSparkScan,
) extends Batch  {
  override def createReaderFactory(): PartitionReaderFactory = {
    throw new UnsupportedOperationException(
      "GpuSparkBatch does not support createReaderFactory()")
  }

  override def planInputPartitions(): Array[InputPartition] = {
    cpuBatch.planInputPartitions().map { partition =>
      new GpuSparkInputPartition(partition.asInstanceOf[SparkInputPartition])
    }
  }

  override def hashCode(): Int = {
    Objects.hash(cpuBatch, parentScan)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: GpuSparkBatch =>
        this.cpuBatch == that.cpuBatch && this.parentScan == that.parentScan
      case _ => false
    }
  }
}
