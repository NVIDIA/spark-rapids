package org.apache.iceberg.spark.source

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition}

class GpuSparkInputPartition(val cpuInputPartition: SparkInputPartition) extends
  InputPartition with HasPartitionKey with Serializable {
  override def preferredLocations(): Array[String] = cpuInputPartition.preferredLocations()

  override def partitionKey(): InternalRow = cpuInputPartition.partitionKey()
}
