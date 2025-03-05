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

package com.nvidia.spark.rapids.iceberg.spark.source;

import com.nvidia.spark.rapids.RapidsConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.types.Types;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.HasPartitionKey;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.util.SerializableConfiguration;

import java.io.Serializable;
import java.util.Collection;

/**
 * This class is derived from SparkInputPartition.
 */
class GpuSparkInputPartition implements InputPartition, HasPartitionKey, Serializable {
  private final Types.StructType groupingKeyType;
  private final ScanTaskGroup<?> taskGroup;
  private final Broadcast<Table> tableBroadcast;
  private final String branch;
  private final String expectedSchemaString;
  private final boolean caseSensitive;
  private final transient String[] preferredLocations;
  private transient Schema expectedSchema = null;

  ///  Spark rapids specific
  private final boolean useChunkedReader;
  private final long maxChunkedReaderMemoryUsageSizeBytes;
  private final Broadcast<SerializableConfiguration> confBroadcast;
  private final int maxBatchSizeRows;
  private final long maxBatchSizeBytes;

  private final long targetBatchSizeBytes;
  private final long maxGpuColumnSizeBytes;
  private final scala.Option<String> parquetDebugDumpPrefix;
  private final boolean parquetDebugDumpAlways;
  private final int numThreads;
  private final int maxNumFileProcessed;

  GpuSparkInputPartition(
      Types.StructType groupingKeyType,
      ScanTaskGroup<?> taskGroup,
      Broadcast<Table> tableBroadcast,
      String branch,
      String expectedSchemaString,
      boolean caseSensitive,
      String[] preferredLocations,
      RapidsConf rapidsConf,
      Broadcast<SerializableConfiguration> confBroadcast) {
    this.groupingKeyType = groupingKeyType;
    this.taskGroup = taskGroup;
    this.tableBroadcast = tableBroadcast;
    this.branch = branch;
    this.expectedSchemaString = expectedSchemaString;
    this.caseSensitive = caseSensitive;
    this.preferredLocations = preferredLocations;

    this.confBroadcast = confBroadcast;
    this.maxBatchSizeRows = rapidsConf.maxReadBatchSizeRows();
    this.maxBatchSizeBytes = rapidsConf.maxReadBatchSizeBytes();
    this.targetBatchSizeBytes = rapidsConf.gpuTargetBatchSizeBytes();
    this.maxGpuColumnSizeBytes = rapidsConf.maxGpuColumnSizeBytes();
    this.parquetDebugDumpPrefix = rapidsConf.parquetDebugDumpPrefix();
    this.parquetDebugDumpAlways = rapidsConf.parquetDebugDumpAlways();
    this.numThreads = rapidsConf.multiThreadReadNumThreads();
    this.maxNumFileProcessed = rapidsConf.maxNumParquetFilesParallel();
    this.useChunkedReader = rapidsConf.chunkedReaderEnabled();
    if (rapidsConf.limitChunkedReaderMemoryUsage()) {
      double limitRatio = rapidsConf.chunkedReaderMemoryUsageRatio();
      this.maxChunkedReaderMemoryUsageSizeBytes = (long) (limitRatio * this.targetBatchSizeBytes);
    } else {
      this.maxChunkedReaderMemoryUsageSizeBytes = 0L;
    }
  }

  @Override
  public String[] preferredLocations() {
    return preferredLocations;
  }

  @Override
  public InternalRow partitionKey() {
    return new StructInternalRow(groupingKeyType).setStruct(taskGroup.groupingKey());
  }

  @SuppressWarnings("unchecked")
  public <T extends ScanTask> ScanTaskGroup<T> taskGroup() {
    return (ScanTaskGroup<T>) taskGroup;
  }

  public <T extends ScanTask> boolean allTasksOfType(Class<T> javaClass) {
    return taskGroup.tasks().stream().allMatch(javaClass::isInstance);
  }

  public Table table() {
    return tableBroadcast.value();
  }

  public String branch() {
    return branch;
  }

  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  public Schema expectedSchema() {
    if (expectedSchema == null) {
      this.expectedSchema = SchemaParser.fromJson(expectedSchemaString);
    }
    return expectedSchema;
  }

  public Configuration getConfiguration() {
    return confBroadcast.value().value();
  }

  public int getMaxBatchSizeRows() {
    return maxBatchSizeRows;
  }

  public long getMaxBatchSizeBytes() {
    return maxBatchSizeBytes;
  }

  public long getTargetBatchSizeBytes() {
    return targetBatchSizeBytes;
  }

  public long getMaxGpuColumnSizeBytes() {
    return maxGpuColumnSizeBytes;
  }

  public scala.Option<String> getParquetDebugDumpPrefix() {
    return parquetDebugDumpPrefix;
  }

  public boolean getParquetDebugDumpAlways() {
    return parquetDebugDumpAlways;
  }

  public int getNumThreads() {
    return numThreads;
  }

  public int getMaxNumFileProcessed() {
    return maxNumFileProcessed;
  }

  public boolean useChunkedReader() {
    return useChunkedReader;
  }

  public long maxChunkedReaderMemoryUsageSizeBytes() {
    return maxChunkedReaderMemoryUsageSizeBytes;
  }
}
