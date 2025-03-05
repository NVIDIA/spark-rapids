/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

import java.util.List;
import java.util.Objects;

import com.nvidia.spark.rapids.RapidsConf;
import com.nvidia.spark.rapids.iceberg.spark.SparkReadConf;
import com.nvidia.spark.rapids.iceberg.spark.SparkUtil;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.util.SerializableConfiguration;

/** Derived from Apache Iceberg's SparkBatch class. */
public class GpuSparkBatch implements Batch {

  private final JavaSparkContext sparkContext;
  private final Table table;
  private final String branch;
  private final SparkReadConf readConf;
  private final Types.StructType groupingKeyType;
  private final List<? extends ScanTaskGroup<?>> taskGroups;
  private final Schema expectedSchema;
  private final boolean caseSensitive;
  private final boolean localityEnabled;
  private final boolean executorCacheLocalityEnabled;
  private final int scanHashCode;

  private final RapidsConf rapidsConf;
  private final GpuSparkScan parentScan;

  GpuSparkBatch(
      JavaSparkContext sparkContext,
      Table table,
      SparkReadConf readConf,
      Types.StructType groupingKeyType,
      List<? extends ScanTaskGroup<?>> taskGroups,
      Schema expectedSchema,
      int scanHashCode,
      RapidsConf rapidsConf,
      GpuSparkScan parentScan) {
    this.sparkContext = sparkContext;
    this.table = table;
    this.branch = readConf.branch();
    this.readConf = readConf;
    this.groupingKeyType = groupingKeyType;
    this.taskGroups = taskGroups;
    this.expectedSchema = expectedSchema;
    this.caseSensitive = readConf.caseSensitive();
    this.localityEnabled = readConf.localityEnabled();
    this.executorCacheLocalityEnabled = readConf.executorCacheLocalityEnabled();
    this.scanHashCode = scanHashCode;

    this.rapidsConf = rapidsConf;
    this.parentScan = parentScan;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    // broadcast the table metadata as input partitions will be sent to executors
    Broadcast<Table> tableBroadcast =
        sparkContext.broadcast(SerializableTableWithSize.copyOf(table));
    String expectedSchemaString = SchemaParser.toJson(expectedSchema);
    String[][] locations = computePreferredLocations();

    Broadcast<SerializableConfiguration> confBroadcast = sparkContext.broadcast(
        new SerializableConfiguration(sparkContext.hadoopConfiguration()));

    InputPartition[] partitions = new InputPartition[taskGroups.size()];

    for (int index = 0; index < taskGroups.size(); index++) {
      partitions[index] =
          new GpuSparkInputPartition(
              groupingKeyType,
              taskGroups.get(index),
              tableBroadcast,
              branch,
              expectedSchemaString,
              caseSensitive,
              locations != null ? locations[index] : SparkPlanningUtil.NO_LOCATION_PREFERENCE,
              rapidsConf,
              confBroadcast);
    }

    return partitions;
  }

  private String[][] computePreferredLocations() {
    if (localityEnabled) {
      return SparkPlanningUtil.fetchBlockLocations(table.io(), taskGroups);

    } else if (executorCacheLocalityEnabled) {
      List<String> executorLocations = SparkUtil.executorLocations();
      if (!executorLocations.isEmpty()) {
        return SparkPlanningUtil.assignExecutors(taskGroups, executorLocations);
      }
    }

    return null;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new GpuReaderFactory(parentScan.metrics(), rapidsConf,
        parentScan.queryUsesInputFile());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GpuSparkBatch that = (GpuSparkBatch) o;
    // Emulating Apache Iceberg old SparkScan behavior where the scan was the batch
    // to fix exchange reuse with DPP.
    return table.name().equals(that.table.name())
        && scanHashCode == that.scanHashCode
        && Objects.equals(parentScan, that.parentScan);
  }

  @Override
  public int hashCode() {
    // Emulating Apache Iceberg old SparkScan behavior where the scan was the batch
    // to fix exchange reuse with DPP.
    return Objects.hash(table.name(), scanHashCode);
  }
}
