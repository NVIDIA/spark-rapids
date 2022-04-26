/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.GpuMetric;
import com.nvidia.spark.rapids.RapidsConf;
import com.nvidia.spark.rapids.iceberg.spark.SparkReadConf;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.util.SerializableConfiguration;

public class SparkBatch implements Batch {

  private final JavaSparkContext sparkContext;
  private final Table table;
  private final SparkReadConf readConf;
  private final List<CombinedScanTask> tasks;
  private final Schema expectedSchema;
  private final boolean caseSensitive;
  private final boolean localityEnabled;
  private final RapidsConf rapidsConf;
  private final scala.collection.immutable.Map<String, GpuMetric> metrics;

  SparkBatch(JavaSparkContext sparkContext, Table table, SparkReadConf readConf,
             List<CombinedScanTask> tasks, Schema expectedSchema,
             RapidsConf rapidsConf,
             scala.collection.immutable.Map<String, GpuMetric> metrics) {
    this.sparkContext = sparkContext;
    this.table = table;
    this.readConf = readConf;
    this.tasks = tasks;
    this.expectedSchema = expectedSchema;
    this.caseSensitive = readConf.caseSensitive();
    this.localityEnabled = readConf.localityEnabled();
    this.rapidsConf = rapidsConf;
    this.metrics = metrics;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    // broadcast the table metadata as input partitions will be sent to executors
    Broadcast<Table> tableBroadcast = sparkContext.broadcast(SerializableTable.copyOf(table));
    Broadcast<SerializableConfiguration> confBroadcast = sparkContext.broadcast(
        new SerializableConfiguration(sparkContext.hadoopConfiguration()));
    String expectedSchemaString = SchemaParser.toJson(expectedSchema);

    InputPartition[] readTasks = new InputPartition[tasks.size()];

    Tasks.range(readTasks.length)
        .stopOnFailure()
        .executeWith(localityEnabled ? ThreadPools.getWorkerPool() : null)
        .run(index -> readTasks[index] = new GpuSparkScan.ReadTask(
            tasks.get(index), tableBroadcast, expectedSchemaString,
            caseSensitive, localityEnabled, rapidsConf, confBroadcast,
            metrics));

    return readTasks;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new GpuSparkScan.ReaderFactory();
  }

//  private int batchSize() {
//    if (parquetOnly() && parquetBatchReadsEnabled()) {
//      return readConf.parquetBatchSize();
//    } else if (orcOnly() && orcBatchReadsEnabled()) {
//      return readConf.orcBatchSize();
//    } else {
//      return 0;
//    }
//  }
//
//  private boolean parquetOnly() {
//    return tasks.stream().allMatch(task -> !task.isDataTask() && onlyFileFormat(task, FileFormat.PARQUET));
//  }
//
//  private boolean parquetBatchReadsEnabled() {
//    return readConf.parquetVectorizationEnabled() && // vectorization enabled
//        expectedSchema.columns().size() > 0 && // at least one column is projected
//        expectedSchema.columns().stream().allMatch(c -> c.type().isPrimitiveType()); // only primitives
//  }
//
//  private boolean orcOnly() {
//    return tasks.stream().allMatch(task -> !task.isDataTask() && onlyFileFormat(task, FileFormat.ORC));
//  }
//
//  private boolean orcBatchReadsEnabled() {
//    return readConf.orcVectorizationEnabled() && // vectorization enabled
//        tasks.stream().noneMatch(TableScanUtil::hasDeletes); // no delete files
//  }
//
//  private boolean onlyFileFormat(CombinedScanTask task, FileFormat fileFormat) {
//    return task.files().stream().allMatch(fileScanTask -> fileScanTask.file().format().equals(fileFormat));
//  }
}
