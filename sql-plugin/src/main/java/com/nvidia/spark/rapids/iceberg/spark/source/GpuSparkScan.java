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

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.nvidia.spark.rapids.GpuMetric;
import com.nvidia.spark.rapids.RapidsConf;
import com.nvidia.spark.rapids.ScanWithMetricsWrapper;
import com.nvidia.spark.rapids.iceberg.spark.Spark3Util;
import com.nvidia.spark.rapids.iceberg.spark.SparkReadConf;
import com.nvidia.spark.rapids.iceberg.spark.SparkSchemaUtil;
import com.nvidia.spark.rapids.iceberg.spark.SparkUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.util.SerializableConfiguration;

/**
 * GPU-accelerated Iceberg Scan.
 * This is derived from Apache Iceberg's SparkScan class.
 */
abstract class GpuSparkScan extends ScanWithMetricsWrapper
    implements Scan, SupportsReportStatistics {
  private static final Logger LOG = LoggerFactory.getLogger(GpuSparkScan.class);

  private final JavaSparkContext sparkContext;
  private final Table table;
  private final SparkReadConf readConf;
  private final boolean caseSensitive;
  private final Schema expectedSchema;
  private final List<Expression> filterExpressions;
  private final boolean readTimestampWithoutZone;
  private final RapidsConf rapidsConf;

  // lazy variables
  private StructType readSchema = null;

  GpuSparkScan(SparkSession spark, Table table, SparkReadConf readConf,
               Schema expectedSchema, List<Expression> filters,
               RapidsConf rapidsConf) {

    SparkSchemaUtil.validateMetadataColumnReferences(table.schema(), expectedSchema);

    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    this.table = table;
    this.readConf = readConf;
    this.caseSensitive = readConf.caseSensitive();
    this.expectedSchema = expectedSchema;
    this.filterExpressions = filters != null ? filters : Collections.emptyList();
    this.readTimestampWithoutZone = readConf.handleTimestampWithoutZone();
    this.rapidsConf = rapidsConf;
  }

  protected Table table() {
    return table;
  }

  protected boolean caseSensitive() {
    return caseSensitive;
  }

  protected Schema expectedSchema() {
    return expectedSchema;
  }

  protected List<Expression> filterExpressions() {
    return filterExpressions;
  }

  protected abstract List<CombinedScanTask> tasks();

  @Override
  public Batch toBatch() {
    return new SparkBatch(sparkContext, table, readConf, tasks(), expectedSchema,
        rapidsConf, this);
  }

  @Override
  public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
    throw new IllegalStateException("Unexpected micro batch stream read");
  }

  @Override
  public StructType readSchema() {
    if (readSchema == null) {
      Preconditions.checkArgument(readTimestampWithoutZone || !SparkUtil.hasTimestampWithoutZone(expectedSchema),
          SparkUtil.TIMESTAMP_WITHOUT_TIMEZONE_ERROR);
      this.readSchema = SparkSchemaUtil.convert(expectedSchema);
    }
    return readSchema;
  }

  @Override
  public Statistics estimateStatistics() {
    return estimateStatistics(table.currentSnapshot());
  }

  protected Statistics estimateStatistics(Snapshot snapshot) {
    // its a fresh table, no data
    if (snapshot == null) {
      return new Stats(0L, 0L);
    }

    // estimate stats using snapshot summary only for partitioned tables (metadata tables are unpartitioned)
    if (!table.spec().isUnpartitioned() && filterExpressions.isEmpty()) {
      LOG.debug("using table metadata to estimate table statistics");
      long totalRecords = PropertyUtil.propertyAsLong(snapshot.summary(),
          SnapshotSummary.TOTAL_RECORDS_PROP, Long.MAX_VALUE);
      return new Stats(
          SparkSchemaUtil.estimateSize(readSchema(), totalRecords),
          totalRecords);
    }

    long numRows = 0L;

    for (CombinedScanTask task : tasks()) {
      for (FileScanTask file : task.files()) {
        // TODO: if possible, take deletes also into consideration.
        double fractionOfFileScanned = ((double) file.length()) / file.file().fileSizeInBytes();
        numRows += (fractionOfFileScanned * file.file().recordCount());
      }
    }

    long sizeInBytes = SparkSchemaUtil.estimateSize(readSchema(), numRows);
    return new Stats(sizeInBytes, numRows);
  }

  @Override
  public String description() {
    String filters = filterExpressions.stream().map(Spark3Util::describe).collect(Collectors.joining(", "));
    return String.format("%s [filters=%s]", table, filters);
  }

  static class ReaderFactory implements PartitionReaderFactory {
    private final scala.collection.immutable.Map<String, GpuMetric> metrics;

    public ReaderFactory(scala.collection.immutable.Map<String, GpuMetric> metrics) {
      this.metrics = metrics;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
      throw new IllegalStateException("non-columnar read");
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
      if (partition instanceof ReadTask) {
        return new BatchReader((ReadTask) partition, metrics);
      } else {
        throw new UnsupportedOperationException("Incorrect input partition type: " + partition);
      }
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
      return true;
    }
  }

  private static class BatchReader extends GpuBatchDataReader implements PartitionReader<ColumnarBatch> {
    BatchReader(ReadTask task, scala.collection.immutable.Map<String, GpuMetric> metrics) {
      super(task.task, task.table(), task.expectedSchema(), task.isCaseSensitive(),
          task.getConfiguration(), task.getMaxBatchSizeRows(), task.getMaxBatchSizeBytes(),
          task.getParquetDebugDumpPrefix(), metrics);
    }
  }

  static class ReadTask implements InputPartition, Serializable {
    private final CombinedScanTask task;
    private final Broadcast<Table> tableBroadcast;
    private final String expectedSchemaString;
    private final boolean caseSensitive;

    private final Broadcast<SerializableConfiguration> confBroadcast;
    private final int maxBatchSizeRows;
    private final long maxBatchSizeBytes;
    private final String parquetDebugDumpPrefix;

    private transient Schema expectedSchema = null;
    private transient String[] preferredLocations = null;

    ReadTask(CombinedScanTask task, Broadcast<Table> tableBroadcast, String expectedSchemaString,
             boolean caseSensitive, boolean localityPreferred, RapidsConf rapidsConf,
             Broadcast<SerializableConfiguration> confBroadcast) {
      this.task = task;
      this.tableBroadcast = tableBroadcast;
      this.expectedSchemaString = expectedSchemaString;
      this.caseSensitive = caseSensitive;
      if (localityPreferred) {
        Table table = tableBroadcast.value();
        this.preferredLocations = Util.blockLocations(table.io(), task);
      } else {
        this.preferredLocations = HadoopInputFile.NO_LOCATION_PREFERENCE;
      }
      this.confBroadcast = confBroadcast;
      this.maxBatchSizeRows = rapidsConf.maxReadBatchSizeRows();
      this.maxBatchSizeBytes = rapidsConf.maxReadBatchSizeBytes();
      this.parquetDebugDumpPrefix = rapidsConf.parquetDebugDumpPrefix();
    }

    @Override
    public String[] preferredLocations() {
      return preferredLocations;
    }

    public Collection<FileScanTask> files() {
      return task.files();
    }

    public Table table() {
      return tableBroadcast.value();
    }

    public boolean isCaseSensitive() {
      return caseSensitive;
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

    public String getParquetDebugDumpPrefix() {
      return parquetDebugDumpPrefix;
    }

    private Schema expectedSchema() {
      if (expectedSchema == null) {
        this.expectedSchema = SchemaParser.fromJson(expectedSchemaString);
      }
      return expectedSchema;
    }
  }
}
