/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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
import com.nvidia.spark.rapids.GpuScanWrapper;
import com.nvidia.spark.rapids.MultiFileReaderUtils;
import com.nvidia.spark.rapids.RapidsConf;
import com.nvidia.spark.rapids.iceberg.spark.Spark3Util;
import com.nvidia.spark.rapids.iceberg.spark.SparkReadConf;
import com.nvidia.spark.rapids.iceberg.spark.SparkSchemaUtil;
import com.nvidia.spark.rapids.iceberg.spark.SparkUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileFormat;
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
abstract class GpuSparkScan extends GpuScanWrapper
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
  private final boolean queryUsesInputFile;

  // lazy variables
  private StructType readSchema = null;

  GpuSparkScan(SparkSession spark, Table table, SparkReadConf readConf,
               Schema expectedSchema, List<Expression> filters,
               RapidsConf rapidsConf, boolean queryUsesInputFile) {

    SparkSchemaUtil.validateMetadataColumnReferences(table.schema(), expectedSchema);

    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    this.table = table;
    this.readConf = readConf;
    this.caseSensitive = readConf.caseSensitive();
    this.expectedSchema = expectedSchema;
    this.filterExpressions = filters != null ? filters : Collections.emptyList();
    this.readTimestampWithoutZone = readConf.handleTimestampWithoutZone();
    this.rapidsConf = rapidsConf;
    this.queryUsesInputFile = queryUsesInputFile;
  }

  protected Table table() {
    return table;
  }

  protected SparkReadConf readConf() {
    return readConf;
  }

  protected RapidsConf rapidsConf() {
    return rapidsConf;
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

  boolean queryUsesInputFile() {
    return queryUsesInputFile;
  }

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
    private final scala.collection.immutable.Set<String> allCloudSchemes;
    private final boolean canUseParquetMultiThread;
    private final boolean canUseParquetCoalescing;
    private final boolean isParquetPerFileReadEnabled;
    private final boolean queryUsesInputFile;

    public ReaderFactory(scala.collection.immutable.Map<String, GpuMetric> metrics,
                         RapidsConf rapidsConf, boolean queryUsesInputFile) {
      this.metrics = metrics;
      this.allCloudSchemes = rapidsConf.getCloudSchemes().toSet();
      this.isParquetPerFileReadEnabled = rapidsConf.isParquetPerFileReadEnabled();
      this.canUseParquetMultiThread = rapidsConf.isParquetMultiThreadReadEnabled();
      // Here ignores the "ignoreCorruptFiles" comparing to the code in
      // "GpuParquetMultiFilePartitionReaderFactory", since "ignoreCorruptFiles" is
      // not honored by Iceberg.
      this.canUseParquetCoalescing = rapidsConf.isParquetCoalesceFileReadEnabled() &&
          !queryUsesInputFile;
      this.queryUsesInputFile = queryUsesInputFile;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
      throw new IllegalStateException("non-columnar read");
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
      if (partition instanceof ReadTask) {
        ReadTask rTask = (ReadTask) partition;
        scala.Tuple3<Boolean, Boolean, FileFormat> ret = multiFileReadCheck(rTask);
        boolean canAccelerateRead = ret._1();
        if (canAccelerateRead) {
          boolean isMultiThread = ret._2();
          FileFormat ff = ret._3();
          return new MultiFileBatchReader(rTask, isMultiThread, ff, metrics, queryUsesInputFile);
        } else {
          return new BatchReader(rTask, metrics);
        }
      } else {
        throw new UnsupportedOperationException("Incorrect input partition type: " + partition);
      }
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
      return true;
    }

    /**
     * Return a tuple as (canAccelerateRead, isMultiThread, fileFormat).
     *   - "canAccelerateRead" Whether the input read task can be accelerated by
     *     multi-threaded or coalescing reading.
     *   - "isMultiThread" Whether to use the multi-threaded reading.
     *   - "fileFormat" The file format of this combined task. Acceleration requires
     *     all the files in a combined task have the same format.
     */
    private scala.Tuple3<Boolean, Boolean, FileFormat> multiFileReadCheck(ReadTask readTask) {
      Collection<FileScanTask> scans = readTask.files();
      boolean isSingleFormat = false, isPerFileReadEnabled = false;
      boolean canUseMultiThread = false, canUseCoalescing = false;
      FileFormat ff = null;
      // Require all the files in a partition have the same file format.
      if (scans.stream().allMatch(t -> t.file().format().equals(FileFormat.PARQUET))) {
        // Now only Parquet is supported.
        canUseMultiThread = canUseParquetMultiThread;
        canUseCoalescing = canUseParquetCoalescing;
        isPerFileReadEnabled = isParquetPerFileReadEnabled;
        isSingleFormat = true;
        ff = FileFormat.PARQUET;
      }
      boolean canAccelerateRead = !isPerFileReadEnabled && isSingleFormat;
      String[] files = scans.stream().map(f -> f.file().path().toString())
          .toArray(String[]::new);
      // Get the final decision for the subtype of the Rapids reader.
      boolean useMultiThread = MultiFileReaderUtils.useMultiThreadReader(
          canUseCoalescing, canUseMultiThread, files, allCloudSchemes, false);
      return scala.Tuple3.apply(canAccelerateRead, useMultiThread, ff);
    }
  }

  private static class MultiFileBatchReader
      extends GpuMultiFileBatchReader implements PartitionReader<ColumnarBatch> {
    MultiFileBatchReader(ReadTask task, boolean useMultiThread, FileFormat ff,
                         scala.collection.immutable.Map<String, GpuMetric> metrics,
                         boolean queryUsesInputFile) {
      super(task.task, task.table(), task.expectedSchema(), task.isCaseSensitive(),
          task.getConfiguration(), task.getMaxBatchSizeRows(), task.getMaxBatchSizeBytes(),
          task.getTargetBatchSizeBytes(), task.getMaxGpuColumnSizeBytes(), task.useChunkedReader(),
          task.maxChunkedReaderMemoryUsageSizeBytes(),
          task.getParquetDebugDumpPrefix(), task.getParquetDebugDumpAlways(),
          task.getNumThreads(), task.getMaxNumFileProcessed(),
          useMultiThread, ff, metrics, queryUsesInputFile);
    }
  }

  private static class BatchReader extends GpuBatchDataReader implements PartitionReader<ColumnarBatch> {
    BatchReader(ReadTask task, scala.collection.immutable.Map<String, GpuMetric> metrics) {
      super(task.task, task.table(), task.expectedSchema(), task.isCaseSensitive(),
          task.getConfiguration(), task.getMaxBatchSizeRows(), task.getMaxBatchSizeBytes(),
          task.getTargetBatchSizeBytes(), task.useChunkedReader(), task.maxChunkedReaderMemoryUsageSizeBytes(),
          task.getParquetDebugDumpPrefix(), task.getParquetDebugDumpAlways(), metrics);
    }
  }

  static class ReadTask implements InputPartition, Serializable {
    private final CombinedScanTask task;
    private final Broadcast<Table> tableBroadcast;
    private final String expectedSchemaString;
    private final boolean caseSensitive;
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
      this.targetBatchSizeBytes = rapidsConf.gpuTargetBatchSizeBytes();
      this.maxGpuColumnSizeBytes = rapidsConf.maxGpuColumnSizeBytes();
      this.parquetDebugDumpPrefix = rapidsConf.parquetDebugDumpPrefix();
      this.parquetDebugDumpAlways = rapidsConf.parquetDebugDumpAlways();
      this.numThreads = rapidsConf.multiThreadReadNumThreads();
      this.maxNumFileProcessed = rapidsConf.maxNumParquetFilesParallel();
      this.useChunkedReader = rapidsConf.chunkedReaderEnabled();
      if(rapidsConf.limitChunkedReaderMemoryUsage()) {
        double limitRatio = rapidsConf.chunkedReaderMemoryUsageRatio();
        this.maxChunkedReaderMemoryUsageSizeBytes = (long)(limitRatio * this.targetBatchSizeBytes);
      } else {
        this.maxChunkedReaderMemoryUsageSizeBytes = 0L;
      }
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

    private Schema expectedSchema() {
      if (expectedSchema == null) {
        this.expectedSchema = SchemaParser.fromJson(expectedSchemaString);
      }
      return expectedSchema;
    }

    public boolean useChunkedReader() {
      return useChunkedReader;
    }

    public long maxChunkedReaderMemoryUsageSizeBytes() {
      return maxChunkedReaderMemoryUsageSizeBytes;
    }
  }
}
