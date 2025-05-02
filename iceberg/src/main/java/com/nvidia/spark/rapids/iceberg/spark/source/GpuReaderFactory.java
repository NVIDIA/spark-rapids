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

import com.nvidia.spark.rapids.GpuMetric;
import com.nvidia.spark.rapids.MultiFileReaderUtils;
import com.nvidia.spark.rapids.RapidsConf;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTask;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.stream.Collectors;

class GpuReaderFactory implements PartitionReaderFactory {
  private static final Logger LOG = LoggerFactory.getLogger(GpuReaderFactory.class);
  private final scala.collection.immutable.Map<String, GpuMetric> metrics;
  private final scala.collection.immutable.Set<String> allCloudSchemes;
  private final boolean canUseParquetMultiThread;
  private final boolean canUseParquetCoalescing;
  private final boolean isParquetPerFileReadEnabled;

  public GpuReaderFactory(scala.collection.immutable.Map<String, GpuMetric> metrics,
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
  }

  static PartitionReader<ColumnarBatch> createBatchReader(
      GpuSparkInputPartition task,
      scala.collection.immutable.Map<String, GpuMetric> metrics) {
    return new GpuBatchDataReader(task.taskGroup(), task.table(), task.expectedSchema(),
        task.isCaseSensitive(), task.getConfiguration(), task.getMaxBatchSizeRows(),
        task.getMaxBatchSizeBytes(), task.getTargetBatchSizeBytes(), task.useChunkedReader(),
        task.maxChunkedReaderMemoryUsageSizeBytes(), task.getParquetDebugDumpPrefix(),
        task.getParquetDebugDumpAlways(), metrics);
  }

  static PartitionReader<ColumnarBatch> createMultiFileBatchReader(
      GpuSparkInputPartition task,
      boolean useMultiThread,
      FileFormat ff,
      scala.collection.immutable.Map<String, GpuMetric> metrics) {
    GpuMultiFileReaderConf conf = GpuMultiFileReaderConf.builder()
        .expectedSchema(task.expectedSchema())
        .caseSensitive(task.isCaseSensitive())
        .conf(task.getConfiguration())
        .maxBatchSizeRows(task.getMaxBatchSizeRows())
        .maxBatchSizeBytes(task.getMaxBatchSizeBytes())
        .targetBatchSizeBytes(task.getTargetBatchSizeBytes())
        .maxGpuColumnSizeBytes(task.getMaxGpuColumnSizeBytes())
        .useChunkedReader(task.useChunkedReader())
        .maxChunkedReaderMemoryUsageSizeBytes(task.maxChunkedReaderMemoryUsageSizeBytes())
        .parquetDebugDumpPrefix(task.getParquetDebugDumpPrefix())
        .parquetDebugDumpAlways(task.getParquetDebugDumpAlways())
        .metrics(metrics)
        .useMultiThread(useMultiThread)
        .numThreads(task.getNumThreads())
        .maxNumFileProcessed(task.getMaxNumFileProcessed())
        .fileFormat(ff)
        .build();
    return new GpuMultiFileBatchReader(task.taskGroup(), task.table(), conf);
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    throw new IllegalStateException("non-columnar read");
  }

  @Override
  public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    if (partition instanceof GpuSparkInputPartition) {
      GpuSparkInputPartition rTask = (GpuSparkInputPartition) partition;
      FileCheckResult ret = multiFileReadCheck(rTask);
      if (ret.canAccelerateRead()) {
        boolean isMultiThread = ret.isMultiThread();
        FileFormat ff = ret.getFileFormat();
        return createMultiFileBatchReader(rTask, isMultiThread, ff, metrics);
      } else {
        return createBatchReader(rTask, metrics);
      }
    } else {
      throw new UnsupportedOperationException("Incorrect input partition type: " + partition);
    }
  }

  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    return true;
  }

  private static class FileCheckResult {
    private final boolean canAccelerateRead;
    private final boolean isMultiThread;
    private final FileFormat fileFormat;

    public FileCheckResult(boolean canAccelerateRead, boolean isMultiThread, FileFormat fileFormat) {
      this.canAccelerateRead = canAccelerateRead;
      this.isMultiThread = isMultiThread;
      this.fileFormat = fileFormat;
    }

    public boolean canAccelerateRead() {
      return canAccelerateRead;
    }

    public boolean isMultiThread() {
      return isMultiThread;
    }

    public FileFormat getFileFormat() {
      return fileFormat;
    }
  }

  /**
   * Return a tuple as (canAccelerateRead, isMultiThread, fileFormat).
   * - "canAccelerateRead" Whether the input read task can be accelerated by
   * multi-threaded or coalescing reading.
   * - "isMultiThread" Whether to use the multi-threaded reading.
   * - "fileFormat" The file format of this combined task. Acceleration requires
   * all the files in a combined task have the same format.
   */
  private FileCheckResult multiFileReadCheck(GpuSparkInputPartition readTask) {
    Collection<FileScanTask> scans = readTask.taskGroup()
        .tasks()
        .stream()
        .map(ScanTask::asFileScanTask)
        .collect(Collectors.toList());
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
    String[] files = scans
        .stream()
        .map(f -> f.file().path().toString())
        .toArray(String[]::new);
    // Get the final decision for the subtype of the Rapids reader.
    boolean useMultiThread = MultiFileReaderUtils.useMultiThreadReader(
        canUseCoalescing, canUseMultiThread, files, allCloudSchemes);
    return new FileCheckResult(canAccelerateRead, useMultiThread, ff);
  }
}
