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
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.mapping.NameMapping;
import scala.Option;

public class GpuMultiFileReaderConf {
  private final Schema expectedSchema;
  private final boolean caseSensitive;
  private final Configuration conf;
  private final int maxBatchSizeRows;
  private final long maxBatchSizeBytes;
  private final long targetBatchSizeBytes;
  private final long maxGpuColumnSizeBytes;

  private final boolean useChunkedReader;
  private final long maxChunkedReaderMemoryUsageSizeBytes;
  private final scala.Option<String> parquetDebugDumpPrefix;
  private final boolean parquetDebugDumpAlways;
  private final scala.collection.immutable.Map<String, GpuMetric> metrics;
  private final boolean useMultiThread;
  private final int numThreads;
  private final int maxNumFileProcessed;
  private final FileFormat fileFormat;

  private GpuMultiFileReaderConf(Schema expectedSchema,
      boolean caseSensitive, Configuration conf,
      int maxBatchSizeRows,
      long maxBatchSizeBytes,
      long targetBatchSizeBytes,
      long maxGpuColumnSizeBytes,
      boolean useChunkedReader,
      long maxChunkedReaderMemoryUsageSizeBytes,
      scala.Option<String> parquetDebugDumpPrefix,
      boolean parquetDebugDumpAlways,
      scala.collection.immutable.Map<String, GpuMetric> metrics,
      boolean useMultiThread,
      int numThreads,
      int maxNumFileProcessed, FileFormat fileFormat) {
    this.expectedSchema = expectedSchema;
    this.caseSensitive = caseSensitive;
    this.conf = conf;
    this.maxBatchSizeRows = maxBatchSizeRows;
    this.maxBatchSizeBytes = maxBatchSizeBytes;
    this.targetBatchSizeBytes = targetBatchSizeBytes;
    this.maxGpuColumnSizeBytes = maxGpuColumnSizeBytes;
    this.useChunkedReader = useChunkedReader;
    this.maxChunkedReaderMemoryUsageSizeBytes = maxChunkedReaderMemoryUsageSizeBytes;
    this.parquetDebugDumpPrefix = parquetDebugDumpPrefix;
    this.parquetDebugDumpAlways = parquetDebugDumpAlways;
    this.metrics = metrics;
    this.useMultiThread = useMultiThread;
    this.numThreads = numThreads;
    this.maxNumFileProcessed = maxNumFileProcessed;
    this.fileFormat = fileFormat;
  }

  public Schema getExpectedSchema() {
    return expectedSchema;
  }

  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  public Configuration getConf() {
    return conf;
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

  public boolean isUseChunkedReader() {
    return useChunkedReader;
  }

  public long getMaxChunkedReaderMemoryUsageSizeBytes() {
    return maxChunkedReaderMemoryUsageSizeBytes;
  }

  public Option<String> getParquetDebugDumpPrefix() {
    return parquetDebugDumpPrefix;
  }

  public boolean isParquetDebugDumpAlways() {
    return parquetDebugDumpAlways;
  }

  public scala.collection.immutable.Map<String, GpuMetric> getMetrics() {
    return metrics;
  }

  public boolean isUseMultiThread() {
    return useMultiThread;
  }

  public int getNumThreads() {
    return numThreads;
  }

  public int getMaxNumFileProcessed() {
    return maxNumFileProcessed;
  }

  public FileFormat getFileFormat() {
    return fileFormat;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Schema expectedSchema;
    private boolean caseSensitive;
    private Configuration conf;
    private int maxBatchSizeRows;
    private long maxBatchSizeBytes;
    private long targetBatchSizeBytes;
    private long maxGpuColumnSizeBytes;
    private boolean useChunkedReader;
    private long maxChunkedReaderMemoryUsageSizeBytes;
    private scala.Option<String> parquetDebugDumpPrefix;
    private boolean parquetDebugDumpAlways;
    private scala.collection.immutable.Map<String, GpuMetric> metrics;
    private boolean useMultiThread;
    private int numThreads;
    private int maxNumFileProcessed;
    private FileFormat fileFormat;


    public Builder expectedSchema(Schema expectedSchema) {
      this.expectedSchema = expectedSchema;
      return this;
    }

    public Builder caseSensitive(boolean caseSensitive) {
      this.caseSensitive = caseSensitive;
      return this;
    }

    public Builder conf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public Builder maxBatchSizeRows(int maxBatchSizeRows) {
      this.maxBatchSizeRows = maxBatchSizeRows;
      return this;
    }

    public Builder maxBatchSizeBytes(long maxBatchSizeBytes) {
      this.maxBatchSizeBytes = maxBatchSizeBytes;
      return this;
    }

    public Builder targetBatchSizeBytes(long targetBatchSizeBytes) {
      this.targetBatchSizeBytes = targetBatchSizeBytes;
      return this;
    }

    public Builder maxGpuColumnSizeBytes(long maxGpuColumnSizeBytes) {
      this.maxGpuColumnSizeBytes = maxGpuColumnSizeBytes;
      return this;
    }

    public Builder useChunkedReader(boolean useChunkedReader) {
      this.useChunkedReader = useChunkedReader;
      return this;
    }

    public Builder maxChunkedReaderMemoryUsageSizeBytes(long maxChunkedReaderMemoryUsageSizeBytes) {
      this.maxChunkedReaderMemoryUsageSizeBytes = maxChunkedReaderMemoryUsageSizeBytes;
      return this;
    }

    public Builder parquetDebugDumpPrefix(scala.Option<String> parquetDebugDumpPrefix) {
      this.parquetDebugDumpPrefix = parquetDebugDumpPrefix;
      return this;
    }

    public Builder parquetDebugDumpAlways(boolean parquetDebugDumpAlways) {
      this.parquetDebugDumpAlways = parquetDebugDumpAlways;
      return this;
    }

    public Builder metrics(scala.collection.immutable.Map<String, GpuMetric> metrics) {
      this.metrics = metrics;
      return this;
    }

    public Builder useMultiThread(boolean useMultiThread) {
      this.useMultiThread = useMultiThread;
      return this;
    }

    public Builder numThreads(int numThreads) {
      this.numThreads = numThreads;
      return this;
    }

    public Builder maxNumFileProcessed(int maxNumFileProcessed) {
      this.maxNumFileProcessed = maxNumFileProcessed;
      return this;
    }

    public Builder fileFormat(FileFormat fileFormat) {
      this.fileFormat = fileFormat;
      return this;
    }

    public GpuMultiFileReaderConf build() {
      return new GpuMultiFileReaderConf(expectedSchema, caseSensitive, conf,
          maxBatchSizeRows, maxBatchSizeBytes, targetBatchSizeBytes, maxGpuColumnSizeBytes,
          useChunkedReader, maxChunkedReaderMemoryUsageSizeBytes, parquetDebugDumpPrefix,
          parquetDebugDumpAlways, metrics, useMultiThread, numThreads, maxNumFileProcessed,
          fileFormat);
    }
  }
}
