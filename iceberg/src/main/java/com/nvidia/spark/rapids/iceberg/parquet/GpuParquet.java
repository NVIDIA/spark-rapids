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

package com.nvidia.spark.rapids.iceberg.parquet;

import java.util.Collection;
import java.util.Map;

import com.nvidia.spark.rapids.GpuMetric;
import com.nvidia.spark.rapids.iceberg.data.GpuDeleteFilter;
import com.nvidia.spark.rapids.shims.PartitionedFileUtilsShim;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * GPU version of Apache Iceberg's Parquet class.
 * The Iceberg version originally accepted a callback function to create the reader to handle
 * vectorized batch vs. row readers, but since the GPU only reads vectorized that abstraction
 * has been removed.
 */
public class GpuParquet {
  private static final Collection<String> READ_PROPERTIES_TO_REMOVE = Sets.newHashSet(
      "parquet.read.filter", "parquet.private.read.filter.predicate", "parquet.read.support.class");

  private GpuParquet() {
  }

  public static ReadBuilder read(InputFile file) {
    return new ReadBuilder(file);
  }

  public static class ReadBuilder {
    private final InputFile file;
    private Long start = null;
    private Long length = null;
    private Schema projectSchema = null;
    private Map<Integer, ?> idToConstant = null;
    private GpuDeleteFilter deleteFilter = null;
    private Expression filter = null;
    private boolean caseSensitive = true;
    private NameMapping nameMapping = null;
    private Configuration conf = null;
    private int maxBatchSizeRows = Integer.MAX_VALUE;
    private long maxBatchSizeBytes = Integer.MAX_VALUE;
    private long targetBatchSizeBytes = Integer.MAX_VALUE;
    private boolean useChunkedReader = false;
    private long maxChunkedReaderMemoryUsageSizeBytes = 0;
    private scala.Option<String> debugDumpPrefix = null;
    private boolean debugDumpAlways = false;
    private scala.collection.immutable.Map<String, GpuMetric> metrics = null;

    private ReadBuilder(InputFile file) {
      this.file = file;
    }

    /**
     * Restricts the read to the given range: [start, start + length).
     *
     * @param newStart the start position for this read
     * @param newLength the length of the range this read should scan
     * @return this builder for method chaining
     */
    public ReadBuilder split(long newStart, long newLength) {
      this.start = newStart;
      this.length = newLength;
      return this;
    }

    public ReadBuilder project(Schema newSchema) {
      this.projectSchema = newSchema;
      return this;
    }

    public ReadBuilder caseSensitive(boolean newCaseSensitive) {
      this.caseSensitive = newCaseSensitive;
      return this;
    }

    public ReadBuilder constants(Map<Integer, ?> constantsMap) {
      this.idToConstant = constantsMap;
      return this;
    }

    public ReadBuilder deleteFilter(GpuDeleteFilter deleteFilter) {
      this.deleteFilter = deleteFilter;
      return this;
    }

    public ReadBuilder filter(Expression newFilter) {
      this.filter = newFilter;
      return this;
    }

    public ReadBuilder withNameMapping(NameMapping newNameMapping) {
      this.nameMapping = newNameMapping;
      return this;
    }

    public ReadBuilder withConfiguration(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public ReadBuilder withMaxBatchSizeRows(int maxBatchSizeRows) {
      this.maxBatchSizeRows = maxBatchSizeRows;
      return this;
    }

    public ReadBuilder withMaxBatchSizeBytes(long maxBatchSizeBytes) {
      this.maxBatchSizeBytes = maxBatchSizeBytes;
      return this;
    }

    public ReadBuilder withTargetBatchSizeBytes(long targetBatchSizeBytes) {
      this.targetBatchSizeBytes = targetBatchSizeBytes;
      return this;
    }

    public ReadBuilder withUseChunkedReader(boolean useChunkedReader,
        long maxChunkedReaderMemoryUsageSizeBytes) {
      this.useChunkedReader = useChunkedReader;
      this.maxChunkedReaderMemoryUsageSizeBytes = maxChunkedReaderMemoryUsageSizeBytes;
      return this;
    }

    public ReadBuilder withDebugDump(scala.Option<String> dumpPrefix, boolean dumpAlways) {
      this.debugDumpPrefix = dumpPrefix;
      this.debugDumpAlways = dumpAlways;
      return this;
    }

    public ReadBuilder withMetrics(scala.collection.immutable.Map<String, GpuMetric> metrics) {
      this.metrics = metrics;
      return this;
    }

    public CloseableIterable<ColumnarBatch> build() {
      ParquetReadOptions options = buildReaderOptions(file, start, length);
      PartitionedFile partFile = PartitionedFileUtilsShim.newPartitionedFile(
          InternalRow.empty(), file.location(), start, length);
      return new GpuParquetReader(file, projectSchema, options, nameMapping, filter, caseSensitive,
          idToConstant, deleteFilter, partFile, conf, maxBatchSizeRows, maxBatchSizeBytes,
          targetBatchSizeBytes, useChunkedReader, maxChunkedReaderMemoryUsageSizeBytes,
          debugDumpPrefix, debugDumpAlways, metrics);
    }
  }

  public static ParquetReadOptions buildReaderOptions(InputFile file, Long start, Long length) {
    ParquetReadOptions.Builder optionsBuilder;
    if (file instanceof HadoopInputFile) {
      // remove read properties already set that may conflict with this read
      Configuration conf = new Configuration(((HadoopInputFile) file).getConf());
      for (String property : READ_PROPERTIES_TO_REMOVE) {
        conf.unset(property);
      }
      optionsBuilder = HadoopReadOptions.builder(conf);
    } else {
      //optionsBuilder = ParquetReadOptions.builder();
      throw new UnsupportedOperationException("Only Hadoop files are supported for now");
    }

    if (start != null) {
      optionsBuilder.withRange(start, start + length);
    }

    return optionsBuilder.build();
  }
}
