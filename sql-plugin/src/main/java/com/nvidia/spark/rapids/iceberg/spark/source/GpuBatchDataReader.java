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

import java.util.Map;

import com.nvidia.spark.rapids.GpuMetric;
import com.nvidia.spark.rapids.iceberg.data.GpuDeleteFilter;
import com.nvidia.spark.rapids.iceberg.parquet.GpuParquet;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/** GPU version of Apache Iceberg's BatchDataReader. */
class GpuBatchDataReader extends BaseDataReader<ColumnarBatch> {
  private final Schema expectedSchema;
  private final String nameMapping;
  private final boolean caseSensitive;
  private final Configuration conf;
  private final int maxBatchSizeRows;
  private final long maxBatchSizeBytes;
  private final long targetBatchSizeBytes;
  private final boolean useChunkedReader;
  private final long maxChunkedReaderMemoryUsageSizeBytes;
  private final scala.Option<String> parquetDebugDumpPrefix;
  private final boolean parquetDebugDumpAlways;
  private final scala.collection.immutable.Map<String, GpuMetric> metrics;

  GpuBatchDataReader(CombinedScanTask task, Table table, Schema expectedSchema, boolean caseSensitive,
                     Configuration conf, int maxBatchSizeRows, long maxBatchSizeBytes,
                     long targetBatchSizeBytes,
                     boolean useChunkedReader, long maxChunkedReaderMemoryUsageSizeBytes,
                     scala.Option<String> parquetDebugDumpPrefix, boolean parquetDebugDumpAlways,
                     scala.collection.immutable.Map<String, GpuMetric> metrics) {
    super(table, task);
    this.expectedSchema = expectedSchema;
    this.nameMapping = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    this.caseSensitive = caseSensitive;
    this.conf = conf;
    this.maxBatchSizeRows = maxBatchSizeRows;
    this.maxBatchSizeBytes = maxBatchSizeBytes;
    this.targetBatchSizeBytes = targetBatchSizeBytes;
    this.useChunkedReader = useChunkedReader;
    this.maxChunkedReaderMemoryUsageSizeBytes = maxChunkedReaderMemoryUsageSizeBytes;
    this.parquetDebugDumpPrefix = parquetDebugDumpPrefix;
    this.parquetDebugDumpAlways = parquetDebugDumpAlways;
    this.metrics = metrics;
  }

  @Override
  CloseableIterator<ColumnarBatch> open(FileScanTask task) {
    DataFile file = task.file();

    // update the current file for Spark's filename() function
    InputFileBlockHolder.set(file.path().toString(), task.start(), task.length());

    Map<Integer, ?> idToConstant = constantsMap(task, expectedSchema);

    CloseableIterable<ColumnarBatch> iter;
    InputFile location = getInputFile(task);
    Preconditions.checkNotNull(location, "Could not find InputFile associated with FileScanTask");
    if (task.file().format() == FileFormat.PARQUET) {
      GpuDeleteFilter deleteFilter = deleteFilter(task);
      // get required schema for filtering out equality-delete rows in case equality-delete uses columns are
      // not selected.
      Schema requiredSchema = requiredSchema(deleteFilter);

      GpuParquet.ReadBuilder builder = GpuParquet.read(location)
          .project(requiredSchema)
          .split(task.start(), task.length())
          .constants(idToConstant)
          .deleteFilter(deleteFilter)
          .filter(task.residual())
          .caseSensitive(caseSensitive)
          .withConfiguration(conf)
          .withMaxBatchSizeRows(maxBatchSizeRows)
          .withMaxBatchSizeBytes(maxBatchSizeBytes)
          .withTargetBatchSizeBytes(targetBatchSizeBytes)
          .withUseChunkedReader(useChunkedReader, maxChunkedReaderMemoryUsageSizeBytes)
          .withDebugDump(parquetDebugDumpPrefix, parquetDebugDumpAlways)
          .withMetrics(metrics);

      if (nameMapping != null) {
        builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
      }

      iter = builder.build();
    } else {
      throw new UnsupportedOperationException(
          "Format: " + task.file().format() + " not supported for batched reads");
    }
    return iter.iterator();
  }

  private GpuDeleteFilter deleteFilter(FileScanTask task) {
    if (task.deletes().isEmpty()) {
      return null;
    }
    return new GpuDeleteFilter(
        task.file().path().toString(),
        task.deletes(),
        table().schema(),
        expectedSchema);
  }

  private Schema requiredSchema(GpuDeleteFilter deleteFilter) {
    if (deleteFilter != null && deleteFilter.hasEqDeletes()) {
      return deleteFilter.requiredSchema();
    } else {
      return expectedSchema;
    }
  }
}
