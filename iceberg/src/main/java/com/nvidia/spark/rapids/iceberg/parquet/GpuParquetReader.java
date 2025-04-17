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

package com.nvidia.spark.rapids.iceberg.parquet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.nvidia.spark.rapids.parquet.iceberg.shaded.ParquetPartitionReader;
import org.apache.iceberg.Schema;
import org.apache.iceberg.parquet.ParquetBloomRowGroupFilter;
import org.apache.iceberg.parquet.ParquetDictionaryRowGroupFilter;
import org.apache.iceberg.parquet.ParquetMetricsRowGroupFilter;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.shaded.org.apache.parquet.ParquetReadOptions;
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.iceberg.shaded.org.apache.parquet.schema.MessageType;
import scala.collection.Seq;

import com.nvidia.spark.rapids.parquet.iceberg.shaded.CpuCompressionConfig$;
import com.nvidia.spark.rapids.DateTimeRebaseCorrected$;
import com.nvidia.spark.rapids.GpuMetric;
import com.nvidia.spark.rapids.parquet.iceberg.shaded.GpuParquetUtils;
import com.nvidia.spark.rapids.PartitionReaderWithBytesRead;
import com.nvidia.spark.rapids.iceberg.data.GpuDeleteFilter;
import com.nvidia.spark.rapids.iceberg.spark.source.GpuIcebergReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;

import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/** GPU version of Apache Iceberg's ParquetReader class */
public class GpuParquetReader extends CloseableGroup implements CloseableIterable<ColumnarBatch> {
  private final InputFile input;
  private final Schema expectedSchema;
  private final ParquetReadOptions options;
  private final Expression filter;
  private final boolean caseSensitive;
  private final NameMapping nameMapping;
  private final Map<Integer, ?> idToConstant;
  private final GpuDeleteFilter deleteFilter;
  private final PartitionedFile partFile;
  private final Configuration conf;
  private final int maxBatchSizeRows;
  private final long maxBatchSizeBytes;
  private final long targetBatchSizeBytes;
  private final boolean useChunkedReader;
  private final long maxChunkedReaderMemoryUsageSizeBytes;
  private final scala.Option<String> debugDumpPrefix;
  private final boolean debugDumpAlways;
  private final scala.collection.immutable.Map<String, GpuMetric> metrics;

  public GpuParquetReader(
      InputFile input, Schema expectedSchema, ParquetReadOptions options,
      NameMapping nameMapping, Expression filter, boolean caseSensitive,
      Map<Integer, ?> idToConstant, GpuDeleteFilter deleteFilter,
      PartitionedFile partFile, Configuration conf, int maxBatchSizeRows,
      long maxBatchSizeBytes, long targetBatchSizeBytes, boolean useChunkedReader,
      long maxChunkedReaderMemoryUsageSizeBytes,
      scala.Option<String> debugDumpPrefix, boolean debugDumpAlways,
      scala.collection.immutable.Map<String, GpuMetric> metrics) {
    this.input = input;
    this.expectedSchema = expectedSchema;
    this.options = options;
    this.nameMapping = nameMapping;
    this.filter = filter;
    this.caseSensitive = caseSensitive;
    this.idToConstant = idToConstant;
    this.deleteFilter = deleteFilter;
    this.partFile = partFile;
    this.conf = conf;
    this.maxBatchSizeRows = maxBatchSizeRows;
    this.maxBatchSizeBytes = maxBatchSizeBytes;
    this.targetBatchSizeBytes = targetBatchSizeBytes;
    this.useChunkedReader = useChunkedReader;
    this.maxChunkedReaderMemoryUsageSizeBytes = maxChunkedReaderMemoryUsageSizeBytes;
    this.debugDumpPrefix = debugDumpPrefix;
    this.debugDumpAlways = debugDumpAlways;
    this.metrics = metrics;
  }

  @Override
  public org.apache.iceberg.io.CloseableIterator<ColumnarBatch> iterator() {
    try (ParquetFileReader reader = newReader(input, options)) {
      MessageType fileSchema = reader.getFileMetaData().getSchema();

      MessageType typeWithIds;
      MessageType fileReadSchema;
      if (ParquetSchemaUtil.hasIds(fileSchema)) {
        typeWithIds = fileSchema;
        fileReadSchema = ParquetSchemaUtil.pruneColumns(fileSchema, expectedSchema);
      } else if (nameMapping != null) {
        typeWithIds = ParquetSchemaUtil.applyNameMapping(fileSchema, nameMapping);
        fileReadSchema = ParquetSchemaUtil.pruneColumns(typeWithIds, expectedSchema);
      } else {
        typeWithIds = ParquetSchemaUtil.addFallbackIds(fileSchema);
        fileReadSchema = ParquetSchemaUtil.pruneColumnsFallback(fileSchema, expectedSchema);
      }


      List<BlockMetaData> filteredRowGroups = filterRowGroups(reader, expectedSchema,
          typeWithIds, filter, caseSensitive);

      Seq<BlockMetaData> clippedBlocks = GpuParquetUtils.clipBlocksToSchema(
          fileReadSchema, filteredRowGroups, caseSensitive);
      StructType partReaderSparkSchema = (StructType) TypeWithSchemaVisitor.visit(
          expectedSchema.asStruct(), fileReadSchema, new SparkSchemaConverter());

      // reuse Parquet scan code to read the raw data from the file
      ParquetPartitionReader parquetPartReader = new ParquetPartitionReader(conf, partFile,
          new Path(input.location()), clippedBlocks, fileReadSchema, caseSensitive,
          partReaderSparkSchema, debugDumpPrefix, debugDumpAlways,
          maxBatchSizeRows, maxBatchSizeBytes, targetBatchSizeBytes, useChunkedReader,
          maxChunkedReaderMemoryUsageSizeBytes,
          CpuCompressionConfig$.MODULE$.disabled(),
          metrics,
          DateTimeRebaseCorrected$.MODULE$, // dateRebaseMode
          DateTimeRebaseCorrected$.MODULE$, // timestampRebaseMode
          true, // hasInt96Timestamps
          false // useFieldId
      );
      PartitionReaderWithBytesRead partReader = new PartitionReaderWithBytesRead(parquetPartReader);

      return new GpuIcebergReader(expectedSchema, fileReadSchema, partReader, deleteFilter,
          idToConstant);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create/close reader for file: " + input, e);
    }
  }

  public static List<BlockMetaData> filterRowGroups(ParquetFileReader reader,
      Schema expectedSchema,
      MessageType typeWithIds,
      Expression filter,
      boolean caseSensitive) {

    ParquetMetricsRowGroupFilter statsFilter = null;
    ParquetDictionaryRowGroupFilter dictFilter = null;
    ParquetBloomRowGroupFilter bloomFilter = null;
    if (filter != null) {
      statsFilter = new ParquetMetricsRowGroupFilter(expectedSchema, filter, caseSensitive);
      dictFilter = new ParquetDictionaryRowGroupFilter(expectedSchema, filter, caseSensitive);
      bloomFilter = new ParquetBloomRowGroupFilter(expectedSchema, filter, caseSensitive);
    }

    List<BlockMetaData> filteredRowGroups = new ArrayList<>(reader.getRowGroups().size());
    for (BlockMetaData rowGroup: reader.getRowGroups()) {
      boolean shouldRead = filter == null
          || (statsFilter.shouldRead(typeWithIds, rowGroup)
          && dictFilter.shouldRead(typeWithIds, rowGroup, reader.getDictionaryReader(rowGroup))
          && bloomFilter.shouldRead(typeWithIds, rowGroup, reader.getBloomFilterDataReader(rowGroup)));
      if (shouldRead) {
        filteredRowGroups.add(rowGroup);
      }
    }

    return filteredRowGroups;
  }

  public static ParquetFileReader newReader(InputFile file, ParquetReadOptions options) {
    try {
      return ParquetFileReader.open(ParquetIO.file(file), options);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to open Parquet file: " + file.location(), e);
    }
  }
}
