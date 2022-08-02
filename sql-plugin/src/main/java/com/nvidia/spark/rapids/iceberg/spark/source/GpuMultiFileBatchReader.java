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

import com.nvidia.spark.rapids.*;
import com.nvidia.spark.rapids.iceberg.data.GpuDeleteFilter;
import com.nvidia.spark.rapids.iceberg.parquet.GpuParquet;
import com.nvidia.spark.rapids.iceberg.parquet.GpuParquetReader;
import com.nvidia.spark.rapids.iceberg.parquet.ParquetSchemaUtil;
import com.nvidia.spark.rapids.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.*;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.rapids.InputFileUtils;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;
import scala.Tuple2;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The wrapper of the GPU multi-threaded and coalescing(TBD) reader for Iceberg */
class GpuMultiFileBatchReader extends BaseDataReader<ColumnarBatch> {
  private static final Logger LOG = LoggerFactory.getLogger(GpuMultiFileBatchReader.class);
  private final Map<String, Tuple2<Map<Integer, ?>, Schema>> constsSchemaMap =
      Maps.newConcurrentMap();
  private final LinkedHashMap<String, FileScanTask> files;
  private final Schema expectedSchema;
  private final boolean caseSensitive;
  private final Configuration conf;
  private final int maxBatchSizeRows;
  private final long maxBatchSizeBytes;
  private final String parquetDebugDumpPrefix;
  private final scala.collection.immutable.Map<String, GpuMetric> metrics;
  private final boolean useMultiThread;
  private final FileFormat fileFormat;
  private final int numThreads;
  private final int maxNumFileProcessed;

  private NameMapping nameMapping = null;
  private boolean needNext = true;
  private boolean isBatchPending;
  // lazy variables
  private FilePartitionReaderBase rapidsReader = null;

  GpuMultiFileBatchReader(CombinedScanTask task, Table table, Schema expectedSchema,
      boolean caseSensitive, Configuration conf, int maxBatchSizeRows, long maxBatchSizeBytes,
      String parquetDebugDumpPrefix, int numThreads, int maxNumFileProcessed,
      boolean useMultiThread, FileFormat fileFormat,
      scala.collection.immutable.Map<String, GpuMetric> metrics) {
    super(table, task);
    this.expectedSchema = expectedSchema;
    this.caseSensitive = caseSensitive;
    this.conf = conf;
    this.maxBatchSizeRows = maxBatchSizeRows;
    this.maxBatchSizeBytes = maxBatchSizeBytes;
    this.parquetDebugDumpPrefix = parquetDebugDumpPrefix;
    this.useMultiThread = useMultiThread;
    this.fileFormat = fileFormat;
    this.metrics = metrics;
    this.numThreads = numThreads;
    this.maxNumFileProcessed = maxNumFileProcessed;
    String nameMapping = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    if (nameMapping != null) {
      this.nameMapping = NameMappingParser.fromJson(nameMapping);
    }
    files = Maps.newLinkedHashMapWithExpectedSize(task.files().size());
    task.files().forEach(fst -> this.files.putIfAbsent(fst.file().path().toString(), fst));
  }

  @Override
  public ColumnarBatch get() {
    if (rapidsReader == null) {
      // Not initialized, return null to align with PerFile reader.
      return null;
    }
    needNext = true;
    isBatchPending = false;
    // The same post-process with PerFile reader.
    try (ColumnarBatch batch = rapidsReader.get()) {
      // The Rapids reader should already set the current file.
      String curFile = InputFileUtils.getCurInputFilePath();
      // <idToConstants, updatedExpectedSchema>
      Tuple2<Map<Integer, ?>, Schema> constsSchema = constsSchemaMap.get(curFile);
      return GpuIcebergReader.addUpcastsIfNeeded(
          GpuIcebergReader.addConstantColumns(batch, constsSchema._2(), constsSchema._1()),
          constsSchema._2());
    }
  }

  @Override
  public boolean next() throws IOException {
    ensureRapidsReader();
    if (needNext) {
      needNext = false;
      isBatchPending = rapidsReader.next();
    }
    return isBatchPending;
  }

  @Override
  public void close() throws IOException {
    if (rapidsReader != null) rapidsReader.close();
    super.close();
  }

  @Override
  CloseableIterator<ColumnarBatch> open(FileScanTask task) {
    // Stub for the required implementation.
    // This method will never be called after overriding the "next" method.
    throw new UnsupportedOperationException();
  }

  private void ensureRapidsReader() {
    if (rapidsReader == null) {
      if (FileFormat.PARQUET.equals(fileFormat)) {
        if (useMultiThread) {
          rapidsReader = createParquetMultiThreadReader();
        } else {
          // TODO Support coalescing reading, tracked by
          // https://github.com/NVIDIA/spark-rapids/issues/5942
          throw new UnsupportedOperationException(
              "Coalescing reading is not supported for Parquet reads yet");
        }
      } else {
        throw new UnsupportedOperationException(
            "Format: " + fileFormat + " is not supported for batched reads");
      }
    }
  }

  private FilePartitionReaderBase createParquetMultiThreadReader() {
    LOG.debug("Using multi-threaded Iceberg Parquet reader, task attempt ID: " +
        TaskContext.get().taskAttemptId());
    // Iceberg will handle partition values itself.
    StructType emptyPartSchema = new StructType();
    InternalRow emptyPartValue = InternalRow.empty();

    PartitionedFile[] files = this.files.values().stream()
      .map(fst -> PartitionedFileUtils.newPartitionedFile(emptyPartValue,
          fst.file().path().toString(), fst.start(), fst.length()))
      .toArray(PartitionedFile[]::new);

    return new MultiFileCloudParquetPartitionReader(conf, files, this::filterParquetBlocks,
        caseSensitive, parquetDebugDumpPrefix, maxBatchSizeRows, maxBatchSizeBytes,
        metrics, emptyPartSchema, numThreads, maxNumFileProcessed,
        true, // ignoreMissingFiles
        false, // ignoreCorruptFiles
        false // useFieldId
    );
  }

  /** The filter function for the Parquet multi-file reader */
  private ParquetFileInfoWithBlockMeta filterParquetBlocks(PartitionedFile file) {
    FileScanTask fst = this.files.get(file.filePath());
    GpuDeleteFilter deleteFilter = deleteFilter(fst);
    if (deleteFilter != null) {
      throw new UnsupportedOperationException("Delete filter is not supported");
    }
    Schema updatedSchema = requiredSchema(deleteFilter);
    Map<Integer, ?> idToConstant = constantsMap(fst, updatedSchema);
    InputFile inFile = getInputFile(fst);
    ParquetReadOptions readOptions =
        GpuParquet.buildReaderOptions(inFile, fst.start(), fst.length());
    try (ParquetFileReader reader = GpuParquetReader.newReader(inFile, readOptions)) {
      MessageType fileSchema = reader.getFileMetaData().getSchema();

      List<BlockMetaData> filteredRowGroups = GpuParquetReader.filterRowGroups(reader,
          nameMapping, updatedSchema, fst.residual(), caseSensitive);

      GpuParquetReader.ReorderColumns reorder = ParquetSchemaUtil.hasIds(fileSchema) ?
          new GpuParquetReader.ReorderColumns(idToConstant) :
          new GpuParquetReader.ReorderColumnsFallback(idToConstant);

      MessageType fileReadSchema = (MessageType) TypeWithSchemaVisitor.visit(
          updatedSchema.asStruct(), fileSchema, reorder);
      Seq<BlockMetaData> clippedBlocks = GpuParquetUtils.clipBlocksToSchema(
          fileReadSchema, filteredRowGroups, caseSensitive);
      StructType partReaderSparkSchema = (StructType) TypeWithSchemaVisitor.visit(
          updatedSchema.asStruct(), fileReadSchema, new GpuParquetReader.SparkSchemaConverter());

      // cache the updated constants
      Map<Integer, ?> updatedConstants =
          GpuParquetReader.addNullsForMissingFields(idToConstant, reorder.getMissingFields());
      constsSchemaMap.put(file.filePath(), Tuple2.apply(updatedConstants, updatedSchema));

      return ParquetFileInfoWithBlockMeta.apply(new Path(new URI(file.filePath())),
          clippedBlocks, InternalRow.empty(), fileReadSchema, partReaderSparkSchema,
          true, // isCorrectedInt96RebaseMode
          true, // isCorrectedRebaseMode
          true //  hasInt96Timestamps
      );
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to open file: " + inFile, e);
    } catch (URISyntaxException ue) {
      throw new IllegalArgumentException("Invalid file path: " + inFile, ue);
    }
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
