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

import com.nvidia.spark.rapids.*;
import com.nvidia.spark.rapids.iceberg.data.GpuDeleteFilter;
import com.nvidia.spark.rapids.iceberg.parquet.GpuParquet;
import com.nvidia.spark.rapids.iceberg.parquet.GpuParquetReader;
import com.nvidia.spark.rapids.iceberg.parquet.ParquetSchemaUtil;
import com.nvidia.spark.rapids.iceberg.parquet.TypeWithSchemaVisitor;
import com.nvidia.spark.rapids.shims.PartitionedFileUtilsShim;
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
import org.apache.spark.sql.rapids.execution.TrampolineUtil;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.Tuple2;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;

/** The wrapper of the GPU multi-threaded and coalescing reader for Iceberg */
class GpuMultiFileBatchReader extends BaseDataReader<ColumnarBatch> {
  private static final Logger LOG = LoggerFactory.getLogger(GpuMultiFileBatchReader.class);
  private final LinkedHashMap<String, FileScanTask> files;
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
  private final FileFormat fileFormat;
  private final int numThreads;
  private final int maxNumFileProcessed;
  private final boolean queryUsesInputFile;

  private NameMapping nameMapping = null;
  private boolean needNext = true;
  private boolean isBatchPending;
  // lazy variables
  private CloseableIterator<ColumnarBatch> batchReader = null;

  GpuMultiFileBatchReader(CombinedScanTask task, Table table, Schema expectedSchema,
      boolean caseSensitive, Configuration conf, int maxBatchSizeRows, long maxBatchSizeBytes,
      long targetBatchSizeBytes, long maxGpuColumnSizeBytes,
      boolean useChunkedReader, long maxChunkedReaderMemoryUsageSizeBytes,
      scala.Option<String> parquetDebugDumpPrefix, boolean parquetDebugDumpAlways,
      int numThreads, int maxNumFileProcessed,
      boolean useMultiThread, FileFormat fileFormat,
      scala.collection.immutable.Map<String, GpuMetric> metrics,
      boolean queryUsesInputFile) {
    super(table, task);
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
    this.useMultiThread = useMultiThread;
    this.fileFormat = fileFormat;
    this.metrics = metrics;
    this.numThreads = numThreads;
    this.maxNumFileProcessed = maxNumFileProcessed;
    this.queryUsesInputFile = queryUsesInputFile;
    String nameMapping = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    if (nameMapping != null) {
      this.nameMapping = NameMappingParser.fromJson(nameMapping);
    }
    files = Maps.newLinkedHashMapWithExpectedSize(task.files().size());
    task.files().forEach(fst -> this.files.putIfAbsent(toEncodedPathString(fst), fst));
  }

  @Override
  public ColumnarBatch get() {
    if (batchReader == null) {
      // Not initialized, return null to align with PerFile reader.
      return null;
    }
    needNext = true;
    isBatchPending = false;
    return batchReader.next();
  }

  @Override
  public boolean next() throws IOException {
    ensureBatchReader();
    if (needNext) {
      needNext = false;
      isBatchPending = batchReader.hasNext();
    }
    return isBatchPending;
  }

  @Override
  public void close() throws IOException {
    if (batchReader != null) batchReader.close();
    super.close();
  }

  @Override
  CloseableIterator<ColumnarBatch> open(FileScanTask task) {
    // Stub for the required implementation.
    // This method will never be called after overriding the "next" method.
    throw new IllegalStateException();
  }

  private void ensureBatchReader() {
    if (batchReader != null) {
      return;
    }
    if (FileFormat.PARQUET.equals(fileFormat)) {
      if (useMultiThread) {
        LOG.debug("Using Iceberg Parquet multi-threaded reader, task attempt ID: " +
            TaskContext.get().taskAttemptId());
        batchReader = new ParquetMultiThreadBatchReader();
      } else {
        LOG.debug("Using Iceberg Parquet coalescing reader, task attempt ID: " +
            TaskContext.get().taskAttemptId());
        batchReader = new ParquetCoalescingBatchReader();
      }
    } else {
      throw new UnsupportedOperationException(
          "Format: " + fileFormat + " is not supported for multi-file batched reads");
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

  /**
   * MultiFiles readers expect the path string is encoded and url safe.
   * Here leverages this conversion to do this encoding because Iceberg
   * gives the raw data path by `file().path()` call.
   */
  private String toEncodedPathString(FileScanTask fst) {
    return new Path(fst.file().path().toString()).toUri().toString();
  }

  static class FilteredParquetFileInfo {
    private final ParquetFileInfoWithBlockMeta parquetBlockMeta;
    private final Map<Integer, ?> idToConstant;
    private final Schema expectedSchema;

    FilteredParquetFileInfo(ParquetFileInfoWithBlockMeta parquetBlockMeta,
        Map<Integer, ?> idToConstant, Schema expectedSchema) {
      this.parquetBlockMeta = parquetBlockMeta;
      this.idToConstant = idToConstant;
      this.expectedSchema = expectedSchema;
    }

    ParquetFileInfoWithBlockMeta parquetBlockMeta() {
      return parquetBlockMeta;
    }

    Map<Integer, ?> idToConstant() {
      return idToConstant;
    }

    Schema expectedSchema() {
      return expectedSchema;
    }
  }

  static class IcebergParquetExtraInfo extends ParquetExtraInfo {
    private final Map<Integer, ?> idToConstant;
    private final Schema expectedSchema;
    private final PartitionSpec partitionSpec;

    IcebergParquetExtraInfo(DateTimeRebaseMode dateRebaseMode,
                            DateTimeRebaseMode timestampRebaseMode,
                            boolean hasInt96Timestamps,
                            Map<Integer, ?> idToConstant, Schema expectedSchema,
                            PartitionSpec partitionSpec) {
      super(dateRebaseMode, timestampRebaseMode, hasInt96Timestamps);
      this.idToConstant = idToConstant;
      this.expectedSchema = expectedSchema;
      this.partitionSpec = partitionSpec;
    }

    Map<Integer, ?> idToConstant() {
      return idToConstant;
    }

    Schema expectedSchema() {
      return expectedSchema;
    }

    PartitionSpec partitionSpec() {
      return partitionSpec;
    }
  }

  abstract class MultiFileBatchReaderBase implements CloseableIterator<ColumnarBatch> {
    protected final FilePartitionReaderBase rapidsReader;

    protected MultiFileBatchReaderBase() {
      // Iceberg will handle partition values itself. So both
      // the partitioned schema and values are empty for the Rapids reader.
      final StructType emptyPartSchema = new StructType();
      final InternalRow emptyPartValue = InternalRow.empty();
      PartitionedFile[] pFiles = files.values().stream()
          .map(fst -> PartitionedFileUtilsShim.newPartitionedFile(emptyPartValue,
              toEncodedPathString(fst), fst.start(), fst.length()))
          .toArray(PartitionedFile[]::new);
      rapidsReader = createRapidsReader(pFiles, emptyPartSchema);
    }

    @Override
    public void close() throws IOException {
      rapidsReader.close();
    }

    @Override
    public boolean hasNext() {
      try {
        return rapidsReader.next();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    protected abstract FilePartitionReaderBase createRapidsReader(PartitionedFile[] pFiles,
        StructType partitionSchema);

    /** The filter function for the Parquet multi-file reader */
    protected FilteredParquetFileInfo filterParquetBlocks(FileScanTask fst,
        String partFilePathString) {
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

        ParquetFileInfoWithBlockMeta parquetBlockMeta = ParquetFileInfoWithBlockMeta.apply(
            // The path conversion aligns with that in Rapids multi-files readers.
            // So here should use the file path of a PartitionedFile.
            new Path(new URI(partFilePathString)), clippedBlocks,
            InternalRow.empty(), fileReadSchema, partReaderSparkSchema,
            DateTimeRebaseCorrected$.MODULE$, // dateRebaseMode
            DateTimeRebaseCorrected$.MODULE$, // timestampRebaseMode
            true //  hasInt96Timestamps
        );
        return new FilteredParquetFileInfo(parquetBlockMeta, updatedConstants, updatedSchema);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to open file: " + inFile, e);
      } catch (URISyntaxException ue) {
        throw new IllegalArgumentException("Invalid file path: " + inFile, ue);
      }
    } // end of filterParquetBlocks
  }

  class ParquetMultiThreadBatchReader extends MultiFileBatchReaderBase {
    private final Map<String, Tuple2<Map<Integer, ?>, Schema>> constsSchemaMap =
        Maps.newConcurrentMap();

    ParquetMultiThreadBatchReader() {
      super();
    }

    @Override
    protected FilePartitionReaderBase createRapidsReader(PartitionedFile[] pFiles,
        StructType partitionSchema) {
      return new MultiFileCloudParquetPartitionReader(conf, pFiles,
          this::filterParquetBlocks, caseSensitive, parquetDebugDumpPrefix, parquetDebugDumpAlways,
          maxBatchSizeRows, maxBatchSizeBytes, targetBatchSizeBytes, maxGpuColumnSizeBytes,
          useChunkedReader, maxChunkedReaderMemoryUsageSizeBytes, metrics, partitionSchema,
          numThreads, maxNumFileProcessed,
          false, // ignoreMissingFiles
          false, // ignoreCorruptFiles
          false, // useFieldId
          scala.collection.immutable.Map$.MODULE$.empty(), // alluxioPathReplacementMap
          false, // alluxioReplacementTaskTime
          queryUsesInputFile,
          true, // keepReadsInOrder
          new CombineConf(
              -1, // combineThresholdsize
              -1) // combineWaitTime
      );
    }

    private ParquetFileInfoWithBlockMeta filterParquetBlocks(PartitionedFile file) {
      String partFilePathString = file.filePath().toString();
      FileScanTask fst = files.get(partFilePathString);
      FilteredParquetFileInfo filteredInfo = filterParquetBlocks(fst, partFilePathString);
      constsSchemaMap.put(partFilePathString,
          Tuple2.apply(filteredInfo.idToConstant(), filteredInfo.expectedSchema()));
      return filteredInfo.parquetBlockMeta();
    }

    @Override
    public ColumnarBatch next() {
      // The same post-process with PerFile reader.
      try (ColumnarBatch batch = rapidsReader.get()) {
        // The Rapids reader should already set the current file.
        String curFile = InputFileUtils.getCurInputFilePath();
        Tuple2<Map<Integer, ?>, Schema> constsSchema = constsSchemaMap.get(curFile);
        Map<Integer, ?> idToConsts = constsSchema._1();
        Schema updatedReadSchema = constsSchema._2();
        return GpuIcebergReader.addUpcastsIfNeeded(
            GpuIcebergReader.addConstantColumns(batch, updatedReadSchema, idToConsts),
            updatedReadSchema);
      }
    }
  }

  class ParquetCoalescingBatchReader extends MultiFileBatchReaderBase {

    ParquetCoalescingBatchReader() {
      super();
    }

    @SuppressWarnings("deprecation")
    @Override
    protected FilePartitionReaderBase createRapidsReader(PartitionedFile[] pFiles,
        StructType partitionSchema) {
      ArrayList<ParquetSingleDataBlockMeta> clippedBlocks = new ArrayList<>();
      Arrays.stream(pFiles).forEach(pFile -> {
        String partFilePathString = pFile.filePath().toString();
        FileScanTask fst = files.get(partFilePathString);
        FilteredParquetFileInfo filteredInfo = filterParquetBlocks(fst, partFilePathString);
        List<ParquetSingleDataBlockMeta> fileSingleMetas =
          JavaConverters.asJavaCollection(filteredInfo.parquetBlockMeta.blocks()).stream()
            .map(b -> ParquetSingleDataBlockMeta.apply(
                filteredInfo.parquetBlockMeta.filePath(),
                ParquetDataBlock.apply(b),
                InternalRow.empty(),
                ParquetSchemaWrapper.apply(filteredInfo.parquetBlockMeta.schema()),
                filteredInfo.parquetBlockMeta.readSchema(),
                new IcebergParquetExtraInfo(
                    filteredInfo.parquetBlockMeta.dateRebaseMode(),
                    filteredInfo.parquetBlockMeta.timestampRebaseMode(),
                    filteredInfo.parquetBlockMeta.hasInt96Timestamps(),
                    filteredInfo.idToConstant(),
                    filteredInfo.expectedSchema(),
                    fst.spec())))
            .collect(Collectors.toList());
        clippedBlocks.addAll(fileSingleMetas);
      });

      return new MultiFileParquetPartitionReader(conf, pFiles,
          JavaConverters.asScalaBuffer(clippedBlocks).toSeq(),
          caseSensitive, parquetDebugDumpPrefix, parquetDebugDumpAlways,
          maxBatchSizeRows, maxBatchSizeBytes, targetBatchSizeBytes, maxGpuColumnSizeBytes,
          useChunkedReader, maxChunkedReaderMemoryUsageSizeBytes,
          metrics, partitionSchema, numThreads,
          false, // ignoreMissingFiles
          false, // ignoreCorruptFiles
          false // useFieldId
      ) {
        @Override
        public boolean checkIfNeedToSplitDataBlock(SingleDataBlockInfo currentBlockInfo,
            SingleDataBlockInfo nextBlockInfo) {
          // Check the read schema. Because it may differ among files in Iceberg.
          if (!TrampolineUtil.sameType(currentBlockInfo.readSchema(),
              nextBlockInfo.readSchema())) {
            return true;
          }
          // Now for Iceberg, blocks with different partition schemas or partition values
          // do not coalesce.
          // Will try to figure out if it is possible to merge and add different
          // partition values correctly in the future, to allow coalescing even
          // partition values differ but with the same partition schema,
          // tracked by https://github.com/NVIDIA/spark-rapids/issues/6423.
          IcebergParquetExtraInfo curEInfo =
              (IcebergParquetExtraInfo)currentBlockInfo.extraInfo();
          IcebergParquetExtraInfo nextEInfo =
              (IcebergParquetExtraInfo)nextBlockInfo.extraInfo();
          if (!samePartitionSpec(curEInfo, nextEInfo)) {
            return true;
          }

          return super.checkIfNeedToSplitDataBlock(currentBlockInfo, nextBlockInfo);
        }

        @Override
        public ColumnarBatch finalizeOutputBatch(ColumnarBatch batch, ExtraInfo extraInfo) {
          Map<Integer, ?> idToConsts = ((IcebergParquetExtraInfo)extraInfo).idToConstant();
          Schema expectedSchema = ((IcebergParquetExtraInfo)extraInfo).expectedSchema();
          return GpuIcebergReader.addUpcastsIfNeeded(
              GpuIcebergReader.addConstantColumns(batch, expectedSchema, idToConsts),
              expectedSchema);
        }

        private boolean samePartitionSpec(IcebergParquetExtraInfo curEInfo,
            IcebergParquetExtraInfo nextEInfo) {
          if (curEInfo.partitionSpec().partitionType()
              .equals(nextEInfo.partitionSpec().partitionType())) {
            // partition schema is equivalent, check the partition value next.
            // Only identity fields were added into constants map.
            return curEInfo.partitionSpec().identitySourceIds().stream().allMatch(id ->
              Objects.deepEquals(
                  curEInfo.idToConstant().get(id),
                  nextEInfo.idToConstant().get(id)));
          }
          return false;
        }
      }; // end of "return new MultiFileParquetPartitionReader"
    }

    @Override
    public ColumnarBatch next() {
      return rapidsReader.get();
    }
  }
}
