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

package com.nvidia.spark.rapids.iceberg.parquet;

import com.nvidia.spark.rapids.*;
import com.nvidia.spark.rapids.iceberg.spark.source.GpuMultiFileReaderConf;
import com.nvidia.spark.rapids.shims.PartitionedFileUtilsShim;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import com.nvidia.shaded.iceberg.org.apache.iceberg.parquet.ParquetSchemaUtil;
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
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.nvidia.spark.rapids.iceberg.spark.source.BaseDataReader.constantsMap;

public abstract class GpuParquetMultiFileReaderBase implements CloseableIterator<ColumnarBatch> {
  private static final Logger LOG = LoggerFactory.getLogger(GpuParquetMultiFileReaderBase.class);
  protected final GpuMultiFileReaderConf conf;
  protected final Map<String, FileScanTask> files;
  protected final NameMapping nameMapping;
  protected final Table table;
  protected final Map<String, InputFile> inputFiles;

  protected final FilePartitionReaderBase rapidsReader;

  GpuParquetMultiFileReaderBase(Map<String, FileScanTask> files, GpuMultiFileReaderConf conf,
      NameMapping nameMapping, Table table, Map<String, InputFile> inputFiles) {
    this.files = files;
    this.conf = conf;
    this.nameMapping = nameMapping;
    this.table = table;
    this.inputFiles = inputFiles;
    // Iceberg will handle partition values itself. So both
    // the partitioned schema and values are empty for the Rapids reader.
    final StructType emptyPartSchema = new StructType();
    final InternalRow emptyPartValue = InternalRow.empty();
    PartitionedFile[] pFiles = files
        .values()
        .stream()
        .map(fst -> PartitionedFileUtilsShim.newPartitionedFile(emptyPartValue,
            fst.file().path().toString(), fst.start(), fst.length()))
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

  public static GpuParquetMultiFileReaderBase create(Map<String, FileScanTask> files,
      GpuMultiFileReaderConf conf, NameMapping nameMapping, Table table,
      Map<String, InputFile> inputFiles) {
    if (conf.isUseMultiThread()) {
      LOG.debug("Using Iceberg Parquet multi-threaded reader, task attempt ID: " +
          TaskContext.get().taskAttemptId());
      return new ParquetMultiThreadBatchReader(files, conf, nameMapping, table, inputFiles);
    } else {
      LOG.debug("Using Iceberg Parquet coalescing reader, task attempt ID: " +
          TaskContext.get().taskAttemptId());
      return new ParquetCoalescingBatchReader(files, conf, nameMapping, table, inputFiles);
    }
  }

  protected abstract FilePartitionReaderBase createRapidsReader(PartitionedFile[] pFiles,
      StructType partitionSchema);

  /**
   * The filter function for the Parquet multi-file reader
   */
  protected FilteredParquetFileInfo filterParquetBlocks(FileScanTask fst,
      String partFilePathString) {
    Map<Integer, ?> idToConstant = constantsMap(fst, conf.getExpectedSchema(), table);
    InputFile inFile = inputFiles.get(fst.file().path().toString());
    ParquetReadOptions readOptions =
        GpuParquet.buildReaderOptions(inFile, fst.start(), fst.length());
    try (ParquetFileReader reader = GpuParquetReader.newReader(inFile, readOptions)) {
      MessageType fileSchema = reader.getFileMetaData().getSchema();

      MessageType typeWithIds;
      MessageType fileReadSchema;
      if (ParquetSchemaUtil.hasIds(fileSchema)) {
        typeWithIds = fileSchema;
        fileReadSchema = ParquetSchemaUtil.pruneColumns(fileSchema, conf.getExpectedSchema());
      } else if (nameMapping != null) {
        typeWithIds = ParquetSchemaUtil.applyNameMapping(fileSchema, nameMapping);
        fileReadSchema = ParquetSchemaUtil.pruneColumns(typeWithIds, conf.getExpectedSchema());
      } else {
        typeWithIds = ParquetSchemaUtil.addFallbackIds(fileSchema);
        fileReadSchema = ParquetSchemaUtil.pruneColumnsFallback(fileSchema, conf.getExpectedSchema());
      }

      List<BlockMetaData> filteredRowGroups = GpuParquetReader.filterRowGroups(reader,
          conf.getExpectedSchema(), typeWithIds,  fst.residual(), conf.isCaseSensitive());

      Seq<BlockMetaData> clippedBlocks = GpuParquetUtils.clipBlocksToSchema(
          fileReadSchema, filteredRowGroups, conf.isCaseSensitive());
      StructType partReaderSparkSchema = (StructType) TypeWithSchemaVisitor.visit(
          conf.getExpectedSchema().asStruct(), fileReadSchema,
          new GpuParquetReader.SparkSchemaConverter());


      ParquetFileInfoWithBlockMeta parquetBlockMeta = ParquetFileInfoWithBlockMeta.apply(
          // The path conversion aligns with that in Rapids multi-files readers.
          // So here should use the file path of a PartitionedFile.
          new Path(new URI(partFilePathString)), clippedBlocks,
          InternalRow.empty(), fileReadSchema, partReaderSparkSchema,
          DateTimeRebaseCorrected$.MODULE$, // dateRebaseMode
          DateTimeRebaseCorrected$.MODULE$, // timestampRebaseMode
          true //  hasInt96Timestamps
      );
      return new FilteredParquetFileInfo(parquetBlockMeta,
          new GpuParquetReaderPostProcessor(fileReadSchema, idToConstant,
              conf.getExpectedSchema()));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to open file: " + inFile, e);
    } catch (URISyntaxException ue) {
      throw new IllegalArgumentException("Invalid file path: " + inFile, ue);
    }
  } // end of filterParquetBlocks


  static class FilteredParquetFileInfo {
    private final ParquetFileInfoWithBlockMeta parquetBlockMeta;
    private final GpuParquetReaderPostProcessor postProcessor;

    FilteredParquetFileInfo(ParquetFileInfoWithBlockMeta parquetBlockMeta, GpuParquetReaderPostProcessor postProcessor) {
      this.parquetBlockMeta = parquetBlockMeta;
      this.postProcessor = postProcessor;
    }

    ParquetFileInfoWithBlockMeta parquetBlockMeta() {
      return parquetBlockMeta;
    }
  }

  static class IcebergParquetExtraInfo extends ParquetExtraInfo {
    private final GpuParquetReaderPostProcessor postProcessor;

    IcebergParquetExtraInfo(DateTimeRebaseMode dateRebaseMode,
        DateTimeRebaseMode timestampRebaseMode,
        boolean hasInt96Timestamps, GpuParquetReaderPostProcessor postProcessor) {
      super(dateRebaseMode, timestampRebaseMode, hasInt96Timestamps);
      this.postProcessor = postProcessor;
    }
  }

  static class ParquetMultiThreadBatchReader extends GpuParquetMultiFileReaderBase {
    private final Map<String, GpuParquetReaderPostProcessor> postProcessorMap =
        Maps.newConcurrentMap();

    public ParquetMultiThreadBatchReader(Map<String, FileScanTask> files,
        GpuMultiFileReaderConf conf,
        NameMapping nameMapping, Table table,
        Map<String, InputFile> inputFiles) {
      super(files, conf, nameMapping, table, inputFiles);
    }

    @Override
    protected FilePartitionReaderBase createRapidsReader(PartitionedFile[] pFiles,
        StructType partitionSchema) {
      return new MultiFileCloudParquetPartitionReader(conf.getConf(), pFiles,
          this::filterParquetBlocks, conf.isCaseSensitive(), conf.getParquetDebugDumpPrefix(),
          conf.isParquetDebugDumpAlways(), conf.getMaxBatchSizeRows(), conf.getMaxBatchSizeBytes(),
          conf.getTargetBatchSizeBytes(), conf.getMaxGpuColumnSizeBytes(),
          conf.isUseChunkedReader(), conf.getMaxChunkedReaderMemoryUsageSizeBytes(),
          CpuCompressionConfig$.MODULE$.disabled(), conf.getMetrics(), partitionSchema,
          conf.getNumThreads(), conf.getMaxNumFileProcessed(),
          false, // ignoreMissingFiles
          false, // ignoreCorruptFiles
          false, // useFieldId
          // We always set this to true to disable combining small files into a larger one
          // as iceberg's parquet file may have different schema due to schema evolution.
          true,
          true, // keepReadsInOrder
          new CombineConf(
              -1, // combineThresholdsize
              -1) // combineWaitTime
      );
    }

    private ParquetFileInfoWithBlockMeta filterParquetBlocks(PartitionedFile file) {
      String partFilePathString = file.filePath().toString();
      FileScanTask fst = files.get(partFilePathString);
      if (fst == null) {
        throw new IllegalStateException("File " + partFilePathString + " not found, files: " +
            Arrays.toString(files.keySet().toArray(new String[0])));
      }
      FilteredParquetFileInfo filteredInfo = filterParquetBlocks(fst, partFilePathString);
      postProcessorMap.put(partFilePathString, filteredInfo.postProcessor);
      return filteredInfo.parquetBlockMeta();
    }

    @Override
    public ColumnarBatch next() {
      // The same post-process with PerFile reader.
      try (ColumnarBatch batch = rapidsReader.get()) {
        // The Rapids reader should already set the current file.
        String curFile = InputFileUtils.getCurInputFilePath();
        return postProcessorMap.get(curFile).process(batch);
      }
    }
  }

  static class ParquetCoalescingBatchReader extends GpuParquetMultiFileReaderBase {

    ParquetCoalescingBatchReader(Map<String, FileScanTask> files,
        GpuMultiFileReaderConf conf,
        NameMapping nameMapping,
        Table table,
        Map<String, InputFile> inputFiles) {
      super(files, conf, nameMapping, table, inputFiles);
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
                    ParquetDataBlock.apply(b, CpuCompressionConfig$.MODULE$.disabled()),
                    InternalRow.empty(),
                    ParquetSchemaWrapper.apply(filteredInfo.parquetBlockMeta.schema()),
                    filteredInfo.parquetBlockMeta.readSchema(),
                    new IcebergParquetExtraInfo(
                        filteredInfo.parquetBlockMeta.dateRebaseMode(),
                        filteredInfo.parquetBlockMeta.timestampRebaseMode(),
                        filteredInfo.parquetBlockMeta.hasInt96Timestamps(),
                        filteredInfo.postProcessor)))
                .collect(Collectors.toList());
        clippedBlocks.addAll(fileSingleMetas);
      });

      return new MultiFileParquetPartitionReader(conf.getConf(), pFiles,
          JavaConverters.asScalaBuffer(clippedBlocks).toSeq(),
          conf.isCaseSensitive(), conf.getParquetDebugDumpPrefix(),
          conf.isParquetDebugDumpAlways(), conf.getMaxBatchSizeRows(),
          conf.getMaxBatchSizeBytes(), conf.getTargetBatchSizeBytes(),
          conf.getMaxGpuColumnSizeBytes(), conf.isUseChunkedReader(),
          conf.getMaxChunkedReaderMemoryUsageSizeBytes(),
          CpuCompressionConfig$.MODULE$.disabled(),
          conf.getMetrics(), partitionSchema, conf.getNumThreads(),
          false, // ignoreMissingFiles
          false, // ignoreCorruptFiles
          false // useFieldId
      ) {
        @Override
        public boolean checkIfNeedToSplitDataBlock(SingleDataBlockInfo currentBlockInfo,
            SingleDataBlockInfo nextBlockInfo) {
          // Iceberg should always use field id for matching columns, so we should always disable
          // coalescing.
          return true;
        }

        @Override
        public ColumnarBatch finalizeOutputBatch(ColumnarBatch batch, ExtraInfo extraInfo) {
          return ((IcebergParquetExtraInfo)extraInfo).postProcessor.process(batch);
        }

      }; // end of "return new MultiFileParquetPartitionReader"
    }

    @Override
    public ColumnarBatch next() {
      return rapidsReader.get();
    }
  }
}
