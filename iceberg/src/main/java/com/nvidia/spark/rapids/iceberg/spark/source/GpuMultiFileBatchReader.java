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

package com.nvidia.spark.rapids.iceberg.spark.source;

import com.nvidia.spark.rapids.iceberg.parquet.GpuParquetMultiFileReaderBase;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.*;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/** The wrapper of the GPU multi-threaded and coalescing reader for Iceberg */
public class GpuMultiFileBatchReader extends BaseDataReader {
  private static final Logger LOG = LoggerFactory.getLogger(GpuMultiFileBatchReader.class);
  private final LinkedHashMap<String, FileScanTask> files;
  private final GpuMultiFileReaderConf conf;

  private NameMapping nameMapping = null;
  private boolean needNext = true;
  private boolean isBatchPending;
  // lazy variables
  private CloseableIterator<ColumnarBatch> batchReader = null;

  GpuMultiFileBatchReader(ScanTaskGroup<? extends ScanTask> task, Table table,
      GpuMultiFileReaderConf conf) {
    super(table, task);
    this.conf = conf;
    String nameMapping = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    if (nameMapping != null) {
      this.nameMapping = NameMappingParser.fromJson(nameMapping);
    }
    files = Maps.newLinkedHashMapWithExpectedSize(tasks.size());
    tasks.forEach(fst -> this.files.putIfAbsent(toEncodedPathString(fst), fst));
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
  }

  private void ensureBatchReader() {
    if (batchReader != null) {
      return;
    }
    if (FileFormat.PARQUET.equals(conf.getFileFormat())) {
      batchReader = GpuParquetMultiFileReaderBase.create(
          files,
          conf,
          nameMapping,
          table(),
          inputFiles);
    } else {
      throw new UnsupportedOperationException(
          "Format: " + conf.getFileFormat() + " is not supported for multi-file batched reads");
    }
  }

  /**
   * MultiFiles readers expect the path string is encoded and url safe.
   * Here leverages this conversion to do this encoding because Iceberg
   * gives the raw data path by `file().path()` call.
   */
  public static String toEncodedPathString(FileScanTask fst) {
    return new Path(fst.file().path().toString()).toUri().toString();
  }
}
