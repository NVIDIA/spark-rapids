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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import ai.rapids.cudf.Scalar;
import com.nvidia.spark.rapids.CastOptions$;
import com.nvidia.spark.rapids.GpuCast;
import com.nvidia.spark.rapids.GpuColumnVector;
import com.nvidia.spark.rapids.GpuScalar;
import com.nvidia.spark.rapids.iceberg.data.GpuDeleteFilter;
import com.nvidia.spark.rapids.iceberg.parquet.ParquetReaderPostProcessor;
import com.nvidia.spark.rapids.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Takes a partition reader output and adds any constant columns and deletion filters
 * that need to be applied after the data is loaded from the raw data files.
 */
public class GpuIcebergReader implements CloseableIterator<ColumnarBatch> {
  private final PartitionReader<ColumnarBatch> partReader;
  private final GpuDeleteFilter deleteFilter;
  private final ParquetReaderPostProcessor postProcessor;
  private boolean needNext = true;
  private boolean isBatchPending;

  public GpuIcebergReader(Schema expectedSchema, MessageType fileReadSchema,
                          PartitionReader<ColumnarBatch> partReader,
                          GpuDeleteFilter deleteFilter,
                          Map<Integer, ?> idToConstant) {
    this.partReader = partReader;
    this.deleteFilter = deleteFilter;
    this.postProcessor = new ParquetReaderPostProcessor(fileReadSchema, idToConstant,
        expectedSchema);
  }

  @Override
  public void close() throws IOException {
    partReader.close();
  }

  @Override
  public boolean hasNext() {
    if (needNext) {
      try {
        isBatchPending = partReader.next();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      needNext = false;
    }
    return isBatchPending;
  }

  @Override
  public ColumnarBatch next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more batches to iterate");
    }
    isBatchPending = false;
    needNext = true;
    try (ColumnarBatch batch = partReader.get()) {
      if (deleteFilter != null) {
        throw new UnsupportedOperationException("Delete filter is not supported");
      }

      return postProcessor.process(batch);
    }
  }
}
