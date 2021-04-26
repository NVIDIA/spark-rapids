/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.HostColumnVectorCore;
import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.NvtxColor;
import ai.rapids.cudf.NvtxRange;
import ai.rapids.cudf.Table;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.CudfUnsafeRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import scala.collection.Iterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * This class converts UnsafeRow instances to ColumnarBatches on the GPU through the magic of
 * code generation. This just provides most of the framework a concrete implementation will
 * be generated based off of the schema.
 */
public abstract class UnsafeRowToColumnarBatchIterator implements Iterator<ColumnarBatch> {
  protected final Iterator<UnsafeRow> input;
  protected UnsafeRow pending = null;
  protected final int numRowsEstimate;
  protected final long dataLength;
  protected final DType[] rapidsTypes;
  protected final DataType[] outputTypes;
  protected final GpuMetric totalTime;
  protected final GpuMetric numInputRows;
  protected final GpuMetric numOutputRows;
  protected final GpuMetric numOutputBatches;

  protected UnsafeRowToColumnarBatchIterator(
      Iterator<UnsafeRow> input,
      Attribute[] schema,
      CoalesceGoal goal,
      GpuMetric totalTime,
      GpuMetric numInputRows,
      GpuMetric numOutputRows,
      GpuMetric numOutputBatches) {
    this.input = input;
    int sizePerRowEstimate = CudfUnsafeRow.getRowSizeEstimate(schema);
    numRowsEstimate = (int)Math.max(1,
        Math.min(Integer.MAX_VALUE - 1, goal.targetSizeBytes() / sizePerRowEstimate));
    dataLength = ((long) sizePerRowEstimate) * numRowsEstimate;
    rapidsTypes = new DType[schema.length];
    outputTypes = new DataType[schema.length];

    for (int i = 0; i < schema.length; i++) {
      rapidsTypes[i] = GpuColumnVector.getNonNestedRapidsType(schema[i].dataType());
      outputTypes[i] = schema[i].dataType();
    }
    this.totalTime = totalTime;
    this.numInputRows = numInputRows;
    this.numOutputRows = numOutputRows;
    this.numOutputBatches = numOutputBatches;
  }

  @Override
  public boolean hasNext() {
    return pending != null || input.hasNext();
  }

  @Override
  public ColumnarBatch next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final int BYTES_PER_OFFSET = DType.INT32.getSizeInBytes();

    ColumnVector devColumn;
    NvtxRange buildRange;
    // The row formatted data is stored as a column of lists of bytes.  The current java CUDF APIs
    // don't do a great job from a performance standpoint with building this type of data structure
    // and we want this to be as efficient as possible so we are going to allocate two host memory
    // buffers.  One will be for the byte data and the second will be for the offsets. We will then
    // write the data directly into those buffers using code generation in a child of this class.
    // that implements fillBatch.
    try (HostMemoryBuffer dataBuffer = HostMemoryBuffer.allocate(dataLength);
         HostMemoryBuffer offsetsBuffer =
             HostMemoryBuffer.allocate(((long)numRowsEstimate + 1) * BYTES_PER_OFFSET)) {

      int[] used = fillBatch(dataBuffer, offsetsBuffer);
      int dataOffset = used[0];
      int currentRow = used[1];
      // We don't want to loop forever trying to copy nothing
      assert (currentRow > 0);
      if (numInputRows != null) {
        numInputRows.add(currentRow);
      }
      if (numOutputRows != null) {
        numOutputRows.add(currentRow);
      }
      if (numOutputBatches != null) {
        numOutputBatches.add(1);
      }
      // Now that we have filled the buffers with the data, we need to turn them into a
      // HostColumnVector and copy them to the device so the GPU can turn it into a Table.
      // To do this we first need to make a HostColumnCoreVector for the data, and then
      // put that into a HostColumnVector as its child.  This the basics of building up
      // a column of lists of bytes in CUDF but it is typically hidden behind the higer level
      // APIs.
      dataBuffer.incRefCount();
      offsetsBuffer.incRefCount();
      try (HostColumnVectorCore dataCv =
               new HostColumnVectorCore(DType.INT8, dataOffset, Optional.of(0L),
                   dataBuffer, null, null, new ArrayList<>());
           HostColumnVector hostColumn = new HostColumnVector(DType.LIST,
               currentRow, Optional.of(0L), null, null,
               offsetsBuffer, Collections.singletonList(dataCv))) {
        // Grab the semaphore because we are about to put data onto the GPU.
        TaskContext tc = TaskContext.get();
        if (tc != null) {
          GpuSemaphore$.MODULE$.acquireIfNecessary(tc);
        }
        if (totalTime != null) {
          buildRange = new NvtxWithMetrics("RowToColumnar", NvtxColor.GREEN, totalTime);
        } else {
          buildRange = new NvtxRange("RowToColumnar", NvtxColor.GREEN);
        }
        devColumn = hostColumn.copyToDevice();
      }
    }
    try (NvtxRange ignored = buildRange;
         ColumnVector cv = devColumn;
         Table tab = Table.convertFromRows(cv, rapidsTypes)) {
      return GpuColumnVector.from(tab, outputTypes);
    }
  }

  /**
   * Fill a batch with data.  This is the abstraction point because it is faster to have a single
   * virtual function call per batch instead of one per row.
   * @param dataBuffer the data buffer to populate
   * @param offsetsBuffer the offsets buffer to populate
   * @return an array of ints where the first index is the amount of data in bytes copied into
   * the data buffer and the second index is the number of rows copied into the buffers.
   */
  public abstract int[] fillBatch(HostMemoryBuffer dataBuffer, HostMemoryBuffer offsetsBuffer);
}
