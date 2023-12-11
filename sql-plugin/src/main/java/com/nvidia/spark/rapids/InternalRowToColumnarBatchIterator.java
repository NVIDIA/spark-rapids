/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Optional;

import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.HostColumnVectorCore;
import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.NvtxColor;
import ai.rapids.cudf.NvtxRange;
import ai.rapids.cudf.Table;
import com.nvidia.spark.rapids.jni.RowConversion;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * This class converts InternalRow instances to ColumnarBatches on the GPU through the magic of
 * code generation. This just provides most of the framework a concrete implementation will
 * be generated based off of the schema.
 * The InternalRow instances are first converted to UnsafeRow, cheaply if the instance is already
 * UnsafeRow, and then the UnsafeRow data is collected into a ColumnarBatch.
 */
public abstract class InternalRowToColumnarBatchIterator implements Iterator<ColumnarBatch> {
  protected final Iterator<InternalRow> input;
  protected UnsafeRow pending = null;
  protected final int numRowsEstimate;
  protected final long dataLength;
  protected final DType[] rapidsTypes;
  protected final DataType[] outputTypes;
  protected final GpuMetric streamTime;
  protected final GpuMetric opTime;
  protected final GpuMetric numInputRows;
  protected final GpuMetric numOutputRows;
  protected final GpuMetric numOutputBatches;

  protected InternalRowToColumnarBatchIterator(
      Iterator<InternalRow> input,
      Attribute[] schema,
      CoalesceSizeGoal goal,
      GpuMetric streamTime,
      GpuMetric opTime,
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
    this.streamTime = streamTime;
    this.opTime = opTime;
    this.numInputRows = numInputRows;
    this.numOutputRows = numOutputRows;
    this.numOutputBatches = numOutputBatches;
  }

  @Override
  public boolean hasNext() {
    boolean ret = true;
    if (pending == null) {
      long start = System.nanoTime();
      ret = input.hasNext();
      long ct = System.nanoTime() - start;
      streamTime.add(ct);
    }
    return ret;
  }

  @Override
  public ColumnarBatch next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final int BYTES_PER_OFFSET = DType.INT32.getSizeInBytes();

    long collectStart = System.nanoTime();

    Tuple2<SpillableColumnarBatch, NvtxRange> batchAndRange;

    // The row formatted data is stored as a column of lists of bytes.  The current java CUDF APIs
    // don't do a great job from a performance standpoint with building this type of data structure
    // and we want this to be as efficient as possible so we are going to allocate two host memory
    // buffers.  One will be for the byte data and the second will be for the offsets. We will then
    // write the data directly into those buffers using code generation in a child of this class.
    // that implements fillBatch.
    HostMemoryBuffer db =
        RmmRapidsRetryIterator.withRetryNoSplit( () -> {
          return HostAlloc$.MODULE$.alloc(dataLength, true);
        });
    try (
        SpillableHostBuffer sdb = SpillableHostBuffer$.MODULE$.apply(db, db.getLength(),
            SpillPriorities$.MODULE$.ACTIVE_ON_DECK_PRIORITY(),
            RapidsBufferCatalog$.MODULE$.singleton());
    ) {
      HostMemoryBuffer ob =
          RmmRapidsRetryIterator.withRetryNoSplit( () -> {
            return HostAlloc$.MODULE$.alloc(
                ((long) numRowsEstimate + 1) * BYTES_PER_OFFSET, true);
          });
      try (
          SpillableHostBuffer sob = SpillableHostBuffer$.MODULE$.apply(ob, ob.getLength(),
              SpillPriorities$.MODULE$.ACTIVE_ON_DECK_PRIORITY(),
              RapidsBufferCatalog$.MODULE$.singleton());
      ) {
        // Fill in buffer under write lock for host buffers
        int[] used = sdb.withHostBufferWriteLock( (dataBuffer) -> {
          return sob.withHostBufferWriteLock( (offsetsBuffer) -> {
            return fillBatch(dataBuffer, offsetsBuffer);
          });
        });
        batchAndRange = sdb.withHostBufferReadOnly( (dataBuffer) -> {
          return sob.withHostBufferReadOnly( (offsetsBuffer) -> {
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

              long ct = System.nanoTime() - collectStart;
              streamTime.add(ct);

              // Grab the semaphore because we are about to put data onto the GPU.
              GpuSemaphore$.MODULE$.acquireIfNecessary(TaskContext.get());
              NvtxRange range = NvtxWithMetrics.apply("RowToColumnar: build", NvtxColor.GREEN,
                  Option.apply(opTime));
              ColumnVector devColumn =
                  RmmRapidsRetryIterator.withRetryNoSplit(hostColumn::copyToDevice);
              return Tuple2.apply(makeSpillableBatch(devColumn), range);
            }
          });
        });
      }
    }
    try (NvtxRange ignored = batchAndRange._2;
         Table tab =
           RmmRapidsRetryIterator.withRetryNoSplit(batchAndRange._1, (attempt) -> {
             try (ColumnarBatch cb = attempt.getColumnarBatch()) {
               return convertFromRowsUnderRetry(cb);
             }
           })) {
      return GpuColumnVector.from(tab, outputTypes);
    }
  }

  /**
   * Take our device column of encoded rows and turn it into a spillable columnar batch.
   * This allows us to go into a retry block and be able to roll back our work.
   */
  private SpillableColumnarBatch makeSpillableBatch(ColumnVector devColumn) {
    // this is kind of ugly, but we make up a batch to hold this device column such that
    // we can make it spillable
    GpuColumnVector gpuCV = null;
    try {
      gpuCV = GpuColumnVector.from(devColumn,
          ArrayType.apply(DataTypes.ByteType, false));
    } finally {
      if (gpuCV == null) {
        devColumn.close();
      }
    }
    return SpillableColumnarBatch.apply(
        new ColumnarBatch(
            new org.apache.spark.sql.vectorized.ColumnVector[]{gpuCV},
            (int)gpuCV.getRowCount()),
        SpillPriorities.ACTIVE_ON_DECK_PRIORITY());
  }

  /**
   * This is exposed so we can verify it is being called N times for OOM retry tests.
   */
  protected Table convertFromRowsUnderRetry(ColumnarBatch cb) {
    ColumnVector devColumn = GpuColumnVector.extractBases(cb)[0];
    return rapidsTypes.length < 100 ?
        // The fixed-width optimized cudf kernel only supports up to 1.5 KB per row which means
        // at most 184 double/long values. We are branching over the size of the output to
        // know which kernel to call. If rapidsTypes.length < 100 we call the fixed-width
        // optimized version, otherwise the generic one
        RowConversion.convertFromRowsFixedWidthOptimized(devColumn, rapidsTypes) :
        RowConversion.convertFromRows(devColumn, rapidsTypes);
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
