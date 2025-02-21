/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.kudo;

import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVectorCore;
import com.nvidia.spark.rapids.jni.schema.HostColumnsVisitor;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import static com.nvidia.spark.rapids.kudo.KudoSerializer.padForHostAlignment;
import static java.lang.Math.toIntExact;

/**
 * This class visits a list of columns and calculates the serialized table header.
 *
 * <p>
 * The columns are visited in post order, and for more details about the visiting process, please refer to
 * {@link HostColumnsVisitor}.
 * </p>
 */
class KudoTableHeaderCalc implements HostColumnsVisitor {
  private final SliceInfo root;
  private final int numFlattenedCols;
  private final byte[] bitset;
  private long validityBufferLen;
  private long offsetBufferLen;
  private long totalDataLen;
  private int nextColIdx;

  private Deque<SliceInfo> sliceInfos = new ArrayDeque<>();

  KudoTableHeaderCalc(int rowOffset, int numRows, int numFlattenedCols) {
    this.root = new SliceInfo(rowOffset, numRows);
    this.totalDataLen = 0;
    sliceInfos.addLast(this.root);
    this.bitset = new byte[(numFlattenedCols + 7) / 8];
    this.numFlattenedCols = numFlattenedCols;
    this.nextColIdx = 0;
  }

  public KudoTableHeader getHeader() {
    return new KudoTableHeader(toIntExact(root.offset),
        toIntExact(root.rowCount),
        toIntExact(validityBufferLen),
        toIntExact(offsetBufferLen),
        toIntExact(totalDataLen),
        numFlattenedCols,
        bitset);
  }

  @Override
  public void visitStruct(HostColumnVectorCore col) {
    SliceInfo parent = sliceInfos.getLast();

    long validityBufferLength = 0;
    if (col.hasValidityVector()) {
      validityBufferLength = padForHostAlignment(parent.getValidityBufferInfo().getBufferLength());
    }

    this.validityBufferLen += validityBufferLength;

    totalDataLen += validityBufferLength;
    this.setHasValidity(col.hasValidityVector());
  }

  @Override
  public void preVisitList(HostColumnVectorCore col) {
    SliceInfo parent = sliceInfos.getLast();


    long validityBufferLength = 0;
    if (col.hasValidityVector() && parent.rowCount > 0) {
      validityBufferLength = padForHostAlignment(parent.getValidityBufferInfo().getBufferLength());
    }

    long offsetBufferLength = 0;
    if (col.getOffsets() != null && parent.rowCount > 0) {
      offsetBufferLength = padForHostAlignment((parent.rowCount + 1) * Integer.BYTES);
    }

    this.validityBufferLen += validityBufferLength;
    this.offsetBufferLen += offsetBufferLength;
    this.totalDataLen += validityBufferLength + offsetBufferLength;

    this.setHasValidity(col.hasValidityVector());

    SliceInfo current;

    if (col.getOffsets() != null) {
      int start = col.getOffsets().getInt(parent.offset * Integer.BYTES);
      int end = col.getOffsets().getInt((parent.offset + parent.rowCount) * Integer.BYTES);
      int rowCount = end - start;
      current = new SliceInfo(start, rowCount);
    } else {
      current = new SliceInfo(0, 0);
    }

    sliceInfos.addLast(current);
  }

  @Override
  public void visitList(HostColumnVectorCore col) {
    sliceInfos.removeLast();
  }


  @Override
  public void visit(HostColumnVectorCore col) {
    SliceInfo parent = sliceInfos.peekLast();
    long validityBufferLen = dataLenOfValidityBuffer(col, parent);
    long offsetBufferLen = dataLenOfOffsetBuffer(col, parent);
    long dataBufferLen = dataLenOfDataBuffer(col, parent);

    this.validityBufferLen += validityBufferLen;
    this.offsetBufferLen += offsetBufferLen;
    this.totalDataLen += validityBufferLen + offsetBufferLen + dataBufferLen;

    this.setHasValidity(col.hasValidityVector());
  }

  private void setHasValidity(boolean hasValidityBuffer) {
    if (hasValidityBuffer) {
      int bytePos = nextColIdx / 8;
      int bitPos = nextColIdx % 8;
      bitset[bytePos] = (byte) (bitset[bytePos] | (1 << bitPos));
    }
    nextColIdx++;
  }

  private static long dataLenOfValidityBuffer(HostColumnVectorCore col, SliceInfo info) {
    if (col.hasValidityVector() && info.getRowCount() > 0) {
      return padForHostAlignment(info.getValidityBufferInfo().getBufferLength());
    } else {
      return 0;
    }
  }

  private static long dataLenOfOffsetBuffer(HostColumnVectorCore col, SliceInfo info) {
    if (DType.STRING.equals(col.getType()) && info.getRowCount() > 0) {
      return padForHostAlignment((info.rowCount + 1) * Integer.BYTES);
    } else {
      return 0;
    }
  }

  private static long dataLenOfDataBuffer(HostColumnVectorCore col, SliceInfo info) {
    if (DType.STRING.equals(col.getType())) {
      if (col.getOffsets() != null) {
        long startByteOffset = col.getOffsets().getInt(info.offset * Integer.BYTES);
        long endByteOffset = col.getOffsets().getInt((info.offset + info.rowCount) * Integer.BYTES);
        return padForHostAlignment(endByteOffset - startByteOffset);
      } else {
        return 0;
      }
    } else {
      if (col.getType().getSizeInBytes() > 0) {
        return padForHostAlignment(col.getType().getSizeInBytes() * info.rowCount);
      } else {
        return 0;
      }
    }
  }
}
