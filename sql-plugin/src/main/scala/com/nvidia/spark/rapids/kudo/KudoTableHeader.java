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

import ai.rapids.cudf.BufferType;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import static com.nvidia.spark.rapids.jni.Preconditions.ensure;
import static com.nvidia.spark.rapids.jni.Preconditions.ensureNonNegative;
import static java.util.Objects.requireNonNull;

/**
 * Holds the metadata about a serialized table. If this is being read from a stream
 * isInitialized will return true if the metadata was read correctly from the stream.
 * It will return false if an EOF was encountered at the beginning indicating that
 * there was no data to be read.
 */
public final class KudoTableHeader {
  /**
   * Magic number "KUD0" in ASCII.
   */
  private static final int SER_FORMAT_MAGIC_NUMBER = 0x4B554430;

  // The offset in the original table where row starts. For example, if we want to serialize rows [3, 9) of the
  // original table, offset would be 3, and numRows would be 6.
  private final int offset;
  private final int numRows;
  private final int validityBufferLen;
  private final int offsetBufferLen;
  private final int totalDataLen;
  private final int numColumns;
  // A bit set to indicate if a column has a validity buffer or not. Each column is represented by a single bit.
  private final byte[] hasValidityBuffer;

  /**
   * Reads the table header from the given input stream.
   *
   * @param din input stream
   * @return the table header. If an EOFException is encountered at the beginning, returns empty result.
   * @throws IOException if an I/O error occurs
   */
  public static Optional<KudoTableHeader> readFrom(DataInputStream din) throws IOException {
    int num;
    try {
      num = din.readInt();
      if (num != SER_FORMAT_MAGIC_NUMBER) {
        throw new IllegalStateException("Kudo format error, expected magic number " + SER_FORMAT_MAGIC_NUMBER +
            " found " + num);
      }
    } catch (EOFException e) {
      // If we get an EOF at the very beginning don't treat it as an error because we may
      // have finished reading everything...
      return Optional.empty();
    }

    int offset = din.readInt();
    int numRows = din.readInt();

    int validityBufferLen = din.readInt();
    int offsetBufferLen = din.readInt();
    int totalDataLen = din.readInt();
    int numColumns = din.readInt();
    int validityBufferLength = lengthOfHasValidityBuffer(numColumns);
    byte[] hasValidityBuffer = new byte[validityBufferLength];
    din.readFully(hasValidityBuffer);

    return Optional.of(new KudoTableHeader(offset, numRows, validityBufferLen, offsetBufferLen, totalDataLen, numColumns,
        hasValidityBuffer));
  }

  KudoTableHeader(int offset, int numRows, int validityBufferLen, int offsetBufferLen,
                  int totalDataLen, int numColumns, byte[] hasValidityBuffer) {
    this.offset = ensureNonNegative(offset, "offset");
    this.numRows = ensureNonNegative(numRows, "numRows");
    this.validityBufferLen = ensureNonNegative(validityBufferLen, "validityBufferLen");
    this.offsetBufferLen = ensureNonNegative(offsetBufferLen, "offsetBufferLen");
    this.totalDataLen = ensureNonNegative(totalDataLen, "totalDataLen");
    this.numColumns = ensureNonNegative(numColumns, "numColumns");

    requireNonNull(hasValidityBuffer, "hasValidityBuffer cannot be null");
    ensure(hasValidityBuffer.length == lengthOfHasValidityBuffer(numColumns),
        () -> numColumns + " columns expects hasValidityBuffer with length " + lengthOfHasValidityBuffer(numColumns) +
            ", but found " + hasValidityBuffer.length);
    this.hasValidityBuffer = hasValidityBuffer;
  }

  /**
   * Returns the size of a buffer needed to read data into the stream.
   */
  public int getTotalDataLen() {
    return totalDataLen;
  }

  /**
   * Returns the number of rows stored in this table.
   */
  public int getNumRows() {
    return numRows;
  }

  public int getOffset() {
    return offset;
  }

  public boolean hasValidityBuffer(int columnIndex) {
    int pos = columnIndex / 8;
    int bit = columnIndex % 8;
    return (hasValidityBuffer[pos] & (1 << bit)) != 0;
  }

  byte[] getHasValidityBuffer() {
    return hasValidityBuffer;
  }

  /**
   * Get the size of the serialized header.
   *
   * <p>
   * It consists of the following fields:
   * <ol>
   *   <li>Magic Number</li>
   *   <li>Row Offset</li>
   *   <li>Number of rows</li>
   *   <li>Validity buffer length</li>
   *   <li>Offset buffer length</li>
   *   <li>Total data length</li>
   *   <li>Number of columns</li>
   *   <li>hasValidityBuffer</li>
   * </ol>
   * <p>
   * For more details of each field, please refer to {@link KudoSerializer}.
   * <p/>
   *
   * @return the size of the serialized header.
   */
  public int getSerializedSize() {
    return 7 * Integer.BYTES + hasValidityBuffer.length;
  }

  public int getNumColumns() {
    return numColumns;
  }

  public int getValidityBufferLen() {
    return validityBufferLen;
  }

  public int getOffsetBufferLen() {
    return offsetBufferLen;
  }

    public int startOffsetOf(BufferType bufferType) {
        switch (bufferType) {
            case VALIDITY:
                return 0;
            case OFFSET:
                return validityBufferLen;
            case DATA:
                return validityBufferLen + offsetBufferLen;
            default:
                throw new IllegalArgumentException("Unsupported buffer type: " + bufferType);
        }
    }

  public void writeTo(DataWriter dout) throws IOException {
    // Now write out the data
    dout.writeInt(SER_FORMAT_MAGIC_NUMBER);

    dout.writeInt(offset);
    dout.writeInt(numRows);
    dout.writeInt(validityBufferLen);
    dout.writeInt(offsetBufferLen);
    dout.writeInt(totalDataLen);
    dout.writeInt(numColumns);
    dout.write(hasValidityBuffer, 0, hasValidityBuffer.length);
  }

  @Override
  public String toString() {
    return "SerializedTableHeader{" +
        "offset=" + offset +
        ", numRows=" + numRows +
        ", validityBufferLen=" + validityBufferLen +
        ", offsetBufferLen=" + offsetBufferLen +
        ", totalDataLen=" + totalDataLen +
        ", numColumns=" + numColumns +
        ", hasValidityBuffer=" + Arrays.toString(hasValidityBuffer) +
        '}';
  }

  static int lengthOfHasValidityBuffer(int numColumns) {
    return (numColumns + 7) / 8;
  }
}
