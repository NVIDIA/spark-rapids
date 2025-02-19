/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.jni.Arms;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static com.nvidia.spark.rapids.kudo.KudoSerializer.readerFrom;
import static java.util.Objects.requireNonNull;

/**
 * Serialized table in kudo format, including a {{@link KudoTableHeader}} and a {@link HostMemoryBuffer} for serialized
 * data.
 */
public class KudoTable implements AutoCloseable {
  private final KudoTableHeader header;
  private final HostMemoryBuffer buffer;

  /**
   * Create a kudo table.
   *
   * @param header kudo table header
   * @param buffer host memory buffer for the table data. KudoTable will take ownership of this buffer, so don't close
   *               it after passing it to this constructor.
   */
  public KudoTable(KudoTableHeader header, HostMemoryBuffer buffer) {
    requireNonNull(header, "Header must not be null");
    this.header = header;
    this.buffer = buffer;
  }

  /**
   * Read a kudo table from an input stream.
   *
   * @param in input stream
   * @return the kudo table, or empty if the input stream is empty.
   * @throws IOException if an I/O error occurs
   */
  public static Optional<KudoTable> from(InputStream in) throws IOException {
    requireNonNull(in, "Input stream must not be null");

    DataInputStream din = readerFrom(in);
    return KudoTableHeader.readFrom(din).map(header -> {
      // Header only
      if (header.getNumColumns() == 0) {
        return new KudoTable(header, null);
      }

      return Arms.closeIfException(HostMemoryBuffer.allocate(header.getTotalDataLen(), false), buffer -> {
        try {
          buffer.copyFromStream(0, din, header.getTotalDataLen());
          return new KudoTable(header, buffer);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    });
  }

  public KudoTableHeader getHeader() {
    return header;
  }

  public HostMemoryBuffer getBuffer() {
    return buffer;
  }

  @Override
  public String toString() {
    return "SerializedTable{" +
        "header=" + header +
        ", buffer=" + buffer +
        '}';
  }

  @Override
  public void close() throws Exception {
    if (buffer != null) {
      buffer.close();
    }
  }
}
