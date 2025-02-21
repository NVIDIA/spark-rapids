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

import ai.rapids.cudf.*;
import com.nvidia.spark.rapids.jni.schema.Visitors;


import static java.util.Objects.requireNonNull;

/**
 * The result of merging several kudo tables into one contiguous table on the host.
 */
public class KudoHostMergeResult implements AutoCloseable {
  private final Schema schema;
  private final ColumnViewInfo[] columnInfoList;
  private HostMemoryBuffer hostBuf;

  KudoHostMergeResult(Schema schema, HostMemoryBuffer hostBuf, ColumnViewInfo[] columnInfoList) {
    requireNonNull(schema, "schema is null");
    requireNonNull(columnInfoList, "columnInfoList is null");
    assert schema.getFlattenedColumnNames().length == columnInfoList.length :
        "Column offsets size does not match flattened schema size, column offsets size: " +
                columnInfoList.length + ", flattened schema size: " +
                schema.getFlattenedColumnNames().length;
    this.schema = schema;
    this.columnInfoList = columnInfoList;
    this.hostBuf = requireNonNull(hostBuf, "hostBuf is null");
  }

  @Override
  public void close() throws Exception {
    hostBuf.close();
    hostBuf = null;
  }

  /**
   * Get the length of the data in the host buffer.
   * @return the length of the data in the host buffer
   */
  public long getDataLength() {
    return hostBuf.getLength();
  }

  /**
   * Convert the host buffer into a cudf table.
   * @return the cudf table
   */
  public Table toTable() {
    try (DeviceMemoryBuffer deviceMemBuf = DeviceMemoryBuffer.allocate(hostBuf.getLength())) {
      if (hostBuf.getLength() > 0) {
        deviceMemBuf.copyFromHostBufferAsync(hostBuf, Cuda.DEFAULT_STREAM);
      }

      try (TableBuilder builder = new TableBuilder(columnInfoList, deviceMemBuf)) {
        Table t = Visitors.visitSchema(schema, builder);

        Cuda.DEFAULT_STREAM.sync();
        return t;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public String toString() {
    return "HostMergeResult{" +
        "columnOffsets=" + columnInfoList +
        ", hostBuf length =" + hostBuf.getLength() +
        '}';
  }
}
