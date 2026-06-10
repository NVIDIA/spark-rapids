/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids;

import java.util.Objects;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.WriteTaskStats;

import scala.collection.Seq;

/**
 * Simple metrics collected during an instance of GpuFileFormatDataWriter.
 * These were first introduced in https://github.com/apache/spark/pull/18159 (SPARK-20703).
 */
public final class BasicColumnarWriteTaskStats implements WriteTaskStats {
  private static final long serialVersionUID = 0L;

  private final Seq<InternalRow> partitions;
  private final int numFiles;
  private final int numWriters;
  private final long numBytes;
  private final long numRows;

  public BasicColumnarWriteTaskStats(
      Seq<InternalRow> partitions,
      int numFiles,
      int numWriters,
      long numBytes,
      long numRows) {
    this.partitions = partitions;
    this.numFiles = numFiles;
    this.numWriters = numWriters;
    this.numBytes = numBytes;
    this.numRows = numRows;
  }

  public Seq<InternalRow> partitions() {
    return partitions;
  }

  public int numFiles() {
    return numFiles;
  }

  public int numWriters() {
    return numWriters;
  }

  public long numBytes() {
    return numBytes;
  }

  public long numRows() {
    return numRows;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof BasicColumnarWriteTaskStats)) {
      return false;
    }
    BasicColumnarWriteTaskStats that = (BasicColumnarWriteTaskStats) other;
    return numFiles == that.numFiles
        && numWriters == that.numWriters
        && numBytes == that.numBytes
        && numRows == that.numRows
        && Objects.equals(partitions, that.partitions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitions, numFiles, numWriters, numBytes, numRows);
  }

  @Override
  public String toString() {
    return "BasicColumnarWriteTaskStats(" + partitions + "," + numFiles + ","
        + numWriters + "," + numBytes + "," + numRows + ")";
  }
}
