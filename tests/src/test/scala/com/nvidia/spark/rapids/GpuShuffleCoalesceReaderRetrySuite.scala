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

package com.nvidia.spark.rapids

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream}

import ai.rapids.cudf.{HostColumnVector, HostMemoryBuffer, JCudfSerialization}
import ai.rapids.cudf.JCudfSerialization.SerializedTableHeader
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.jni.RmmSpark

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuShuffleCoalesceReaderRetrySuite extends RmmSparkRetrySuiteBase {

  private def serializedOneIntColumnBatch(ints: Int*): ColumnarBatch = {
    val outStream = new ByteArrayOutputStream(100)
    withResource(HostColumnVector.fromInts(ints: _*)) { col =>
      JCudfSerialization.writeToStream(Array(col), outStream, 0, col.getRowCount)
    }
    val inStream = new DataInputStream(new ByteArrayInputStream(outStream.toByteArray))
    val header = new SerializedTableHeader(inStream)
    closeOnExcept(HostMemoryBuffer.allocate(header.getDataLen, false)) { hostBuffer =>
      JCudfSerialization.readTableIntoBuffer(inStream, header, hostBuffer)
      SerializedTableColumn.from(header, hostBuffer)
    }
  }

  private def serializedBatches = Seq(
    serializedOneIntColumnBatch(1),
    serializedOneIntColumnBatch(3),
    serializedOneIntColumnBatch(2),
    serializedOneIntColumnBatch(5),
    serializedOneIntColumnBatch(4))

  test("GpuShuffleCoalesceReader split-retry") {
    val iter = closeOnExcept(serializedBatches) { _ =>
      val reader = new GpuShuffleCoalesceReader(
        Iterator(serializedBatches: _*),
        targetBatchSize = 390, // each is 64 due to padding, then total is 320 (=64x5)
        dataTypes = Array(IntegerType),
        metricsMap = Map.empty.withDefaultValue(NoopMetric))
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId)
      reader.asIterator
    }
    withResource(iter.toSeq) { batches =>
      // 2 batches because of the split-retry
      assertResult(expected = 2)(batches.length)
      // still 5 rows
      assertResult(expected = 5)(batches.map(_.numRows()).sum)
    }
  }

}
