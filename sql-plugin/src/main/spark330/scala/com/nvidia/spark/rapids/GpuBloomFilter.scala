/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332cdh"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "350"}
{"spark": "351"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import ai.rapids.cudf.{BaseDeviceMemoryBuffer, ColumnVector, Cuda, DeviceMemoryBuffer, DType}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.jni.BloomFilter

import org.apache.spark.sql.types.{BinaryType, NullType}

/**
 * GPU version of Spark's BloomFilterImpl.
 * @param buffer device buffer containing the Bloom filter data in the Spark Bloom filter
 *               serialization format, including the header. The buffer will be closed by
 *               this GpuBloomFilter instance.
 */
class GpuBloomFilter(buffer: DeviceMemoryBuffer) extends AutoCloseable {
  private val spillableBuffer = SpillableBuffer(buffer, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)

  /**
   * Given an input column of longs, return a boolean column with the same row count where each
   * output row indicates whether the corresponding input row may have been placed into this
   * Bloom filter. A false value indicates definitively that the value was not placed in the filter.
   */
  def mightContainLong(col: ColumnVector): ColumnVector = {
    require(col.getType == DType.INT64, s"expected longs, got ${col.getType}")
    withResource(spillableBuffer.getDeviceBuffer()) { buffer =>
      BloomFilter.probe(buffer, col)
    }
  }

  override def close(): Unit = {
    spillableBuffer.close()
  }
}

object GpuBloomFilter {
  // Spark serializes their bloom filters in a specific format, see BloomFilterImpl.readFrom.
  // Data is written via DataOutputStream, so everything is big-endian.
  // Byte Offset  Size  Description
  // 0            4     Version ID (see Spark's BloomFilter.Version)
  // 4            4     Number of hash functions
  // 8            4     Number of longs, N
  // 12           N*8   Bloom filter data buffer as longs
  private val HEADER_SIZE = 12

  def apply(s: GpuScalar): GpuBloomFilter = {
    s.dataType match {
      case BinaryType if s.isValid =>
        withResource(s.getBase.getListAsColumnView) { childView =>
          require(childView.getType == DType.UINT8, s"expected UINT8 got ${childView.getType}")
          deserialize(childView.getData)
        }
      case BinaryType | NullType => null
      case t => throw new IllegalArgumentException(s"Expected binary or null scalar, found $t")
    }
  }

  def deserialize(data: BaseDeviceMemoryBuffer): GpuBloomFilter = {
    // Sanity check bloom filter header
    val totalLen = data.getLength
    val bitBufferLen = totalLen - HEADER_SIZE
    require(totalLen >= HEADER_SIZE, s"header size is $totalLen")
    require(bitBufferLen % 8 == 0, "buffer length not a multiple of 8")
    val filterBuffer = DeviceMemoryBuffer.allocate(totalLen)
    closeOnExcept(filterBuffer) { buf =>
      buf.copyFromDeviceBufferAsync(0, data, 0, buf.getLength, Cuda.DEFAULT_STREAM)
    }
    new GpuBloomFilter(filterBuffer)
  }
}
