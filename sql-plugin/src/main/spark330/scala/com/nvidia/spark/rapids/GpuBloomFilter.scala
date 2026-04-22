/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "411"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import ai.rapids.cudf.{BaseDeviceMemoryBuffer, ColumnVector, Cuda, DeviceMemoryBuffer, DType,
  HostMemoryBuffer}
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
  // Spark serializes bloom filters in one of two formats. All values are big-endian.
  //
  // V1 (12-byte header):
  //   Byte Offset  Size  Description
  //   0            4     Version ID (1)
  //   4            4     Number of hash functions
  //   8            4     Number of longs, N
  //   12           N*8   Bloom filter data buffer as longs
  //
  // V2 (16-byte header):
  //   Byte Offset  Size  Description
  //   0            4     Version ID (2)
  //   4            4     Number of hash functions
  //   8            4     Hash seed
  //   12           4     Number of longs, N
  //   16           N*8   Bloom filter data buffer as longs
  private val HEADER_SIZE_V1 = 12
  private val HEADER_SIZE_V2 = 16

  private def readVersionFromDevice(data: BaseDeviceMemoryBuffer): Int = {
    withResource(data.sliceWithCopy(0, 4)) { versionSlice =>
      withResource(HostMemoryBuffer.allocate(4)) { hostBuf =>
        hostBuf.copyFromDeviceBuffer(versionSlice)
        Integer.reverseBytes(hostBuf.getInt(0))
      }
    }
  }

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
    val totalLen = data.getLength
    require(totalLen >= HEADER_SIZE_V1, s"buffer too small: $totalLen")

    val version = readVersionFromDevice(data)
    val headerSize = version match {
      case 1 => HEADER_SIZE_V1
      case 2 => HEADER_SIZE_V2
      case _ => throw new IllegalArgumentException(
        s"Unknown bloom filter version: $version")
    }
    require(totalLen >= headerSize,
      s"buffer too small for bloom filter V$version: $totalLen")
    val bitBufferLen = totalLen - headerSize
    require(bitBufferLen % 8 == 0,
      s"bit buffer length ($bitBufferLen) not a multiple of 8")

    val filterBuffer = DeviceMemoryBuffer.allocate(totalLen)
    closeOnExcept(filterBuffer) { buf =>
      buf.copyFromDeviceBufferAsync(0, data, 0, buf.getLength, Cuda.DEFAULT_STREAM)
    }
    new GpuBloomFilter(filterBuffer)
  }
}
