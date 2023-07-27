/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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
{"spark": "332db"}
{"spark": "333"}
{"spark": "340"}
{"spark": "341"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import java.io.DataInputStream

import ai.rapids.cudf.{BaseDeviceMemoryBuffer, ColumnVector, Cuda, DeviceMemoryBuffer, DType, HostMemoryBuffer}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.jni.BloomFilter

import org.apache.spark.sql.types.{BinaryType, NullType}

/**
 * GPU version of Spark's BloomFilterImpl.
 * @param numHashes number of hash functions to use in the Bloom filter
 * @param buffer device buffer containing the Bloom filter data in the Spark Bloom filter
 *               serialization format. The device buffer will be closed when this GpuBloomFilter
 *               instance is closed.
 */
class GpuBloomFilter(val numHashes: Int, buffer: DeviceMemoryBuffer) extends AutoCloseable {
  private val spillableBuffer = SpillableBuffer(buffer, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
  private val numFilterBits = buffer.getLength * 8

  /**
   * Given an input column of longs, return a boolean column with the same row count where each
   * output row indicates whether the corresponding input row may have been placed into this
   * Bloom filter. A false value indicates definitively that the value was not placed in the filter.
   */
  def mightContainLong(col: ColumnVector): ColumnVector = {
    require(col.getType == DType.INT64, s"expected longs, got ${col.getType}")
    withResource(spillableBuffer.getDeviceBuffer()) { buffer =>
      BloomFilter.probe(numHashes, numFilterBits, buffer, col)
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

  // version numbers from BloomFilter.Version enum
  private val VERSION_V1 = 1

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
    val bufferLen = totalLen - HEADER_SIZE
    require(totalLen >= HEADER_SIZE, s"header size is $totalLen")
    require(bufferLen % 8 == 0, "buffer length not a multiple of 8")
    val numHashes = withResource(HostMemoryBuffer.allocate(HEADER_SIZE, false)) { hostHeader =>
      hostHeader.copyFromMemoryBuffer(0, data, 0, HEADER_SIZE, Cuda.DEFAULT_STREAM)
      parseHeader(hostHeader, bufferLen)
    }
    // TODO: Can we avoid this copy?  Would either need the ability to release data buffers
    //       from scalars or make scalars spillable.
    val filterBuffer = DeviceMemoryBuffer.allocate(bufferLen)
    closeOnExcept(filterBuffer) { buf =>
      buf.copyFromDeviceBufferAsync(0, data, HEADER_SIZE, buf.getLength, Cuda.DEFAULT_STREAM)
    }
    new GpuBloomFilter(numHashes, filterBuffer)
  }

  /**
   * Parses the Spark Bloom filter serialization header performing sanity checks
   * and retrieving the number of hash functions used for the filter.
   * @param buffer serialized header data
   * @param dataLen size of the serialized Bloom filter data without header
   * @return number of hash functions used in the Bloom filter
   */
  private def parseHeader(buffer: HostMemoryBuffer, dataLen: Long): Int = {
    val in = new DataInputStream(new HostMemoryInputStream(buffer, buffer.getLength))
    val version = in.readInt
    require(version == VERSION_V1, s"unsupported serialization format version $version")
    val numHashes = in.readInt()
    val sizeFromHeader = in.readInt() * 8L
    require(dataLen == sizeFromHeader,
      s"data size from header is $sizeFromHeader, received $dataLen")
    numHashes
  }
}
