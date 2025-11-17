/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{DType, HostMemoryBuffer}
import ai.rapids.cudf.HostColumnVector.{BasicType, ListType}
import com.nvidia.spark.rapids.jni.{CpuRetryOOM, RmmSpark}

class RapidsHostColumnBuilderSuite extends RmmSparkRetrySuiteBase {
  test("growing buffer preserves correctness") {
    val b1 = new RapidsHostColumnBuilder(new BasicType(false, DType.INT32), 0) // grows
    val b2 = new RapidsHostColumnBuilder(new BasicType(false, DType.INT32), 8) // does not grow
    for (i <- 0 to 7) {
      b1.append(i)
      b2.append(i)
    }
    val v1 = b1.build()
    val v2 = b2.build()
    for (i <- 0 to 7) {
      assertResult(v1.getInt(i))(v2.getInt(i))
    }
    v1.close()
    v2.close()
    b1.close()
    b2.close()
  }

  private def getLongField(instance: AnyRef, name: String): Long = {
    val field = instance.getClass.getDeclaredField(name)
    field.setAccessible(true)
    field.getLong(instance)
  }

  private def getBufferField(instance: AnyRef, name: String): HostMemoryBuffer = {
    val field = instance.getClass.getDeclaredField(name)
    field.setAccessible(true)
    field.get(instance).asInstanceOf[HostMemoryBuffer]
  }

  test("reserveRows preallocates fixed width buffers") {
    val builder = new RapidsHostColumnBuilder(new BasicType(false, DType.INT32), 0)
    try {
      builder.reserveRows(8)
      val rowCapacity = getLongField(builder, "rowCapacity")
      val rows = getLongField(builder, "rows")
      assert(rowCapacity >= 8)
      assert(rows == 0)
      val data = getBufferField(builder, "data")
      assert(data != null)
      assert(data.getLength >= rowCapacity * java.lang.Integer.BYTES)
    } finally {
      builder.close()
    }
  }

  test("reserveRows preallocates offsets for strings") {
    val builder = new RapidsHostColumnBuilder(new BasicType(false, DType.STRING), 0)
    try {
      builder.reserveRows(4)
      val rowCapacity = getLongField(builder, "rowCapacity")
      val offsets = getBufferField(builder, "offsets")
      val data = getBufferField(builder, "data")
      assert(rowCapacity >= 4)
      assert(offsets != null)
      assert(offsets.getLength >= (rowCapacity + 1L) * java.lang.Integer.BYTES)
      assert(data != null)
      assert(data.getLength >= 1) // std::string builder keeps at least byte
    } finally {
      builder.close()
    }
  }

  test("reserveRows cascades to child builders for lists") {
    val listType = new ListType(false, new BasicType(false, DType.INT32))
    val builder = new RapidsHostColumnBuilder(listType, 0)
    try {
      builder.reserveRows(5)
      val rowCapacity = getLongField(builder, "rowCapacity")
      val offsets = getBufferField(builder, "offsets")
      assert(rowCapacity >= 5)
      assert(offsets != null)
      val child = builder.getChild(0)
      val childCapacity = getLongField(child, "rowCapacity")
      assert(childCapacity >= 5)
    } finally {
      builder.close()
    }
  }

  test("reserveRows insufficient triggers CPU OOM on growth") {
    val builder = new RapidsHostColumnBuilder(new BasicType(false, DType.INT32), 0)
    try {
      builder.reserveRows(2)
      builder.append(0)
      builder.append(1)
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.CPU.ordinal, 0)
      assertThrows[CpuRetryOOM] {
        builder.append(2)
      }
    } finally {
      builder.close()
    }
  }
}
