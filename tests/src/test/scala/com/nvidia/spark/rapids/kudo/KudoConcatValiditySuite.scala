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

package com.nvidia.spark.rapids.kudo

import java.util.Random

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import java.util
import org.scalatest.funsuite.AnyFunSuite

class KudoConcatValiditySuite extends AnyFunSuite {
  private val SEED = 7863832

  private def getRandom: Random = new Random(SEED)

  private def fillValidityBuffer(startRow: Int, values: Array[Boolean]): HostMemoryBuffer = {
    val sliceInfo = new SliceInfo(startRow, values.length)
    val bufferSize =
      KudoSerializer.padForHostAlignment(sliceInfo.getValidityBufferInfo.getBufferLength).toInt
    val startBit = sliceInfo.getValidityBufferInfo.getBeginBit
    closeOnExcept(HostMemoryBuffer.allocate(bufferSize)) { buffer => {
      for (i <- 0 until bufferSize) {
        buffer.setByte(i, 0x00.toByte)
      }
      for (i <- values.indices) {
        if (values(i)) {
          val index = startBit + i
          val byteIdx = index / 8
          val bitIdx = index % 8
          var b = buffer.getByte(byteIdx)
          b = (b | (1 << bitIdx)).toByte
          buffer.setByte(byteIdx, b)
        }
      }
      buffer
    }
    }
  }

  private def getValidityBuffer(buffer: HostMemoryBuffer, len: Int): Array[Boolean] = {
    val arr = new Array[Boolean](len)
    for (i <- 0 until len) {
      val byteIdx = i / 8
      val bitIdx = i % 8
      arr(i) = (buffer.getByte(byteIdx) & (1 << bitIdx) & 0xFF) != 0
    }
    arr
  }

  // When srcBitIdx < destBitIdx, srcIntBufLen = 1
  test("testConcatValidityCase1") {
    val random = getRandom
    var accuArrLen = 0
    val dest = HostMemoryBuffer.allocate(4096)
    try {
      val merger = new KudoTableMerger.ValidityBufferMerger(
        dest, 0, new Array[Int](64), new Array[Int](64))
      // When srcBitIdx == destBitIdx
      val arr1 = ValidityConcatArray.create(0, 29, random, "Array 1", accuArrLen)
      arr1.appendToDest(merger)
      accuArrLen += arr1.array.length

      // Now destBitIdx = 29

      // srcBitIdx < destBitIdx, srcIntBufLen = 1
      val arr2 = ValidityConcatArray.create(7, 27, random, "Array 2", accuArrLen)
      arr2.appendToDest(merger)
      accuArrLen += arr2.array.length

      val result = getValidityBuffer(dest, accuArrLen)
      arr1.verifyData(result)
      arr2.verifyData(result)
    } finally {
      dest.close()
    }
  }

  // When srcBitIdx < destBitIdx, srcIntBufLen > 1, last leftRowCount < 0
  test("testConcatValidityCase2") {
    val random = getRandom
    var accuArrLen = 0
    val dest = HostMemoryBuffer.allocate(4096)
    try {
      val merger = new KudoTableMerger.ValidityBufferMerger(
        dest, 0, new Array[Int](64), new Array[Int](64))
      // When srcBitIdx == destBitIdx
      val arr1 = ValidityConcatArray.create(0, 29, random, "Array 1", accuArrLen)
      arr1.appendToDest(merger)
      accuArrLen += arr1.array.length

      // Now destBitIdx = 29

      // srcBitIdx < destBitIdx, srcIntBufLen > 1
      val arr2 = ValidityConcatArray.create(7, 127, random, "Array 2", accuArrLen)
      arr2.appendToDest(merger)
      accuArrLen += arr2.array.length

      val result = getValidityBuffer(dest, accuArrLen)
      arr1.verifyData(result)
      arr2.verifyData(result)
    } finally {
      dest.close()
    }
  }

  // When srcBitIdx < destBitIdx, srcIntBufLen > 1, last leftRowCount > 0
  test("testConcatValidityCase3") {
    val random = getRandom
    var accuArrLen = 0
    val dest = HostMemoryBuffer.allocate(4096)
    try {
      val merger = new KudoTableMerger.ValidityBufferMerger(
        dest, 0, new Array[Int](64), new Array[Int](64))
      // When srcBitIdx == destBitIdx
      val arr1 = ValidityConcatArray.create(0, 29, random, "Array 1", accuArrLen)
      arr1.appendToDest(merger)
      accuArrLen += arr1.array.length

      // Now destBitIdx = 29

      // srcBitIdx < destBitIdx, srcIntBufLen > 1
      val arr2 = ValidityConcatArray.create(7, 133, random, "Array 2", accuArrLen)
      arr2.appendToDest(merger)
      accuArrLen += arr2.array.length

      val result = getValidityBuffer(dest, accuArrLen)
      arr1.verifyData(result)
      arr2.verifyData(result)
    } finally {
      dest.close()
    }
  }

  // When srcBitIdx == destBitIdx, srcIntBufLen == 1
  test("testConcatValidityCase4") {

    val random = getRandom
    var accuArrLen = 0
    val dest = HostMemoryBuffer.allocate(4096)
    try {
      val merger = new KudoTableMerger.ValidityBufferMerger(
        dest, 0, new Array[Int](64), new Array[Int](64))
      // When srcBitIdx == destBitIdx
      val arr1 = ValidityConcatArray.create(0, 29, random, "Array 1", accuArrLen)
      arr1.appendToDest(merger)
      accuArrLen += arr1.array.length

      val result = getValidityBuffer(dest, accuArrLen)
      arr1.verifyData(result)
    } finally {
      dest.close()
    }
  }

  test("testConcatValidityCase5") {

    val random = getRandom
    var accuArrLen = 0
    val dest = HostMemoryBuffer.allocate(4096)
    try {
      val merger = new KudoTableMerger.ValidityBufferMerger(
        dest, 0, new Array[Int](64), new Array[Int](64))
      // When srcBitIdx == destBitIdx
      val arr1 = ValidityConcatArray.create(0, 29, random, "Array 1", accuArrLen)
      arr1.appendToDest(merger)
      accuArrLen += arr1.array.length

      // destBitIdx = 29
      val arr2 = ValidityConcatArray.create(29, 105, random, "Array 2", accuArrLen)
      arr2.appendToDest(merger)
      accuArrLen += arr2.array.length

      val result = getValidityBuffer(dest, accuArrLen)
      arr1.verifyData(result)
      arr2.verifyData(result)
    } finally {
      dest.close()
    }
  }

  // When srcBitIdx > destBitIdx, srcIntBufLen = 1
  test("testConcatValidityCase6") {

    val random = getRandom
    var accuArrLen = 0
    val dest = HostMemoryBuffer.allocate(4096)
    try {
      val merger = new KudoTableMerger.ValidityBufferMerger(
        dest, 0, new Array[Int](64), new Array[Int](64))
      // When srcBitIdx == destBitIdx
      val arr1 = ValidityConcatArray.create(0, 14, random, "Array 1", accuArrLen)
      arr1.appendToDest(merger)
      accuArrLen += arr1.array.length

      // destBitIdx = 14
      val arr2 = ValidityConcatArray.create(17, 9, random, "Array 2", accuArrLen)
      arr2.appendToDest(merger)
      accuArrLen += arr2.array.length

      val result = getValidityBuffer(dest, accuArrLen)
      arr1.verifyData(result)
      arr2.verifyData(result)
    } finally {
      dest.close()
    }
  }

  // When srcBitIdx > destBitIdx, srcIntBufLen > 1, last leftRowCount > 0
  test("testConcatValidityCase7") {

    val random = getRandom
    var accuArrLen = 0
    val dest = HostMemoryBuffer.allocate(4096)
    try {
      val merger = new KudoTableMerger.ValidityBufferMerger(
        dest, 0, new Array[Int](64), new Array[Int](64))
      // When srcBitIdx == destBitIdx
      val arr1 = ValidityConcatArray.create(0, 14, random, "Array 1", accuArrLen)
      arr1.appendToDest(merger)
      accuArrLen += arr1.array.length

      // destBitIdx = 14
      val arr2 = ValidityConcatArray.create(17, 87, random, "Array 2", accuArrLen)
      arr2.appendToDest(merger)
      accuArrLen += arr2.array.length

      val result = getValidityBuffer(dest, accuArrLen)
      arr1.verifyData(result)
      arr2.verifyData(result)
    } finally {
      dest.close()
    }
  }

  // When srcBitIdx > destBitIdx, srcIntBufLen > 1, last leftRowCount < 0
  test("testConcatValidityCase8") {

    val random = getRandom
    var accuArrLen = 0
    val dest = HostMemoryBuffer.allocate(4096)
    try {
      val merger = new KudoTableMerger.ValidityBufferMerger(
        dest, 0, new Array[Int](64), new Array[Int](64))
      // When srcBitIdx == destBitIdx
      val arr1 = ValidityConcatArray.create(0, 8, random, "Array 1", accuArrLen)
      arr1.appendToDest(merger)
      accuArrLen += arr1.array.length

      // destBitIdx = 8
      val arr2 = ValidityConcatArray.create(12, 85, random, "Array 2", accuArrLen)
      arr2.appendToDest(merger)
      accuArrLen += arr2.array.length

      val result = getValidityBuffer(dest, accuArrLen)
      arr1.verifyData(result)
      arr2.verifyData(result)
    } finally {
      dest.close()
    }
  }

  test("testConcatValidity") {

    val random = getRandom
    var accuArrLen = 0
    val dest = HostMemoryBuffer.allocate(4096)
    try {
      val merger = new KudoTableMerger.ValidityBufferMerger(
        dest, 0, new Array[Int](64), new Array[Int](64))
      // Second case when srcBitIdx > destBitIdx
      val arr1 = ValidityConcatArray.create(3, 129, random, "Array 1", accuArrLen)
      arr1.appendToDest(merger)
      accuArrLen += arr1.array.length

      // Second case when srcBitIdx > destBitIdx
      val arr2 = ValidityConcatArray.create(7, 79, random, "Array 2", accuArrLen)
      arr2.appendToDest(merger)
      accuArrLen += arr2.array.length

      // Append all validity
      val arr3 = ValidityConcatArray.create(-1, 129, null, "Array 3", accuArrLen)
      arr3.appendToDest(merger)
      accuArrLen += arr3.array.length

      // First case when srcBitIdx < destBitIdx
      val arr4 = ValidityConcatArray.create(3, 70, random, "Array 4", accuArrLen)
      arr4.appendToDest(merger)
      accuArrLen += arr4.array.length

      // First case when srcBitIdx < destBitIdx
      val arr5 = ValidityConcatArray.create(3, 62, random, "Array 5", accuArrLen)
      arr5.appendToDest(merger)
      accuArrLen += arr5.array.length

      // Third case when srcBitIdx == destBitIdx
      val arr6 = ValidityConcatArray.create(21, 79, random, "Array 5", accuArrLen)
      arr6.appendToDest(merger)
      accuArrLen += arr6.array.length

      val result = getValidityBuffer(dest, accuArrLen)
      arr1.verifyData(result)
      arr2.verifyData(result)
      arr3.verifyData(result)
      arr4.verifyData(result)
      arr5.verifyData(result)
      arr6.verifyData(result)
    } finally {
      dest.close()
    }
  }

  test("testConcatValidityWithEmpty") {

    val random = getRandom
    var accuArrLen = 0
    val dest = HostMemoryBuffer.allocate(64)
    try {
      val merger = new KudoTableMerger.ValidityBufferMerger(
        dest, 0, new Array[Int](64), new Array[Int](64))

      val arr1 = ValidityConcatArray.create(3, 512, random, "Array 1", accuArrLen)
      arr1.appendToDest(merger)
      accuArrLen += arr1.array.length

      // Should not throw
      merger.appendAllValid(0)

      val src = HostMemoryBuffer.allocate(32)
      try {
        // Should not throw
        merger.copyValidityBuffer(src, 0, new SliceInfo(0, 0))
      } finally {
        src.close()
      }

      val result = getValidityBuffer(dest, accuArrLen)
      arr1.verifyData(result)
    } finally {
      dest.close()
    }
  }

  case class ValidityConcatArray(
      var startRow: Int,
      var nullCount: Int,
      var name: String,
      var array: Array[Boolean],
      var allValid: Boolean,
      var resultStart: Int
  ) {

    // Method to append to the destination
    def appendToDest(merger: KudoTableMerger.ValidityBufferMerger): Unit = {
      assert(
        this.resultStart == merger.getTotalRowCount,
        s"$name result start not match"
      )
      if (allValid) {
        merger.appendAllValid(this.array.length)
      } else {
        // Using a fictional fillValidityBuffer method to handle resource management
        withResource(fillValidityBuffer(this.startRow, array)) { src =>
          val nullCount = merger.copyValidityBuffer(
            src, 0, new SliceInfo(this.startRow, array.length))
          assert(
            this.nullCount == nullCount,
            s"$name null count not match"
          )
        }
      }
    }

    // Method to verify data
    def verifyData(result: Array[Boolean]): Unit = {
      for (i <- array.indices) {
        val index = i
        assert(
          this.array(i) == result(this.resultStart + i),
          s"$name index $index value not match"
        )
      }
    }
  }

  object ValidityConcatArray {


    def create(startRow: Int, numRow: Int, random: Random, name: String, resultStart: Int)
    : ValidityConcatArray = {
      val ret = new ValidityConcatArray(0, 0, "", Array[Boolean](), false, 0)

      ret.startRow = startRow
      ret.array = new Array[Boolean](numRow)
      ret.resultStart = resultStart
      if (random == null) {
        ret.allValid = true
        util.Arrays.fill(ret.array, true)
        ret.nullCount = 0
      }
      else {
        ret.allValid = false
        var nullCount = 0
        for (i <- 0 until numRow) {
          ret.array(i) = random.nextBoolean
          if (!ret.array(i)) nullCount += 1
        }
        ret.nullCount = nullCount
      }

      ret.name = name
      ret
    }
  }
}