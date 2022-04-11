/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import java.nio.ByteBuffer

import scala.collection.mutable

import org.apache.arrow.memory.ReferenceManager
import org.apache.arrow.vector.ValueVector

object ArrowShims {

  def getBufferAndAddReference(getter: => (ByteBuffer, ReferenceManager),
                               referenceManagers: mutable.ListBuffer[ReferenceManager]):
  ByteBuffer = {
    val (buf, ref) = getter
    referenceManagers += ref
    buf
  }

  def getArrowDataBuf(vec: ValueVector): (ByteBuffer, ReferenceManager) = {
    val arrowBuf = vec.getDataBuffer
    (arrowBuf.nioBuffer(), arrowBuf.getReferenceManager)
  }

  def getArrowValidityBuf(vec: ValueVector): (ByteBuffer, ReferenceManager) = {
    val arrowBuf = vec.getValidityBuffer
    (arrowBuf.nioBuffer(), arrowBuf.getReferenceManager)
  }

  def getArrowOffsetsBuf(vec: ValueVector): (ByteBuffer, ReferenceManager) = {
    val arrowBuf = vec.getOffsetBuffer
    (arrowBuf.nioBuffer(), arrowBuf.getReferenceManager)
  }

}
