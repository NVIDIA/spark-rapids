/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import org.apache.arrow.memory.{ArrowBuf, ReferenceManager}
import org.apache.arrow.vector.ValueVector

object ArrowShims {

  def getBufferAndAddReference(buf: ArrowBuf,
    referenceManagers: mutable.ListBuffer[ReferenceManager]): ByteBuffer = {
    referenceManagers += buf.getReferenceManager
    buf.nioBuffer()
  }

  def getArrowDataBuf(vec: ValueVector): ArrowBuf = {
    vec.getDataBuffer
  }

  def getArrowValidityBuf(vec: ValueVector): ArrowBuf = {
    vec.getValidityBuffer
  }

  def getArrowOffsetsBuf(vec: ValueVector): ArrowBuf = {
    vec.getOffsetBuffer
  }

}
