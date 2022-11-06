
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

import org.apache.orc.impl.OutStream
import org.apache.orc.protobuf.{AbstractMessage, CodedOutputStream}

class OrcProtoWriterShim(orcOutStream: OutStream) {
  val proxied = CodedOutputStream.newInstance(orcOutStream)
  def writeAndFlush(obj: Any): Unit = obj match {
    case m: AbstractMessage =>
      m.writeTo(proxied)
      proxied.flush()
      orcOutStream.flush()
    case _ =>
      require(obj.isInstanceOf[AbstractMessage],
        s"Unexpected protobuf message type: $obj")
  }
}

object OrcProtoWriterShim {
  def apply(orcOutStream: OutStream) = {
    new OrcProtoWriterShim(orcOutStream)
  }
}
