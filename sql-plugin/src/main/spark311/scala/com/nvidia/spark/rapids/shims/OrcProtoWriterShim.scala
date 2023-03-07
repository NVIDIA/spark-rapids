/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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
{"spark": "311"}
{"spark": "312"}
{"spark": "313"}
{"spark": "314"}
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "321db"}
{"spark": "322"}
{"spark": "323"}
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.google.protobuf.{AbstractMessage, CodedOutputStream}
import org.apache.orc.impl.OutStream

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
