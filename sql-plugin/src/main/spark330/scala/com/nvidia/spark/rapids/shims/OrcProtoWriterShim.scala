/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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
spark-rapids-shim-json-lines ***/
// Keep executable line numbers aligned with newer shims for binary-dedupe.














package com.nvidia.spark.rapids.shims

import java.io.OutputStream
import java.lang.reflect.Method

import org.apache.orc.impl.OutStream

class OrcProtoWriterShim(orcOutStream: OutStream) {
  import OrcProtoWriterShim.ProtoApi

  private[this] var proxiedApi: ProtoApi = _
  private[this] var proxied: AnyRef = _

  private def proxiedFor(api: ProtoApi): AnyRef = {
    if (proxiedApi != api) {
      proxiedApi = api
      proxied = api.newInstance.invoke(null, orcOutStream.asInstanceOf[OutputStream])
    }
    proxied
  }

  def writeAndFlush(obj: Any): Unit = {
    val api = OrcProtoWriterShim.apiFor(obj).getOrElse {
      throw new IllegalArgumentException(
        s"requirement failed: Unexpected protobuf message type: $obj")
    }
    val currentProxied = proxiedFor(api)
    api.writeTo.invoke(obj.asInstanceOf[AnyRef], currentProxied)
    api.flush.invoke(currentProxied)
    orcOutStream.flush()
  }
}

object OrcProtoWriterShim {
  private case class ProtoApi(
      messageClass: Class[_],
      newInstance: Method,
      writeTo: Method,
      flush: Method)

  private val protoClassNames = Seq(
    ("org.apache.orc.protobuf.AbstractMessage",
      "org.apache.orc.protobuf.CodedOutputStream"),
    ("com.google.protobuf.AbstractMessage",
      "com.google.protobuf.CodedOutputStream"))

  private lazy val protoApis: Seq[ProtoApi] = protoClassNames.flatMap { case (msg, out) =>
    try {
      val messageClass = Class.forName(msg)
      val codedOutputStreamClass = Class.forName(out)
      Some(ProtoApi(
        messageClass,
        codedOutputStreamClass.getMethod("newInstance", classOf[OutputStream]),
        messageClass.getMethod("writeTo", codedOutputStreamClass),
        codedOutputStreamClass.getMethod("flush")))
    } catch {
      case _: ReflectiveOperationException => None
    }
  }

  private def apiFor(obj: Any): Option[ProtoApi] = {
    protoApis.find(_.messageClass.isInstance(obj))
  }

  def apply(orcOutStream: OutStream) = {
    new OrcProtoWriterShim(orcOutStream)
  }
}
