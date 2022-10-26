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

import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex

object FileIndexOptionsShims {
  val BASE_PATH_PARAM = PartitioningAwareFileIndex.BASE_PATH_PARAM
}

class CodedOutputStreamShim(x: org.apache.orc.impl.OutStream) {
  val realCodedOutputStream = com.google.protobuf.CodedOutputStream.newInstance(x)
  def writeAndFlush(obj: Any) = {
    obj.asInstanceOf[com.google.protobuf.AbstractMessage].writeTo(realCodedOutputStream)
    realCodedOutputStream.flush()
    x.flush()
  }
}

object CodedOutputStreamShim {
  def apply(x: org.apache.orc.impl.OutStream) = {
    new CodedOutputStreamShim(x)
  }
}
