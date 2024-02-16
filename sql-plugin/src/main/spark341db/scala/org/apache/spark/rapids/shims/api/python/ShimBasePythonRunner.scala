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
{"spark": "341db"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.rapids.shims.api.python

import java.io.DataInputStream
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{BasePythonRunner, PythonWorker}

abstract class ShimBasePythonRunner[IN, OUT](
    funcs : scala.Seq[org.apache.spark.api.python.ChainedPythonFunctions],
    evalType : scala.Int, argOffsets : scala.Array[scala.Array[scala.Int]]
) extends BasePythonRunner[IN, OUT](funcs, evalType, argOffsets, None) {
  protected abstract class ShimReaderIterator(
    stream: DataInputStream,
    writer: Writer,
    startTime: Long,
    env: SparkEnv,
    worker: PythonWorker,
    pid: Option[Int],
    releasedOrClosed: AtomicBoolean,
    context: TaskContext
  ) extends ReaderIterator(stream, writer, startTime, env, worker, pid,
    releasedOrClosed, context)
}
