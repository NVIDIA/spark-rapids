
package org.apache.spark.rapids.api.python

import java.io.DataInputStream
import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean

import com.nvidia.spark.rapids.Arm

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.BasePythonRunner
import org.apache.spark.sql.vectorized.ColumnarBatch

// pid is not a constructor argument in 30x and 31x
abstract class ShimBasePythonRunner[IN, OUT](
    funcs : scala.Seq[org.apache.spark.api.python.ChainedPythonFunctions],
    evalType : scala.Int, argOffsets : scala.Array[scala.Array[scala.Int]]
) extends BasePythonRunner[ColumnarBatch, ColumnarBatch](funcs, evalType, argOffsets)
    with Arm {
  protected abstract class ShimReaderIterator(
    stream: DataInputStream,
    writerThread: WriterThread,
    startTime: Long,
    env: SparkEnv,
    worker: Socket,
    pid: Option[Int],
    releasedOrClosed: AtomicBoolean,
    context: TaskContext
  ) extends ReaderIterator(stream, writerThread, startTime, env, worker, releasedOrClosed, context)
}
