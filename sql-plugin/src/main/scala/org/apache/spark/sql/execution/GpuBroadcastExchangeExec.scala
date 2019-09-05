/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

package org.apache.spark.sql.execution

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.util.concurrent._
import java.util.UUID

import scala.concurrent.{ExecutionContext, Promise}
import scala.util.control.NonFatal

import ai.rapids.cudf.{JCudfSerialization, Table}
import ai.rapids.spark.{ConcatAndConsumeAll, GpuColumnVector, GpuExec}
import ai.rapids.spark.RapidsPluginImplicits._
import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.apache.spark.{broadcast, SparkException}
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, BroadcastPartitioning, Partitioning}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.vectorized.ColumnarBatch

@SerialVersionUID(100L)
class SerializableGpuColumnarBatch(var batch: ColumnarBatch, val closeAfterSerialize: Boolean)
  extends Serializable with AutoCloseable {

  @transient private var columns = GpuColumnVector.extractBases(batch)

  // Don't want them to be closed before we get a chance to read them
  incRefCount()
  if (!closeAfterSerialize) {
    // We are going to leak because we will not close the batch after serializeing, so don't
    // warn about it.
    columns.foreach(_.noWarnLeakExpected())
  }

  def incRefCount(): SerializableGpuColumnarBatch = {
    columns.foreach(_.incRefCount())
    this
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    columns.foreach(_.ensureOnHost())
    JCudfSerialization.writeToStream(columns, out, 0, batch.numRows)
    // In some cases, like an RDD, we want to close the batch once it is serialized out or we will
    // leak GPU memory (technically it will just wait for GC to release it and probably not a lot
    // because this is used for a broadcast that really should be small)
    // In the case of broadcast the life cycle of the object is tied to GC and there is no clean
    // way to separate the two right now.  So we accept the leak.
    if (closeAfterSerialize) {
      batch.close()
      batch = null
      columns = null
    }
  }

  private def readObject(in: ObjectInputStream): Unit = {
    val table = JCudfSerialization.readTableFrom(in)
    try {
      this.batch = GpuColumnVector.from(table)
      columns = GpuColumnVector.extractBases(batch)
    } finally {
      table.close()
    }
  }

  def ensureOnHost(): Unit = {
    (0 until batch.numCols()).foreach {
      batch.column(_).asInstanceOf[GpuColumnVector].getBase.ensureOnHost()
    }
  }

  def dataSize: Long = {
    JCudfSerialization.getSerializedSizeInBytes(columns, 0, batch.numRows())
  }

  override def close(): Unit = {
    columns.safeClose()
  }
}

case class GpuBroadcastExchangeExec(
    mode: BroadcastMode,
    child: SparkPlan) extends Exchange with GpuExec {

  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "collectTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to collect"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build"),
    "broadcastTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to broadcast"))

  override def outputPartitioning: Partitioning = BroadcastPartitioning(mode)

  override def doCanonicalize(): SparkPlan = {
    GpuBroadcastExchangeExec(mode.canonicalized, child.canonicalized)
  }

  @transient
  private lazy val promise = Promise[broadcast.Broadcast[Any]]()

  /**
   * For registering callbacks on `relationFuture`.
   * Note that calling this field will not start the execution of broadcast job.
   */
  @transient
  lazy val completionFuture: scala.concurrent.Future[broadcast.Broadcast[Any]] = promise.future

  @transient
  private val timeout: Long = SQLConf.get.broadcastTimeout

  val runId: UUID = UUID.randomUUID

  @transient
  lazy val relationFuture: Future[broadcast.Broadcast[Any]] = {
    // relationFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val task = new Callable[broadcast.Broadcast[Any]]() {
      override def call(): broadcast.Broadcast[Any] = {
        // This will run in another thread. Set the execution id so that we can connect these jobs
        // with the correct execution.
        SQLExecution.withExecutionId(sqlContext.sparkSession, executionId) {
          try {
            // Setup a job group here so later it may get cancelled by groupId if necessary.
            sparkContext.setJobGroup(runId.toString, s"broadcast exchange (runId $runId)",
              interruptOnCancel = true)
            val beforeCollect = System.nanoTime()
            val data = child.executeColumnar().map(cb => try {
              new SerializableGpuColumnarBatch(cb, true)
            } finally {
              cb.close()
            })
            // TODO this requires a GPU on the driver, which we do not want.  We should have a way
            // to deserialize to just host memory.
            val v = data.collect()
            val batch = if (v.length == 1) {
              val b = v(0).batch
              // Wrap it in a new Serializable batch because they are single use
              val ret = new SerializableGpuColumnarBatch(b, false)
              b.close()
              ret
            } else {
              val justBatches = v.map(_.batch).iterator
              val c = ConcatAndConsumeAll(justBatches, output)
              new SerializableGpuColumnarBatch(c, false)
            }

            val numRows = batch.batch.numRows()
            if (numRows >= 512000000) {
              throw new SparkException(
                s"Cannot broadcast the table with 512 million or more rows: $numRows rows")
            }
            val beforeBuild = System.nanoTime()
            longMetric("collectTime") +=
              TimeUnit.NANOSECONDS.toMillis(beforeBuild - beforeCollect)

            //we only support hashjoin so this is a noop val relation = mode.transform(input, Some(numRows))

            batch.ensureOnHost()
            val dataSize = batch.dataSize

            longMetric("dataSize") += dataSize
            if (dataSize >= (8L << 30)) {
              throw new SparkException(
                s"Cannot broadcast the table that is larger than 8GB: ${dataSize >> 30} GB")
            }

            val beforeBroadcast = System.nanoTime()
            longMetric("buildTime") += TimeUnit.NANOSECONDS.toMillis(beforeBroadcast - beforeBuild)

            // Broadcast the relation
            val broadcasted = sparkContext.broadcast(batch.asInstanceOf[Any])
            longMetric("broadcastTime") += TimeUnit.NANOSECONDS.toMillis(
              System.nanoTime() - beforeBroadcast)

            SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
            promise.success(broadcasted)
            broadcasted
          } catch {
            // SPARK-24294: To bypass scala bug: https://github.com/scala/bug/issues/9554, we throw
            // SparkFatalException, which is a subclass of Exception. ThreadUtils.awaitResult
            // will catch this exception and re-throw the wrapped fatal throwable.
            case oe: OutOfMemoryError =>
              val ex = new Exception(
                new OutOfMemoryError("Not enough memory to build and broadcast the table to all " +
                  "worker nodes. As a workaround, you can either disable broadcast by setting " +
                  s"${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1 or increase the spark " +
                  s"driver memory by setting ${SparkLauncher.DRIVER_MEMORY} to a higher value.")
                  .initCause(oe.getCause))
              promise.failure(ex)
              throw ex
            case e if !NonFatal(e) =>
              val ex = new Exception(e)
              promise.failure(ex)
              throw ex
            case e: Throwable =>
              promise.failure(e)
              throw e
          }
        }
      }
    }
    GpuBroadcastExchangeExec.executionContext.submit[broadcast.Broadcast[Any]](task)
  }

  override protected def doPrepare(): Unit = {
    // Materialize the future.
    relationFuture
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "GpuBroadcastExchange does not support the execute() code path.")
  }

  override protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    try {
      relationFuture.get(timeout, TimeUnit.SECONDS).asInstanceOf[broadcast.Broadcast[T]]
    } catch {
      case ex: TimeoutException =>
        logError(s"Could not execute broadcast in $timeout secs.", ex)
        if (!relationFuture.isDone) {
          sparkContext.cancelJobGroup(runId.toString)
          relationFuture.cancel(true)
        }
        throw new SparkException(s"Could not execute broadcast in $timeout secs. " +
          s"You can increase the timeout for broadcasts via ${SQLConf.BROADCAST_TIMEOUT.key} or " +
          s"disable broadcast join by setting ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1",
          ex)
    }
  }

  final def executeColumnarBroadcast[T](): broadcast.Broadcast[T] = {
    if (isCanonicalizedPlan) {
      throw new IllegalStateException("A canonicalized plan is not supposed to be executed.")
    }
    try {
      relationFuture.get(timeout, TimeUnit.SECONDS).asInstanceOf[broadcast.Broadcast[T]]
    } catch {
      case ex: TimeoutException =>
        logError(s"Could not execute broadcast in $timeout secs.", ex)
        if (!relationFuture.isDone) {
          sparkContext.cancelJobGroup(runId.toString)
          relationFuture.cancel(true)
        }
        throw new SparkException(s"Could not execute broadcast in $timeout secs. " +
          s"You can increase the timeout for broadcasts via ${SQLConf.BROADCAST_TIMEOUT.key} or " +
          s"disable broadcast join by setting ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1",
          ex)
    }
  }
}


object GpuBroadcastExchangeExec {
  /**
   * Create a thread factory that names threads with a prefix and also sets the threads to daemon.
   */
  private def namedThreadFactory(prefix: String): ThreadFactory = {
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(prefix + "-%d").build()
  }

  /**
   * Create a cached thread pool whose max number of threads is `maxThreadNumber`. Thread names
   * are formatted as prefix-ID, where ID is a unique, sequentially assigned integer.
   */
  private def newDaemonCachedThreadPool(
      prefix: String, maxThreadNumber: Int, keepAliveSeconds: Int = 60): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    val threadPool = new ThreadPoolExecutor(
      maxThreadNumber, // corePoolSize: the max number of threads to create before queuing the tasks
      maxThreadNumber, // maximumPoolSize: because we use LinkedBlockingDeque, this one is not used
      keepAliveSeconds,
      TimeUnit.SECONDS,
      new LinkedBlockingQueue[Runnable],
      threadFactory)
    threadPool.allowCoreThreadTimeOut(true)
    threadPool
  }

  private val executionContext = ExecutionContext.fromExecutorService(
    newDaemonCachedThreadPool("gpu-broadcast-exchange",
      SQLConf.get.getConf(StaticSQLConf.BROADCAST_EXCHANGE_MAX_THREAD_THRESHOLD)))
}
