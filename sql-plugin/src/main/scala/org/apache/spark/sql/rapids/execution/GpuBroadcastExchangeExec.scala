/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.execution

import java.io._
import java.util.UUID
import java.util.concurrent._

import scala.concurrent.{ExecutionContext, Promise}
import scala.util.control.NonFatal

import ai.rapids.cudf.{HostMemoryBuffer, JCudfSerialization, NvtxColor, NvtxRange}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.SparkException
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, BroadcastPartitioning, Partitioning}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

@SerialVersionUID(100L)
class SerializeConcatHostBuffersDeserializeBatch(
    private val data: Array[SerializeBatchDeserializeHostBuffer],
    private val output: Seq[Attribute])
  extends Serializable with Arm with AutoCloseable {
  @transient private val headers = data.map(_.header)
  @transient private val buffers = data.map(_.buffer)
  @transient private var batchInternal: ColumnarBatch = null

  def batch: ColumnarBatch = this.synchronized {
    if (batchInternal == null) {
      // TODO we should come up with a better way for this to happen directly...
      val out = new ByteArrayOutputStream()
      val oout = new ObjectOutputStream(out)
      writeObject(oout)
      val barr = out.toByteArray
      val oin = new ObjectInputStream(new ByteArrayInputStream(barr))
      readObject(oin)
    }
    batchInternal
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    if (headers.length == 0) {
      import scala.collection.JavaConverters._
      // We didn't get any data back, but we need to write out an empty table that matches
      withResource(GpuColumnVector.emptyHostColumns(output.asJava)) { hostVectors =>
        JCudfSerialization.writeToStream(hostVectors, out, 0, 0)
      }
      out.writeObject(output.map(_.dataType).toArray)
    } else if (headers.head.getNumColumns == 0) {
      JCudfSerialization.writeRowsToStream(out, numRows)
    } else {
      JCudfSerialization.writeConcatedStream(headers, buffers, out)
      out.writeObject(output.map(_.dataType).toArray)
    }
  }

  private def readObject(in: ObjectInputStream): Unit = {
    val range = new NvtxRange("DeserializeBatch", NvtxColor.PURPLE)
    try {
      val tableInfo: JCudfSerialization.TableAndRowCountPair =
        JCudfSerialization.readTableFrom(in)
      try {
        val table = tableInfo.getContiguousTable
        if (table == null) {
          val numRows = tableInfo.getNumRows
          this.batchInternal = new ColumnarBatch(new Array[ColumnVector](0), numRows)
        } else {
          val colDataTypes = in.readObject().asInstanceOf[Array[DataType]]
          this.batchInternal = GpuColumnVectorFromBuffer.from(table, colDataTypes)
          GpuColumnVector.extractBases(this.batchInternal).foreach(_.noWarnLeakExpected())
        }
      } finally {
        tableInfo.close()
      }
    } finally {
      range.close()
    }
  }

  def numRows: Int = {
    if (batchInternal != null) {
      batchInternal.numRows()
    } else {
      headers.map(_.getNumRows).sum
    }
  }

  def dataSize: Long = {
    if (batchInternal != null) {
      val bases = GpuColumnVector.extractBases(batchInternal).map(_.copyToHost())
      try {
        JCudfSerialization.getSerializedSizeInBytes(bases, 0, batchInternal.numRows())
      } finally {
        bases.safeClose()
      }
    } else {
      buffers.map(_.getLength).sum
    }
  }

  override def close(): Unit = {
    data.safeClose()
    buffers.safeClose()
    if (batchInternal != null) {
      batchInternal.close()
    }
  }
}

@SerialVersionUID(100L)
class SerializeBatchDeserializeHostBuffer(batch: ColumnarBatch)
  extends Serializable with AutoCloseable {
  @transient private var columns = GpuColumnVector.extractBases(batch).map(_.copyToHost())
  @transient var header: JCudfSerialization.SerializedTableHeader = null
  @transient var buffer: HostMemoryBuffer = null
  @transient private val numRows = batch.numRows()

  private def writeObject(out: ObjectOutputStream): Unit = {
    val range = new NvtxRange("SerializeBatch", NvtxColor.PURPLE)
    try {
      if (buffer != null) {
        throw new IllegalStateException("Cannot re-serialize a batch this way...")
      } else {
        JCudfSerialization.writeToStream(columns, out, 0, numRows)
        // In this case an RDD, we want to close the batch once it is serialized out or we will
        // leak GPU memory (technically it will just wait for GC to release it and probably
        // not a lot because this is used for a broadcast that really should be small)
        // In the case of broadcast the life cycle of the object is tied to GC and there is no clean
        // way to separate the two right now.  So we accept the leak.
        columns.safeClose()
        columns = null
      }
    } finally {
      range.close()
    }
  }

  private def readObject(in: ObjectInputStream): Unit = {
    val range = new NvtxRange("HostDeserializeBatch", NvtxColor.PURPLE)
    try {
      val din = new DataInputStream(in)
      header = new JCudfSerialization.SerializedTableHeader(din)
      if (!header.wasInitialized()) {
        throw new IllegalStateException("Could not read data")
      }
      buffer = HostMemoryBuffer.allocate(header.getDataLen)
      // This one is a little odd. When deserialized this object is passed to
      // SerializeConcatHostBuffersDeserializeBatch.  But we cannot close this (the input data)
      // after serializing it because in local mode it is up to spark and GC if it is the same
      // object, or if it tries to deserialize it, so that means we have to rely on GC to clean up
      // this buffer too.
      buffer.noWarnLeakExpected()
      JCudfSerialization.readTableIntoBuffer(din, header, buffer)
      if (!header.wasDataRead()) {
        throw new IllegalStateException("Could not read data")
      }
    } finally {
      range.close()
    }
  }

  def dataSize: Long = {
    JCudfSerialization.getSerializedSizeInBytes(columns, 0, numRows)
  }

  override def close(): Unit = {
    columns.safeClose()
    if (buffer != null) {
      buffer.close()
    }
  }
}

class GpuBroadcastMeta(
    exchange: BroadcastExchangeExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule) extends
  SparkPlanMeta[BroadcastExchangeExec](exchange, conf, parent, rule) {

  override def tagPlanForGpu(): Unit = {
    if (!TrampolineUtil.isSupportedRelation(exchange.mode)) {
      willNotWorkOnGpu(
        "Broadcast exchange is only supported for HashedJoin or BroadcastNestedLoopJoin")
    }
    def isSupported(rm: RapidsMeta[_, _, _]): Boolean = rm.wrapped match {
      case _: BroadcastHashJoinExec => true
      case _: BroadcastNestedLoopJoinExec => true
      case _ => false
    }
    if (parent.isDefined) {
      if (!parent.exists(isSupported)) {
        willNotWorkOnGpu("BroadcastExchange only works on the GPU if being used " +
            "with a GPU version of BroadcastHashJoinExec or BroadcastNestedLoopJoinExec")
      }
    }
    // when AQE is enabled and we are planning a new query stage, we need to look at meta-data
    // previously stored on the spark plan to determine whether this exchange can run on GPU
    wrapped.getTagValue(gpuSupportedTag).foreach(_.foreach(willNotWorkOnGpu))
  }

  override def convertToGpu(): GpuExec = {
    ShimLoader.getSparkShims.getGpuBroadcastExchangeExec(
      exchange.mode, childPlans.head.convertIfNeeded())
  }

}

abstract class GpuBroadcastExchangeExecBase(
    val mode: BroadcastMode,
    child: SparkPlan) extends Exchange with GpuExec {

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics = Map(
    TOTAL_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_TOTAL_TIME),
    "dataSize" -> createSizeMetric(ESSENTIAL_LEVEL, "data size"),
    COLLECT_TIME -> createNanoTimingMetric(ESSENTIAL_LEVEL, DESCRIPTION_COLLECT_TIME),
    BUILD_TIME -> createNanoTimingMetric(ESSENTIAL_LEVEL, DESCRIPTION_BUILD_TIME),
    "broadcastTime" -> createNanoTimingMetric(ESSENTIAL_LEVEL, "time to broadcast"))

  override def outputPartitioning: Partitioning = BroadcastPartitioning(mode)

  // For now all broadcasts produce a single batch. We might need to change that at some point
  override def outputBatching: CoalesceGoal = RequireSingleBatch

  @transient
  protected lazy val promise = Promise[Broadcast[Any]]()

  /**
   * For registering callbacks on `relationFuture`.
   * Note that calling this field will not start the execution of broadcast job.
   */
  @transient
  lazy val completionFuture: concurrent.Future[Broadcast[Any]] = promise.future

  @transient
  protected val timeout: Long = SQLConf.get.broadcastTimeout

  val _runId: UUID = UUID.randomUUID()

  @transient
  lazy val relationFuture: Future[Broadcast[Any]] = {
    // relationFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val totalTime = gpuLongMetric(TOTAL_TIME)
    val collectTime = gpuLongMetric(COLLECT_TIME)
    val buildTime = gpuLongMetric(BUILD_TIME)
    val broadcastTime = gpuLongMetric("broadcastTime")

    val task = new Callable[Broadcast[Any]]() {
      override def call(): Broadcast[Any] = {
        // This will run in another thread. Set the execution id so that we can connect these jobs
        // with the correct execution.
        SQLExecution.withExecutionId(sqlContext.sparkSession, executionId) {
          val totalRange = new MetricRange(totalTime)
          try {
            // Setup a job group here so later it may get cancelled by groupId if necessary.
            sparkContext.setJobGroup(_runId.toString, s"broadcast exchange (runId ${_runId})",
              interruptOnCancel = true)
            val collectRange = new NvtxWithMetrics("broadcast collect", NvtxColor.GREEN,
              collectTime)
            val batch = try {
              val data = child.executeColumnar().map(cb => try {
                new SerializeBatchDeserializeHostBuffer(cb)
              } finally {
                cb.close()
              })
              val d = data.collect()
              new SerializeConcatHostBuffersDeserializeBatch(d, output)
            } finally {
              collectRange.close()
            }

            val numRows = batch.numRows
            checkRowLimit(numRows)
            numOutputBatches += 1
            numOutputRows += numRows

            val buildRange = new NvtxWithMetrics("broadcast build", NvtxColor.DARK_GREEN, buildTime)
            try {
              // we only support hashjoin so this is a noop
              // val relation = mode.transform(input, Some(numRows))
              val dataSize = batch.dataSize

              gpuLongMetric("dataSize") += dataSize
              if (dataSize >= (8L << 30)) {
                throw new SparkException(
                  s"Cannot broadcast the table that is larger than 8GB: ${dataSize >> 30} GB")
              }
            } finally {
              buildRange.close()
            }
            val broadcastRange = new NvtxWithMetrics("broadcast", NvtxColor.CYAN, broadcastTime)
            val broadcasted = try {
              // Broadcast the relation
              sparkContext.broadcast(batch.asInstanceOf[Any])
            } finally {
              broadcastRange.close()
            }

            SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
            promise.success(broadcasted)
            broadcasted
          } catch {
            // SPARK-24294: To bypass scala bug: https://github.com/scala/bug/issues/9554, we throw
            // SparkFatalException, which is a subclass of Exception. ThreadUtils.awaitResult
            // will catch this exception and re-throw the wrapped fatal throwable.
            case oe: OutOfMemoryError =>
              val ex = createOutOfMemoryException(oe)
              promise.failure(ex)
              throw ex
            case e if !NonFatal(e) =>
              val ex = new Exception(e)
              promise.failure(ex)
              throw ex
            case e: Throwable =>
              promise.failure(e)
              throw e
          } finally {
            totalRange.close()
          }
        }
      }
    }
    GpuBroadcastExchangeExec.executionContext.submit[Broadcast[Any]](task)
  }

  protected def createOutOfMemoryException(oe: OutOfMemoryError) = {
    new Exception(
      new OutOfMemoryError("Not enough memory to build and broadcast the table to all " +
        "worker nodes. As a workaround, you can either disable broadcast by setting " +
        s"${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1 or increase the spark " +
        s"driver memory by setting ${SparkLauncher.DRIVER_MEMORY} to a higher value.")
        .initCause(oe.getCause))
  }

  protected def checkRowLimit(numRows: Int) = {
    // Spark restricts the size of broadcast relations to be less than 512000000 rows and we
    // enforce the same limit
    // scalastyle:off line.size.limit
    // https://github.com/apache/spark/blob/v3.1.1/sql/core/src/main/scala/org/apache/spark/sql/execution/joins/HashedRelation.scala#L586
    // scalastyle:on line.size.limit
    if (numRows >= 512000000) {
      throw new SparkException(
        s"Cannot broadcast the table with 512 million or more rows: $numRows rows")
    }
  }

  override protected def doPrepare(): Unit = {
    // Materialize the future.
    relationFuture
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "GpuBroadcastExchange does not support the execute() code path.")
  }

  override protected[sql] def doExecuteBroadcast[T](): Broadcast[T] = {
    try {
      relationFuture.get(timeout, TimeUnit.SECONDS).asInstanceOf[Broadcast[T]]
    } catch {
      case ex: TimeoutException =>
        logError(s"Could not execute broadcast in $timeout secs.", ex)
        if (!relationFuture.isDone) {
          sparkContext.cancelJobGroup(_runId.toString)
          relationFuture.cancel(true)
        }
        throw new SparkException(s"Could not execute broadcast in $timeout secs. " +
          s"You can increase the timeout for broadcasts via ${SQLConf.BROADCAST_TIMEOUT.key} or " +
          s"disable broadcast join by setting ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1",
          ex)
    }
  }

  final def executeColumnarBroadcast[T](): Broadcast[T] = {
    if (isCanonicalizedPlan) {
      throw new IllegalStateException("A canonicalized plan is not supposed to be executed.")
    }
    try {
      relationFuture.get(timeout, TimeUnit.SECONDS).asInstanceOf[Broadcast[T]]
    } catch {
      case ex: TimeoutException =>
        logError(s"Could not execute broadcast in $timeout secs.", ex)
        if (!relationFuture.isDone) {
          sparkContext.cancelJobGroup(_runId.toString)
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

  val executionContext = ExecutionContext.fromExecutorService(
    newDaemonCachedThreadPool("gpu-broadcast-exchange",
      SQLConf.get.getConf(StaticSQLConf.BROADCAST_EXCHANGE_MAX_THREAD_THRESHOLD)))
}
