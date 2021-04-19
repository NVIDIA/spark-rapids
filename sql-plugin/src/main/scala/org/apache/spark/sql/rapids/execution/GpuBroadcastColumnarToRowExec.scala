package org.apache.spark.sql.rapids.execution

import java.util.UUID
import java.util.concurrent.{Callable, Future, TimeoutException, TimeUnit}

import scala.concurrent.Promise
import scala.util.control.NonFatal

import ai.rapids.cudf.NvtxColor
import com.nvidia.spark.rapids.{GpuColumnarToRowExecParent, GpuExec, GpuMetric, MetricRange, NoopMetric, NvtxWithMetrics}

import org.apache.spark.SparkException
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution, UnaryExecNode}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec.MAX_BROADCAST_TABLE_BYTES
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.KnownSizeEstimation

/**
 * This is a specialized version of GpuColumnarToRow that wraps a GpuBroadcastExchange and
 * converts the columnar results containing cuDF tables into Spark rows so that the results
 * can feed a CPU BroadcastHashJoin. This is required for exchange reuse in AQE.
 *
 * @param broadcast GpuBroadcastExchangeExecBase
 */
case class GpuBroadcastColumnarToRowExec(broadcast: GpuBroadcastExchangeExecBase)
    extends UnaryExecNode with GpuExec {

  import GpuMetric._
  // We need to do this so the assertions don't fail
  override def supportsColumnar = false

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  // Override the original metrics to remove NUM_OUTPUT_BATCHES, which makes no sense.
  override lazy val allMetrics: Map[String, GpuMetric] = Map(
    "dataSize" -> createSizeMetric(ESSENTIAL_LEVEL, "data size"),
    NUM_OUTPUT_ROWS -> createMetric(outputRowsLevel, DESCRIPTION_NUM_OUTPUT_ROWS),
    BUILD_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_BUILD_TIME),
    "broadcastTime" -> createNanoTimingMetric(ESSENTIAL_LEVEL, "time to broadcast"),
    COLLECT_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_COLLECT_TIME),
    TOTAL_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_TOTAL_TIME),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES))

  @transient
  private lazy val promise = Promise[Broadcast[Any]]()

  @transient
  private val timeout: Long = SQLConf.get.broadcastTimeout

  /**
   * For registering callbacks on `relationFuture`.
   * Note that calling this field will not start the execution of broadcast job.
   */
  @transient
  lazy val completionFuture: concurrent.Future[Broadcast[Any]] = promise.future

  val _runId: UUID = UUID.randomUUID()

  @transient
  lazy val relationFuture: Future[Broadcast[Any]] = {
    // relationFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val collectTime = gpuLongMetric(COLLECT_TIME)
    val buildTime = gpuLongMetric(BUILD_TIME)
    val broadcastTime = gpuLongMetric(BUILD_TIME)
    val totalTime = gpuLongMetric(TOTAL_TIME)

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

            val f = GpuColumnarToRowExecParent
                .makeIteratorFunc(broadcast.child.output, numOutputRows, NoopMetric, totalTime)

            val collectRange = new NvtxWithMetrics("broadcast collect", NvtxColor.GREEN,
              collectTime)
            val serializedBatch: SerializeConcatHostBuffersDeserializeBatch = try {
              val data = broadcast.child.executeColumnar().map(cb => try {
                new SerializeBatchDeserializeHostBuffer(cb)
              } finally {
                cb.close()
              })
              val d = data.collect()
              new SerializeConcatHostBuffersDeserializeBatch(d, output)
            } finally {
              collectRange.close()
            }

            val batch = serializedBatch.batch
            val numRows = batch.numRows
            if (numRows >= 512000000) {
              throw new SparkException(
                s"Cannot broadcast the table with 512 million or more rows: $numRows rows")
            }
            numOutputRows += numRows

            val buildRange = new NvtxWithMetrics("broadcast build", NvtxColor.DARK_GREEN, buildTime)
            val relation = try {

              // Convert batches to rows and create a broadcast relation
              val rows = f(Seq(batch).iterator).toArray
              val relation = broadcast.mode.transform(rows)

              val dataSize = relation match {
                case map: KnownSizeEstimation =>
                  map.estimatedSize
                case arr: Array[InternalRow] =>
                  arr.map(_.asInstanceOf[UnsafeRow].getSizeInBytes.toLong).sum
                case _ =>
                  throw new SparkException("[BUG] BroadcastMode.transform returned unexpected " +
                      s"type: ${relation.getClass.getName}")
              }
              longMetric("dataSize") += dataSize
              if (dataSize >= MAX_BROADCAST_TABLE_BYTES) {
                throw new SparkException(
                  s"Cannot broadcast the table that is larger than 8GB: ${dataSize >> 30} GB")
              }
              relation
            } finally {
              buildRange.close()
            }

            val broadcastRange = new NvtxWithMetrics("broadcast", NvtxColor.CYAN, broadcastTime)
            val broadcasted = try {
              // Broadcast the relation
              sparkContext.broadcast(relation)
            } finally {
              broadcastRange.close()
            }

            val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
            SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
            promise.trySuccess(broadcasted)
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
          } finally {
            totalRange.close()
          }
        }
      }
    }
    GpuBroadcastExchangeExec.executionContext.submit[Broadcast[Any]](task)
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

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException()
  }
}