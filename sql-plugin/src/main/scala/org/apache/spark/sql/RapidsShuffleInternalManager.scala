/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

package org.apache.spark.sql

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import ai.rapids.spark._
import ai.rapids.spark.format.TableMeta
import ai.rapids.spark.shuffle.{RapidsShuffleRequestHandler, RapidsShuffleServer, RapidsShuffleTransport}
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage._
import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}

class GpuShuffleHandle[K, V](
    val wrapped: ShuffleHandle,
    override val dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(wrapped.shuffleId, dependency) {

  override def toString: String = s"GPU SHUFFLE HANDLE $shuffleId"
}

class GpuShuffleBlockResolver(private val wrapped: ShuffleBlockResolver,
    private val blockManager: BlockManager,
    private val compressionCodec: CompressionCodec,
    catalog: ShuffleBufferCatalog)
  extends ShuffleBlockResolver with Logging {
  override def getBlockData(blockId: BlockId, dirs: Option[Array[String]]): ManagedBuffer = {
    val hasActiveShuffle: Boolean = blockId match {
      case sbbid: ShuffleBlockBatchId =>
        catalog.hasActiveShuffle(sbbid.shuffleId)
      case sbid: ShuffleBlockId =>
        catalog.hasActiveShuffle(sbid.shuffleId)
      case _ => throw new IllegalArgumentException(s"${blockId.getClass} $blockId is not currently supported")
    }
    if (hasActiveShuffle) {
      throw new IllegalStateException(s"The block $blockId is being managed by the catalog")
    }
    wrapped.getBlockData(blockId)
  }

  override def stop(): Unit = wrapped.stop()
}


object RapidsShuffleInternalManager extends Logging {
  def unwrapHandle(handle: ShuffleHandle): ShuffleHandle = handle match {
    case gh: GpuShuffleHandle[_, _] => gh.wrapped
    case other => other
  }
}

class RapidsCachingWriter[K, V](
    conf: SparkConf,
    blockManager: BlockManager,
    blockResolver: IndexShuffleBlockResolver,
    // Never keep a reference to the ShuffleHandle in the cache as it being GCed triggers
    // the data being released
    handle: GpuShuffleHandle[K, V],
    mapId: Long,
    compressionCodec: CompressionCodec,
    metrics: ShuffleWriteMetricsReporter,
    catalog: ShuffleBufferCatalog,
    shuffleStorage: RapidsDeviceMemoryStore,
    rapidsShuffleServer: Option[RapidsShuffleServer]) extends ShuffleWriter[K, V] with Logging {

  private val numParts = handle.dependency.partitioner.numPartitions
  private val sizes = new Array[Long](numParts)

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val nvtxRange = new NvtxRange("RapidsCachingWriter.write", NvtxColor.CYAN)
    try {
      var bytesWritten: Long = 0L
      var recordsWritten: Long = 0L
      records.foreach { p =>
        val partId = p._1.asInstanceOf[Int]
        val batch = p._2.asInstanceOf[ColumnarBatch]
        logDebug(s"Caching shuffle_id=${handle.shuffleId} map_id=$mapId, partId=$partId, batch=[num_cols=${batch.numCols()}, num_rows=${batch.numRows()}]")
        val partSize = GpuColumnVector.extractBases(batch).map(_.getDeviceMemorySize).sum
        recordsWritten = recordsWritten + batch.numRows()
        bytesWritten = bytesWritten + partSize
        sizes(partId) += partSize
        val blockId = ShuffleBlockId(handle.shuffleId, mapId, partId)
        val bufferId = catalog.nextShuffleBufferId(blockId)
        if (batch.numRows > 0 && batch.numCols > 0) {
          val buffer = {
            val buff = batch.column(0).asInstanceOf[GpuColumnVectorFromBuffer].getBuffer
            buff.slice(0, buff.getLength)
          }

          // Add the table to the shuffle store
          shuffleStorage.addTable(
            bufferId,
            GpuColumnVector.from(batch),
            buffer,
            SpillPriorities.OUTPUT_FOR_SHUFFLE_INITIAL_PRIORITY)
        } else {
          // no device data, tracking only metadata
          val tableMeta = MetaUtils.buildDegenerateTableMeta(bufferId.tableId, batch)
          catalog.registerNewBuffer(new DegenerateRapidsBuffer(bufferId, tableMeta))

          // The size of the data is really only used to tell if the data should be shuffled or not
          // a 0 indicates that we should not shuffle anything.  This is here for the special case
          // where we have no columns, because of predicate push down, but we have a row count as
          // metadata.  We still want to shuffle it. The 100 is an arbitrary number and can be really
          // any non-zero number that is not too large
          if (batch.numRows > 0) {
            sizes(partId) += 100
          }
        }
      }
      metrics.incBytesWritten(bytesWritten)
      metrics.incRecordsWritten(recordsWritten)
    } finally {
      nvtxRange.close()
    }
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    val nvtxRange = new NvtxRange("RapidsCachingWriter.close", NvtxColor.CYAN)
    try {
      if (!success) {
        shuffleStorage.close()
        None
      } else {
        // upon seeing this port, the other side will try to connect to the port
        // in order to establish an UCX endpoint (on demand), if the topology has "rapids" in it.
        val shuffleServerId = if (rapidsShuffleServer.isDefined) {
          val originalShuffleServerId = rapidsShuffleServer.get.originalShuffleServerId
          val server = rapidsShuffleServer.get
          BlockManagerId(
            originalShuffleServerId.executorId,
            originalShuffleServerId.host,
            originalShuffleServerId.port,
            Some(s"${RapidsShuffleTransport.BLOCK_MANAGER_ID_TOPO_PREFIX}=${server.getPort}"))
        } else {
          blockManager.shuffleServerId
        }
        logInfo(s"Done caching shuffle success=$success, server_id=$shuffleServerId, map_id=$mapId, sizes=${sizes.mkString(",")}")
        Some(MapStatus(shuffleServerId, sizes, mapId))
      }
    } finally {
      nvtxRange.close()
    }
  }
}

/**
 * A shuffle manager optimized for the RAPIDS Plugin For Apache Spark.
 * @note This is an internal class to obtain access to the private
 *       [[ShuffleManager]] and [[SortShuffleManager]] classes. When configuring
 *       Apache Spark to use the RAPIDS shuffle manager,
 *       [[ai.rapids.spark.RapidsShuffleManager]] should be used as that is
 *       the public class.
 */
class RapidsShuffleInternalManager(conf: SparkConf, isDriver: Boolean) extends ShuffleManager with Logging {

  import RapidsShuffleInternalManager._

  private val rapidsConf = new RapidsConf(conf)

  private val wrapped = new SortShuffleManager(conf)
  GpuShuffleEnv.setRapidsShuffleManagerInitialized(true, this.getClass.getCanonicalName)
  logWarning("Rapids Shuffle Plugin Enabled")

  //Many of these values like blockManager are not initialized when the constructor is called,
  // so they all need to be lazy values that are executed when things are first called
  private[this] lazy val catalog = GpuShuffleEnv.getCatalog
  private lazy val env = SparkEnv.get
  private lazy val blockManager = env.blockManager
  private lazy val shouldFallThroughOnEverything = {
    val fallThroughDueToExternalShuffle = !GpuShuffleEnv.isRapidsShuffleEnabled
    if (fallThroughDueToExternalShuffle) {
      logWarning("Rapids Shuffle Plugin is falling back to SortShuffleManager because " +
        "external shuffle is enabled")
    }
    fallThroughDueToExternalShuffle
  }

  private lazy val localBlockManagerId = blockManager.blockManagerId
  private lazy val compressionEnabled: Boolean = conf.get(config.SHUFFLE_COMPRESS)
  private lazy val compressionCodec: CompressionCodec =
    if (compressionEnabled) {
      CompressionCodec.createCodec(conf)
    } else {
      null
    }

  private lazy val resolver = if (shouldFallThroughOnEverything) {
    wrapped.shuffleBlockResolver
  } else {
    new GpuShuffleBlockResolver(
      wrapped.shuffleBlockResolver,
      blockManager,
      compressionCodec,
      catalog)
  }

  private[this] lazy val transport: Option[RapidsShuffleTransport] = if (rapidsConf.shuffleTransportEnabled && !isDriver) {
    Some(RapidsShuffleTransport.makeTransport(blockManager.shuffleServerId, rapidsConf))
  } else {
    None
  }

  private[this] lazy val server: Option[RapidsShuffleServer] = if (rapidsConf.shuffleTransportEnabled && !isDriver) {
    val requestHandler = new RapidsShuffleRequestHandler() {
      override def acquireShuffleBuffer(tableId: Int): RapidsBuffer = {
        val shuffleBufferId = catalog.getShuffleBufferId(tableId)
        catalog.acquireBuffer(shuffleBufferId)
      }

      override def getShuffleBufferMetas(shuffleBlockBatchId: ShuffleBlockBatchId): Seq[TableMeta] = {
        (shuffleBlockBatchId.startReduceId to shuffleBlockBatchId.endReduceId).flatMap(rid => {
          catalog.blockIdToMetas(ShuffleBlockId(shuffleBlockBatchId.shuffleId, shuffleBlockBatchId.mapId, rid))
        })
      }
    }
    val server = transport.get.makeServer(requestHandler)
    server.start()
    Some(server)
  } else {
    None
  }

  override def registerShuffle[K, V, C](shuffleId: Int, dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    // Always register with the wrapped handler so we can write to it ourselves if needed
    val orig = wrapped.registerShuffle(shuffleId, dependency)
    if (!shouldFallThroughOnEverything && dependency.isInstanceOf[GpuShuffleDependency[K, V, C]]) {
      val handle = new GpuShuffleHandle(orig, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
      handle
    } else {
      orig
    }
  }

  override def getWriter[K, V](handle: ShuffleHandle, mapId: Long, context: TaskContext, metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    handle match {
      case gpu: GpuShuffleHandle[_, _] =>
        registerGpuShuffle(handle.shuffleId)
        new RapidsCachingWriter(conf,
          env.blockManager,
          wrapped.shuffleBlockResolver,
          gpu.asInstanceOf[GpuShuffleHandle[K, V]],
          mapId,
          compressionCodec,
          metrics,
          GpuShuffleEnv.getCatalog,
          GpuShuffleEnv.getDeviceStorage,
          server)
      case other =>
        wrapped.getWriter(other, mapId, context, metrics)
    }
  }

  override def getReaderForRange[K, C](handle: ShuffleHandle,
                                       startMapIndex: Int, endMapIndex: Int, startPartition: Int, endPartition: Int,
                                       context: TaskContext, metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    // NOTE: This type of reader is not possible for gpu shuffle, as we'd need use the the optimization
    // within our manager, and we don't.
    wrapped.getReaderForRange(unwrapHandle(handle), startMapIndex, endMapIndex,
      startPartition, endPartition, context, metrics)
  }

  override def getReader[K, C](handle: ShuffleHandle, startPartition: Int, endPartition: Int,
                               context: TaskContext, metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    handle match {
      case gpu: GpuShuffleHandle[_, _] =>
        logInfo(s"Asking map output tracker for dependency ${gpu.dependency}, " +
          s"map output sizes for: ${gpu.shuffleId}, parts=$startPartition-$endPartition")
        if (gpu.dependency.keyOrdering.isDefined) {
          // very unlikely, but just in case
          throw new IllegalStateException(
            s"A key ordering was requested for a gpu shuffle dependency ${gpu.dependency.keyOrdering.get}, " +
              s"this is not supported.")
        }

        val nvtxRange = new NvtxRange("getMapSizesByExecId", NvtxColor.CYAN)
        val blocksByAddress = try {
          env.mapOutputTracker.getMapSizesByExecutorId(gpu.shuffleId, startPartition, endPartition)
        } finally {
          nvtxRange.close()
        }

        new RapidsCachingReader(rapidsConf, localBlockManagerId,
          blocksByAddress,
          gpu,
          context,
          metrics,
          transport,
          catalog)
      case other => {
        wrapped.getReader(unwrapHandle(other), startPartition, endPartition, context, metrics)
      }
    }
  }

  def registerGpuShuffle(shuffleId: Int): Unit = {
    if (catalog != null) {
      // Note that in local mode this can be called multiple times.
      logInfo(s"Registering shuffle $shuffleId")
      catalog.registerShuffle(shuffleId)
    }
  }

  def unregisterGpuShuffle(shuffleId: Int): Unit = {
    if (catalog != null) {
      logInfo(s"Unregistering shuffle $shuffleId")
      catalog.unregisterShuffle(shuffleId)
    }
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    unregisterGpuShuffle(shuffleId)
    wrapped.unregisterShuffle(shuffleId)
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = resolver

  override def stop(): Unit = {
    GpuShuffleEnv.closeStorage()
    wrapped.stop()
    server.foreach(_.close())
    transport.foreach(_.close())
  }
}
