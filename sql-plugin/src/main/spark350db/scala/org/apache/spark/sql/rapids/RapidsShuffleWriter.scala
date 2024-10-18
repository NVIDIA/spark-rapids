/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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
{"spark": "350db"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.rapids

import scala.collection.mutable

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shuffle.{RapidsShuffleServer, RapidsShuffleTransport}

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatusWithStats
import org.apache.spark.shuffle.ShuffleWriter
import org.apache.spark.storage._

abstract class RapidsShuffleWriter[K, V]()
      extends ShuffleWriter[K, V]
        with Logging {
  protected var myMapStatus: Option[MapStatusWithStats] = None
  protected val diskBlockObjectWriters = new mutable.HashMap[Int, (Int, DiskBlockObjectWriter)]()
  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
  private var stopping = false

  def getMapStatus(
    loc: BlockManagerId,
    uncompressedSizes: Array[Long],
    mapTaskId: Long): MapStatusWithStats = {
    MapStatusWithStats(loc, uncompressedSizes, mapTaskId)
  }

  override def stop(success: Boolean): Option[MapStatusWithStats] = {
    if (stopping) {
      None
    } else {
      stopping = true
      if (success) {
        if (myMapStatus.isEmpty) {
          // should not happen, but adding it just in case (this differs from Spark)
          cleanupTempData()
          throw new IllegalStateException("Cannot call stop(true) without having called write()");
        }
        myMapStatus
      } else {
        cleanupTempData()
        None
      }
    }
  }

  private def cleanupTempData(): Unit = {
    // The map task failed, so delete our output data.
    try {
      diskBlockObjectWriters.values.foreach { case (_, writer) =>
        val file = writer.revertPartialWritesAndClose()
        if (!file.delete()) logError(s"Error while deleting file ${file.getAbsolutePath()}")
      }
    } finally {
      diskBlockObjectWriters.clear()
    }
  }
}

abstract class RapidsCachingWriterBase[K, V](
   blockManager: BlockManager,
   handle: GpuShuffleHandle[K, V],
   mapId: Long,
   rapidsShuffleServer: Option[RapidsShuffleServer],
   catalog: ShuffleBufferCatalog)
  extends ShuffleWriter[K, V]
    with Logging {
  protected val numParts = handle.dependency.partitioner.numPartitions
  protected val sizes = new Array[Long](numParts)

  /**
   * Used to remove shuffle buffers when the writing task detects an error, calling `stop(false)`
   */
  private def cleanStorage(): Unit = {
    catalog.removeCachedHandles()
  }

  override def stop(success: Boolean): Option[MapStatusWithStats] = {
    val nvtxRange = new NvtxRange("RapidsCachingWriter.close", NvtxColor.CYAN)
    try {
      if (!success) {
        cleanStorage()
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
        logInfo(s"Done caching shuffle success=$success, server_id=$shuffleServerId, "
          + s"map_id=$mapId, sizes=${sizes.mkString(",")}")
        Some(MapStatusWithStats(shuffleServerId, sizes, mapId))
      }
    } finally {
      nvtxRange.close()
    }
  }

}
