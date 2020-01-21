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

package org.apache.spark.sql

import java.util.concurrent.ConcurrentLinkedQueue

import ai.rapids.cudf.{NvtxColor, NvtxRange}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.spark.{GpuResourceManager, GpuSemaphore, RapidsConf, RapidsUCXShuffleIterator}

import ai.rapids.spark.ucx.{Transaction, UCX}

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.{BaseShuffleHandle, BlockStoreShuffleReader, ShuffleReadMetricsReporter, ShuffleReader}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockBatchId, ShuffleBlockId}
import org.apache.spark.TaskContext

class RapidsCachingReader[K, C](
    rapidsConf: RapidsConf,
    localId: BlockManagerId,
    blocksByAddress: Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])],
    gpuHandle: GpuShuffleHandle[_, _],
    context: TaskContext,
    metrics: ShuffleReadMetricsReporter,
    ucx: Option[UCX])
  extends ShuffleReader[K, C]  with Logging {

  override def read(): Iterator[Product2[K, C]] = {
    val readRange = new NvtxRange("RapidsCachingReader.read", NvtxColor.DARK_GREEN)
    try {
      val notHandledBlocksByAddress = new ArrayBuffer[(BlockManagerId, Seq[(BlockId, Long, Int)])]()
      val cachedBatches = new ArrayBuffer[ColumnarBatch]()
      // all incoming batches end up in this array
      var nvtxRanges = new ArrayBuffer[NvtxRange]()
      val blocksByAddressMap: Map[BlockManagerId, Seq[(BlockId, Long, Int)]] = blocksByAddress.toMap

      // find block to the left of this executor (where i is this executor)
      // if this is running in local mode, the executorId is "driver", so this is not going to work
      // this code also protects against the executorId not being numeric, in case it changes in the future
      val (sortedBlockManagerAddresses: Seq[BlockManagerId], startAtOffset: Int) = try {
        val localExecutorIdInt = localId.executorId.toInt
        // e1, e2, e3, e4, ..., eN
        val sortedBlockManagerAddresses = blocksByAddressMap.keys.toList.sortWith((left, right) => {left.executorId.toInt < right.executorId.toInt})
        if (sortedBlockManagerAddresses.size == 0) {
          return Iterator.empty
        }
        val pos = sortedBlockManagerAddresses.indexWhere(_.executorId.toInt == localExecutorIdInt)
        val startAtOffset = if (sortedBlockManagerAddresses.size <= 1) { 0 } else {Math.floorMod(pos - 1, sortedBlockManagerAddresses.size)}
        logDebug(s"[RapidsCachingReader] I am ${localId.executorId}, I will start fetching from ${sortedBlockManagerAddresses(startAtOffset)}, out of ${sortedBlockManagerAddresses}")
        (sortedBlockManagerAddresses, startAtOffset)
      } catch {
        case e: java.lang.NumberFormatException => (blocksByAddressMap.keys.toSeq, 0)
      }

      val range = 0 until sortedBlockManagerAddresses.size

      val rapidsUcxShuffleIter = new RapidsUCXShuffleIterator(rapidsConf, localId)

      range.foreach(i => {
        val currentOffset = Math.floorMod(startAtOffset - i, sortedBlockManagerAddresses.size)
        val blockManagerId: BlockManagerId = sortedBlockManagerAddresses(currentOffset)
        val blockInfos: Seq[(BlockId, Long, Int)] = blocksByAddressMap(blockManagerId)

        logDebug("Trying to read block from manager: " + blockManagerId)
        if (blockManagerId.executorId == localId.executorId) {
          //println(s"[RapidsCachingReader] Reading LOCAL BLOCKS from ${blockManagerId} => ${blockInfos}")
          val readLocalRange = new NvtxRange("Read Local", NvtxColor.GREEN)
          try {
            // This is local so we might be able to get it directly from the cache
            val notHandledLocalBlocks = new ArrayBuffer[(BlockId, Long, Int)]()
            blockInfos.foreach(
              blockInfo => {
                val blockId = blockInfo._1
                val (shuffleId, mapId, startPart, endPart) = blockId match {
                  case sbbid: ShuffleBlockBatchId =>
                    (sbbid.shuffleId, sbbid.mapId, sbbid.startReduceId, sbbid.endReduceId)
                  case sbid: ShuffleBlockId =>
                    (sbid.shuffleId, sbid.mapId, sbid.reduceId, sbid.reduceId + 1)
                  case _ => throw new IllegalArgumentException(s"${blockId.getClass} $blockId is not currently supported")
                }
                var locallyFetched = false
                RapidsShuffleManager.getCached(shuffleId, mapId) match {
                  case Some(mapCache: GpuMapCache) =>
                    (startPart until endPart).foreach(part => {
                      mapCache.get(part) match {
                        case Some(pc: PartitionCache) =>
                          cachedBatches ++= pc.read

                          pc.markAsRead()
                          locallyFetched = true
                        case None =>
                          val failedBid = ShuffleBlockId(shuffleId, mapId, part)
                          notHandledLocalBlocks.append((failedBid, blockInfo._2, blockInfo._3))
                      }
                    })
                  case None =>
                    notHandledLocalBlocks.append(blockInfo)
                }

                if (locallyFetched) {
                  metrics.incLocalBlocksFetched(1)
                }
              })
            if (notHandledLocalBlocks.nonEmpty) {
              notHandledBlocksByAddress.append((blockManagerId, notHandledLocalBlocks))
            }
          } finally {
            readLocalRange.close()
          }
        } else if (blockManagerIsUcxAccessible(blockManagerId)) {
          rapidsUcxShuffleIter.fetch(localId, ucx.get, blockManagerId, blockInfos)
          metrics.incRemoteBlocksFetched(blockInfos.size)
        } else {
          notHandledBlocksByAddress.append((blockManagerId, blockInfos))
        }
      })
      nvtxRanges.foreach(r => r.close())

      val itRange = new NvtxRange("Shuffle Iterator prep", NvtxColor.BLUE)
      try {
        val handle = gpuHandle.wrapped.asInstanceOf[BaseShuffleHandle[K, _, C]]
        val otherReader = new BlockStoreShuffleReader(handle, notHandledBlocksByAddress.iterator, context, metrics)

        val cachedIt = cachedBatches.iterator.map(cb => {
          GpuSemaphore.acquireIfNecessary(context)
          val cachedBytesRead = GpuResourceManager.deviceMemoryUsed(cb)
          metrics.incLocalBytesRead(cachedBytesRead)
          metrics.incRecordsRead(1)
          (0, cb)
        }).asInstanceOf[Iterator[(K, C)]]

        val cbArrayFromUcx: Iterator[(K, C)] = rapidsUcxShuffleIter.map(cb => {
          GpuSemaphore.acquireIfNecessary(context)
          val ucxBytesFetched = GpuResourceManager.deviceMemoryUsed(cb)
          metrics.incRemoteBytesRead(ucxBytesFetched)
          metrics.incRecordsRead(1)
          (0, cb)
        }).asInstanceOf[Iterator[(K, C)]]

        cachedIt ++ otherReader.read() ++ cbArrayFromUcx
      } finally {
        itRange.close()
      }
    } finally {
      readRange.close()
    }
  }

  private def blockManagerIsUcxAccessible(otherBlockManagerId: BlockManagerId): Boolean = {
    val isUcxCapableManager = 
      otherBlockManagerId.topologyInfo.isDefined &&
        otherBlockManagerId.topologyInfo.get.startsWith("rapids=") 

    val isLocalBlock = otherBlockManagerId.host.equalsIgnoreCase(localId.host)
    val ucxEnabledLocally = rapidsConf.shuffleUcxHandleLocal
    val ucxEnabledRemotely = rapidsConf.shuffleUcxHandleRemote

    isUcxCapableManager && 
      (isLocalBlock && ucxEnabledLocally || 
       !isLocalBlock && ucxEnabledRemotely)
  }
                   
}
