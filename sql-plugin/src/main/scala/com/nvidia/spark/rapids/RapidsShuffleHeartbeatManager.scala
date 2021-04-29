/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import scala.collection.mutable.ArrayBuffer

import com.google.common.util.concurrent.ThreadFactoryBuilder
import java.util
import org.apache.commons.lang3.mutable.MutableLong

import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.{GpuShuffleEnv, RapidsShuffleInternalManagerBase}
import org.apache.spark.storage.BlockManagerId


/**
 * This is the first message sent from the executor to the driver.
 * @param id `BlockManagerId` for the executor
 */
case class RapidsExecutorStartupMsg(id: BlockManagerId)

/**
 * Executor heartbeat message.
 * This gives the driver an opportunity to respond with `RapidsExecutorUpdateMsg`
 */
case class RapidsExecutorHeartbeatMsg(id: BlockManagerId)

/**
 * Driver response to an startup or heartbeat message, with new (to the peer) executors
 * from the last heartbeat.
 */
case class RapidsExecutorUpdateMsg(ids: Array[BlockManagerId])

class RapidsShuffleHeartbeatManager extends Logging {
  private case class ExecutorRegistration(id: BlockManagerId, lastExecutorSeen: Long)

  // Executors ordered from most recently registered to least-recently registered
  private[this] val executors = new ArrayBuffer[ExecutorRegistration]()

  // A mapping of executor IDs to registration index (ordered by arrival)
  private[this] val lastRegistrationSeen = new util.HashMap[BlockManagerId, MutableLong]

  /**
   * Called by the driver plugin to handle a new registration from an executor.
   * @param id `BlockManagerId` for the peer
   * @return `RapidsExecutorUpdateMsg` with all known executors.
   */
  def registerExecutor(id: BlockManagerId): RapidsExecutorUpdateMsg = synchronized {
    logDebug(s"Registration from RAPIDS executor at $id")
    require(!lastRegistrationSeen.containsKey(id), s"Executor $id already registered")
    val allExecutors = executors.map(e => e.id).toArray

    executors.append(ExecutorRegistration(id, System.nanoTime))

    lastRegistrationSeen.put(id, new MutableLong(allExecutors.length))
    RapidsExecutorUpdateMsg(allExecutors)
  }

  /**
   * Called by the driver plugin to handle an executor heartbeat.
   * @param id `BlockManagerId` for the peer
   * @return `RapidsExecutorUpdateMsg` with new executors, since the last heartbeat was received.
   */
  def executorHeartbeat(id: BlockManagerId): RapidsExecutorUpdateMsg = synchronized {
    val lastRegistration = lastRegistrationSeen.get(id)
    if (lastRegistration == null) {
      throw new IllegalStateException(s"Heartbeat from unknown executor $id")
    }

    val newExecutors = new ArrayBuffer[BlockManagerId]
    val iter = executors.zipWithIndex.reverseIterator
    var done = false
    while (iter.hasNext && !done) {
      val (entry, index) = iter.next()

      if (index > lastRegistration.getValue) {
        if (entry.id != id) {
          newExecutors += entry.id
        }
      } else {
        // We are iterating backwards and have found the last index previously inspected
        // for this peer. We can stop since all peers below this have been sent to this peer.
        done = true
      }
    }

    lastRegistration.setValue(executors.size - 1)
    RapidsExecutorUpdateMsg(newExecutors.toArray)
  }
}

trait RapidsShuffleHeartbeatHandler {
  /** Called when a new peer is seen via heartbeats */
  def addPeer(peer: BlockManagerId): Unit
}

class RapidsShuffleHeartbeatEndpoint(pluginContext: PluginContext, conf: RapidsConf)
  extends Logging with AutoCloseable {
  // Number of milliseconds between heartbeats to driver
  private[this] val heartbeatIntervalMillis =
    conf.shuffleTransportEarlyStartHeartbeatInterval

  private[this] val executorService: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
      .setNameFormat("rapids-shuffle-hb")
      .setDaemon(true)
      .build())

  private class InitializeShuffleManager(ctx: PluginContext,
      shuffleManager: RapidsShuffleInternalManagerBase) extends Runnable {
    override def run(): Unit = {
      try {
        val serverId = shuffleManager.getServerId
        logInfo(s"Registering executor $serverId with driver")
        ctx.ask(RapidsExecutorStartupMsg(shuffleManager.getServerId)) match {
          case RapidsExecutorUpdateMsg(peers) => updatePeers(shuffleManager, peers)
        }
        val heartbeat = new Runnable {
          override def run(): Unit = {
            try {
              logTrace("Performing executor heartbeat to driver")
              ctx.ask(RapidsExecutorHeartbeatMsg(shuffleManager.getServerId)) match {
                case RapidsExecutorUpdateMsg(peers) => updatePeers(shuffleManager, peers)
              }
            } catch {
              case t: Throwable => logError("Error during heartbeat", t)
            }
          }
        }
        executorService.scheduleWithFixedDelay(
          heartbeat,
          0,
          heartbeatIntervalMillis,
          TimeUnit.MILLISECONDS)
      } catch {
        case t: Throwable => logError("Error initializing shuffle", t)
      }
    }
  }

  def updatePeers(shuffleManager: RapidsShuffleHeartbeatHandler,
      peers: Seq[BlockManagerId]): Unit = {
    peers.foreach { peer =>
      logInfo(s"Updating shuffle manager for new executor $peer")
      shuffleManager.addPeer(peer)
    }
  }

  GpuShuffleEnv.mgr.foreach { mgr =>
    executorService.submit(new InitializeShuffleManager(pluginContext, mgr))
  }

  override def close(): Unit = {
    executorService.shutdown()
  }
}
