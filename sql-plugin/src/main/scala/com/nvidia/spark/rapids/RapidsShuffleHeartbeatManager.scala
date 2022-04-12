/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

import org.apache.commons.lang3.mutable.MutableLong

import org.apache.spark.SparkEnv
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.RapidsShuffleInternalManagerBase
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

class RapidsShuffleHeartbeatManager(heartbeatIntervalMillis: Long,
                                    heartbeatTimeoutMillis: Long) extends Logging {
  require(heartbeatIntervalMillis > 0,
    s"The interval value: $heartbeatIntervalMillis ms is not > 0")

  require(heartbeatTimeoutMillis > heartbeatIntervalMillis,
    s"The interval $heartbeatIntervalMillis ms and timeout $heartbeatTimeoutMillis ms " +
      s"values are invalid." +
      s"${RapidsConf.SHUFFLE_TRANSPORT_EARLY_START_HEARTBEAT_TIMEOUT.key} " +
      s"Must be set higher than " +
      s"${RapidsConf.SHUFFLE_TRANSPORT_EARLY_START_HEARTBEAT_INTERVAL.key}.")

  // exposed so that it can be mocked in the tests
  def getCurrentTimeMillis: Long = System.currentTimeMillis()

  private case class ExecutorRegistration(
      id: BlockManagerId,
      // this is this executor's registration order, as given by this manager
      registrationOrder: Long,
      // this is the last registration order this executor is aware of overall
      lastRegistrationOrderSeen: MutableLong,
      // last heartbeat received from this executor in millis
      lastHeartbeatMillis: MutableLong)

  // a counter used to mark each new executor registration with an order
  var registrationOrder = 0L

  // Executors ordered from most recently registered to least-recently registered
  private[this] var executors = new ArrayBuffer[ExecutorRegistration]()

  // A mapping of executor IDs to its registration, populated in `registerExecutor`
  private[this] val executorRegistrations =
    new java.util.HashMap[BlockManagerId, ExecutorRegistration]

  // Min-heap with the root node being the executor that least recently heartbeated
  private[this] val leastRecentHeartbeat =
    new HashedPriorityQueue[BlockManagerId]((b1, b2) => {
      val e1 = executorRegistrations.get(b1)
      val e2 = executorRegistrations.get(b2)
      e1.lastHeartbeatMillis.compareTo(e2.lastHeartbeatMillis)
  })

  /**
   * Called by the driver plugin to handle a new registration from an executor.
   * @param id `BlockManagerId` for the peer
   * @return `RapidsExecutorUpdateMsg` with all known executors.
   */
  def registerExecutor(id: BlockManagerId): RapidsExecutorUpdateMsg = synchronized {
    logDebug(s"Registration from RAPIDS executor at $id")
    require(!executorRegistrations.containsKey(id), s"Executor $id already registered")
    removeDeadExecutors(getCurrentTimeMillis)
    val allExecutors = executors.map(e => e.id).toArray
    val newReg = ExecutorRegistration(id,
      registrationOrder,
      new MutableLong(registrationOrder),
      new MutableLong(getCurrentTimeMillis))
    registrationOrder = registrationOrder + 1
    executors.append(newReg)
    executorRegistrations.put(id, newReg)
    leastRecentHeartbeat.offer(id)
    RapidsExecutorUpdateMsg(allExecutors)
  }

  /**
   * Called by the driver plugin to handle an executor heartbeat.
   * @param id `BlockManagerId` for the peer
   * @return `RapidsExecutorUpdateMsg` with new executors, since the last heartbeat was received.
   */
  def executorHeartbeat(id: BlockManagerId): RapidsExecutorUpdateMsg = synchronized {
    removeDeadExecutors(getCurrentTimeMillis)
    val lastRegistration = executorRegistrations.get(id)
    if (lastRegistration == null) {
      logDebug(s"Heartbeat from unknown executor $id")
      registerExecutor(id)
    } else {
      val newExecutors = new ArrayBuffer[BlockManagerId]
      val iter = executors.reverseIterator
      var done = false
      while (iter.hasNext && !done) {
        val entry = iter.next()

        if (entry.registrationOrder >= lastRegistration.lastRegistrationOrderSeen.getValue) {
          if (entry.id != id) {
            logDebug(s"Found new executor (to $id): $entry while handling a heartbeat.")
            newExecutors += entry.id
          }
        } else {
          // We are iterating backwards and have found the last previously inspected executor
          // since `registrationOrder` monotonically increases and follows insertion order.
          // We can stop since all peers below this have been sent to the heartbeating peer.
          done = true
        }
      }

      // update this executor's registration with a new heartbeat time, and that last order
      // from the executors list, indicating the order we should stop at next time
      lastRegistration.lastHeartbeatMillis.setValue(getCurrentTimeMillis)
      lastRegistration.lastRegistrationOrderSeen.setValue(registrationOrder)

      // since we updated our heartbeat, update our min-heap
      leastRecentHeartbeat.priorityUpdated(lastRegistration.id)
      RapidsExecutorUpdateMsg(newExecutors.toArray)
    }
  }

  // the timeout amount has elapsed, assume this executor is dead
  private def isStaleHeartbeat(hbMillis: Long, currentTime: Long): Boolean = {
    (currentTime - hbMillis) > heartbeatTimeoutMillis
  }

  // must be called holding the object lock
  private def removeDeadExecutors(currentTime: Long): Unit = {
    val leastRecentHb = leastRecentHeartbeat.peek() // look at the executor that is lagging most
    if (leastRecentHb != null &&
        isStaleHeartbeat(
          executorRegistrations.get(leastRecentHb).lastHeartbeatMillis.getValue, currentTime)) {
      // make a new buffer of alive executors and replace the old one
      val aliveExecutors = new ArrayBuffer[ExecutorRegistration]()
      executors.foreach { e =>
        if (isStaleHeartbeat(e.lastHeartbeatMillis.getValue, currentTime)) {
          logDebug(s"Stale exec, removing $e")
          executorRegistrations.remove(e.id)
          leastRecentHeartbeat.remove(e.id)
        } else {
          aliveExecutors.append(e)
        }
      }
      executors = aliveExecutors
    }
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
    Executors.newSingleThreadScheduledExecutor(
      GpuDeviceManager.wrapThreadFactory(new ThreadFactoryBuilder()
        .setNameFormat("rapids-shuffle-hb")
        .setDaemon(true)
        .build()))

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

  def registerShuffleHeartbeat(): Unit = {
    val rapidsShuffleManager = SparkEnv.get.shuffleManager.asInstanceOf[Proxy].self
        .asInstanceOf[RapidsShuffleInternalManagerBase]
    if (rapidsShuffleManager.isDriver) {
      logDebug("Local mode detected. Skipping shuffle heartbeat registration.")
    } else {
      executorService.submit(new InitializeShuffleManager(pluginContext, rapidsShuffleManager))
    }
  }

  override def close(): Unit = {
    executorService.shutdown()
  }
}
