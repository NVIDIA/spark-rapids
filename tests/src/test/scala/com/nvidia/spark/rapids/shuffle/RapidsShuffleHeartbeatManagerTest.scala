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

package com.nvidia.spark.rapids.shuffle

import com.nvidia.spark.rapids.{RapidsConf, RapidsExecutorUpdateMsg, RapidsShuffleHeartbeatEndpoint, RapidsShuffleHeartbeatHandler, RapidsShuffleHeartbeatManager}
import org.mockito.Mockito._

import org.apache.spark.sql.rapids.execution.TrampolineUtil

class RapidsShuffleHeartbeatManagerTest extends RapidsShuffleTestHelper {
  test("adding an executor") {
    val hbMgr = new RapidsShuffleHeartbeatManager
    val updateMsg = hbMgr.registerExecutor(
      TrampolineUtil.newBlockManagerId("1", "peer", 123, Some("rapids=123")))
    assertResult(0)(updateMsg.ids.length)
  }

  test("a heartbeat picks up new executors") {
    val hbMgr = new RapidsShuffleHeartbeatManager

    val exec1 = TrampolineUtil.newBlockManagerId("1", "peer", 123, Some("rapids=123"))
    val exec2 = TrampolineUtil.newBlockManagerId("2", "peer2", 456, Some("rapids=456"))

    // first executor
    hbMgr.registerExecutor(exec1)
    // second executor
    hbMgr.registerExecutor(exec2)

    // first executor heartbeat (finding out about executor 2)
    hbMgr.executorHeartbeat(exec1)

    match {
      case RapidsExecutorUpdateMsg(idAndExecutorData) =>
        assertResult(1)(idAndExecutorData.length)
        val peerBlockManager = idAndExecutorData.head
        assertResult(exec2)(peerBlockManager)
    }
  }

  test("a subsequent heartbeat with no new executors is empty") {
    val hbMgr = new RapidsShuffleHeartbeatManager

    val exec1 = TrampolineUtil.newBlockManagerId("1", "peer", 123, Some("rapids=123"))
    val exec2 = TrampolineUtil.newBlockManagerId("2", "peer2", 456, Some("rapids=456"))

    // first executor
    hbMgr.registerExecutor(exec1)
    // second executor
    hbMgr.registerExecutor(exec2)

    // first executor heartbeat (finding out about executor 2)
    val firstUpdate = hbMgr.executorHeartbeat(exec1)
    val peerBlockManager = firstUpdate.ids.head
    assertResult(exec2)(peerBlockManager)

    // second heartbeat from exec1, should be empty
    val secondUpdate = hbMgr.executorHeartbeat(exec1)
    assertResult(0)(secondUpdate.ids.length)

    // first heartbeat from exec2 returns empty, it already knew about exec1 on registration
    val firstUpdateExec2 = hbMgr.executorHeartbeat(exec2)
    assertResult(0)(firstUpdateExec2.ids.length)
  }

  test("an executor tells heartbeat handler about new peers") {
    val conf = new RapidsConf(Map[String, String]())
    val hbEndpoint = new RapidsShuffleHeartbeatEndpoint(null, conf)
    val exec1 = TrampolineUtil.newBlockManagerId("1", "peer", 123, Some("rapids=123"))
    val hbHandler = mock[RapidsShuffleHeartbeatHandler]
    hbEndpoint.updatePeers(hbHandler, Seq(exec1))
    verify(hbHandler, times(1)).addPeer(exec1)
  }
}
