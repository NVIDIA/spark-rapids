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
package ai.rapids.spark.ucx

import ai.rapids.cudf.CudfColumnVector
import ai.rapids.cudf.DeviceMemoryBuffer
import ai.rapids.cudf.HostMemoryBuffer
import org.scalatest.{BeforeAndAfter, FunSuite}
import java.nio.file.Files
import java.nio.file.FileSystems

class UCXTestSuite extends FunSuite with BeforeAndAfter {

  var ucxExecutor1: UCX = _
  var ucxExecutor2: UCX = _
  val msgSize = 10 * 1024 * 1024

  case class Buffer(msgSize: Long) extends AutoCloseable {
    val device = DeviceMemoryBuffer.allocate(msgSize)
    val host= HostMemoryBuffer.allocate(msgSize)
    val deviceAddr = CudfColumnVector.getAddress(device)
    val hostAddr = CudfColumnVector.getAddress(host)

    def getAddress(device: Boolean) = if (device) {
      deviceAddr
    } else {
      hostAddr
    }

    override def close() = {
      device.close()
      host.close()
    }
  }

  var recvBuff: Buffer = _
  var reqBuff: Buffer = _
  var respBuff: Buffer = _
  var respRecvBuff: Buffer = _

  var ucx: UCX = _
  var ucxClient: UCX = _

  before {
    recvBuff = Buffer(msgSize)
    reqBuff = Buffer(msgSize)
    respBuff = Buffer(msgSize)
    respRecvBuff = Buffer(msgSize)

    ucx = new UCX(1)
    ucxClient = new UCX(2)
    ucx.init()
    ucxClient.init()
  }

  after {
    ucx.close()
    ucxClient.close()
    recvBuff.close()
    reqBuff.close()
    respBuff.close()
    respRecvBuff.close()
  }

  import scala.reflect.ClassTag

  def initUCXWorkers(device: Boolean): (Connection) = {
    val counter = new java.util.concurrent.atomic.AtomicLong(0L)
    def handleReceive(connection: Connection): Unit = {
      val tx = connection.createTransaction
      tx.receive(recvBuff.getAddress(device), msgSize, tx => {
        handleReceive(connection)
        val respTx = connection.createTransaction
        val respTag = counter.getAndIncrement()
        respTx.send(respRecvBuff.getAddress(device), msgSize, respTag, tx => {})
      })
    }

    val port: Int = ucx.startManagementPort("localhost", (connection: Connection) => {
      // start that receive
      handleReceive(connection)
    })

    val sendConnection = ucxClient.connect("localhost", port)
    (sendConnection)
  }

  def sendMessages(sendConnection: Connection, device: Boolean) = {
    val numReps = 100
    var respTag: Long = 0
    var txSeq = Seq[Transaction]()
    for (i <- 0 until numReps) {
      val sendTx = sendConnection.createTransaction
      val respTag = i
      sendTx.request(reqBuff.getAddress(device),
        msgSize,
        respBuff.getAddress(device),
        msgSize,
        respTag,
        tx => {})
      txSeq = txSeq :+ sendTx
    }

    txSeq.foreach(sendTx => {
      sendTx.waitForCompletion
    })
  }

  test("send requests between UCX workers using device memory") {
    val sendConnection = initUCXWorkers(true)
    sendMessages(sendConnection, true)
  }

  test("send requests between UCX workers using host memory") {
    val sendConnection = initUCXWorkers(false)
    sendMessages(sendConnection, false)
  }

  test("send requests between UCX workers host to device memory") {
    val sendConnection = initUCXWorkers(false)
    sendMessages(sendConnection, true)
  }

  test("send requests between UCX workers device to host memory") {
    val sendConnection = initUCXWorkers(true)
    sendMessages(sendConnection, false)
  }

}

