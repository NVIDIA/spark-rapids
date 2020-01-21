/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import ai.rapids.cudf.{CudfColumnVector, DeviceMemoryBuffer, HostMemoryBuffer}

//import org.apache.spark.SparkConf
//TODO: errors? import org.apache.spark.sql.vectorized.ColumnarBatch
//import org.apache.spark.SparkConf
//import org.apache.spark.internal.Logging
import scala.collection.mutable

case class TestParams(
  partsMin: Int = 100, 
  partsMax: Int = 200, 
  step: Int = 10,
  totalSize: Int = 1024 * 1024 * 1024, 
  ucxPendingSizeLimit: Long = 200 * 1024 * 1024,
  prealloc: Boolean = true,
  verify: Boolean = false,
  useEpoll: Boolean = false,
  numIter: Int = 10)

class UCXBenchTest(tp: TestParams) {
  var ucx: UCX = _

  def initUCX(id: Int): UCX = {
    ucx = new UCX(id, tp.useEpoll)
    ucx
  }

  val BYTE_TO_GB = 1024 * 1024 * 1024

  case class TestSpec(
    outstandingTags: AtomicInteger,
    partSize: Int,
    buffs: Seq[DeviceMemoryBuffer])

  def prepare(numParts: Int): TestSpec = {
    val doneTags = new mutable.HashSet[Int]() // out comes parts
    val partSize: Int = Math.ceil(tp.totalSize / numParts).toInt


    val hb = HostMemoryBuffer.allocate(partSize)
    val buffs: Seq[DeviceMemoryBuffer] = if (tp.prealloc) {
      (0 until numParts).map(tagId => {
        (0 until partSize).foreach(t => {
          hb.setByte(t, (t % 127).toByte)
        })

        // hack to see if we can get around the memory issue
        val db = DeviceMemoryBuffer.allocate(partSize)
        db.copyFromHostBuffer(hb)
        db
      })
    } else {
      Seq[DeviceMemoryBuffer]()
    }
    hb.close()

    TestSpec(new AtomicInteger(numParts), partSize, buffs)
  }

  var done = false

  def cleanup(spec: TestSpec): Unit = {
    spec.buffs.foreach(b => {
      b.close()
    })
  }

  def close(): Unit = {
    ucx.close()
  }

  val testService = Executors.newFixedThreadPool(10)
  
  var receiveSide: Boolean = false

  class Stats {
    
    var totalLat: Double = 0.0D
    var totalBw: Double = 0.0D
    var minLat: Double = Double.MaxValue
    var maxLat: Double = 0.0D
    var minBw: Double = Double.MaxValue
    var maxBw: Double = 0.0D
    var numIter: Int = 0
   
    def record(lat: Double, bandwidth: Double) = {
      totalLat = totalLat + lat 
      totalBw = totalBw + bandwidth
      minLat = scala.math.min(minLat, lat)
      maxLat = scala.math.max(maxLat, lat)
      minBw = scala.math.min(minBw, bandwidth)
      maxBw = scala.math.max(maxBw, bandwidth)
      numIter = numIter + 1
    }

    def done(): String = {
      s"${numIter},${minLat},${totalLat/numIter},${maxLat}," + 
      s"${minBw},${totalBw/numIter},${maxBw}"
    }
  }

  // just for a test
  def init(host: String, port: Int, numConnections: Int) = {
    initUCX(port)
    /*
    val sparkConf = new SparkConf()
    sparkConf
      .set(RapidsConf.SHUFFLE_UCX_ENABLE.key, "true")
      .set(RapidsConf.SHUFFLE_UCX_MGMT_SERVER_HOST.key, host)
      .set(RapidsConf.SHUFFLE_UCX_MGMT_SERVER_PORT.key, port.toString)
    val config = new RapidsConf(sparkConf)
    ucx.init(config) // one one side, this will be a server
     */
    done = false
    ucx.init()

    var connections = Seq[Connection]()
    println(s"action,num_parts,size_bytes,time_ms,GB_sec")
    ucx.startManagementPort(host, (connection: Connection) => {
      println("Got a connection: " + connection)
      connections = connections :+ connection
      if (connections.size == numConnections) {
        connections.foreach(c => {
          var prep: TestSpec = null
          testService.execute(() => {
            var iter = 0

            val sendCallback = new UCXTagCallback {
              override def onSuccess(tag: Long): Unit = {
                prep.outstandingTags.decrementAndGet
              }

              override def onError(tag: Long, ucxStatus: Int, errorMsg: String): Unit = {
                println("error sending" + errorMsg)
              }
            }

            (tp.partsMin.to(tp.partsMax).by(tp.step)).foreach(numParts => {
              prep = prepare(numParts)
              val stats = new Stats()
              (0 until tp.numIter).foreach(_ => {
                prep.outstandingTags.set(numParts)
                val startTime = System.nanoTime()
                (0 until numParts).foreach(tagId => {
                  val dbb = if (!tp.prealloc) {
                    DeviceMemoryBuffer.allocate(prep.partSize)
                  } else {
                    prep.buffs(tagId)
                  }

                  val sendTag = c.composeSendTag(iter + tagId)
                  c.send(sendTag, CudfColumnVector.getAddress(dbb), prep.partSize, sendCallback)
                })
                iter = iter + numParts
                while (prep.outstandingTags.get > 0) {
                  //println("outstanding tags: " + outstandingTags.size)
                }
                val endTime = System.nanoTime()
                val timeDiff = (endTime - startTime) / 1000000.0D // nanos to ms
                val GBsec = numParts * prep.partSize / (timeDiff / 1000.0D) / BYTE_TO_GB
                stats.record(timeDiff, GBsec)
                if (numParts == tp.partsMax) {
                  done = true
                }
              })
              println(s"sent,${numParts},${prep.partSize},${stats.done}")
              cleanup(prep)
            })
          })
        })
      }
    })
  }

  def waitUntilDone() = {
    while (!done) {
      Thread.sleep(1000)
    }
    Thread.sleep(1000)
  }

  def connect(targetHost: String, targetPort: Int) = {
    receiveSide = true
    done = false
    val connection = ucx.connect(targetHost, targetPort) // connect to the server from the other side
    receive(connection)
  }

  /**
    * fetch some blocks from the other side, you should get back blockId + CB as an iterator
    * call next on that iterator. This just uses int block ids for now.
    * @param blockIdMetas - metas to request with
    * @return - iterator of blockId to CB
    */

  case class ReceiveSpec(partSize: Int, totalSize: Int)

  private def receive(connection: Connection) = {
    println(s"action,num_parts,size_bytes,time_ms,GB_sec")
    var iter = 0

    var prep: TestSpec = null

    val recvCallback = new UCXTagCallback {
      override def onSuccess(tag: Long): Unit = {
        prep.outstandingTags.decrementAndGet
      }

      override def onError(tag: Long, ucxStatus: Int, errorMsg: String): Unit = {
        println("error receiving" + errorMsg)
      }
    }

    (tp.partsMin.to(tp.partsMax).by(tp.step)).foreach(numParts => {
      prep = prepare(numParts)
      val stats = new Stats
      (0 until tp.numIter).foreach(_ => {
        prep.outstandingTags.set(numParts)

        val startTime = System.nanoTime
        (0 until numParts).map(msgId => {
          val buff = if (!tp.prealloc) {
            DeviceMemoryBuffer.allocate(prep.partSize)
          } else {
            prep.buffs(msgId)
          }
          val recvTag = connection.composeReceiveTag(iter + msgId)
          ucx.receive(recvTag, CudfColumnVector.getAddress(buff), prep.partSize, recvCallback)
        })

        iter = iter + numParts

        //TODO: after future wait? ucx.waitForRequests(Seq(metadataRequest))

        // TODO: reconstitute a CB based on an address
        // TODO: what about errors! timeouts!
        // TODO: For now, wait for ALL tags to be returned.
        //  we can later try to do some partial responses with a few CBs
        while (prep.outstandingTags.get > 0) {
          //println("outstanding tags: " + outstandingTags)
        }

        val endTime = System.nanoTime

        val timeDiff = (endTime - startTime) / 1000000.0D // nanos to ms
        val GBsec = numParts * prep.partSize / (timeDiff / 1000.0D) / BYTE_TO_GB
        stats.record(timeDiff, GBsec)

        if (tp.verify) {
          val db = prep.buffs.last
          val hb = HostMemoryBuffer.allocate(db.getLength)
          CudfColumnVector.copyFromDeviceBuffer(hb, db)
          (0 until prep.partSize).foreach(t => {
            if (hb.getByte(t) != t % 127) {
              println(s"ERROR AT BYTE ${t}, got ${hb.getByte(t)} instead of ${t % 127}")
            }
          })
        }

        if (numParts == tp.partsMax) {
          done = true
        }

        /*
        // all the tags are done
        // using the meta, reconstitute the CBs
        if (reconstituted == null) {
          reconstituted = reconstituted + ()
        }
         */
      })
      cleanup(prep)
      println(s"received,${numParts},${prep.partSize},${stats.done}")

    })
  }

}
