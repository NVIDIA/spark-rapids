/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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


package com.nvidia.spark.rapids.shuffle.ucx

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import ai.rapids.cudf.{DeviceMemoryBuffer, HostMemoryBuffer, MemoryBuffer}
import com.nvidia.spark.rapids.{GpuDeviceManager, RapidsConf}
import com.nvidia.spark.rapids.shuffle.{AddressLengthTag, Transaction, TransactionStatus}
import org.apache.commons.cli.{GnuParser, HelpFormatter, Options}
import org.openucx.jucx.ucp.UcpRequest
import org.openucx.jucx.ucs.UcsConstants

import org.apache.spark.SparkConf
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.rapids.execution.TrampolineUtil

object UcxBenchTest {

  case class PerfOptions(remoteAddress: InetSocketAddress, numBlocks: Int, blockSize: Long,
                         serverPort: Int, numIterations: Int, numThreads: Int, memoryType: Int,
                         gpuId: Int)

  private val HELP_OPTION = "h"
  private val ADDRESS_OPTION = "a"
  private val NUM_BLOCKS_OPTION = "n"
  private val SIZE_OPTION = "s"
  private val PORT_OPTION = "p"
  private val ITER_OPTION = "i"
  private val MEMORY_TYPE_OPTION = "m"
  private val NUM_THREADS_OPTION = "t"
  private val GPU_OPTION = "G"
  private val sparkConf = new SparkConf()
  private val rapidsConf = new RapidsConf(sparkConf)
  private var ucx: UCX = _

  private def initOptions(): Options = {
    val options = new Options()
    options.addOption(HELP_OPTION, "help", false,
      "display help message")
    options.addOption(ADDRESS_OPTION, "address", true,
      "address of the management service on a remote host")
    options.addOption(NUM_BLOCKS_OPTION, "num-blocks", true,
      "number of blocks to transfer. Default: 1")
    options.addOption(SIZE_OPTION, "block-size", true,
      "size of block to transfer. Default: 4m")
    options.addOption(PORT_OPTION, "server-port", true,
      "server port. Default: 12345")
    options.addOption(ITER_OPTION, "num-iterations", true,
      "number of iterations. Default: 5")
    options.addOption(NUM_THREADS_OPTION, "num-threads", true,
      "number of threads. Default: 1")
    options.addOption(MEMORY_TYPE_OPTION, "memory-type", true,
      "memory type: host (default), cuda")
    options.addOption(GPU_OPTION, "gpu", true,
      "gpu id to use. Default: 0")
  }

  private def parseOptions(args: Array[String]): PerfOptions = {
    val parser = new GnuParser()
    val options = initOptions()
    val cmd = parser.parse(options, args)

    if (cmd.hasOption(HELP_OPTION)) {
      new HelpFormatter().printHelp("UcxShufflePerfTool", options)
      System.exit(0)
    }

    val inetAddress = if (cmd.hasOption(ADDRESS_OPTION)) {
      val Array(host, port) = cmd.getOptionValue(ADDRESS_OPTION).split(":")
      new InetSocketAddress(host, Integer.parseInt(port))
    } else {
      null
    }

    val serverPort = Integer.parseInt(cmd.getOptionValue(PORT_OPTION, "12345"))

    val numIterations =  Integer.parseInt(cmd.getOptionValue(ITER_OPTION, "5"))

    val threadsNumber = Integer.parseInt(cmd.getOptionValue(NUM_THREADS_OPTION, "1"))

    var memoryType = UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST
    if (cmd.hasOption(MEMORY_TYPE_OPTION) && cmd.getOptionValue(MEMORY_TYPE_OPTION) == "cuda") {
      memoryType = UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_CUDA
    }

    val gpuId = Integer.parseInt(cmd.getOptionValue(GPU_OPTION, "0"))

    PerfOptions(inetAddress,
      Integer.parseInt(cmd.getOptionValue(NUM_BLOCKS_OPTION, "1")),
      JavaUtils.byteStringAsBytes(cmd.getOptionValue(SIZE_OPTION, "4m")),
      serverPort, numIterations, threadsNumber, memoryType, gpuId)
  }

  private def startServer(options: PerfOptions): Unit = {
    val serverConnection = ucx.getServerConnection
    val sendMemory: MemoryBuffer =
      if (options.memoryType == UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_CUDA) {
        DeviceMemoryBuffer.allocate(options.numThreads * options.numBlocks * options.blockSize)
      } else {
        HostMemoryBuffer.allocate(options.numThreads * options.numBlocks * options.blockSize)
      }
    val rpcBuf = ByteBuffer.allocateDirect(1)

    for (i <- 1 to options.numIterations) {
      val txs = new Array[Transaction](options.numThreads * options.numBlocks)
      for (tid <- 0 until options.numThreads) {
        for (blockId <- 0 until options.numBlocks) {
          val tag = tid * options.numBlocks + blockId
          txs(tag) = serverConnection.receive(AddressLengthTag.from(rpcBuf, tag), tx => {
            val buffer = sendMemory.slice((tid * options.numBlocks * options.blockSize) +
                blockId * options.blockSize, options.blockSize)
            val response = AddressLengthTag.from(buffer, tag)
            serverConnection.send(executorId = 1, response, new UCXTagCallback {

              override def onSuccess(alt: AddressLengthTag): Unit = {
                buffer.close()
              }

              override def onError(alt: AddressLengthTag,
                                   ucsStatus: Int, errorMsg: String): Unit = {
                buffer.close()
              }

              override def onMessageStarted(ucxMessage: UcpRequest): Unit = { }

              override def onCancel(alt: AddressLengthTag): Unit = { }
            })
          })
        }
      }
      while (txs.forall(t => (t.getStatus == TransactionStatus.InProgress) ||
        (t.getStatus == TransactionStatus.NotStarted))) {}
    }
    sendMemory.close()
  }

  private def startClient(options: PerfOptions): Unit = {
    val connection = ucx.getConnection(0,
      options.remoteAddress.getHostString, options.remoteAddress.getPort)
    val resultSize = options.numThreads * options.numBlocks * options.blockSize
    val receiveMemory: MemoryBuffer =
      if (options.memoryType == UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_CUDA) {
      DeviceMemoryBuffer.allocate(resultSize)
    } else {
      HostMemoryBuffer.allocate(resultSize)
    }
    val rpcBuf = ByteBuffer.allocateDirect(1)

    val threadPool =  Executors.newFixedThreadPool(options.numThreads)

    for (i <- 1 to options.numIterations) {
      val startTime = System.nanoTime()
      val countDownLatch = new CountDownLatch(options.numThreads)

      for (tid <- 0 until options.numThreads) {
        threadPool.execute(() => {
          val txs = new Array[Transaction](options.numBlocks)
          for (blockId <- 0 until options.numBlocks) {
            val tag = tid * options.numBlocks + blockId
            val block = receiveMemory.slice((tid * options.numBlocks * options.blockSize) +
                blockId * options.blockSize, options.blockSize)
            val response = AddressLengthTag.from(block, tag)
            val request = AddressLengthTag.from(rpcBuf, tag)
            txs(blockId) = connection.request(request, response, tx => {
              block.close()
            })
          }
          while (txs.forall(t => (t.getStatus == TransactionStatus.InProgress) ||
            (t.getStatus == TransactionStatus.NotStarted))) {}
          countDownLatch.countDown()
        })
      }

      countDownLatch.await()
      val elapsedTime = System.nanoTime() - startTime
      val totalTime  = if (elapsedTime < TimeUnit.MILLISECONDS.toNanos(1)) {
        s"$elapsedTime ns"
      } else {
        s"${TimeUnit.NANOSECONDS.toMillis(elapsedTime)} ms"
      }
      val throughput: Double = (resultSize / 1024.0D / 1024.0D / 1024.0D) /
        (elapsedTime / 1e9D)
      if ((i % 100 == 0) || i == options.numIterations) {
        println(f"${s"[$i/${options.numIterations}]"}%12s" +
          s" numBlocks: ${options.numBlocks}" +
          s" numThreads: ${options.numThreads}" +
          s" size: $options.blockSize}," +
          s" total size: ${resultSize * options.numThreads}," +
          f" time: $totalTime%3s" +
          f" throughput: $throughput%.5f GB/s")
      }
    }

    receiveMemory.close()
    threadPool.shutdown()
  }

  def main(args: Array[String]): Unit = {
    val perfOptions = parseOptions(args)

    GpuDeviceManager.initializeMemory(Option(perfOptions.gpuId), Option(rapidsConf))

    var host = System.getenv("SPARK_LOCAL_IP")
    if (host == null) {
      host = "0.0.0.0"
    }

    val blockManagerId = if (perfOptions.remoteAddress == null) "0" else "1"
    val blockManager = TrampolineUtil.newBlockManagerId(blockManagerId, host,
      perfOptions.serverPort, None)
    ucx = new UCX(blockManager, rapidsConf)
    ucx.init()
    ucx.startManagementPort(host, perfOptions.serverPort)

    if (perfOptions.remoteAddress == null) {
      startServer(perfOptions)
    } else {
      startClient(perfOptions)
    }

    ucx.close()
  }
}
