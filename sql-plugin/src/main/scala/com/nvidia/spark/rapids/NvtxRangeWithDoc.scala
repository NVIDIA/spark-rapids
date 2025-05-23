/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import java.io.{File, FileOutputStream}
import scala.collection.mutable

sealed case class NvtxId private(name: String, color: NvtxColor, doc: String) {
  def help(): Unit = println(s"$name|$doc")

  def push(): NvtxId = {
    NvtxRange.pushRange(name, color)
    this
  }

  def pop(): Unit = NvtxRange.popRange()

  def apply[V](block: => V): V = {
    try {
      push()
      block
    } finally {
      pop()
    }
  }
}

object NvtxRegistry {
  val registeredRanges: mutable.Map[String, NvtxId] = mutable.Map[String, NvtxId]()

  private def register(id: NvtxId): Unit = {
    if (registeredRanges.contains(id.name)) {
      throw new IllegalArgumentException(s"Collision detected for key: ${id.name}")
    } else {
      registeredRanges += (id.name -> id)
    }
  }

  val ACQUIRE_GPU: NvtxId = NvtxId("Acquire GPU", NvtxColor.RED,
      "Time waiting for GPU semaphore to be acquired")

  val RELEASE_GPU: NvtxId = NvtxId("Release GPU", NvtxColor.RED,
    "Releasing the GPU semaphore")

  val THREADED_WRITER_WRITE: NvtxId = NvtxId("ThreadedWriter.write", NvtxColor.RED,
    "Rapids Shuffle Manager (multi threaded) writing")

  val THREADED_READER_READ: NvtxId = NvtxId("ThreadedReader.read", NvtxColor.PURPLE,
    "Rapids Shuffle Manager (multi threaded) reading")

  val WAITING_FOR_WRITES: NvtxId = NvtxId("WaitingForWrites", NvtxColor.PURPLE,
    "Rapids Shuffle Manager (multi threaded) is waiting for any queued writes to finish before " +
      "finalizing the map output writer")

  val COMMIT_SHUFFLE: NvtxId = NvtxId("CommitShuffle", NvtxColor.RED,
    "After all temporary shuffle writes are done, produce a single file " +
      "(shuffle_[map_id]_0) in the commit phase")

  val PARALLEL_DESERIALIZER_ITERATOR_NEXT: NvtxId = NvtxId("ParallelDeserializerIterator.next",
    NvtxColor.CYAN, "Calling next on the MT shuffle reader iterator")

  val BATCH_WAIT: NvtxId = NvtxId("BatchWait", NvtxColor.CYAN,
    "Rapids Shuffle Manager (multi threaded) reader blocked waiting for batches to finish decoding")

  val QUEUE_FETCHED: NvtxId = NvtxId("queueFetched", NvtxColor.YELLOW, "MT shuffle manager is " +
    "using the RapidsShuffleBlockFetcherIterator to queue the next set of fetched results")

  val RAPIDS_CACHING_WRITER_WRITE: NvtxId = NvtxId("RapidsCachingWriter.write", NvtxColor.CYAN,
    "Rapids Shuffle Manager (ucx) writing")

  val GET_MAP_SIZES_BY_EXEC_ID: NvtxId = NvtxId("getMapSizesByExecId", NvtxColor.CYAN,
    "Call to internal Spark API for retrieving size and location of shuffle map output blocks")

  val GPU_COALESCE_BATCHES_COLLECT: NvtxId = NvtxId("GpuCoalesceBatches: collect", NvtxColor.BLUE,
    "GPU combining of small batches post-kernel processing")

  val BUILD_BATCH_COLLECT: NvtxId = NvtxId("build batch: collect", NvtxColor.BLUE,
    "Perform a join where the build side fits in a single GPU batch")

  val GPU_COALESCE_ITERATOR: NvtxId = NvtxId("AbstractGpuCoalesceIterator", NvtxColor.BLUE,
    "Default range for a code path in the AbstractGpuCoalesceIterator for an op which " +
      "is not explicitly documented in its own range")

  val SHUFFLED_JOIN_STREAM: NvtxId = NvtxId("shuffled join stream", NvtxColor.BLUE,
    "GpuShuffledHashJoinExec op is preparing build batches for join")

  val HASH_JOIN_BUILD: NvtxId = NvtxId("hash join build", NvtxColor.BLUE,
    "")

  val PROBE_LEFT: NvtxId = NvtxId("probe left", NvtxColor.BLUE,
    "Probing the left side of a join input iterator to get the data size for preparing the join")

  val PROBE_RIGHT: NvtxId = NvtxId("probe right", NvtxColor.BLUE,
    "Probing the right side of a join input iterator to get the data size for preparing the join")

  val FETCH_JOIN_STREAM: NvtxId = NvtxId("fetch join stream", NvtxColor.BLUE,
    "stream iterator time for GpuShuffleSizeHashJoinExec")

  val BROADCAST_JOIN_STREAM: NvtxId = NvtxId("broadcast join stream", NvtxColor.BLUE,
    "GpuBroadcastHashJoinExec.getBroadcastBuiltBatchAndStreamIter -  Gets the ColumnarBatch for " +
      "the build side and the stream iterator by acquiring the GPU only after first stream batch " +
      "has been streamed to GPU.")

  def init(): Unit = {
    register(ACQUIRE_GPU)
    register(RELEASE_GPU)
    register(THREADED_WRITER_WRITE)
    register(THREADED_READER_READ)
    register(WAITING_FOR_WRITES)
    register(COMMIT_SHUFFLE)
    register(PARALLEL_DESERIALIZER_ITERATOR_NEXT)
    register(BATCH_WAIT)
    register(QUEUE_FETCHED)
    register(RAPIDS_CACHING_WRITER_WRITE)
    register(GET_MAP_SIZES_BY_EXEC_ID)
    register(GPU_COALESCE_BATCHES_COLLECT)
    register(BUILD_BATCH_COLLECT)
    register(GPU_COALESCE_ITERATOR)
    register(SHUFFLED_JOIN_STREAM)
    register(HASH_JOIN_BUILD)
    register(PROBE_LEFT)
    register(PROBE_RIGHT)
    register(FETCH_JOIN_STREAM)
    register(BROADCAST_JOIN_STREAM)
  }
}

object NvtxRangeDocs {
  def helpCommon(): Unit = {
    println("---")
    println("layout: page")
    println("title: NVTX Ranges")
    println("nav_order: 5")
    println("parent: Developer Overview")
    println("---")
    println(s"<!-- Generated by NvtxRangeDocs.help. DO NOT EDIT! -->")
    // scalastyle:off line.size.limit
    println("""# RAPIDS Accelerator for Apache Spark Nvtx Range Glossary
              |The following is the list of Nvtx ranges that are used throughout
              |the plugin. To add your own Nvtx range to the code, create an NvtxId
              |entry in NvtxRangeWithDoc.scala and create an `NvtxRangeWithDoc` in the
              |code location that you want to cover, passing in the newly created NvtxId.
              |
              |See [nvtx_profiling.md](https://nvidia.github.io/spark-rapids/docs/dev/nvtx_profiling.html) for more info.
              |
              |""".stripMargin)
    // scalastyle:on line.size.limit
    println("\n## Nvtx Ranges\n")
    println("Name | Description")
    println("-----|-------------")
  }

  def main(args: Array[String]): Unit = {
    NvtxRegistry.init()
    val configs = new FileOutputStream(new File(args(0)))
    Console.withOut(configs) {
      Console.withErr(configs) {
        helpCommon()
        NvtxRegistry.registeredRanges.values.foreach(_.help())
      }
    }
  }
}
