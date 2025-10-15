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

import java.io.{File, FileOutputStream}

import scala.collection.mutable

import ai.rapids.cudf.{NvtxColor, NvtxRange}

import org.apache.spark.internal.Logging

object RangeDebugger extends Logging {
  val threadLocalStack = new ThreadLocal[mutable.ArrayStack[NvtxId]] {
    override def initialValue(): mutable.ArrayStack[NvtxId] = mutable.ArrayStack[NvtxId]()
  }

  private def dumpOrderErrorMessage(popped: Option[NvtxId], elem: NvtxId): Unit = {
    logError(s"OUT OF ORDER POP of $elem")
    logError(s"TOP OF STACK IS ${popped.getOrElse("<nil>")}")
    val stackTrace = Thread.currentThread.getStackTrace
    stackTrace.foreach(elem => logError(elem.toString))
  }

  def push(elem: NvtxId): Unit = {
    threadLocalStack.get().push(elem)
  }

  def pop(elem: NvtxId): Unit = {
    val stack = threadLocalStack.get()
    if (stack.nonEmpty) {
      val popped = stack.pop()
      if (!popped.equals(elem)) {
        dumpOrderErrorMessage(Some(popped), elem)
      }
    } else {
      dumpOrderErrorMessage(None, elem)
    }
  }
}

sealed case class NvtxId private(name: String, color: NvtxColor, doc: String) {
  private val isEnabled = java.lang.Boolean.getBoolean("ai.rapids.cudf.nvtx.enabled")
  private val isDebug = java.lang.Boolean.getBoolean("ai.rapids.cudf.nvtx.debug")

  def help(): Unit = println(s"$name|$doc")

  def push(): NvtxId = {
    if (isEnabled) {
      NvtxRange.pushRange(name, color)
      if (isDebug) {
        RangeDebugger.push(this)
      }
    }
    this
  }

  def pop(): Unit = {
    if (isEnabled) {
      if (isDebug) {
        RangeDebugger.pop(this)
      }
      NvtxRange.popRange()
    }
  }

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
    "IO time on the build side data for the following join")

  val PROBE_LEFT: NvtxId = NvtxId("probe left", NvtxColor.BLUE,
    "Probing the left side of a join input iterator to get the data size for preparing the join")

  val PROBE_RIGHT: NvtxId = NvtxId("probe right", NvtxColor.BLUE,
    "Probing the right side of a join input iterator to get the data size for preparing the join")

  val FETCH_JOIN_STREAM: NvtxId = NvtxId("fetch join stream", NvtxColor.BLUE,
    "IO time on the stream side data for the following join")

  val BROADCAST_JOIN_STREAM: NvtxId = NvtxId("broadcast join stream", NvtxColor.BLUE,
    "time it takes to materialize a broadcast batch on the host")

  val GPU_KUDO_SERIALIZE: NvtxId = NvtxId("gpuKudoSerialize", NvtxColor.YELLOW,
    "Perform kudo serialization on the gpu")

  val GPU_KUDO_COPY_TO_HOST: NvtxId = NvtxId("gpuKudoCopyToHost", NvtxColor.GREEN,
    "copy gpu kudo serialized outputs back to the host")

  val GPU_KUDO_SLICE_BUFFERS: NvtxId = NvtxId("gpuKudoSliceBuffers", NvtxColor.RED,
    "slice kudo serialized buffers on host into partitions")

  val GPU_KUDO_WRITE_BUFFERS: NvtxId = NvtxId("gpuKudoWriteBuffers", NvtxColor.CYAN,
    "write sliced kudo serialized buffers to output blocks")

  val AGG_PRE_PROCESS: NvtxId = NvtxId("agg pre-process", NvtxColor.DARK_GREEN,
    "Pre-processing step for aggregation before calling cuDF aggregate, " +
      "including casting and struct creation")

  val AGG_REDUCE: NvtxId = NvtxId("agg reduce", NvtxColor.BLUE,
    "Reduction aggregation for operations without grouping keys")

  val AGG_GROUPBY: NvtxId = NvtxId("agg groupby", NvtxColor.BLUE,
    "Group-by aggregation using cuDF groupBy operation")

  val AGG_POST_PROCESS: NvtxId = NvtxId("agg post-process", NvtxColor.ORANGE,
    "Post-processing step for aggregation, including casting and struct decomposition")

  val HASH_PARTITION: NvtxId = NvtxId("hash partition", NvtxColor.PURPLE,
    "Partitioning data based on hash values")

  val HASH_PARTITION_SLICE: NvtxId = NvtxId("hash partition slice", NvtxColor.BLUE,
    "Slicing partitioned table into individual partitions")

  val SORT_COPY_BOUNDARIES: NvtxId = NvtxId("sort copy boundaries", NvtxColor.PURPLE,
    "Copying boundary data for sort operation")

  val SORT_TO_UNSAFE_ROW: NvtxId = NvtxId("sort to unsafe row", NvtxColor.RED,
    "Converting sorted data to unsafe row format")

  val SORT_LOWER_BOUNDARIES: NvtxId = NvtxId("sort lower boundaries", NvtxColor.ORANGE,
    "Computing lower boundaries for sort operation")

  val JOIN_FIRST_STREAM_BATCH: NvtxId = NvtxId("join first stream batch", NvtxColor.RED,
    "Fetching and processing first batch from stream side of join")

  val JOIN_ASYMMETRIC_PROBE_FETCH: NvtxId = NvtxId("join asymmetric probe fetch",
    NvtxColor.YELLOW, "Asymmetric join probe side data fetch")

  val JOIN_ASYMMETRIC_FETCH: NvtxId = NvtxId("join asymmetric fetch", NvtxColor.YELLOW,
    "Asymmetric join data fetch")

  val PARQUET_READ_FOOTER_BYTES: NvtxId = NvtxId("parquet read footer bytes",
    NvtxColor.YELLOW, "Reading raw footer bytes from Parquet file")

  val PARQUET_PARSE_FILTER_FOOTER: NvtxId = NvtxId("parquet parse filter footer",
    NvtxColor.RED, "Parsing and filtering Parquet footer by range")

  val PARQUET_READ_FOOTER: NvtxId = NvtxId("parquet read footer", NvtxColor.YELLOW,
    "Reading and parsing complete Parquet footer")

  val PARQUET_FILTER_BLOCKS: NvtxId = NvtxId("parquet filter blocks", NvtxColor.PURPLE,
    "Filtering Parquet row group blocks based on predicates")

  val PARQUET_READ_FILTERED_FOOTER: NvtxId = NvtxId("parquet read filtered footer",
    NvtxColor.YELLOW, "Reading filtered Parquet footer with selected row groups")

  val PARQUET_GET_BLOCKS_WITH_FILTER: NvtxId = NvtxId("parquet get blocks with filter",
    NvtxColor.CYAN, "Retrieving Parquet blocks after applying filters")

  val PARQUET_CLIP_SCHEMA: NvtxId = NvtxId("parquet clip schema", NvtxColor.DARK_GREEN,
    "Clipping Parquet schema to required columns")

  val PARQUET_BUFFER_FILE_SPLIT: NvtxId = NvtxId("parquet buffer file split",
    NvtxColor.YELLOW, "Splitting Parquet file into buffer chunks for reading")

  val ORC_BUFFER_FILE_SPLIT: NvtxId = NvtxId("orc buffer file split", NvtxColor.YELLOW,
    "Splitting ORC file into buffer chunks for reading")

  val AVRO_BUFFER_FILE_SPLIT: NvtxId = NvtxId("avro buffer file split", NvtxColor.YELLOW,
    "Splitting Avro file into buffer chunks for reading")

  val JSON_CONVERT_TABLE: NvtxId = NvtxId("json convert table", NvtxColor.RED,
    "Converting JSON table to desired schema type")

  val JSON_CONVERT_DATETIME: NvtxId = NvtxId("json convert datetime", NvtxColor.RED,
    "Converting JSON datetime types to Spark datetime types")

  val SHUFFLE_FETCH_FIRST_BATCH: NvtxId = NvtxId("shuffle fetch first batch",
    NvtxColor.YELLOW, "Fetching first batch in shuffle coalesce operation")

  val SHUFFLE_CONCAT_LOAD_BATCH: NvtxId = NvtxId("shuffle concat load batch",
    NvtxColor.YELLOW, "Concatenating and loading batch in shuffle operation")

  val CARTESIAN_PRODUCT_SERIALIZE: NvtxId = NvtxId("cartesian product serialize",
    NvtxColor.PURPLE, "Serializing batch for cartesian product operation")

  val CARTESIAN_PRODUCT_DESERIALIZE: NvtxId = NvtxId("cartesian product deserialize",
    NvtxColor.PURPLE, "Deserializing batch from cartesian product operation")

  val ROW_TO_COLUMNAR: NvtxId = NvtxId("row to columnar", NvtxColor.CYAN,
    "Converting row-based data to columnar format")

  val GENERATE_GET_ROW_BYTE_COUNT: NvtxId = NvtxId("generate get row byte count",
    NvtxColor.GREEN, "Computing byte count for generated rows")

  val GENERATE_ESTIMATE_REPETITION: NvtxId = NvtxId("generate estimate repetition",
    NvtxColor.BLUE, "Estimating repetition count for generate operation")

  val REDUCTION_MERGE_M2: NvtxId = NvtxId("reduction merge m2", NvtxColor.ORANGE,
    "Merging M2 values during variance/stddev reduction")

  val PROJECT_TIER: NvtxId = NvtxId("project tier", NvtxColor.ORANGE,
    "Executing tiered projection operation")

  val RANDOM_EXPR: NvtxId = NvtxId("random expr", NvtxColor.RED,
    "Generating random values in expression evaluation")

  val RAPIDS_CACHING_READER_READ: NvtxId = NvtxId("RapidsCachingReader.read",
    NvtxColor.DARK_GREEN, "Reading shuffle data from cache or remote transport")

  val RAPIDS_CACHING_READER_READ_LOCAL: NvtxId = NvtxId("RapidsCachingReader read local",
    NvtxColor.GREEN, "Reading shuffle blocks from local cache")

  val RAPIDS_SHUFFLE_ITERATOR_PREP: NvtxId = NvtxId("RapidsShuffleIterator prep",
    NvtxColor.BLUE, "Preparing shuffle iterator with cached and remote blocks")

  val RAPIDS_SHUFFLE_ITERATOR_NEXT: NvtxId = NvtxId("RapidsShuffleIterator.next",
    NvtxColor.RED, "Fetching next batch from Rapids shuffle iterator")

  val RAPIDS_SHUFFLE_ITERATOR_GOT_BATCH: NvtxId = NvtxId("RapidsShuffleIterator.gotBatch",
    NvtxColor.PURPLE, "Processing batch received from Rapids shuffle")

  val RAPIDS_CACHING_WRITER_CLOSE: NvtxId = NvtxId("RapidsCachingWriter.close",
    NvtxColor.CYAN, "Closing Rapids caching writer and finalizing shuffle output")

  val COLUMNAR_BATCH_SERIALIZE: NvtxId = NvtxId("Columnar batch serialize",
    NvtxColor.YELLOW, "Serializing columnar batch for shuffle or storage")

  val COLUMNAR_BATCH_SERIALIZE_ROW_ONLY: NvtxId = NvtxId("Columnar batch serialize row only",
    NvtxColor.YELLOW, "Serializing row-only batch (no GPU data)")

  val TRANSPORT_COPY_BUFFER: NvtxId = NvtxId("Transport copy buffer", NvtxColor.RED,
    "Copying buffer for Rapids shuffle transport")

  val BRING_BACK_TO_HOST: NvtxId = NvtxId("Bring back to host", NvtxColor.RED,
    "Copying GPU data back to host memory")

  val ROUND_ROBIN_PARTITION: NvtxId = NvtxId("Round robin partition", NvtxColor.PURPLE,
    "Partitioning data using round-robin strategy")

  val ROUND_ROBIN_PARTITION_SLICE: NvtxId = NvtxId("Round robin partition slice",
    NvtxColor.BLUE, "Slicing data for round-robin partitioning")

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
    register(GPU_KUDO_SERIALIZE)
    register(GPU_KUDO_COPY_TO_HOST)
    register(GPU_KUDO_SLICE_BUFFERS)
    register(AGG_PRE_PROCESS)
    register(AGG_REDUCE)
    register(AGG_GROUPBY)
    register(AGG_POST_PROCESS)
    register(HASH_PARTITION)
    register(HASH_PARTITION_SLICE)
    register(SORT_COPY_BOUNDARIES)
    register(SORT_TO_UNSAFE_ROW)
    register(SORT_LOWER_BOUNDARIES)
    register(JOIN_FIRST_STREAM_BATCH)
    register(JOIN_ASYMMETRIC_PROBE_FETCH)
    register(JOIN_ASYMMETRIC_FETCH)
    register(PARQUET_READ_FOOTER_BYTES)
    register(PARQUET_PARSE_FILTER_FOOTER)
    register(PARQUET_READ_FOOTER)
    register(PARQUET_FILTER_BLOCKS)
    register(PARQUET_READ_FILTERED_FOOTER)
    register(PARQUET_GET_BLOCKS_WITH_FILTER)
    register(PARQUET_CLIP_SCHEMA)
    register(PARQUET_BUFFER_FILE_SPLIT)
    register(ORC_BUFFER_FILE_SPLIT)
    register(AVRO_BUFFER_FILE_SPLIT)
    register(JSON_CONVERT_TABLE)
    register(JSON_CONVERT_DATETIME)
    register(SHUFFLE_FETCH_FIRST_BATCH)
    register(SHUFFLE_CONCAT_LOAD_BATCH)
    register(CARTESIAN_PRODUCT_SERIALIZE)
    register(CARTESIAN_PRODUCT_DESERIALIZE)
    register(ROW_TO_COLUMNAR)
    register(GENERATE_GET_ROW_BYTE_COUNT)
    register(GENERATE_ESTIMATE_REPETITION)
    register(REDUCTION_MERGE_M2)
    register(PROJECT_TIER)
    register(RANDOM_EXPR)
    register(RAPIDS_CACHING_READER_READ)
    register(RAPIDS_CACHING_READER_READ_LOCAL)
    register(RAPIDS_SHUFFLE_ITERATOR_PREP)
    register(RAPIDS_SHUFFLE_ITERATOR_NEXT)
    register(RAPIDS_SHUFFLE_ITERATOR_GOT_BATCH)
    register(RAPIDS_CACHING_WRITER_CLOSE)
    register(COLUMNAR_BATCH_SERIALIZE)
    register(COLUMNAR_BATCH_SERIALIZE_ROW_ONLY)
    register(TRANSPORT_COPY_BUFFER)
    register(BRING_BACK_TO_HOST)
    register(ROUND_ROBIN_PARTITION)
    register(ROUND_ROBIN_PARTITION_SLICE)
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
