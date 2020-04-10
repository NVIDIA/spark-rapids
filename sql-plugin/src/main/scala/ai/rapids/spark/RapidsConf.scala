/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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
package ai.rapids.spark

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.{ByteUnit, JavaUtils}
import org.apache.spark.sql.internal.SQLConf

object ConfHelper {
  def toBoolean(s: String, key: String): Boolean = {
    try {
      s.trim.toBoolean
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be boolean, but was $s")
    }
  }

  def toInteger(s: String, key: String): Integer = {
    try {
      s.trim.toInt
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be integer, but was $s")
    }
  }

  def toLong(s: String, key: String): Long = {
    try {
      s.trim.toLong
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be long, but was $s")
    }
  }

  def toDouble(s: String, key: String): Double = {
    try {
      s.trim.toDouble
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be integer, but was $s")
    }
  }

  def stringToSeq(str: String): Seq[String] = {
    str.split(",").map(_.trim()).filter(_.nonEmpty)
  }

  def stringToSeq[T](str: String, converter: String => T): Seq[T] = {
    stringToSeq(str).map(converter)
  }

  def seqToString[T](v: Seq[T], stringConverter: T => String): String = {
    v.map(stringConverter).mkString(",")
  }

  def byteFromString(str: String, unit: ByteUnit, key: String): Long = {
    val (input, multiplier) =
      if (str.length() > 0 && str.charAt(0) == '-') {
        (str.substring(1), -1)
      } else {
        (str, 1)
      }
    multiplier * JavaUtils.byteStringAs(input, unit)
  }
}

abstract class ConfEntry[T](val key: String, val converter: String => T,
    val doc: String, val isInternal: Boolean) {

  def get(conf: Map[String, String]): T
  def help(asTable: Boolean = false): Unit

  override def toString: String = key
}

class ConfEntryWithDefault[T](key: String, converter: String => T, doc: String,
    isInternal: Boolean, val defaultValue: T)
  extends ConfEntry[T](key, converter, doc, isInternal) {

  override def get(conf: Map[String, String]): T = {
    conf.get(key).map(converter).getOrElse(defaultValue)
  }

  override def help(asTable: Boolean = false): Unit = {
    if (!isInternal) {
      if (asTable) {
        println(s"${key}|${doc}|${defaultValue}")
      } else {
        println(s"${key}:")
        println(s"\t${doc}")
        println(s"\tdefault ${defaultValue}")
        println()
      }
    }
  }
}

class TypedConfBuilder[T](
    val parent: ConfBuilder,
    val converter: String => T,
    val stringConverter: T => String) {

  def this(parent: ConfBuilder, converter: String => T) = {
    this(parent, converter, Option(_).map(_.toString).orNull)
  }

  /** Apply a transformation to the user-provided values of the config entry. */
  def transform(fn: T => T): TypedConfBuilder[T] = {
    new TypedConfBuilder(parent, s => fn(converter(s)), stringConverter)
  }

  /** Checks if the user-provided value for the config matches the validator. */
  def checkValue(validator: T => Boolean, errorMsg: String): TypedConfBuilder[T] = {
    transform { v =>
      if (!validator(v)) {
        throw new IllegalArgumentException(errorMsg)
      }
      v
    }
  }

  def createWithDefault(value: T): ConfEntry[T] = {
    val ret = new ConfEntryWithDefault[T](parent.key, converter,
      parent.doc, parent.isInternal, value)
    parent.register(ret)
    ret
  }

  /** Turns the config entry into a sequence of values of the underlying type. */
  def toSequence: TypedConfBuilder[Seq[T]] = {
    new TypedConfBuilder(parent, ConfHelper.stringToSeq(_, converter), ConfHelper.seqToString(_, stringConverter))
  }
}

class ConfBuilder(val key: String, val register: ConfEntry[_] => Unit) {

  import ConfHelper._

  var doc: String = null
  var isInternal: Boolean = false

  def doc(data: String): ConfBuilder = {
    this.doc = data
    this
  }

  def internal(): ConfBuilder = {
    this.isInternal = true
    this
  }

  def booleanConf: TypedConfBuilder[Boolean] = {
    new TypedConfBuilder[Boolean](this, toBoolean(_, key))
  }

  def bytesConf(unit: ByteUnit): TypedConfBuilder[Long] = {
    new TypedConfBuilder[Long](this, byteFromString(_, unit, key))
  }

  def integerConf: TypedConfBuilder[Integer] = {
    new TypedConfBuilder[Integer](this, toInteger(_, key))
  }

  def longConf: TypedConfBuilder[Long] = {
    new TypedConfBuilder[Long](this, toLong(_, key))
  }

  def doubleConf: TypedConfBuilder[Double] = {
    new TypedConfBuilder(this, toDouble(_, key))
  }

  def stringConf: TypedConfBuilder[String] = {
    new TypedConfBuilder[String](this, identity[String])
  }
}

object RapidsConf {
  private val registeredConfs = new ListBuffer[ConfEntry[_]]()

  private def register(entry: ConfEntry[_]): Unit = {
    registeredConfs += entry
  }

  def conf(key: String): ConfBuilder = {
    new ConfBuilder(key, register)
  }

  // Resource Configuration

  val PINNED_POOL_SIZE = conf("spark.rapids.memory.pinnedPool.size")
    .doc("The size of the pinned memory pool in bytes unless otherwise specified. " +
      "Use 0 to disable the pool.")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(0)

  val RMM_DEBUG = conf("spark.rapids.memory.gpu.debug")
    .doc("Provides a log of GPU memory allocations and frees. If set to " +
      "STDOUT or STDERR the logging will go there. Setting it to NONE disables logging. " +
      "All other values are reserved for possible future expansion and in the mean time will " +
      "disable logging.")
    .stringConf
    .createWithDefault("NONE")

  val RMM_ALLOC_FRACTION = conf("spark.rapids.memory.gpu.allocFraction")
    .doc("The fraction of total GPU memory that should be initially allocated " +
      "for pooled memory. Extra memory will be allocated as needed, but it may " +
      "result in more fragmentation.")
    .doubleConf
    .checkValue(v => v >= 0 && v <= 1, "The fraction value must be in [0, 1].")
    .createWithDefault(0.9)

  val RMM_SPILL_ASYNC_START = conf("spark.rapids.memory.gpu.spillAsyncStart")
    .doc("Fraction of device memory utilization at which data will start " +
        "spilling asynchronously to free up device memory")
    .doubleConf
    .checkValue(v => v >= 0 && v <= 1, "The fraction value must be in [0, 1].")
    .createWithDefault(0.9)

  val RMM_SPILL_ASYNC_STOP = conf("spark.rapids.memory.gpu.spillAsyncStop")
    .doc("Fraction of device memory utilization at which data will stop " +
        "spilling asynchronously to free up device memory")
    .doubleConf
    .checkValue(v => v >= 0 && v <= 1, "The fraction value must be in [0, 1].")
    .createWithDefault(0.8)

  val HOST_SPILL_STORAGE_SIZE = conf("spark.rapids.memory.host.spillStorageSize")
    .doc("Amount of off-heap host memory to use for buffering spilled GPU data " +
        "before spilling to local disk")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(ByteUnit.GiB.toBytes(1))

  val POOLED_MEM = conf("spark.rapids.memory.gpu.pooling.enabled")
    .doc("Should RMM act as a pooling allocator for GPU memory, or should it just pass " +
      "through to CUDA memory allocation directly.")
    .booleanConf
    .createWithDefault(true)

  val UVM_ENABLED = conf("spark.rapids.memory.uvm.enabled")
    .doc("UVM or universal memory can allow main host memory to act essentially as swap " +
      "for device(GPU) memory. This allows the GPU to process more data than fits in memory, but " +
      "can result in slower processing. This is an experimental feature.")
    .booleanConf
    .createWithDefault(false)

  val CONCURRENT_GPU_TASKS = conf("spark.rapids.sql.concurrentGpuTasks")
      .doc("Set the number of tasks that can execute concurrently per GPU. " +
          "Tasks may temporarily block when the number of concurrent tasks in the executor " +
          "exceeds this amount. Allowing too many concurrent tasks on the same GPU may lead to " +
          "GPU out of memory errors.")
      .integerConf
      .createWithDefault(1)

  val SHUFFLE_SPILL_THREADS = conf("spark.rapids.sql.shuffle.spillThreads")
    .doc("Number of threads used to spill shuffle data to disk in the background.")
    .integerConf
    .createWithDefault(6)

  val GPU_BATCH_SIZE_BYTES = conf("spark.rapids.sql.batchSizeBytes")
    .doc("Set the target number of bytes for a GPU batch. Splits sizes for input data " +
      "is covered by separate configs.")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(Integer.MAX_VALUE)

  val MAX_READER_BATCH_SIZE_ROWS = conf("spark.rapids.sql.reader.batchSizeRows")
    .doc("Soft limit on the maximum number of rows the reader will read per batch. The orc and parquet " +
      "readers will read row groups until this limit is met or exceeded. The limit is respected by the csv reader.")
    .integerConf
    .createWithDefault(Integer.MAX_VALUE)

  val MAX_READER_BATCH_SIZE_BYTES = conf("spark.rapids.sql.reader.batchSizeBytes")
    .doc("Soft limit on the maximum number of bytes the reader reads per batch. The readers " +
      "will read chunks of data until this limit is met or exceeded. Note that the reader may estimate the " +
      "number of bytes that will be used on the GPU in some cases based on the schema and number of rows in " +
      "each batch.")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(Integer.MAX_VALUE)

  // Internal Features

  val EXPORT_COLUMNAR_RDD = conf("spark.rapids.sql.exportColumnarRdd")
    .doc("Spark has no simply way to export columnar RDD data.  This turns on special " +
      "processing/tagging that allows the RDD to be picked back apart into a Columnar RDD.")
    .internal()
    .booleanConf
    .createWithDefault(false)

  // ENABLE/DISABLE PROCESSING

  val SQL_ENABLED = conf("spark.rapids.sql.enabled")
    .doc("Enable (true) or disable (false) sql operations on the GPU")
    .booleanConf
    .createWithDefault(true)

  val INCOMPATIBLE_OPS = conf("spark.rapids.sql.incompatibleOps.enabled")
    .doc("For operations that work, but are not 100% compatible with the Spark equivalent " +
      "set if they should be enabled by default or disabled by default.")
    .booleanConf
    .createWithDefault(false)

  val HAS_NANS = conf("spark.rapids.sql.hasNans")
    .doc("Config to indicate if your data has NaN's. Cudf doesn't " +
      "currently support NaN's properly so you can get corrupt data if you have NaN's in your " +
      "data and it runs on the GPU.")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_FLOAT_AGG = conf("spark.rapids.sql.variableFloatAgg.enabled")
    .doc("Spark assumes that all operations produce the exact same result each time. " +
      "This is not true for some floating point aggregations, which can produce slightly " +
      "different results on the GPU as the aggregation is done in parallel.  This can enable " +
      "those operations if you know the query is only computing it once.")
    .booleanConf
    .createWithDefault(false)

  val ENABLE_REPLACE_SORTMERGEJOIN = conf("spark.rapids.sql.replaceSortMergeJoin.enabled")
    .doc("Allow replacing sortMergeJoin with HashJoin")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_HASH_OPTIMIZE_SORT = conf("spark.rapids.sql.hashOptimizeSort.enabled")
    .doc("Whether sorts should be inserted after some hashed operations to improve " +
      "output ordering. This can improve output file sizes when saving to columnar formats.")
    .booleanConf
    .createWithDefault(false)

  // INTERNAL TEST AND DEBUG CONFIGS

  val TEST_CONF = conf("spark.rapids.sql.test.enabled")
    .doc("Intended to be used by unit tests, if enabled all operations must run on the GPU " +
      "or an error happens.")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val TEST_ALLOWED_NONGPU = conf("spark.rapids.sql.test.allowedNonGpu")
    .doc("Comma separate string of exec or expression class names that are allowed " +
      "to not be GPU accelerated for testing.")
    .internal()
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  val PARQUET_DEBUG_DUMP_PREFIX = conf("spark.rapids.sql.parquet.debug.dumpPrefix")
    .doc("A path prefix where Parquet split file data is dumped for debugging.")
    .internal()
    .stringConf
    .createWithDefault(null)

  val ORC_DEBUG_DUMP_PREFIX = conf("spark.rapids.sql.orc.debug.dumpPrefix")
    .doc("A path prefix where ORC split file data is dumped for debugging.")
    .internal()
    .stringConf
    .createWithDefault(null)

  val HASH_AGG_REPLACE_MODE = conf("spark.rapids.sql.hashAgg.replaceMode")
    .doc("Only when hash aggregate exec has these modes (\"all\" by default): " +
      "\"all\" (try to replace all aggregates, default), " +
      "\"partial\" (exclusively replace partial aggregates), " +
      "\"final\" (exclusively replace final aggregates)")
    .internal()
    .stringConf
    .createWithDefault("all")

  // USER FACING SHUFFLE CONFIGS
  val SHUFFLE_UCX_ENABLE = conf("spark.rapids.shuffle.ucx.enabled")
    .doc("When set to true, enable the UCX transfer method for shuffle files.")
    .booleanConf
    .createWithDefault(false)

  val SHUFFLE_UCX_USE_WAKEUP = conf("spark.rapids.shuffle.ucx.useWakeup")
    .doc("When set to true, use UCX's event-based progress (epoll) in order to wake up " +
      "the progress thread when needed, instead of a hot loop.")
    .booleanConf
    .createWithDefault(false)

  val SHUFFLE_UCX_THROTTLE_ENABLE = conf("spark.rapids.shuffle.ucx.throttle.enabled")
    .doc("When set to true, enable the UCX throttle on send.")
    .booleanConf
    .createWithDefault(false)

  val SHUFFLE_UCX_WAIT_PERIOD_UPDATE_ENABLE = conf("spark.rapids.shuffle.ucx.wait.period.update.enabled")
    .doc("Decides if wait period should be updates based on transaction stats")
    .booleanConf
    .createWithDefault(false)

  val SHUFFLE_UCX_RECV_ASYNC = conf("spark.rapids.shuffle.ucx.receive.async")
    .doc("Decides if fetches should be async")
    .booleanConf
    .createWithDefault(false)

  val SHUFFLE_UCX_WAIT_PERIOD = conf("spark.rapids.shuffle.ucx.wait.period")
    .doc("Initial time to wait in ms for requests that cannot be processed by the shuffle manager")
    .integerConf
    .createWithDefault(5)

  val SHUFFLE_UCX_MAX_FETCH_WAIT_PERIOD = conf("spark.rapids.shuffle.ucx.fetch.wait.period")
    .doc("Maximum value of the Initial time to waitin sending fetch requests")
    .integerConf
    .createWithDefault(50)

  val SHUFFLE_UCX_HANDLE_LOCAL = conf("spark.rapids.shuffle.ucx.handleLocalShuffle")
    .doc("When set to true, allow UCX to transfer host-local shuffle files.")
    .booleanConf
    .createWithDefault(true)

  val SHUFFLE_UCX_HANDLE_REMOTE = conf("spark.rapids.shuffle.ucx.handleRemoteShuffle")
    .doc("When set to true, allow UCX to transfer remote shuffle files.")
    .booleanConf
    .createWithDefault(false)

  val SHUFFLE_UCX_MGMT_SERVER_HOST = conf("spark.rapids.shuffle.ucx.managementServerHost")
    .doc("The host to be used to start the management server")
    .stringConf
    .createWithDefault(null)

  // USER FACING DEBUG CONFIGS

  val EXPLAIN = conf("spark.rapids.sql.explain")
    .doc("Explain why some parts of a query were not placed on a GPU or not. Possible " +
      "values are ALL: print everything, NONE: print nothing, NOT_ON_GPU: print only did not go " +
      "on the GPU")
    .stringConf
    .createWithDefault("NONE")

  private def printToggleHeader(category: String): Unit = {
    println(s"\n### ${category}")
    println("Name | Description | Default Value | Incompatibilities")
    println("-----|-------------|---------------|------------------")
  }

  def help(asTable: Boolean = false): Unit = {
    if (asTable) {
      println(s"<!-- Generated by RapidsConf.help. DO NOT EDIT! -->")
      println("""# Rapids Plugin 4 Spark Configuration
        |The following is the list of options that `rapids-plugin-4-spark` supports.
        |
        |On startup use: `--conf [conf key]=[conf value]`. For example:
        |
        |```
        |${SPARK_HOME}/bin/spark --jars 'rapids-4-spark-0.1-SNAPSHOT.jar,cudf-0.14-SNAPSHOT-cuda10.jar' \
        |--conf spark.plugins=ai.rapids.spark.SQLPlugin \
        |--conf spark.rapids.sql.incompatibleOps.enabled=true
        |```
        |
        |At runtime use: `spark.conf.set("[conf key]", [conf value])`. For example:
        |
        |```
        |scala> spark.conf.set("spark.rapids.sql.incompatibleOps.enabled", true)
        |```""".stripMargin)

      println("\n## General Configuration")
      println("Name | Description | Default Value")
      println("-----|-------------|--------------")
    } else {
      println("Rapids Configs:")
    }
    registeredConfs.sortBy(_.key).foreach(_.help(asTable))
    if (asTable) {
      println("")
      println("""## Fine Tuning
        |_Rapids Plugin 4 Spark_ can be further configured to enable or disable specific
        |expressions and to control what parts of the query execute using the GPU or
        |the CPU.
        |
        |Please leverage the `spark.rapids.sql.explain` setting to get feedback from the
        |plugin as to why parts of a query may not be executing on the GPU.
        |
        |**NOTE:** Setting `spark.rapids.sql.incompatibleOps.enabled=true` will enable all
        |the settings in the table below which are not enabled by default due to
        |incompatibilities.""".stripMargin)

      printToggleHeader("Expressions")
    }
    GpuOverrides.expressions.values.toSeq.sortBy(_.tag.toString).foreach(_.confHelp(asTable))
    if (asTable) {
      printToggleHeader("Execution")
    }
    GpuOverrides.execs.values.toSeq.sortBy(_.tag.toString).foreach(_.confHelp(asTable))
    if (asTable) {
      printToggleHeader("Scans")
    }
    GpuOverrides.scans.values.toSeq.sortBy(_.tag.toString).foreach(_.confHelp(asTable))
    if (asTable) {
      printToggleHeader("Partitioning")
    }
    GpuOverrides.parts.values.toSeq.sortBy(_.tag.toString).foreach(_.confHelp(asTable))
  }
}

class RapidsConf(conf: Map[String, String]) extends Logging {
  import RapidsConf._
  import ConfHelper._

  def this(sqlConf: SQLConf) = {
    this(sqlConf.getAllConfs)
  }

  def this(sparkConf: SparkConf) = {
    this(Map(sparkConf.getAll: _*))
  }

  def get[T](entry: ConfEntry[T]): T = {
    entry.get(conf)
  }

  lazy val rapidsConfMap: util.Map[String, String] = conf.filterKeys(
    _.startsWith("spark.rapids.")).asJava

  lazy val isSqlEnabled: Boolean = get(SQL_ENABLED)

  lazy val exportColumnarRdd: Boolean = get(EXPORT_COLUMNAR_RDD)

  lazy val isIncompatEnabled: Boolean = get(INCOMPATIBLE_OPS)

  lazy val pinnedPoolSize: Long = get(PINNED_POOL_SIZE)

  lazy val concurrentGpuTasks: Int = get(CONCURRENT_GPU_TASKS)

  lazy val shuffleSpillThreads: Int = get(SHUFFLE_SPILL_THREADS)

  lazy val isTestEnabled: Boolean = get(TEST_CONF)

  lazy val testingAllowedNonGpu: Seq[String] = get(TEST_ALLOWED_NONGPU)

  lazy val rmmDebugLocation: String = get(RMM_DEBUG)

  lazy val isUvmEnabled: Boolean = get(UVM_ENABLED)

  lazy val isPooledMemEnabled: Boolean = get(POOLED_MEM)

  lazy val rmmAllocFraction: Double = get(RMM_ALLOC_FRACTION)

  lazy val rmmSpillAsyncStart: Double = get(RMM_SPILL_ASYNC_START)

  lazy val rmmSpillAsyncStop: Double = get(RMM_SPILL_ASYNC_STOP)

  lazy val hostSpillStorageSize: Long = get(HOST_SPILL_STORAGE_SIZE)

  lazy val hasNans: Boolean = get(HAS_NANS)

  lazy val gpuTargetBatchSizeBytes: Long = get(GPU_BATCH_SIZE_BYTES)

  lazy val isFloatAggEnabled: Boolean = get(ENABLE_FLOAT_AGG)

  lazy val explain: String = get(EXPLAIN)

  lazy val maxReadBatchSizeRows: Int = get(MAX_READER_BATCH_SIZE_ROWS)

  lazy val maxReadBatchSizeBytes: Long = get(MAX_READER_BATCH_SIZE_BYTES)

  lazy val parquetDebugDumpPrefix: String = get(PARQUET_DEBUG_DUMP_PREFIX)

  lazy val orcDebugDumpPrefix: String = get(ORC_DEBUG_DUMP_PREFIX)

  lazy val hashAggReplaceMode: String = get(HASH_AGG_REPLACE_MODE)

  lazy val enableReplaceSortMergeJoin: Boolean = get(ENABLE_REPLACE_SORTMERGEJOIN)

  lazy val enableHashOptimizeSort: Boolean = get(ENABLE_HASH_OPTIMIZE_SORT)

  lazy val shuffleUcxEnable: Boolean = get(SHUFFLE_UCX_ENABLE)

  lazy val shuffleUcxUseWakeup: Boolean = get(SHUFFLE_UCX_USE_WAKEUP)

  lazy val shuffleUcxThrottleEnable: Boolean = get(SHUFFLE_UCX_THROTTLE_ENABLE)

  lazy val shuffleUcxRecvAsync: Boolean = get(SHUFFLE_UCX_RECV_ASYNC)

  lazy val shuffleUcxUpdateWaitPeriod: Boolean = get(SHUFFLE_UCX_WAIT_PERIOD_UPDATE_ENABLE)

  lazy val shuffleUcxWaitPeriod: Int = get(SHUFFLE_UCX_WAIT_PERIOD)

  lazy val shuffleUcxMaxFetchWaitPeriod: Int = get(SHUFFLE_UCX_MAX_FETCH_WAIT_PERIOD)

  lazy val shuffleUcxHandleLocal: Boolean = get(SHUFFLE_UCX_HANDLE_LOCAL)

  lazy val shuffleUcxHandleRemote: Boolean = get(SHUFFLE_UCX_HANDLE_REMOTE)

  lazy val shuffleUcxMgmtHost: String = get(SHUFFLE_UCX_MGMT_SERVER_HOST)

  def isOperatorEnabled(key: String, incompat: Boolean): Boolean = {
    val default = !incompat || (incompat && isIncompatEnabled)
    conf.get(key).map(toBoolean(_, key)).getOrElse(default)
  }
}
