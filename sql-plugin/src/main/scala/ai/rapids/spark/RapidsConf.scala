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

  val RMM_DEBUG = conf("spark.rapids.memory.gpu.debug.enabled")
    .doc("If memory management is enabled and this is true GPU memory allocations are " +
      "tracked and printed out when the process exits. This should not be used in production.")
    .booleanConf
    .createWithDefault(false)

  val RMM_ASYNC_SPILL_PCT = conf("spark.rapids.memory.gpu.asyncSpillPct")
    .doc("When reported GPU memory usage is above this percentage, " +
      "cached data will spill asynchronously.")
    .doubleConf
    .createWithDefault(0.75)

  val RMM_SPILL_PCT = conf("spark.rapids.memory.gpu.spillPct")
    .doc("When reported GPU memory usage is above this percentage, " +
      "the allocator thread will block until spilling of cached data has completed.")
    .doubleConf
    .createWithDefault(0.85)

  val RMM_ALLOC_PCT = conf("spark.rapids.memory.gpu.allocPct")
    .doc("The percentage of total GPU memory that should be initially allocated " +
      "for pooled memory. Extra memory will be allocated as needed, but it may " +
      "result in more fragmentation.")
    .doubleConf
    .createWithDefault(0.9)

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

  val GPU_BATCH_SIZE_ROWS = conf("spark.rapids.sql.batchSizeRows")
    .doc("Set the target number of rows for a GPU batch. Splits sizes for input data " +
      "is covered by separate configs.")
    .integerConf
    .createWithDefault(1000000)

  val MAX_READER_BATCH_SIZE = conf("spark.rapids.sql.reader.batchSizeRows")
    .doc("Maximum number of rows the reader reads at a time")
    .integerConf
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

  val STRING_GPU_HASH_GROUP_BY_ENABLED = conf("spark.rapids.sql.stringHashGroupBy.enabled")
    .doc("Config to allow grouping by strings using the GPU in the hash aggregate. " +
      "Currently they are really slow")
    .booleanConf
    .createWithDefault(false)

  val ENABLE_TOTAL_ORDER_SORT = conf("spark.rapids.sql.totalOrderSort.enabled")
    .doc("Allow for total ordering sort where the partitioning runs on CPU and " +
      "sort runs on GPU.")
    .booleanConf
    .createWithDefault(false)

  val ENABLE_REPLACE_SORTMERGEJOIN = conf("spark.rapids.sql.replaceSortMergeJoin.enabled")
    .doc("Allow replacing sortMergeJoin with HashJoin")
    .booleanConf
    .createWithDefault(true)

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
      println("""# Rapids Plugin 4 Spark Configuration
        |The following is the list of options that `rapids-plugin-4-spark` supports.
        |
        |On startup use: `--conf [conf key]=[conf value]`. For example:
        |
        |```
        |${SPARK_HOME}/bin/spark --jars 'rapids-4-spark-0.1-SNAPSHOT.jar,cudf-0.10-SNAPSHOT-cuda10.jar' \
        |--conf spark.sql.extensions=ai.rapids.spark.Plugin \
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

  lazy val isTestEnabled: Boolean = get(TEST_CONF)

  lazy val testingAllowedNonGpu: Seq[String] = get(TEST_ALLOWED_NONGPU)

  lazy val isRmmDebugEnabled: Boolean = get(RMM_DEBUG)

  lazy val isUvmEnabled: Boolean = get(UVM_ENABLED)

  lazy val isPooledMemEnabled: Boolean = get(POOLED_MEM)

  lazy val rmmSpillPct: Double = get(RMM_SPILL_PCT)

  lazy val rmmAsyncSpillPct: Double = get(RMM_ASYNC_SPILL_PCT)

  lazy val rmmAllocPct: Double = get(RMM_ALLOC_PCT)

  lazy val hasNans: Boolean = get(HAS_NANS)

  lazy val gpuTargetBatchSizeRows: Integer = get(GPU_BATCH_SIZE_ROWS)

  lazy val isFloatAggEnabled: Boolean = get(ENABLE_FLOAT_AGG)

  lazy val stringHashGroupByEnabled: Boolean = get(STRING_GPU_HASH_GROUP_BY_ENABLED)

  lazy val explain: String = get(EXPLAIN)

  lazy val maxReadBatchSize: Int = get(MAX_READER_BATCH_SIZE)

  lazy val parquetDebugDumpPrefix: String = get(PARQUET_DEBUG_DUMP_PREFIX)

  lazy val orcDebugDumpPrefix: String = get(ORC_DEBUG_DUMP_PREFIX)

  lazy val hashAggReplaceMode: String = get(HASH_AGG_REPLACE_MODE)

  lazy val enableTotalOrderSort: Boolean = get(ENABLE_TOTAL_ORDER_SORT)

  lazy val enableReplaceSortMergeJoin: Boolean = get(ENABLE_REPLACE_SORTMERGEJOIN)

  def isOperatorEnabled(key: String, incompat: Boolean): Boolean = {
    val default = !incompat || (incompat && isIncompatEnabled)
    conf.get(key).map(toBoolean(_, key)).getOrElse(default)
  }
}
