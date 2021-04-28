/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
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
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, ListBuffer}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.{ByteUnit, JavaUtils}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
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

  def byteFromString(str: String, unit: ByteUnit): Long = {
    val (input, multiplier) =
      if (str.nonEmpty && str.head == '-') {
        (str.substring(1), -1)
      } else {
        (str, 1)
      }
    multiplier * JavaUtils.byteStringAs(input, unit)
  }

  def makeConfAnchor(key: String, text: String = null): String = {
    val t = if (text != null) text else key
    // The anchor cannot be too long, so for now
    val a = key.replaceFirst("spark.rapids.", "")
    "<a name=\"" + s"$a" + "\"></a>" + t
  }

  def getSqlFunctionsForClass[T](exprClass: Class[T]): Option[Seq[String]] = {
    sqlFunctionsByClass.get(exprClass.getCanonicalName)
  }

  lazy val sqlFunctionsByClass: Map[String, Seq[String]] = {
    val functionsByClass = new HashMap[String, Seq[String]]
    FunctionRegistry.expressions.foreach { case (sqlFn, (expressionInfo, _)) =>
      val className = expressionInfo.getClassName
      val fnSeq = functionsByClass.getOrElse(className, Seq[String]())
      val fnCleaned = if (sqlFn != "|") {
        sqlFn
      } else {
        "\\|"
      }
      functionsByClass.update(className, fnSeq :+ s"`$fnCleaned`")
    }
    functionsByClass.toMap
  }
}

abstract class ConfEntry[T](val key: String, val converter: String => T,
    val doc: String, val isInternal: Boolean) {

  def get(conf: Map[String, String]): T
  def get(conf: SQLConf): T
  def help(asTable: Boolean = false): Unit

  override def toString: String = key
}

class ConfEntryWithDefault[T](key: String, converter: String => T, doc: String,
    isInternal: Boolean, val defaultValue: T)
  extends ConfEntry[T](key, converter, doc, isInternal) {

  override def get(conf: Map[String, String]): T = {
    conf.get(key).map(converter).getOrElse(defaultValue)
  }

  override def get(conf: SQLConf): T = {
    val tmp = conf.getConfString(key, null)
    if (tmp == null) {
      defaultValue
    } else {
      converter(tmp)
    }
  }

  override def help(asTable: Boolean = false): Unit = {
    if (!isInternal) {
      if (asTable) {
        import ConfHelper.makeConfAnchor
        println(s"${makeConfAnchor(key)}|$doc|$defaultValue")
      } else {
        println(s"$key:")
        println(s"\t$doc")
        println(s"\tdefault $defaultValue")
        println()
      }
    }
  }
}

class OptionalConfEntry[T](key: String, val rawConverter: String => T, doc: String,
    isInternal: Boolean)
  extends ConfEntry[Option[T]](key, s => Some(rawConverter(s)), doc, isInternal) {

  override def get(conf: Map[String, String]): Option[T] = {
    conf.get(key).map(rawConverter)
  }

  override def get(conf: SQLConf): Option[T] = {
    val tmp = conf.getConfString(key, null)
    if (tmp == null) {
      None
    } else {
      Some(rawConverter(tmp))
    }
  }

  override def help(asTable: Boolean = false): Unit = {
    if (!isInternal) {
      if (asTable) {
        import ConfHelper.makeConfAnchor
        println(s"${makeConfAnchor(key)}|$doc|None")
      } else {
        println(s"$key:")
        println(s"\t$doc")
        println("\tNone")
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

  /** Check that user-provided values for the config match a pre-defined set. */
  def checkValues(validValues: Set[T]): TypedConfBuilder[T] = {
    transform { v =>
      if (!validValues.contains(v)) {
        throw new IllegalArgumentException(
          s"The value of ${parent.key} should be one of ${validValues.mkString(", ")}, but was $v")
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
    new TypedConfBuilder(parent, ConfHelper.stringToSeq(_, converter),
      ConfHelper.seqToString(_, stringConverter))
  }

  def createOptional: OptionalConfEntry[T] = {
    val ret = new OptionalConfEntry[T](parent.key, converter,
      parent.doc, parent.isInternal)
    parent.register(ret)
    ret
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
    new TypedConfBuilder[Long](this, byteFromString(_, unit))
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

  val GPU_OOM_DUMP_DIR = conf("spark.rapids.memory.gpu.oomDumpDir")
    .doc("The path to a local directory where a heap dump will be created if the GPU " +
      "encounters an unrecoverable out-of-memory (OOM) error. The filename will be of the " +
      "form: \"gpu-oom-<pid>.hprof\" where <pid> is the process ID.")
    .stringConf
    .createOptional

  private val RMM_ALLOC_MAX_FRACTION_KEY = "spark.rapids.memory.gpu.maxAllocFraction"
  private val RMM_ALLOC_RESERVE_KEY = "spark.rapids.memory.gpu.reserve"

  val RMM_ALLOC_FRACTION = conf("spark.rapids.memory.gpu.allocFraction")
    .doc("The fraction of total GPU memory that should be initially allocated " +
      "for pooled memory. Extra memory will be allocated as needed, but it may " +
      "result in more fragmentation. This must be less than or equal to the maximum limit " +
      s"configured via $RMM_ALLOC_MAX_FRACTION_KEY.")
    .doubleConf
    .checkValue(v => v >= 0 && v <= 1, "The fraction value must be in [0, 1].")
    .createWithDefault(0.9)

  val RMM_ALLOC_MAX_FRACTION = conf(RMM_ALLOC_MAX_FRACTION_KEY)
    .doc("The fraction of total GPU memory that limits the maximum size of the RMM pool. " +
        s"The value must be greater than or equal to the setting for $RMM_ALLOC_FRACTION. " +
        "Note that this limit will be reduced by the reserve memory configured in " +
        s"$RMM_ALLOC_RESERVE_KEY.")
    .doubleConf
    .checkValue(v => v >= 0 && v <= 1, "The fraction value must be in [0, 1].")
    .createWithDefault(1)

  val RMM_ALLOC_RESERVE = conf(RMM_ALLOC_RESERVE_KEY)
      .doc("The amount of GPU memory that should remain unallocated by RMM and left for " +
          "system use such as memory needed for kernels, kernel launches or JIT compilation.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(ByteUnit.MiB.toBytes(1024))

  val HOST_SPILL_STORAGE_SIZE = conf("spark.rapids.memory.host.spillStorageSize")
    .doc("Amount of off-heap host memory to use for buffering spilled GPU data " +
        "before spilling to local disk")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(ByteUnit.GiB.toBytes(1))

  val UNSPILL = conf("spark.rapids.memory.gpu.unspill.enabled")
    .doc("When a spilled GPU buffer is needed again, should it be unspilled, or only copied " +
        "back into GPU memory temporarily. Unspilling may be useful for GPU buffers that are " +
        "needed frequently, for example, broadcast variables; however, it may also increase GPU " +
        "memory usage")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val GDS_SPILL = conf("spark.rapids.memory.gpu.direct.storage.spill.enabled")
    .doc("Should GPUDirect Storage (GDS) be used to spill GPU memory buffers directly to disk. " +
      "GDS must be enabled and the directory `spark.local.dir` must support GDS. This is an " +
      "experimental feature. For more information on GDS, see " +
      "https://docs.nvidia.com/gpudirect-storage/.")
    .booleanConf
    .createWithDefault(false)

  val POOLED_MEM = conf("spark.rapids.memory.gpu.pooling.enabled")
    .doc("Should RMM act as a pooling allocator for GPU memory, or should it just pass " +
      "through to CUDA memory allocation directly. DEPRECATED: please use " +
      "spark.rapids.memory.gpu.pool instead.")
    .booleanConf
    .createWithDefault(true)

  val RMM_POOL = conf("spark.rapids.memory.gpu.pool")
    .doc("Select the RMM pooling allocator to use. Valid values are \"DEFAULT\", \"ARENA\", and " +
      "\"NONE\". With \"DEFAULT\", `rmm::mr::pool_memory_resource` is used; with \"ARENA\", " +
      "`rmm::mr::arena_memory_resource` is used. If set to \"NONE\", pooling is disabled and RMM " +
      "just passes through to CUDA memory allocation directly. Note: \"ARENA\" is the " +
      "recommended pool allocator if CUDF is built with Per-Thread Default Stream (PTDS), " +
      "as \"DEFAULT\" is known to be unstable (https://github.com/NVIDIA/spark-rapids/issues/1141)")
    .stringConf
    .createWithDefault("ARENA")

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
      "is covered by separate configs. The maximum setting is 2 GB to avoid exceeding the " +
      "cudf row count limit of a column.")
    .bytesConf(ByteUnit.BYTE)
    .checkValue(v => v >= 0 && v <= Integer.MAX_VALUE,
      s"Batch size must be positive and not exceed ${Integer.MAX_VALUE} bytes.")
    .createWithDefault(Integer.MAX_VALUE)

  val MAX_READER_BATCH_SIZE_ROWS = conf("spark.rapids.sql.reader.batchSizeRows")
    .doc("Soft limit on the maximum number of rows the reader will read per batch. " +
      "The orc and parquet readers will read row groups until this limit is met or exceeded. " +
      "The limit is respected by the csv reader.")
    .integerConf
    .createWithDefault(Integer.MAX_VALUE)

  val MAX_READER_BATCH_SIZE_BYTES = conf("spark.rapids.sql.reader.batchSizeBytes")
    .doc("Soft limit on the maximum number of bytes the reader reads per batch. " +
      "The readers will read chunks of data until this limit is met or exceeded. " +
      "Note that the reader may estimate the number of bytes that will be used on the GPU " +
      "in some cases based on the schema and number of rows in each batch.")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(Integer.MAX_VALUE)

  // Internal Features

  val UVM_ENABLED = conf("spark.rapids.memory.uvm.enabled")
    .doc("UVM or universal memory can allow main host memory to act essentially as swap " +
      "for device(GPU) memory. This allows the GPU to process more data than fits in memory, but " +
      "can result in slower processing. This is an experimental feature.")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val EXPORT_COLUMNAR_RDD = conf("spark.rapids.sql.exportColumnarRdd")
    .doc("Spark has no simply way to export columnar RDD data.  This turns on special " +
      "processing/tagging that allows the RDD to be picked back apart into a Columnar RDD.")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val STABLE_SORT = conf("spark.rapids.sql.stableSort.enabled")
      .doc("Enable or disable stable sorting. Apache Spark's sorting is typically a stable " +
          "sort, but sort stability cannot be guaranteed in distributed work loads because the " +
          "order in which upstream data arrives to a task is not guaranteed. Sort stability then " +
          "only matters when reading and sorting data from a file using a single task/partition. " +
          "Because of limitations in the plugin when you enable stable sorting all of the data " +
          "for a single task will be combined into a single batch before sorting. This currently " +
          "disables spilling from GPU memory if the data size is too large.")
      .booleanConf
      .createWithDefault(false)

  // METRICS

  val METRICS_LEVEL = conf("spark.rapids.sql.metrics.level")
      .doc("GPU plans can produce a lot more metrics than CPU plans do. In very large " +
          "queries this can sometimes result in going over the max result size limit for the " +
          "driver. Supported values include " +
          "DEBUG which will enable all metrics supported and typically only needs to be enabled " +
          "when debugging the plugin. " +
          "MODERATE which should output enough metrics to understand how long each part of the " +
          "query is taking and how much data is going to each part of the query. " +
          "ESSENTIAL which disables most metrics except those Apache Spark CPU plans will also " +
          "report or their equivalents.")
      .stringConf
      .transform(_.toUpperCase(java.util.Locale.ROOT))
      .checkValues(Set("DEBUG", "MODERATE", "ESSENTIAL"))
      .createWithDefault("MODERATE")

  // ENABLE/DISABLE PROCESSING

  val IMPROVED_TIMESTAMP_OPS =
    conf("spark.rapids.sql.improvedTimeOps.enabled")
      .doc("When set to true, some operators will avoid overflowing by converting epoch days " +
          " directly to seconds without first converting to microseconds")
      .booleanConf
      .createWithDefault(false)

  val SQL_ENABLED = conf("spark.rapids.sql.enabled")
    .doc("Enable (true) or disable (false) sql operations on the GPU")
    .booleanConf
    .createWithDefault(true)

  val UDF_COMPILER_ENABLED = conf("spark.rapids.sql.udfCompiler.enabled")
    .doc("When set to true, Scala UDFs will be considered for compilation as Catalyst expressions")
    .booleanConf
    .createWithDefault(false)

  val INCOMPATIBLE_OPS = conf("spark.rapids.sql.incompatibleOps.enabled")
    .doc("For operations that work, but are not 100% compatible with the Spark equivalent " +
      "set if they should be enabled by default or disabled by default.")
    .booleanConf
    .createWithDefault(false)

  val INCOMPATIBLE_DATE_FORMATS = conf("spark.rapids.sql.incompatibleDateFormats.enabled")
    .doc("When parsing strings as dates and timestamps in functions like unix_timestamp, some " +
         "formats are fully supported on the GPU and some are unsupported and will fall back to " + 
         "the CPU.  Some formats behave differently on the GPU than the CPU.  Spark on the CPU " +
         "interprets date formats with unsupported trailing characters as nulls, while Spark on " +
         "the GPU will parse the date with invalid trailing characters. More detail can be found " +
         "at [parsing strings as dates or timestamps]" + 
         "(compatibility.md#parsing-strings-as-dates-or-timestamps).")
      .booleanConf
      .createWithDefault(false)

  val IMPROVED_FLOAT_OPS = conf("spark.rapids.sql.improvedFloatOps.enabled")
    .doc("For some floating point operations spark uses one way to compute the value " +
      "and the underlying cudf implementation can use an improved algorithm. " +
      "In some cases this can result in cudf producing an answer when spark overflows. " +
      "Because this is not as compatible with spark, we have it disabled by default.")
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

  val DECIMAL_TYPE_ENABLED = conf("spark.rapids.sql.decimalType.enabled")
      .doc("Enable decimal type support on the GPU.  Decimal support on the GPU is limited to " +
          "less than 18 digits.  This can result in a lot of data movement to and from the GPU, " +
          "which can slow down processing in some cases.")
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

  val ENABLE_CAST_FLOAT_TO_DECIMAL = conf("spark.rapids.sql.castFloatToDecimal.enabled")
    .doc("Casting from floating point types to decimal on the GPU returns results that have " +
      "tiny difference compared to results returned from CPU.")
    .booleanConf
    .createWithDefault(false)

  val ENABLE_CAST_FLOAT_TO_STRING = conf("spark.rapids.sql.castFloatToString.enabled")
    .doc("Casting from floating point types to string on the GPU returns results that have " +
      "a different precision than the default results of Spark.")
    .booleanConf
    .createWithDefault(false)

  val ENABLE_CAST_FLOAT_TO_INTEGRAL_TYPES =
    conf("spark.rapids.sql.castFloatToIntegralTypes.enabled")
      .doc("Casting from floating point types to integral types on the GPU supports a " +
          "slightly different range of values when using Spark 3.1.0 or later. Refer to the CAST " +
          "documentation for more details.")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_CAST_STRING_TO_FLOAT = conf("spark.rapids.sql.castStringToFloat.enabled")
    .doc("When set to true, enables casting from strings to float types (float, double) " +
      "on the GPU. Currently hex values aren't supported on the GPU. Also note that casting from " +
      "string to float types on the GPU returns incorrect results when the string represents any " +
      "number \"1.7976931348623158E308\" <= x < \"1.7976931348623159E308\" " +
      "and \"-1.7976931348623158E308\" >= x > \"-1.7976931348623159E308\" in both these cases " +
      "the GPU returns Double.MaxValue while CPU returns \"+Infinity\" and \"-Infinity\" " +
      "respectively")
    .booleanConf
    .createWithDefault(false)

  val ENABLE_CAST_STRING_TO_DECIMAL = conf("spark.rapids.sql.castStringToDecimal.enabled")
    .doc("When set to true, enables casting from strings to decimal type on the GPU. Currently " +
      "string to decimal type on the GPU might produce results which slightly differed from the " +
      "correct results when the string represents any number exceeding the max precision that " +
      "CAST_STRING_TO_FLOAT can keep. For instance, the GPU returns 99999999999999987 given " +
      "input string \"99999999999999999\". The cause of divergence is that we can not cast " +
      "strings containing scientific notation to decimal directly. So, we have to cast strings " +
      "to floats firstly. Then, cast floats to decimals. The first step may lead to precision " +
      "loss.")
    .booleanConf
    .createWithDefault(false)

  val ENABLE_CAST_STRING_TO_TIMESTAMP = conf("spark.rapids.sql.castStringToTimestamp.enabled")
    .doc("When set to true, casting from string to timestamp is supported on the GPU. The GPU " +
      "only supports a subset of formats when casting strings to timestamps. Refer to the CAST " +
      "documentation for more details.")
    .booleanConf
    .createWithDefault(false)

  val ENABLE_CAST_STRING_TO_INTEGER = conf("spark.rapids.sql.castStringToInteger.enabled")
    .doc("When set to true, enables casting from strings to integer types (byte, short, " +
      "int, long) on the GPU. Casting from string to integer types on the GPU returns incorrect " +
      "results when the string represents a number larger than Long.MaxValue or smaller than " +
      "Long.MinValue.")
    .booleanConf
    .createWithDefault(false)

  val ENABLE_CAST_DECIMAL_TO_STRING = conf("spark.rapids.sql.castDecimalToString.enabled")
      .doc("When set to true, casting from decimal to string is supported on the GPU. The GPU " +
        "does NOT produce exact same string as spark produces, but producing strings which are " +
        "semantically equal. For instance, given input BigDecimal(123, -2), the GPU produces " +
        "\"12300\", which spark produces \"1.23E+4\".")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_INNER_JOIN = conf("spark.rapids.sql.join.inner.enabled")
      .doc("When set to true inner joins are enabled on the GPU")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_CROSS_JOIN = conf("spark.rapids.sql.join.cross.enabled")
      .doc("When set to true cross joins are enabled on the GPU")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_LEFT_OUTER_JOIN = conf("spark.rapids.sql.join.leftOuter.enabled")
      .doc("When set to true left outer joins are enabled on the GPU")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_RIGHT_OUTER_JOIN = conf("spark.rapids.sql.join.rightOuter.enabled")
      .doc("When set to true right outer joins are enabled on the GPU")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_FULL_OUTER_JOIN = conf("spark.rapids.sql.join.fullOuter.enabled")
      .doc("When set to true full outer joins are enabled on the GPU")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_LEFT_SEMI_JOIN = conf("spark.rapids.sql.join.leftSemi.enabled")
      .doc("When set to true left semi joins are enabled on the GPU")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_LEFT_ANTI_JOIN = conf("spark.rapids.sql.join.leftAnti.enabled")
      .doc("When set to true left anti joins are enabled on the GPU")
      .booleanConf
      .createWithDefault(true)

  // FILE FORMATS
  val ENABLE_PARQUET = conf("spark.rapids.sql.format.parquet.enabled")
    .doc("When set to false disables all parquet input and output acceleration")
    .booleanConf
    .createWithDefault(true)

  object ParquetReaderType extends Enumeration {
    val AUTO, COALESCING, MULTITHREADED, PERFILE = Value
  }

  val PARQUET_READER_TYPE = conf("spark.rapids.sql.format.parquet.reader.type")
    .doc("Sets the parquet reader type. We support different types that are optimized for " +
      "different environments. The original Spark style reader can be selected by setting this " +
      "to PERFILE which individually reads and copies files to the GPU. Loading many small files " +
      "individually has high overhead, and using either COALESCING or MULTITHREADED is " +
      "recommended instead. The COALESCING reader is good when using a local file system where " +
      "the executors are on the same nodes or close to the nodes the data is being read on. " +
      "This reader coalesces all the files assigned to a task into a single host buffer before " +
      "sending it down to the GPU. It copies blocks from a single file into a host buffer in " +
      "separate threads in parallel, see " +
      "spark.rapids.sql.format.parquet.multiThreadedRead.numThreads. " +
      "MULTITHREADED is good for cloud environments where you are reading from a blobstore " +
      "that is totally separate and likely has a higher I/O read cost. Many times the cloud " +
      "environments also get better throughput when you have multiple readers in parallel. " +
      "This reader uses multiple threads to read each file in parallel and each file is sent " +
      "to the GPU separately. This allows the CPU to keep reading while GPU is also doing work. " +
      "See spark.rapids.sql.format.parquet.multiThreadedRead.numThreads and " +
      "spark.rapids.sql.format.parquet.multiThreadedRead.maxNumFilesParallel to control " +
      "the number of threads and amount of memory used. " +
      "By default this is set to AUTO so we select the reader we think is best. This will " +
      "either be the COALESCING or the MULTITHREADED based on whether we think the file is " +
      "in the cloud. See spark.rapids.cloudSchemes.")
    .stringConf
    .transform(_.toUpperCase(java.util.Locale.ROOT))
    .checkValues(ParquetReaderType.values.map(_.toString))
    .createWithDefault(ParquetReaderType.AUTO.toString)

  val CLOUD_SCHEMES = conf("spark.rapids.cloudSchemes")
    .doc("Comma separated list of additional URI schemes that are to be considered cloud based " +
      "filesystems. Schemes already included: dbfs, s3, s3a, s3n, wasbs, gs. Cloud based stores " +
      "generally would be total separate from the executors and likely have a higher I/O read " +
      "cost. Many times the cloud filesystems also get better throughput when you have multiple " +
      "readers in parallel. This is used with spark.rapids.sql.format.parquet.reader.type")
    .stringConf
    .toSequence
    .createOptional

  val PARQUET_MULTITHREAD_READ_NUM_THREADS =
    conf("spark.rapids.sql.format.parquet.multiThreadedRead.numThreads")
      .doc("The maximum number of threads, on the executor, to use for reading small " +
        "parquet files in parallel. This can not be changed at runtime after the executor has " +
        "started. Used with COALESCING and MULTITHREADED reader, see " +
        "spark.rapids.sql.format.parquet.reader.type.")
      .integerConf
      .createWithDefault(20)

  val PARQUET_MULTITHREAD_READ_MAX_NUM_FILES_PARALLEL =
    conf("spark.rapids.sql.format.parquet.multiThreadedRead.maxNumFilesParallel")
      .doc("A limit on the maximum number of files per task processed in parallel on the CPU " +
        "side before the file is sent to the GPU. This affects the amount of host memory used " +
        "when reading the files in parallel. Used with MULTITHREADED reader, see " +
        "spark.rapids.sql.format.parquet.reader.type")
      .integerConf
      .checkValue(v => v > 0, "The maximum number of files must be greater than 0.")
      .createWithDefault(Integer.MAX_VALUE)

  val ENABLE_PARQUET_READ = conf("spark.rapids.sql.format.parquet.read.enabled")
    .doc("When set to false disables parquet input acceleration")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_PARQUET_WRITE = conf("spark.rapids.sql.format.parquet.write.enabled")
    .doc("When set to false disables parquet output acceleration")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_ORC = conf("spark.rapids.sql.format.orc.enabled")
    .doc("When set to false disables all orc input and output acceleration")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_ORC_READ = conf("spark.rapids.sql.format.orc.read.enabled")
    .doc("When set to false disables orc input acceleration")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_ORC_WRITE = conf("spark.rapids.sql.format.orc.write.enabled")
    .doc("When set to false disables orc output acceleration")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_CSV = conf("spark.rapids.sql.format.csv.enabled")
    .doc("When set to false disables all csv input and output acceleration. " +
      "(only input is currently supported anyways)")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_CSV_READ = conf("spark.rapids.sql.format.csv.read.enabled")
    .doc("When set to false disables csv input acceleration")
    .booleanConf
    .createWithDefault(true)

  // TODO should we change this config?
  val ENABLE_CSV_TIMESTAMPS = conf("spark.rapids.sql.csvTimestamps.enabled")
      .doc("When set to true, enables the CSV parser to read timestamps. The default output " +
          "format for Spark includes a timezone at the end. Anything except the UTC timezone is " +
          "not supported. Timestamps after 2038 and before 1902 are also not supported.")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_READ_CSV_DATES = conf("spark.rapids.sql.csv.read.date.enabled")
      .doc("Parsing invalid CSV dates produces different results from Spark")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_READ_CSV_BOOLS = conf("spark.rapids.sql.csv.read.bool.enabled")
      .doc("Parsing an invalid CSV boolean value produces true instead of null")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_READ_CSV_BYTES = conf("spark.rapids.sql.csv.read.byte.enabled")
      .doc("Parsing CSV bytes is much more lenient and will return 0 for some " +
          "malformed values instead of null")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_READ_CSV_SHORTS = conf("spark.rapids.sql.csv.read.short.enabled")
      .doc("Parsing CSV shorts is much more lenient and will return 0 for some " +
          "malformed values instead of null")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_READ_CSV_INTEGERS = conf("spark.rapids.sql.csv.read.integer.enabled")
      .doc("Parsing CSV integers is much more lenient and will return 0 for some " +
          "malformed values instead of null")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_READ_CSV_LONGS = conf("spark.rapids.sql.csv.read.long.enabled")
      .doc("Parsing CSV longs is much more lenient and will return 0 for some " +
          "malformed values instead of null")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_READ_CSV_FLOATS = conf("spark.rapids.sql.csv.read.float.enabled")
      .doc("Parsing CSV floats has some issues at the min and max values for floating" +
          "point numbers and can be more lenient on parsing inf and -inf values")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_READ_CSV_DOUBLES = conf("spark.rapids.sql.csv.read.double.enabled")
      .doc("Parsing CSV double has some issues at the min and max values for floating" +
          "point numbers and can be more lenient on parsing inf and -inf values")
      .booleanConf
      .createWithDefault(false)

  // INTERNAL TEST AND DEBUG CONFIGS

  val TEST_CONF = conf("spark.rapids.sql.test.enabled")
    .doc("Intended to be used by unit tests, if enabled all operations must run on the " +
      "GPU or an error happens.")
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

  val TEST_VALIDATE_EXECS_ONGPU = conf("spark.rapids.sql.test.validateExecsInGpuPlan")
    .doc("Comma separate string of exec class names to validate they " +
      "are GPU accelerated. Used for testing.")
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
      "\"complete\" (exclusively replace complete aggregates), " +
      "\"partial\" (exclusively replace partial aggregates), " +
      "\"final\" (exclusively replace final aggregates)")
    .internal()
    .stringConf
    .createWithDefault("all")

  val PARTIAL_MERGE_DISTINCT_ENABLED = conf("spark.rapids.sql.partialMerge.distinct.enabled")
    .doc("Enables aggregates that are in PartialMerge mode to run on the GPU if true")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val SHUFFLE_TRANSPORT_ENABLE = conf("spark.rapids.shuffle.transport.enabled")
    .doc("Enable the RAPIDS Shuffle Transport for accelerated shuffle. By default, this " +
        "requires UCX to be installed in the system. Consider setting to false if running with " +
        "a single executor and UCX is not available, for short-circuit cached shuffle " +
        "(i.e. for testing purposes)")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val SHUFFLE_TRANSPORT_EARLY_START = conf("spark.rapids.shuffle.transport.earlyStart")
    .doc("Enable early connection establishment for RAPIDS Shuffle")
    .booleanConf
    .createWithDefault(true)

  val SHUFFLE_TRANSPORT_EARLY_START_HEARTBEAT_INTERVAL =
    conf("spark.rapids.shuffle.transport.earlyStart.heartbeatInterval")
      .doc("Shuffle early start heartbeat interval (milliseconds)")
      .integerConf
      .createWithDefault(5000)

  val SHUFFLE_TRANSPORT_CLASS_NAME = conf("spark.rapids.shuffle.transport.class")
    .doc("The class of the specific RapidsShuffleTransport to use during the shuffle.")
    .internal()
    .stringConf
    .createWithDefault("com.nvidia.spark.rapids.shuffle.ucx.UCXShuffleTransport")

  val SHUFFLE_TRANSPORT_MAX_RECEIVE_INFLIGHT_BYTES =
    conf("spark.rapids.shuffle.transport.maxReceiveInflightBytes")
      .doc("Maximum aggregate amount of bytes that be fetched at any given time from peers " +
        "during shuffle")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(1024 * 1024 * 1024)

  val SHUFFLE_UCX_USE_WAKEUP = conf("spark.rapids.shuffle.ucx.useWakeup")
    .doc("When set to true, use UCX's event-based progress (epoll) in order to wake up " +
      "the progress thread when needed, instead of a hot loop.")
    .booleanConf
    .createWithDefault(true)

  val SHUFFLE_UCX_MGMT_SERVER_HOST = conf("spark.rapids.shuffle.ucx.managementServerHost")
    .doc("The host to be used to start the management server")
    .stringConf
    .createWithDefault(null)

  val SHUFFLE_UCX_BOUNCE_BUFFERS_SIZE = conf("spark.rapids.shuffle.ucx.bounceBuffers.size")
    .doc("The size of bounce buffer to use in bytes. Note that this size will be the same " +
      "for device and host memory")
    .internal()
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(4 * 1024  * 1024)

  val SHUFFLE_UCX_BOUNCE_BUFFERS_DEVICE_COUNT =
    conf("spark.rapids.shuffle.ucx.bounceBuffers.device.count")
    .doc("The number of bounce buffers to pre-allocate from device memory")
    .internal()
    .integerConf
    .createWithDefault(32)

  val SHUFFLE_UCX_BOUNCE_BUFFERS_HOST_COUNT =
    conf("spark.rapids.shuffle.ucx.bounceBuffers.host.count")
    .doc("The number of bounce buffers to pre-allocate from host memory")
    .internal()
    .integerConf
    .createWithDefault(32)

  val SHUFFLE_MAX_CLIENT_THREADS = conf("spark.rapids.shuffle.maxClientThreads")
    .doc("The maximum number of threads that the shuffle client should be allowed to start")
    .internal()
    .integerConf
    .createWithDefault(50)

  val SHUFFLE_MAX_CLIENT_TASKS = conf("spark.rapids.shuffle.maxClientTasks")
    .doc("The maximum number of tasks shuffle clients will queue before adding threads " +
      s"(up to spark.rapids.shuffle.maxClientThreads), or slowing down the transport")
    .internal()
    .integerConf
    .createWithDefault(100)

  val SHUFFLE_CLIENT_THREAD_KEEPALIVE = conf("spark.rapids.shuffle.clientThreadKeepAlive")
    .doc("The number of seconds that the ThreadPoolExecutor will allow an idle client " +
      "shuffle thread to stay alive, before reclaiming.")
    .internal()
    .integerConf
    .createWithDefault(30)

  val SHUFFLE_MAX_SERVER_TASKS = conf("spark.rapids.shuffle.maxServerTasks")
    .doc("The maximum number of tasks the shuffle server will queue up for its thread")
    .internal()
    .integerConf
    .createWithDefault(1000)

  val SHUFFLE_MAX_METADATA_SIZE = conf("spark.rapids.shuffle.maxMetadataSize")
    .doc("The maximum size of a metadata message used in the shuffle.")
    .internal()
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(50 * 1024)

  val SHUFFLE_COMPRESSION_CODEC = conf("spark.rapids.shuffle.compression.codec")
      .doc("The GPU codec used to compress shuffle data when using RAPIDS shuffle. " +
          "Supported codecs: lz4, copy, none")
      .internal()
      .stringConf
      .createWithDefault("none")

  // ALLUXIO CONFIGS

  val ALLUXIO_PATHS_REPLACE = conf("spark.rapids.alluxio.pathsToReplace")
    .doc("List of paths to be replaced with corresponding alluxio scheme. Eg, when configure" +
      "is set to \"s3:/foo->alluxio://0.1.2.3:19998/foo,gcs:/bar->alluxio://0.1.2.3:19998/bar\", " +
      "which means:  " +
      "     s3:/foo/a.csv will be replaced to alluxio://0.1.2.3:19998/foo/a.csv and " +
      "     gcs:/bar/b.csv will be replaced to alluxio://0.1.2.3:19998/bar/b.csv")
    .stringConf
    .toSequence
    .createOptional

  // USER FACING DEBUG CONFIGS

  val SHUFFLE_COMPRESSION_MAX_BATCH_MEMORY =
    conf("spark.rapids.shuffle.compression.maxBatchMemory")
      .internal()
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(1024 * 1024 * 1024)

  val EXPLAIN = conf("spark.rapids.sql.explain")
    .doc("Explain why some parts of a query were not placed on a GPU or not. Possible " +
      "values are ALL: print everything, NONE: print nothing, NOT_ON_GPU: print only parts of " +
      "a query that did not go on the GPU")
    .stringConf
    .createWithDefault("NONE")

  val SHIMS_PROVIDER_OVERRIDE = conf("spark.rapids.shims-provider-override")
    .internal()
    .doc("Overrides the automatic Spark shim detection logic and forces a specific shims " +
      "provider class to be used. Set to the fully qualified shims provider class to use. " +
      "If you are using a custom Spark version such as Spark 3.0.0.0 then this can be used to " +
      "specify the shims provider that matches the base Spark version of Spark 3.0.0, i.e.: " +
      "com.nvidia.spark.rapids.shims.spark300.SparkShimServiceProvider. If you modified Spark " +
      "then there is no guarantee the RAPIDS Accelerator will function properly.")
    .stringConf
    .createOptional

  val CUDF_VERSION_OVERRIDE = conf("spark.rapids.cudfVersionOverride")
    .internal()
    .doc("Overrides the cudf version compatibility check between cudf jar and RAPIDS Accelerator " +
      "jar. If you are sure that the cudf jar which is mentioned in the classpath is compatible " +
      "with the RAPIDS Accelerator version, then set this to true.")
    .booleanConf
    .createWithDefault(false)

  val ALLOW_DISABLE_ENTIRE_PLAN = conf("spark.rapids.allowDisableEntirePlan")
    .internal()
    .doc("The plugin has the ability to detect possibe incompatibility with some specific " +
      "queries and cluster configurations. In those cases the plugin will disable GPU support " +
      "for the entire query. Set this to false if you want to override that behavior, but use " +
      "with caution.")
    .booleanConf
    .createWithDefault(true)

  val OPTIMIZER_ENABLED = conf("spark.rapids.sql.optimizer.enabled")
      .internal()
      .doc("Enable cost-based optimizer that will attempt to avoid " +
          "transitions to GPU for operations that will not result in improved performance " +
          "over CPU")
      .booleanConf
      .createWithDefault(false)

  val OPTIMIZER_EXPLAIN = conf("spark.rapids.sql.optimizer.explain")
      .internal()
      .doc("Explain why some parts of a query were not placed on a GPU due to " +
          "optimization rules. Possible values are ALL: print everything, NONE: print nothing")
      .stringConf
      .createWithDefault("NONE")

  val OPTIMIZER_DEFAULT_GPU_OPERATOR_COST = conf("spark.rapids.sql.optimizer.defaultExecGpuCost")
      .internal()
      .doc("Default relative GPU cost of running an operator on the GPU")
      .doubleConf
      .createWithDefault(0.8)

  val OPTIMIZER_DEFAULT_GPU_EXPRESSION_COST = conf("spark.rapids.sql.optimizer.defaultExprGpuCost")
      .internal()
      .doc("Default relative GPU cost of running an expression on the GPU")
      .doubleConf
      .createWithDefault(0.8)

  val OPTIMIZER_DEFAULT_TRANSITION_TO_CPU_COST = conf(
    "spark.rapids.sql.optimizer.defaultTransitionToCpuCost")
      .internal()
      .doc("Default cost of transitioning from GPU to CPU")
      .doubleConf
      .createWithDefault(0.1)

  val OPTIMIZER_DEFAULT_TRANSITION_TO_GPU_COST = conf(
    "spark.rapids.sql.optimizer.defaultTransitionToGpuCost")
      .internal()
      .doc("Default cost of transitioning from CPU to GPU")
      .doubleConf
      .createWithDefault(0.1)

  val USE_ARROW_OPT = conf("spark.rapids.arrowCopyOptimizationEnabled")
    .doc("Option to turn off using the optimized Arrow copy code when reading from " +
      "ArrowColumnVector in HostColumnarToGpu. Left as internal as user shouldn't " +
      "have to turn it off, but its convenient for testing.")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val CPU_RANGE_PARTITIONING_ALLOWED = conf("spark.rapids.allowCpuRangePartitioning")
    .doc("Option to control enforcement of range partitioning on GPU.")
    .internal()
    .booleanConf
    .createWithDefault(true)

  private def printSectionHeader(category: String): Unit =
    println(s"\n### $category")

  private def printToggleHeader(category: String): Unit = {
    printSectionHeader(category)
    println("Name | Description | Default Value | Notes")
    println("-----|-------------|---------------|------------------")
  }

  private def printToggleHeaderWithSqlFunction(category: String): Unit = {
    printSectionHeader(category)
    println("Name | SQL Function(s) | Description | Default Value | Notes")
    println("-----|-----------------|-------------|---------------|------")
  }

  def help(asTable: Boolean = false): Unit = {
    if (asTable) {
      println("---")
      println("layout: page")
      println("title: Configuration")
      println("nav_order: 4")
      println("---")
      println(s"<!-- Generated by RapidsConf.help. DO NOT EDIT! -->")
      // scalastyle:off line.size.limit
      println("""# RAPIDS Accelerator for Apache Spark Configuration
        |The following is the list of options that `rapids-plugin-4-spark` supports.
        |
        |On startup use: `--conf [conf key]=[conf value]`. For example:
        |
        |```
        |${SPARK_HOME}/bin/spark --jars 'rapids-4-spark_2.12-0.6.0-SNAPSHOT.jar,cudf-0.20-SNAPSHOT-cuda11.jar' \
        |--conf spark.plugins=com.nvidia.spark.SQLPlugin \
        |--conf spark.rapids.sql.incompatibleOps.enabled=true
        |```
        |
        |At runtime use: `spark.conf.set("[conf key]", [conf value])`. For example:
        |
        |```
        |scala> spark.conf.set("spark.rapids.sql.incompatibleOps.enabled", true)
        |```
        |
        | All configs can be set on startup, but some configs, especially for shuffle, will not
        | work if they are set at runtime.
        |""".stripMargin)
      // scalastyle:on line.size.limit

      println("\n## General Configuration\n")
      println("Name | Description | Default Value")
      println("-----|-------------|--------------")
    } else {
      println("Rapids Configs:")
    }
    registeredConfs.sortBy(_.key).foreach(_.help(asTable))
    if (asTable) {
      println("")
      // scalastyle:off line.size.limit
      println("""## Supported GPU Operators and Fine Tuning
        |_The RAPIDS Accelerator for Apache Spark_ can be configured to enable or disable specific
        |GPU accelerated expressions.  Enabled expressions are candidates for GPU execution. If the
        |expression is configured as disabled, the accelerator plugin will not attempt replacement,
        |and it will run on the CPU.
        |
        |Please leverage the [`spark.rapids.sql.explain`](#sql.explain) setting to get
        |feedback from the plugin as to why parts of a query may not be executing on the GPU.
        |
        |**NOTE:** Setting
        |[`spark.rapids.sql.incompatibleOps.enabled=true`](#sql.incompatibleOps.enabled)
        |will enable all the settings in the table below which are not enabled by default due to
        |incompatibilities.""".stripMargin)
      // scalastyle:on line.size.limit

      printToggleHeaderWithSqlFunction("Expressions\n")
    }
    GpuOverrides.expressions.values.toSeq.sortBy(_.tag.toString).foreach { rule =>
      val sqlFunctions =
        ConfHelper.getSqlFunctionsForClass(rule.tag.runtimeClass).map(_.mkString(", "))

      // this is only for formatting, this is done to ensure the table has a column for a
      // row where there isn't a SQL function
      rule.confHelp(asTable, Some(sqlFunctions.getOrElse(" ")))
    }
    if (asTable) {
      printToggleHeader("Execution\n")
    }
    GpuOverrides.execs.values.toSeq.sortBy(_.tag.toString).foreach(_.confHelp(asTable))
    if (asTable) {
      printToggleHeader("Scans\n")
    }
    GpuOverrides.scans.values.toSeq.sortBy(_.tag.toString).foreach(_.confHelp(asTable))
    if (asTable) {
      printToggleHeader("Partitioning\n")
    }
    GpuOverrides.parts.values.toSeq.sortBy(_.tag.toString).foreach(_.confHelp(asTable))
    if (asTable) {
      printSectionHeader("JIT Kernel Cache Path")
      println("""
      |  CUDF can compile GPU kernels at runtime using a just-in-time (JIT) compiler. The
      |  resulting kernels are cached on the filesystem. The default location for this cache is
      |  under the `.cudf` directory in the user's home directory. When running in an environment
      |  where the user's home directory cannot be written, such as running in a container
      |  environment on a cluster, the JIT cache path will need to be specified explicitly with
      |  the `LIBCUDF_KERNEL_CACHE_PATH` environment variable.
      |  The specified kernel cache path should be specific to the user to avoid conflicts with
      |  others running on the same host. For example, the following would specify the path to a
      |  user-specific location under `/tmp`:
      |
      |  ```
      |  --conf spark.executorEnv.LIBCUDF_KERNEL_CACHE_PATH="/tmp/cudf-$USER"
      |  ```
      |""".stripMargin)
    }
  }
  def main(args: Array[String]): Unit = {
    // Include the configs in PythonConfEntries
    com.nvidia.spark.rapids.python.PythonConfEntries.init()
    val out = new FileOutputStream(new File(args(0)))
    Console.withOut(out) {
      Console.withErr(out) {
        RapidsConf.help(true)
      }
    }
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

  lazy val metricsLevel: String = get(METRICS_LEVEL)

  lazy val isSqlEnabled: Boolean = get(SQL_ENABLED)

  lazy val isUdfCompilerEnabled: Boolean = get(UDF_COMPILER_ENABLED)

  lazy val exportColumnarRdd: Boolean = get(EXPORT_COLUMNAR_RDD)

  lazy val stableSort: Boolean = get(STABLE_SORT)

  lazy val isIncompatEnabled: Boolean = get(INCOMPATIBLE_OPS)

  lazy val incompatDateFormats: Boolean = get(INCOMPATIBLE_DATE_FORMATS)

  lazy val includeImprovedFloat: Boolean = get(IMPROVED_FLOAT_OPS)

  lazy val pinnedPoolSize: Long = get(PINNED_POOL_SIZE)

  lazy val concurrentGpuTasks: Int = get(CONCURRENT_GPU_TASKS)

  lazy val isTestEnabled: Boolean = get(TEST_CONF)

  lazy val testingAllowedNonGpu: Seq[String] = get(TEST_ALLOWED_NONGPU)

  lazy val validateExecsInGpuPlan: Seq[String] = get(TEST_VALIDATE_EXECS_ONGPU)

  lazy val rmmDebugLocation: String = get(RMM_DEBUG)

  lazy val gpuOomDumpDir: Option[String] = get(GPU_OOM_DUMP_DIR)

  lazy val isUvmEnabled: Boolean = get(UVM_ENABLED)

  lazy val isPooledMemEnabled: Boolean = get(POOLED_MEM)

  lazy val rmmPool: String = get(RMM_POOL)

  lazy val rmmAllocFraction: Double = get(RMM_ALLOC_FRACTION)

  lazy val rmmAllocMaxFraction: Double = get(RMM_ALLOC_MAX_FRACTION)

  lazy val rmmAllocReserve: Long = get(RMM_ALLOC_RESERVE)

  lazy val hostSpillStorageSize: Long = get(HOST_SPILL_STORAGE_SIZE)

  lazy val isUnspillEnabled: Boolean = get(UNSPILL)

  lazy val isGdsSpillEnabled: Boolean = get(GDS_SPILL)

  lazy val hasNans: Boolean = get(HAS_NANS)

  lazy val gpuTargetBatchSizeBytes: Long = get(GPU_BATCH_SIZE_BYTES)

  lazy val isFloatAggEnabled: Boolean = get(ENABLE_FLOAT_AGG)

  lazy val decimalTypeEnabled: Boolean = get(DECIMAL_TYPE_ENABLED)

  lazy val explain: String = get(EXPLAIN)

  lazy val isImprovedTimestampOpsEnabled: Boolean = get(IMPROVED_TIMESTAMP_OPS)

  lazy val maxReadBatchSizeRows: Int = get(MAX_READER_BATCH_SIZE_ROWS)

  lazy val maxReadBatchSizeBytes: Long = get(MAX_READER_BATCH_SIZE_BYTES)

  lazy val parquetDebugDumpPrefix: String = get(PARQUET_DEBUG_DUMP_PREFIX)

  lazy val orcDebugDumpPrefix: String = get(ORC_DEBUG_DUMP_PREFIX)

  lazy val hashAggReplaceMode: String = get(HASH_AGG_REPLACE_MODE)

  lazy val partialMergeDistinctEnabled: Boolean = get(PARTIAL_MERGE_DISTINCT_ENABLED)

  lazy val enableReplaceSortMergeJoin: Boolean = get(ENABLE_REPLACE_SORTMERGEJOIN)

  lazy val enableHashOptimizeSort: Boolean = get(ENABLE_HASH_OPTIMIZE_SORT)

  lazy val areInnerJoinsEnabled: Boolean = get(ENABLE_INNER_JOIN)

  lazy val areCrossJoinsEnabled: Boolean = get(ENABLE_CROSS_JOIN)

  lazy val areLeftOuterJoinsEnabled: Boolean = get(ENABLE_LEFT_OUTER_JOIN)

  lazy val areRightOuterJoinsEnabled: Boolean = get(ENABLE_RIGHT_OUTER_JOIN)

  lazy val areFullOuterJoinsEnabled: Boolean = get(ENABLE_FULL_OUTER_JOIN)

  lazy val areLeftSemiJoinsEnabled: Boolean = get(ENABLE_LEFT_SEMI_JOIN)

  lazy val areLeftAntiJoinsEnabled: Boolean = get(ENABLE_LEFT_ANTI_JOIN)

  lazy val isCastFloatToDecimalEnabled: Boolean = get(ENABLE_CAST_FLOAT_TO_DECIMAL)

  lazy val isCastFloatToStringEnabled: Boolean = get(ENABLE_CAST_FLOAT_TO_STRING)

  lazy val isCastStringToTimestampEnabled: Boolean = get(ENABLE_CAST_STRING_TO_TIMESTAMP)

  lazy val isCastStringToFloatEnabled: Boolean = get(ENABLE_CAST_STRING_TO_FLOAT)

  lazy val isCastStringToDecimalEnabled: Boolean = get(ENABLE_CAST_STRING_TO_DECIMAL)

  lazy val isCastFloatToIntegralTypesEnabled: Boolean = get(ENABLE_CAST_FLOAT_TO_INTEGRAL_TYPES)

  lazy val isCsvTimestampReadEnabled: Boolean = get(ENABLE_CSV_TIMESTAMPS)

  lazy val isCsvDateReadEnabled: Boolean = get(ENABLE_READ_CSV_DATES)

  lazy val isCsvBoolReadEnabled: Boolean = get(ENABLE_READ_CSV_BOOLS)

  lazy val isCsvByteReadEnabled: Boolean = get(ENABLE_READ_CSV_BYTES)

  lazy val isCsvShortReadEnabled: Boolean = get(ENABLE_READ_CSV_SHORTS)

  lazy val isCsvIntReadEnabled: Boolean = get(ENABLE_READ_CSV_INTEGERS)

  lazy val isCsvLongReadEnabled: Boolean = get(ENABLE_READ_CSV_LONGS)

  lazy val isCsvFloatReadEnabled: Boolean = get(ENABLE_READ_CSV_FLOATS)

  lazy val isCsvDoubleReadEnabled: Boolean = get(ENABLE_READ_CSV_DOUBLES)

  lazy val isCastDecimalToStringEnabled: Boolean = get(ENABLE_CAST_DECIMAL_TO_STRING)

  lazy val isParquetEnabled: Boolean = get(ENABLE_PARQUET)

  lazy val isParquetPerFileReadEnabled: Boolean =
    ParquetReaderType.withName(get(PARQUET_READER_TYPE)) == ParquetReaderType.PERFILE

  lazy val isParquetAutoReaderEnabled: Boolean =
    ParquetReaderType.withName(get(PARQUET_READER_TYPE)) == ParquetReaderType.AUTO

  lazy val isParquetCoalesceFileReadEnabled: Boolean = isParquetAutoReaderEnabled ||
    ParquetReaderType.withName(get(PARQUET_READER_TYPE)) == ParquetReaderType.COALESCING

  lazy val isParquetMultiThreadReadEnabled: Boolean = isParquetAutoReaderEnabled ||
    ParquetReaderType.withName(get(PARQUET_READER_TYPE)) == ParquetReaderType.MULTITHREADED

  lazy val parquetMultiThreadReadNumThreads: Int = get(PARQUET_MULTITHREAD_READ_NUM_THREADS)

  lazy val maxNumParquetFilesParallel: Int = get(PARQUET_MULTITHREAD_READ_MAX_NUM_FILES_PARALLEL)

  lazy val isParquetReadEnabled: Boolean = get(ENABLE_PARQUET_READ)

  lazy val isParquetWriteEnabled: Boolean = get(ENABLE_PARQUET_WRITE)

  lazy val isOrcEnabled: Boolean = get(ENABLE_ORC)

  lazy val isOrcReadEnabled: Boolean = get(ENABLE_ORC_READ)

  lazy val isOrcWriteEnabled: Boolean = get(ENABLE_ORC_WRITE)

  lazy val isCsvEnabled: Boolean = get(ENABLE_CSV)

  lazy val isCsvReadEnabled: Boolean = get(ENABLE_CSV_READ)

  lazy val shuffleTransportEnabled: Boolean = get(SHUFFLE_TRANSPORT_ENABLE)

  lazy val shuffleTransportClassName: String = get(SHUFFLE_TRANSPORT_CLASS_NAME)

  lazy val shuffleTransportEarlyStartHeartbeatInterval: Int = get(
    SHUFFLE_TRANSPORT_EARLY_START_HEARTBEAT_INTERVAL)

  lazy val shuffleTransportEarlyStart: Boolean = get(SHUFFLE_TRANSPORT_EARLY_START)

  lazy val shuffleTransportMaxReceiveInflightBytes: Long = get(
    SHUFFLE_TRANSPORT_MAX_RECEIVE_INFLIGHT_BYTES)

  lazy val shuffleUcxUseWakeup: Boolean = get(SHUFFLE_UCX_USE_WAKEUP)

  lazy val shuffleUcxMgmtHost: String = get(SHUFFLE_UCX_MGMT_SERVER_HOST)

  lazy val shuffleUcxBounceBuffersSize: Long = get(SHUFFLE_UCX_BOUNCE_BUFFERS_SIZE)

  lazy val shuffleUcxDeviceBounceBuffersCount: Int = get(SHUFFLE_UCX_BOUNCE_BUFFERS_DEVICE_COUNT)

  lazy val shuffleUcxHostBounceBuffersCount: Int = get(SHUFFLE_UCX_BOUNCE_BUFFERS_HOST_COUNT)

  lazy val shuffleMaxClientThreads: Int = get(SHUFFLE_MAX_CLIENT_THREADS)

  lazy val shuffleMaxClientTasks: Int = get(SHUFFLE_MAX_CLIENT_TASKS)

  lazy val shuffleClientThreadKeepAliveTime: Int = get(SHUFFLE_CLIENT_THREAD_KEEPALIVE)

  lazy val shuffleMaxServerTasks: Int = get(SHUFFLE_MAX_SERVER_TASKS)

  lazy val shuffleMaxMetadataSize: Long = get(SHUFFLE_MAX_METADATA_SIZE)

  lazy val shuffleCompressionCodec: String = get(SHUFFLE_COMPRESSION_CODEC)

  lazy val shuffleCompressionMaxBatchMemory: Long = get(SHUFFLE_COMPRESSION_MAX_BATCH_MEMORY)

  lazy val shimsProviderOverride: Option[String] = get(SHIMS_PROVIDER_OVERRIDE)

  lazy val cudfVersionOverride: Boolean = get(CUDF_VERSION_OVERRIDE)

  lazy val allowDisableEntirePlan: Boolean = get(ALLOW_DISABLE_ENTIRE_PLAN)

  lazy val useArrowCopyOptimization: Boolean = get(USE_ARROW_OPT)

  lazy val getCloudSchemes: Option[Seq[String]] = get(CLOUD_SCHEMES)

  lazy val optimizerEnabled: Boolean = get(OPTIMIZER_ENABLED)

  lazy val optimizerExplain: String = get(OPTIMIZER_EXPLAIN)

  lazy val defaultOperatorCost: Double = get(OPTIMIZER_DEFAULT_GPU_OPERATOR_COST)

  lazy val defaultExpressionCost: Double = get(OPTIMIZER_DEFAULT_GPU_EXPRESSION_COST)

  lazy val defaultTransitionToCpuCost: Double = get(OPTIMIZER_DEFAULT_TRANSITION_TO_CPU_COST)

  lazy val defaultTransitionToGpuCost: Double = get(OPTIMIZER_DEFAULT_TRANSITION_TO_GPU_COST)

  lazy val getAlluxioPathsToReplace: Option[Seq[String]] = get(ALLUXIO_PATHS_REPLACE)

  lazy val cpuRangePartitioningPermitted = get(CPU_RANGE_PARTITIONING_ALLOWED)

  def isOperatorEnabled(key: String, incompat: Boolean, isDisabledByDefault: Boolean): Boolean = {
    val default = !(isDisabledByDefault || incompat) || (incompat && isIncompatEnabled)
    conf.get(key).map(toBoolean(_, key)).getOrElse(default)
  }

  /**
   * Get the GPU cost of an expression, for use in the cost-based optimizer.
   */
  def getExpressionCost(operatorName: String): Option[Double] = {
    val key = s"spark.rapids.sql.optimizer.expr.$operatorName"
    conf.get(key).map(toDouble(_, key))
  }

  /**
   * Get the GPU cost of an operator, for use in the cost-based optimizer.
   */
  def getOperatorCost(operatorName: String): Option[Double] = {
    val key = s"spark.rapids.sql.optimizer.exec.$operatorName"
    conf.get(key).map(toDouble(_, key))
  }

}
