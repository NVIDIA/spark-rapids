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

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
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
}

abstract class ConfEntry[T](val key: String, val converter: String => T,
    val doc: String, val isInternal: Boolean) {

  def get(conf: Map[String, String]): T
  def help(): Unit

  override def toString: String = key
}

class ConfEntryWithDefault[T](key: String, converter: String => T, doc: String,
    isInternal: Boolean, val defaultValue: T)
  extends ConfEntry[T](key, converter, doc, isInternal) {

  override def get(conf: Map[String, String]): T = {
    conf.get(key).map(converter).getOrElse(defaultValue)
  }

  override def help(): Unit = {
    if (!isInternal) {
      println(s"${key}:")
      println(s"\t${doc}")
      println(s"\tdefault ${defaultValue}")
      println()
    }
  }
}

class TypedConfBuilder[T](
    val parent: ConfBuilder,
    val converter: String => T) {

  def createWithDefault(value: T): ConfEntry[T] = {
    val ret = new ConfEntryWithDefault[T](parent.key, converter,
      parent.doc, parent.isInternal, value)
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
}

object RapidsConf {
  private val registeredConfs = new ListBuffer[ConfEntry[_]]()

  private def register(entry: ConfEntry[_]): Unit = {
    registeredConfs += entry
  }

  def conf(key: String): ConfBuilder = {
    new ConfBuilder(key, register)
  }

  val SQL_ENABLED = conf("spark.rapids.sql.enabled")
    .doc("Enable (true) or disable (false) sql operations on the GPU")
    .booleanConf
    .createWithDefault(true)

  val INCOMPATIBLE_OPS = conf("spark.rapids.sql.incompatible_ops")
    .doc("For operations that work, but are not 100% compatible with the Spark equivalent" +
      " set if they should be enabled by default or disabled by default.")
    .booleanConf
    .createWithDefault(false)

  val TEST_CONF = conf("spark.rapids.sql.testing")
    .doc("Intended to be used by unit tests, if enabled all operations must run on the GPU" +
      " or an error happens.")
    .internal
    .booleanConf
    .createWithDefault(false)

  val EXPLAIN = conf("spark.rapids.sql.explain")
    .doc("Explain why some parts of a query were not placed on a GPU")
    .booleanConf
    .createWithDefault(true)

  val MEM_DEBUG = conf("spark.rapids.memory_debug")
    .doc("If memory management is enabled and this is true GPU memory allocations are" +
      " tracked and printed out when the process exits.  This should not be used in production.")
    .booleanConf
    .createWithDefault(false)

  def help(): Unit = {
    println("Rapids Configs:")
    registeredConfs.foreach(_.help())
    GpuOverrides.expressions.values.foreach(_.confHelp())
    GpuOverrides.execs.values.foreach(_.confHelp())
    GpuOverrides.scans.values.foreach(_.confHelp())
    GpuOverrides.parts.values.foreach(_.confHelp())
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

  lazy val isSqlEnabled: Boolean = get(SQL_ENABLED)

  lazy val isIncompatEnabled: Boolean = get(INCOMPATIBLE_OPS)

  lazy val isTestEnabled: Boolean = get(TEST_CONF)

  lazy val isMemDebugEnabled: Boolean = get(MEM_DEBUG)

  lazy val explain: Boolean = get(EXPLAIN)

  def isOperatorEnabled(key: String, incompat: Boolean): Boolean = {
    val default = !incompat || (incompat && isIncompatEnabled)
    conf.get(key).map(toBoolean(_, key)).getOrElse(default)
  }
}
