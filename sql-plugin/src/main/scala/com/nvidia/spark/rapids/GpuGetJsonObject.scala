/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION.
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

import scala.util.parsing.combinator.RegexParsers

import ai.rapids.cudf.{ColumnVector, GetJsonObjectOptions, Scalar}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.jni.JSONUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, GetJsonObject}
import org.apache.spark.sql.rapids.test.CpuGetJsonObject
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

// Copied from Apache Spark org/apache/spark/sql/catalyst/expressions/jsonExpressions.scala
sealed trait PathInstruction
object PathInstruction {
  case object Subscript extends PathInstruction
  case object Wildcard extends PathInstruction
  case object Key extends PathInstruction
  case class Index(index: Long) extends PathInstruction
  case class Named(name: String) extends PathInstruction
}

object JsonPathParser extends RegexParsers {
  import PathInstruction._

  def root: Parser[Char] = '$'

  def long: Parser[Long] = "\\d+".r ^? {
    case x => x.toLong
  }

  // parse `[*]` and `[123]` subscripts
  def subscript: Parser[List[PathInstruction]] =
    for {
      operand <- '[' ~> ('*' ^^^ Wildcard | long ^^ Index) <~ ']'
    } yield {
      Subscript :: operand :: Nil
    }

  // parse `.name` or `['name']` child expressions
  def named: Parser[List[PathInstruction]] =
    for {
      name <- '.' ~> "[^\\.\\[]+".r | "['" ~> "[^\\'\\?]+".r <~ "']"
    } yield {
      Key :: Named(name) :: Nil
    }

  // child wildcards: `..`, `.*` or `['*']`
  def wildcard: Parser[List[PathInstruction]] =
    (".*" | "['*']") ^^^ List(Wildcard)

  def node: Parser[List[PathInstruction]] =
    wildcard |
      named |
      subscript

  val expression: Parser[List[PathInstruction]] = {
    phrase(root ~> rep(node) ^^ (x => x.flatten))
  }

  def parse(str: String): Option[List[PathInstruction]] = {
    this.parseAll(expression, str) match {
      case Success(result, _) =>
        Some(result)

      case _ =>
        None
    }
  }

  def fallbackCheck(instructions: List[PathInstruction]): Boolean = {
    // JNI kernel has a limit of 16 nested nodes, fallback to CPU if we exceed that
    instructions.length > JSONUtils.MAX_PATH_DEPTH
  }

  def unzipInstruction(instruction: PathInstruction): (String, String, Long) = {
    instruction match {
      case Subscript => ("subscript", "", -1)
      case Key => ("key", "", -1)
      case Wildcard => ("wildcard", "", -1)
      case Index(index) => ("index", "", index)
      case Named(name) => ("named", name, -1)
    }
  }

  def convertToJniObject(instructions: List[PathInstruction]): 
      Array[JSONUtils.PathInstructionJni] = {
    instructions.map { instruction =>
      val (tpe, name, index) = unzipInstruction(instruction)
      new JSONUtils.PathInstructionJni(tpe match {
        case "subscript" => JSONUtils.PathInstructionType.SUBSCRIPT
        case "key" => JSONUtils.PathInstructionType.KEY
        case "wildcard" => JSONUtils.PathInstructionType.WILDCARD
        case "index" => JSONUtils.PathInstructionType.INDEX
        case "named" => JSONUtils.PathInstructionType.NAMED
      }, name, index)
    }.toArray
  }

  def containsUnsupportedPath(instructions: List[PathInstruction]): Boolean = {
    // Gpu GetJsonObject is not supported if JSON path contains wildcard [*]
    // see https://github.com/NVIDIA/spark-rapids/issues/10216
    instructions.exists {
      case Wildcard => true
      case Named("*")  => true
      case _ => false
    }
  }

  def normalize(instructions: List[PathInstruction]): String = {
    // convert List[PathInstruction] to String
    "$" + instructions.map {
      case Subscript | Key => ""
      case Wildcard => "[*]"
      case Index(index) => s"[$index]"
      case Named(name) => s"['$name']"
      case _ => throw new IllegalArgumentException(s"Invalid instruction in path")
    }.mkString
  }
}

class GpuGetJsonObjectMeta(
    expr: GetJsonObject,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule
  ) extends BinaryExprMeta[GetJsonObject](expr, conf, parent, rule) {

  override def tagExprForGpu(): Unit = {
    val lit = GpuOverrides.extractLit(expr.right)
    lit.foreach { l =>
      val instructions = JsonPathParser.parse(l.value.asInstanceOf[UTF8String].toString)
      if (!conf.isLegacyGetJsonObjectEnabled) {
        if (instructions.exists(JsonPathParser.fallbackCheck)) {
          willNotWorkOnGpu("get_json_object on GPU does not support more than 16 nested paths")
        }
      } else {
        if (instructions.exists(JsonPathParser.containsUnsupportedPath)) {
          willNotWorkOnGpu("get_json_object on GPU does not support wildcard [*] in path")
        }
      }
    }
  }

  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
    if (!conf.isLegacyGetJsonObjectEnabled) {
      GpuGetJsonObject(lhs, rhs,
        conf.testGetJsonObjectSavePath, conf.testGetJsonObjectSaveRows)
    } else {
      GpuGetJsonObjectLegacy(lhs, rhs,
        conf.testGetJsonObjectSavePath, conf.testGetJsonObjectSaveRows)
    }
  }
}

case class GpuGetJsonObject(
     json: Expression,
     path: Expression,
     savePathForVerify: Option[String],
     saveRowsForVerify: Int)
    extends GpuBinaryExpressionArgsAnyScalar
        with ExpectsInputTypes {
  // Get a Hadoop conf for the JSON Object
  val hconf: Option[SerializableConfiguration] = savePathForVerify.map { _ =>
    val spark = SparkSession.active
    new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)
  }
  val seed = System.nanoTime()

  override def left: Expression = json
  override def right: Expression = path
  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)
  override def nullable: Boolean = true
  override def prettyName: String = "get_json_object"

  private var cachedInstructions:
      Option[Option[List[PathInstruction]]] = None

  def parseJsonPath(path: GpuScalar): Option[List[PathInstruction]] = {
    if (path.isValid) {
      val pathStr = path.getValue.toString()
      JsonPathParser.parse(pathStr)
    } else {
      None
    }
  }

  /**
   * get_json_object(any_json, '$.*') always return null.
   * '$.*' will be parsed to be a single `Wildcard`.
   * Check whether has separated `Wildcard`
   *
   * @param instructions query path instructions
   * @return true if has separated `Wildcard`, false otherwise.
   */
  private def hasSeparateWildcard(instructions: Option[List[PathInstruction]]): Boolean = {
    import PathInstruction._
    def hasSeparate(ins: List[PathInstruction], idx: Int): Boolean = {
      if (idx == 0) {
        ins(0) match {
          case Wildcard => true
          case _ => false
        }
      } else {
        (ins(idx - 1), ins(idx)) match {
          case (Key, Wildcard) => false
          case (Subscript, Wildcard) => false
          case (_, Wildcard) => true
          case _ => false
        }
      }
    }

    if (instructions.isEmpty) {
      false
    } else {
      val list = instructions.get
      list.indices.exists { idx => hasSeparate(list, idx) }
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val fromGpu = cachedInstructions.getOrElse {
      val pathInstructions = parseJsonPath(rhs)
      val checkedPathInstructions = if (hasSeparateWildcard(pathInstructions)) {
        // If has separate wildcard path, should return all nulls
        None
      } else {
        pathInstructions
      }
      cachedInstructions = Some(checkedPathInstructions)
      checkedPathInstructions
    } match {
      case Some(instructions) => {
        val jniInstructions = JsonPathParser.convertToJniObject(instructions)
        JSONUtils.getJsonObject(lhs.getBase, jniInstructions)
      }
      case None => GpuColumnVector.columnVectorFromNull(lhs.getRowCount.toInt, StringType)
    }

    // Below is only for testing purpose
    savePathForVerify.foreach { debugPath =>
      closeOnExcept(fromGpu) { _ =>
        val path = rhs.getValue.asInstanceOf[UTF8String]
        withResource(CpuGetJsonObject.getJsonObjectOnCpu(lhs, path)) { fromCpu =>
          // verify result, save diffs if have
          CpuGetJsonObject.verify(isLegacy = false, seed,
            lhs.getBase, path, fromGpu, fromCpu, debugPath, saveRowsForVerify,
            hconf.get.value)
        }
      }
    }

    fromGpu
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }
}

case class GpuGetJsonObjectLegacy(
     json: Expression,
     path: Expression,
     savePathForVerify: Option[String],
     saveRowsForVerify: Int)
    extends GpuBinaryExpressionArgsAnyScalar
        with ExpectsInputTypes {
  // Get a Hadoop conf for the JSON Object
  val hconf: Option[SerializableConfiguration] = savePathForVerify.map { _ =>
    val spark = SparkSession.active
    new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)
  }
  val seed = System.nanoTime()

  override def left: Expression = json
  override def right: Expression = path
  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)
  override def nullable: Boolean = true
  override def prettyName: String = "get_json_object"

  private var cachedNormalizedPath: Option[Option[String]] = None

  def normalizeJsonPath(path: GpuScalar): Option[String] = {
    if (path.isValid) {
      val pathStr = path.getValue.toString()
      JsonPathParser.parse(pathStr).map(JsonPathParser.normalize)
    } else {
      None
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val fromGpu = cachedNormalizedPath.getOrElse {
      val normalizedPath: Option[String] = normalizeJsonPath(rhs)
      cachedNormalizedPath = Some(normalizedPath)
      normalizedPath
    } match {
      case Some(normalizedStr) => 
        withResource(Scalar.fromString(normalizedStr)) { scalar =>
          lhs.getBase().getJSONObject(scalar, 
              GetJsonObjectOptions.builder().allowSingleQuotes(true).build())
        }
      case None => GpuColumnVector.columnVectorFromNull(lhs.getRowCount.toInt, StringType)
    }

    // Below is only for testing purpose
    savePathForVerify.foreach { debugPath =>
      closeOnExcept(fromGpu) { _ =>
        val path = rhs.getValue.asInstanceOf[UTF8String]
        withResource(CpuGetJsonObject.getJsonObjectOnCpu(lhs, path)) { fromCpu =>
          // verify result, save diffs if have
          CpuGetJsonObject.verify(isLegacy = true, seed,
            lhs.getBase, path, fromGpu, fromCpu, debugPath, saveRowsForVerify,
            hconf.get.value)
        }
      }
    }

    fromGpu
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }
}
