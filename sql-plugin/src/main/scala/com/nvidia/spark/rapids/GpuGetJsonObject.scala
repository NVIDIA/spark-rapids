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
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, GetJsonObject}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

// Copied from Apache Spark org/apache/spark/sql/catalyst/expressions/jsonExpressions.scala
private[this] sealed trait PathInstruction
private[this] object PathInstruction {
  case object Subscript extends PathInstruction
  case object Wildcard extends PathInstruction
  case object Key extends PathInstruction
  case class Index(index: Long) extends PathInstruction
  case class Named(name: String) extends PathInstruction
}

private[this] object JsonPathParser extends RegexParsers {
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

  def containsUnsupportedPath(instructions: List[PathInstruction]): Boolean = {
    // Gpu GetJsonObject is not supported if JSON path contains wildcard [*]
    // see https://github.com/NVIDIA/spark-rapids/issues/10216
    instructions.exists {
      case Wildcard => true
      case Named(name) if name == "*" => true
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
    lit.map { l =>
      val instructions = JsonPathParser.parse(l.value.asInstanceOf[UTF8String].toString)
      if (instructions.exists(JsonPathParser.containsUnsupportedPath)) {
        willNotWorkOnGpu("get_json_object on GPU does not support wildcard [*] in path")
      }
    }
  }

  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuGetJsonObject(lhs, rhs)
}

case class GpuGetJsonObject(json: Expression, path: Expression)
    extends GpuBinaryExpressionArgsAnyScalar
        with ExpectsInputTypes {
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
    cachedNormalizedPath.getOrElse {
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
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }
}
