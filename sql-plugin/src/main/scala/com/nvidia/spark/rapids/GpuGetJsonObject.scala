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

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.JSONUtils

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.types.{DataType, StringType}

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

  def unzipInstruction(instruction: PathInstruction): (Int, String, Long) = {
    instruction match {
      case Subscript => (0, "", -1)
      case Wildcard => (1, "", -1)
      case Key => (2, "", -1)
      case Index(index) => (3, "", index)
      case Named(name) => (4, name, -1)
    }
  }

  def splitInstructions(instructions: List[PathInstruction]): 
      (List[Int], List[String], List[Long]) = {
    instructions.map(unzipInstruction).unzip3
  }
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

  private var cachedInstructions: 
      Option[Option[(List[Int], List[String], List[Long])]] = None

  def normalizeJsonPath(path: GpuScalar): Option[(List[Int], List[String], List[Long])] = {
    if (path.isValid) {
      val pathStr = path.getValue.toString()
      JsonPathParser.parse(pathStr).map(JsonPathParser.splitInstructions)
    } else {
      None
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    cachedInstructions.getOrElse {
      val pathInstructions = normalizeJsonPath(rhs)
      cachedInstructions = Some(pathInstructions)
      pathInstructions
    } match {
      case Some(instructions) => instructions match {
        case (a: List[Int], b: List[String], c: List[Long]) => {
          JSONUtils.getJsonObject(lhs.getBase, a.toArray, b.toArray, c.toArray)
        }
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
