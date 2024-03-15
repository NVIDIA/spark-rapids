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

import ai.rapids.cudf
import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.JSONUtils

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.types.{DataType, StringType}

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

  def unzipInstruction(instruction: PathInstruction): (String, String, Long) = {
    instruction match {
      case Subscript => ("subscript", "", -1)
      case Key => ("key", "", -1)
      case Wildcard => ("wildcard", "", -1)
      case Index(index) => ("index", "", index)
      case Named(name) => ("named", name, -1)
    }
  }

  def splitInstructions(instructions: List[PathInstruction]): 
      (Array[String], Array[String], Array[Long]) = {
    instructions.map(unzipInstruction).unzip3 match {
      case (types, names, values) =>
        (types.toArray, names.toArray, values.toArray)
    }
  }

  def createTable(instructions: List[PathInstruction]): cudf.Table = {
    val (types, names, values) = splitInstructions(instructions)
    withResource(ColumnVector.fromStrings(types: _*)) { typesColumn =>
      withResource(ColumnVector.fromStrings(names: _*)) { namesColumn =>
        withResource(ColumnVector.fromLongs(values: _*)) { valuesColumn =>
          new cudf.Table(typesColumn, namesColumn, valuesColumn)
        }
      }
    }
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
      Option[Option[List[PathInstruction]]] = None

  def parseJsonPath(path: GpuScalar): Option[List[PathInstruction]] = {
    if (path.isValid) {
      val pathStr = path.getValue.toString()
      JsonPathParser.parse(pathStr)
    } else {
      None
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    cachedInstructions.getOrElse {
      val pathInstructions = parseJsonPath(rhs)
      cachedInstructions = Some(pathInstructions)
      pathInstructions
    } match {
      case Some(instructions) => {
        withResource(JsonPathParser.createTable(instructions)) { instructions =>
          JSONUtils.getJsonObject(lhs.getBase, instructions)
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
