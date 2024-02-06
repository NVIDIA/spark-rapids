/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.test

import java.io.{ByteArrayOutputStream, FileWriter, StringWriter}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Objects

import scala.util.parsing.combinator.RegexParsers

import ai.rapids.cudf.{ColumnVector, DType, HostColumnVector}
import com.fasterxml.jackson.core.{JsonEncoding, JsonFactoryBuilder, JsonGenerator, JsonParser, JsonProcessingException, JsonToken}
import com.fasterxml.jackson.core.json.JsonReadFeature
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuColumnVector
import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}
import java.util

import org.apache.spark.sql.catalyst.json.CreateJacksonParser
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

case class CsvWriterWrapper(filePath: String) extends AutoCloseable {
  // create file writer with append mode
  val writer: FileWriter = new FileWriter(filePath, true)
  val csvWriter: CsvWriter = new CsvWriter(writer, new CsvWriterSettings())

  override def close(): Unit = {
    if (csvWriter != null) {
      csvWriter.close()
    }
    if (writer != null) {
      writer.close()
    }
  }

  def writeHeaders(headers: util.Collection[String]): Unit = {
    csvWriter.writeHeaders(headers)
  }

  def writeRow(row: Array[String]): Unit = {
    csvWriter.writeRow(row)
  }
}

object CpuGetJsonObject {
  // verify results from Cpu and Gpu, save diffs if have
  def verify(
      dataCv: ColumnVector,
      path: UTF8String,
      fromGpuCv: ColumnVector,
      fromCpuHCV: HostColumnVector,
      savePathForVerify: String,
      saveRowsForVerify: Int): Unit = {
    withResource(dataCv.copyToHost()) { dataHCV =>
      withResource(fromGpuCv.copyToHost()) { fromGpuHCV =>
        val savePath = savePathForVerify +
            DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDate.now()) + ".csv"
        withResource(CsvWriterWrapper(savePath)) { csvWriter =>
          val pathStr = if (path == null) "null" else path.toString
          var currRow = 0
          var diffRowsNum = 0
          while (currRow < dataCv.getRowCount.toInt &&
              diffRowsNum < saveRowsForVerify
          ) {
            val str = dataHCV.getJavaString(currRow)
            val cpuStr = if (fromCpuHCV.isNull(currRow)) null else fromCpuHCV.getJavaString(currRow)
            val gpuStr = if (fromGpuHCV.isNull(currRow)) null else fromGpuHCV.getJavaString(currRow)
            if (!Objects.equals(cpuStr, gpuStr)) { // if have diff
              diffRowsNum += 1
              // write to csv file
              csvWriter.writeRow(Array(pathStr, str, cpuStr, gpuStr))
            }
            currRow += 1
          }
        }
      }
    }
  }

  // get_json_object on Cpu
  def getJsonObjectOnCpu(dataCv: GpuColumnVector, path: UTF8String): HostColumnVector = {
    withResource(dataCv.copyToHost()) { dataHCV =>
      withResource(HostColumnVector.builder(DType.STRING, dataHCV.getRowCount.toInt)) {
        resultBuilder =>
          val cpuGetJsonObject = GetJsonObject(path)
          for (i <- 0 until dataHCV.getRowCount.toInt) {
            val s = dataHCV.getUTF8String(i)
            // Call Cpu get_json_object
            val utf8String = cpuGetJsonObject.eval(s)
            if (utf8String == null) {
              resultBuilder.appendNull()
            } else {
              resultBuilder.append(utf8String.toString)
            }
          }
          resultBuilder.build()
      }
    }
  }
}

// below code is copied from Spark
private[this] sealed trait PathInstruction

private[this] object PathInstruction {
  private[test] case object Subscript extends PathInstruction

  private[test] case object Wildcard extends PathInstruction

  private[test] case object Key extends PathInstruction

  private[test] case class Index(index: Long) extends PathInstruction

  private[test] case class Named(name: String) extends PathInstruction
}

private[this] sealed trait WriteStyle

private[this] object WriteStyle {
  private[test] case object RawStyle extends WriteStyle

  private[test] case object QuotedStyle extends WriteStyle

  private[test] case object FlattenStyle extends WriteStyle
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
}

private[this] object SharedFactory {
  val jsonFactory = new JsonFactoryBuilder()
      // The two options below enabled for Hive compatibility
      .enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS)
      .enable(JsonReadFeature.ALLOW_SINGLE_QUOTES)
      .build()
}

private[this] case class GetJsonObject(path: UTF8String) {

  import com.fasterxml.jackson.core.JsonToken._

  import PathInstruction._
  import SharedFactory._
  import WriteStyle._

  private lazy val parsedPath = parsePath(path)

  private[test] def eval(jsonStr: UTF8String): UTF8String = {
    if (jsonStr == null) {
      return null
    }

    val parsed = parsedPath

    if (parsed.isDefined) {
      try {
        /* We know the bytes are UTF-8 encoded. Pass a Reader to avoid having Jackson
          detect character encoding which could fail for some malformed strings */
        withResource(CreateJacksonParser.utf8String(jsonFactory, jsonStr)) { parser =>
          val output = new ByteArrayOutputStream()
          val matched = withResource(
            jsonFactory.createGenerator(output, JsonEncoding.UTF8)) { generator =>
            parser.nextToken()
            evaluatePath(parser, generator, RawStyle, parsed.get)
          }
          if (matched) {
            UTF8String.fromBytes(output.toByteArray)
          } else {
            null
          }
        }
      } catch {
        case _: JsonProcessingException => null
      }
    } else {
      null
    }
  }

  private def parsePath(path: UTF8String): Option[List[PathInstruction]] = {
    if (path != null) {
      JsonPathParser.parse(path.toString)
    } else {
      None
    }
  }

  // advance to the desired array index, assumes to start at the START_ARRAY token
  private def arrayIndex(p: JsonParser, f: () => Boolean): Long => Boolean = {
    case _ if p.getCurrentToken == END_ARRAY =>
      // terminate, nothing has been written
      false

    case 0 =>
      // we've reached the desired index
      val dirty = f()

      while (p.nextToken() != END_ARRAY) {
        // advance the token stream to the end of the array
        p.skipChildren()
      }

      dirty

    case i if i > 0 =>
      // skip this token and evaluate the next
      p.skipChildren()
      p.nextToken()
      arrayIndex(p, f)(i - 1)
  }

  /**
   * Evaluate a list of JsonPath instructions, returning a bool that indicates if any leaf nodes
   * have been written to the generator
   */
  private def evaluatePath(
      p: JsonParser,
      g: JsonGenerator,
      style: WriteStyle,
      path: List[PathInstruction]): Boolean = {
    (p.getCurrentToken, path) match {
      case (VALUE_STRING, Nil) if style == RawStyle =>
        // there is no array wildcard or slice parent, emit this string without quotes
        if (p.hasTextCharacters) {
          g.writeRaw(p.getTextCharacters, p.getTextOffset, p.getTextLength)
        } else {
          g.writeRaw(p.getText)
        }
        true

      case (START_ARRAY, Nil) if style == FlattenStyle =>
        // flatten this array into the parent
        var dirty = false
        while (p.nextToken() != END_ARRAY) {
          dirty |= evaluatePath(p, g, style, Nil)
        }
        dirty

      case (_, Nil) =>
        // general case: just copy the child tree verbatim
        g.copyCurrentStructure(p)
        true

      case (START_OBJECT, Key :: xs) =>
        var dirty = false
        while (p.nextToken() != END_OBJECT) {
          if (dirty) {
            // once a match has been found we can skip other fields
            p.skipChildren()
          } else {
            dirty = evaluatePath(p, g, style, xs)
          }
        }
        dirty

      case (START_ARRAY, Subscript :: Wildcard :: Subscript :: Wildcard :: xs) =>
        // special handling for the non-structure preserving double wildcard behavior in Hive
        var dirty = false
        g.writeStartArray()
        while (p.nextToken() != END_ARRAY) {
          dirty |= evaluatePath(p, g, FlattenStyle, xs)
        }
        g.writeEndArray()
        dirty

      case (START_ARRAY, Subscript :: Wildcard :: xs) if style != QuotedStyle =>
        // retain Flatten, otherwise use Quoted... cannot use Raw within an array
        val nextStyle = style match {
          case RawStyle => QuotedStyle
          case FlattenStyle => FlattenStyle
          case QuotedStyle => throw new IllegalStateException()
        }

        // temporarily buffer child matches, the emitted json will need to be
        // modified slightly if there is only a single element written
        val buffer = new StringWriter()

        var dirty = 0
        Utils.tryWithResource(jsonFactory.createGenerator(buffer)) { flattenGenerator =>
          flattenGenerator.writeStartArray()

          while (p.nextToken() != END_ARRAY) {
            // track the number of array elements and only emit an outer array if
            // we've written more than one element, this matches Hive's behavior
            dirty += (if (evaluatePath(p, flattenGenerator, nextStyle, xs)) 1 else 0)
          }
          flattenGenerator.writeEndArray()
        }

        val buf = buffer.getBuffer
        if (dirty > 1) {
          g.writeRawValue(buf.toString)
        } else if (dirty == 1) {
          // remove outer array tokens
          g.writeRawValue(buf.substring(1, buf.length() - 1))
        } // else do not write anything

        dirty > 0

      case (START_ARRAY, Subscript :: Wildcard :: xs) =>
        var dirty = false
        g.writeStartArray()
        while (p.nextToken() != END_ARRAY) {
          // wildcards can have multiple matches, continually update the dirty count
          dirty |= evaluatePath(p, g, QuotedStyle, xs)
        }
        g.writeEndArray()

        dirty

      case (START_ARRAY, Subscript :: Index(idx) :: (xs@Subscript :: Wildcard :: _)) =>
        p.nextToken()
        // we're going to have 1 or more results, switch to QuotedStyle
        arrayIndex(p, () => evaluatePath(p, g, QuotedStyle, xs))(idx)

      case (START_ARRAY, Subscript :: Index(idx) :: xs) =>
        p.nextToken()
        arrayIndex(p, () => evaluatePath(p, g, style, xs))(idx)

      case (FIELD_NAME, Named(name) :: xs) if p.getCurrentName == name =>
        // exact field match
        if (p.nextToken() != JsonToken.VALUE_NULL) {
          evaluatePath(p, g, style, xs)
        } else {
          false
        }

      case (FIELD_NAME, Wildcard :: xs) =>
        // wildcard field match
        p.nextToken()
        evaluatePath(p, g, style, xs)

      case _ =>
        p.skipChildren()
        false
    }
  }
}
