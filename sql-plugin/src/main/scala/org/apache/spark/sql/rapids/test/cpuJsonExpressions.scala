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

import java.io.FileWriter
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Objects

import scala.collection.mutable
import scala.util.Random

import ai.rapids.cudf.{ColumnVector, DType, HostColumnVector}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuColumnVector
import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}

import org.apache.spark.sql.catalyst.expressions.{GetJsonObject, Literal}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

case class CsvWriterWrapper(filePath: String) extends AutoCloseable {
  private val appendableFileWriter: FileWriter = new FileWriter(filePath, true)
  private val csvWriter: CsvWriter = new CsvWriter(appendableFileWriter, new CsvWriterSettings())

  override def close(): Unit = {
    if (csvWriter != null) {
      // csv writer will close under file writer
      csvWriter.close()
    }
  }

  def writeRow(row: Array[String]): Unit = {
    csvWriter.writeRow(row)
  }
}

/**
 * Used to mask customer data to avoid customer data leakage.
 */
object GetJsonObjectMask {

  /**
   * Used by mask data
   */
  private def getRetainChars: Set[Char] = {
    val s = mutable.Set[Char]()
    for (i <- 0 to 32) {
      s += i.toChar
    }
    val others = Array[Char](
      '{', '}', '[', ']', ',', ':', '"', '\'',
      '\\', '/', 'b', 'f', 'n', 'r', 't', 'u',
      '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'e', 'E',
      'u', 'A', 'a', 'B', 'b', 'C', 'c', 'D', 'd', 'E', 'e', 'F', 'f',
      't', 'r', 'u', 'e',
      'f', 'a', 'l', 's', 'e',
      'n', 'u', 'l', 'l',
      '$', '[', ']', '.', '*', '\'', '?'
    )
    s ++= others
    s.toSet
  }

  private val RETAIN_CHARS = getRetainChars

  private def getCharsForKey: Set[Char] = {
    val buf = new mutable.ArrayBuffer[Char]
    for (c <- 'A' to 'Z') {
      buf.append(c)
    }
    for (c <- 'a' to 'z') {
      buf.append(c)
    }
    for (c <- '0' to '9') {
      buf.append(c)
    }
    buf.toSet
  }

  private val oneToOneMappingChars: Set[Char] = getCharsForKey -- RETAIN_CHARS

  /**
   * Mask data. RAPIDS Accelerator should not dump the original Customer data.
   * This dump tool only care about the functionality of get-json-object, the masked data should
   * reproduce issues if original data/path can reproduce issues. The mask is to find a way to
   * mask data and reproduce issues by using masked data.
   *
   * Special/retain chars, the following chars will not be masked:
   *     ASCII chars [0, 31] including space char
   *     { } [ ] , : " ' :  JSON structure chars, should not mask
   *     \  :  escape char, should not mask
   *     / b f n r t u : can follow \, should not mask
   *     - :  used by number, should not mask
   *     0-9  : used by number, should not mask
   *     e E  : used by number, e.g.: 1.0E-3, should not mask
   *     u A-F a-f : used by JSON string by unicode, e.g.: \u1e2F
   *     true :  should not mask
   *     false :  should not mask
   *     null :  should not mask
   *     $ [ ] . * '  : used by path, should not mask
   *     ?  : json path supports although Spark does not support, also add this because has no side
   *     effect
   * Above special/retain chars should not be masked, or the JSON will be invalid.
   *
   * Mask logic:
   *     - Assume path only contains a-z, A-Z, '-' and [0-9]
   *     - For char set [a-z, A-Z] minus special/retain chars like [eE1-9], create a random one to
   *       one mapping to mask data. e.g.: a -> b, b -> c, ..., z -> a
   *     - For other chars, e.g.: Chinese chars, map to a const char 's'
   *
   * @param jsonOrPath original JSON data or original Path
   * @return masked data
   */
  def mask(
      pathStr: String,
      jsonStr: String,
      cpuResult: String,
      gpuResult: String): Array[String] = {
    val random = new Random
    // generate one to one map
    // Note: path/json/result should use the same mask way
    val map = getMap(random.nextInt())
    Array(
      doMask(pathStr, RETAIN_CHARS, map),
      doMask(jsonStr, RETAIN_CHARS, map),
      doMask(cpuResult, RETAIN_CHARS, map),
      doMask(gpuResult, RETAIN_CHARS, map)
    )
  }

  private def getMap(seed: Int): Map[Char, Char] = {
    val random = new Random(seed)
    val charsFrom = random.shuffle(oneToOneMappingChars.toList)
    val charsTo = random.shuffle(oneToOneMappingChars.toList)
    val map = mutable.Map[Char, Char]()
    for( i <- charsFrom.indices) {
      map(charsFrom(i)) = charsTo(i)
    }
    map.toMap
  }

  /**
   * Mask chars
   * @param originStr origin json/path/result string
   * @param retainChars retain chars, should not be masked
   * @param oneToOneMap char to char map for masking
   * @return masked string
   */
  private def doMask(
      originStr: String,
      retainChars: Set[Char],
      oneToOneMap: Map[Char, Char]): String = {
    if (originStr != null) {
      val buf = new StringBuffer(originStr.length)
      var idx = 0
      while (idx < originStr.length) {
        val originChar = originStr(idx)
        if (oneToOneMappingChars.contains(originChar)) {
          val toChar = oneToOneMap(originChar)
          buf.append(toChar)
        } else {
          if (!retainChars.contains(originChar)) {
            // if it's not a retain char, replace to a const char 's'
            buf.append('s')
          } else {
            buf.append(originChar)
          }
        }
        idx += 1
      }
      buf.toString
    } else {
      null
    }
  }
}

object CpuGetJsonObject {
  /**
   * verify results from Cpu and Gpu, save diffs if have
   * @param dataCv original JSON data
   * @param path the path to extract JSON data
   * @param fromGpuCv result from GPU
   * @param fromCpuHCV result from CPU
   * @param savePathForVerify save path if have diffs
   * @param saveRowsForVerify max diff rows to save, Note: only take effective for current data
   */
  def verify(
      dataCv: ColumnVector,
      path: UTF8String,
      fromGpuCv: ColumnVector,
      fromCpuHCV: HostColumnVector,
      savePathForVerify: String,
      saveRowsForVerify: Int): Unit = {
    withResource(dataCv.copyToHost()) { dataHCV =>
      withResource(fromGpuCv.copyToHost()) { fromGpuHCV =>
        // save file is generated by date: yyyyMMdd.csv
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
              // mask customer data
              val masked = GetJsonObjectMask.mask(pathStr, str, cpuStr, gpuStr)
              // append to csv file: yyyyMMdd.csv
              csvWriter.writeRow(masked)
            }
            currRow += 1
          }
        }
      }
    }
  }

  /**
   * Run get-json-object on CPU
   * @param dataCv original JSON data
   * @param path path scalar
   * @return CPU result of get-json-object
   */
  def getJsonObjectOnCpu(dataCv: GpuColumnVector, path: UTF8String): HostColumnVector = {
    withResource(dataCv.copyToHost()) { dataHCV =>
      withResource(HostColumnVector.builder(DType.STRING, dataHCV.getRowCount.toInt)) {
        resultBuilder =>
          val pathLiteral = Literal.create(path, StringType)
          for (i <- 0 until dataHCV.getRowCount.toInt) {
            val json = dataHCV.getUTF8String(i)
            // In order to use `GetJsonObject` directly,
            // here use a literal json and a literal path
            val jsonLiteral = Literal.create(json, StringType)
            val cpuGetJsonObject = GetJsonObject(jsonLiteral, pathLiteral)
            // input null is safe because both json and path are literal
            val utf8String = cpuGetJsonObject.eval(null)
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
