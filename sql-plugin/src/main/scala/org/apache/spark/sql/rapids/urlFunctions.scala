/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import java.net.URISyntaxException

import ai.rapids.cudf.{ColumnVector, DType, RegexProgram, Scalar, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.shims.RapidsErrorUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

object GpuParseUrl {
  private val HOST = "HOST"
  private val PATH = "PATH"
  private val QUERY = "QUERY"
  private val REF = "REF"
  private val PROTOCOL = "PROTOCOL"
  private val FILE = "FILE"
  private val AUTHORITY = "AUTHORITY"
  private val USERINFO = "USERINFO"
  private val REGEXPREFIX = """(&|^|\?)"""
  private val REGEXSUBFIX = "=([^&]*)"
}

case class GpuParseUrl(children: Seq[Expression], 
    failOnErrorOverride: Boolean = SQLConf.get.ansiEnabled) 
  extends GpuExpression with ShimExpression with ExpectsInputTypes {

  def this(children: Seq[Expression]) = this(children, SQLConf.get.ansiEnabled)

  override def nullable: Boolean = true
  override def inputTypes: Seq[DataType] = Seq.fill(children.size)(StringType)
  override def dataType: DataType = StringType
  override def prettyName: String = "parse_url"

  import GpuParseUrl._
  
  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.size > 3 || children.size < 2) {
      RapidsErrorUtils.parseUrlWrongNumArgs(children.size) match {
        case res: Some[TypeCheckResult] => return res.get
        case _ => // error message has been thrown
      }
    }
    super[ExpectsInputTypes].checkInputDataTypes()
  }

  private def getPattern(key: UTF8String): RegexProgram = {
    val regex = REGEXPREFIX + key.toString + REGEXSUBFIX
    new RegexProgram(regex)
  }

  private def reValid(url: ColumnVector): ColumnVector = {
    // TODO: Validite the url
    val regex = """^[^ ]*$"""
    val prog = new RegexProgram(regex)
    withResource(url.matchesRe(prog)) { isMatch =>
      if (failOnErrorOverride) {
        withResource(isMatch.all()) { allMatch =>
          if (!allMatch.getBoolean) {
            val invalidUrl = UTF8String.fromString(url.toString())
            val exception = new URISyntaxException("", "")
            throw RapidsErrorUtils.invalidUrlException(invalidUrl, exception)
          }
        }
      }
      withResource(Scalar.fromNull(DType.STRING)) { nullScalar =>
        isMatch.ifElse(url, nullScalar)
      }
    }
  }

  private def reMatch(url: ColumnVector, partToExtract: String): ColumnVector = {
    val regex = """^(([^:/?#]+):)(//((([^:]*:?[^\@]*)\@)?(\[[0-9A-Za-z%.:]*\]|[^/?#:]*)""" + 
                """(:[0-9]+)?))?(([^?#]*)(\?([^#]*))?)(#(.*))?"""
    val prog = new RegexProgram(regex)
    withResource(url.extractRe(prog)) { table: Table =>
      partToExtract match {
        case HOST => table.getColumn(6).incRefCount()
        case PATH => table.getColumn(9).incRefCount()
        case QUERY => table.getColumn(10).incRefCount()
        case REF => table.getColumn(12).incRefCount()
        case PROTOCOL => table.getColumn(1).incRefCount()
        case FILE => table.getColumn(8).incRefCount()
        case AUTHORITY => table.getColumn(3).incRefCount()
        case USERINFO => table.getColumn(5).incRefCount()
        case _ => throw new IllegalArgumentException(s"Invalid partToExtract: $partToExtract")
      }
    }    
  }

  private def emptyToNulls(cv: ColumnVector): ColumnVector = {
    withResource(ColumnVector.fromStrings("")) { empty =>
      withResource(ColumnVector.fromStrings(null)) { nulls =>
        cv.findAndReplaceAll(empty, nulls)
      }
    }
  }

  // private def isHost(cv: ColumnVector): ColumnVector = {
  //   // TODO: Valid if it is a valid host name, including ipv4, ipv6 and hostname
  //   cv
  // }

  def doColumnar(numRows: Int, url: GpuScalar, partToExtract: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(url, numRows, StringType)) { urlCol =>
      doColumnar(urlCol, partToExtract)
    }
  }

  def doColumnar(url: GpuColumnVector, partToExtract: GpuScalar): ColumnVector = {
    val valid = reValid(url.getBase)
    val part = partToExtract.getValue.asInstanceOf[UTF8String].toString
    val matched = withResource(valid) { _ =>
      reMatch(valid, part)
    }
    if (part == HOST) {
      // withResource(matched) { _ =>
      //   isHost(matched)
      // }
      withResource(matched) { _ =>
        emptyToNulls(matched)
      }
    } else if (part == QUERY || part == REF) {
      val resWithNulls = withResource(matched) { _ =>
        emptyToNulls(matched)
      }
      withResource(resWithNulls) { _ =>
        resWithNulls.substring(1)
      }
    } else if (part == PATH || part == FILE) {
      matched
    } else {
      withResource(matched) { _ =>
        emptyToNulls(matched)
      }
    }
  }

  def doColumnar(url: GpuColumnVector, partToExtract: GpuScalar, key: GpuScalar): ColumnVector = {
    val query = partToExtract.getValue.asInstanceOf[UTF8String].toString
    if (query != QUERY) {
      // return a null columnvector
      return ColumnVector.fromStrings(null, null)
    }
    val matched = reMatch(url.getBase, query)
    val keyStr = key.getValue.asInstanceOf[UTF8String]
    val queryValue = withResource(matched) { _ =>
      withResource(matched.extractRe(getPattern(keyStr))) { table: Table =>
        table.getColumn(1).incRefCount()
      }
    }
    withResource(queryValue) { _ =>
      emptyToNulls(queryValue)
    }
  }

  override def columnarEval(batch: ColumnarBatch): Any = {
    if (children.size == 2) {
      val Seq(url, partToExtract) = children
      withResourceIfAllowed(url.columnarEval(batch)) { val0 =>
        withResourceIfAllowed(partToExtract.columnarEval(batch)) { val1 =>
          (val0, val1) match {
            case (v0: GpuColumnVector, v1: GpuScalar) =>
              GpuColumnVector.from(doColumnar(v0, v1), dataType)
            case _ =>
              throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
          }
        }
      }
    } else {
      // 3-arg, i.e. QUERY with key
      assert(children.size == 3)
      val Seq(url, partToExtract, key) = children
      withResourceIfAllowed(url.columnarEval(batch)) { val0 =>
        withResourceIfAllowed(partToExtract.columnarEval(batch)) { val1 =>
          withResourceIfAllowed(key.columnarEval(batch)) { val2 =>
            (val0, val1, val2) match {
              case (v0: GpuColumnVector, v1: GpuScalar, v2: GpuScalar) =>
                GpuColumnVector.from(doColumnar(v0, v1, v2), dataType)
              case _ =>
                throw new 
                    UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
            }
          }
        }
      }
    }
  }
}