/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.ParseURI
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

object GpuParseUrl {
  val HOST = "HOST"
  val PATH = "PATH"
  val QUERY = "QUERY"
  val REF = "REF"
  val PROTOCOL = "PROTOCOL"
  val FILE = "FILE"
  val AUTHORITY = "AUTHORITY"
  val USERINFO = "USERINFO"

  def isSupportedPart(part: String): Boolean = {
    part match {
      case PROTOCOL | HOST | QUERY | PATH =>
        true
      case _ =>
        false
    }
  }
}

case class GpuParseUrl(children: Seq[Expression]) 
  extends GpuExpression with ShimExpression with ExpectsInputTypes {

  override def nullable: Boolean = true
  override def inputTypes: Seq[DataType] = Seq.fill(children.size)(StringType)
  override def dataType: DataType = StringType
  override def prettyName: String = "parse_url"

  import GpuParseUrl._

  def doColumnar(url: GpuColumnVector, partToExtract: GpuScalar): ColumnVector = {
    val part = partToExtract.getValue.asInstanceOf[UTF8String].toString
    part match {
      case PROTOCOL =>
        ParseURI.parseURIProtocol(url.getBase)
      case HOST =>
        ParseURI.parseURIHost(url.getBase)
      case QUERY =>
        ParseURI.parseURIQuery(url.getBase)
      case PATH =>
        ParseURI.parseURIPath(url.getBase)
      case REF | FILE | AUTHORITY | USERINFO =>
        throw new UnsupportedOperationException(s"$this is not supported partToExtract=$part. " +
            s"Only PROTOCOL, HOST, QUERY and PATH are supported")
      case _ =>
        throw new IllegalArgumentException(s"Invalid partToExtract: $partToExtract")
    }
  }

  def doColumnar(col: GpuColumnVector, partToExtract: GpuScalar, key: GpuScalar): ColumnVector = {
    val part = partToExtract.getValue.asInstanceOf[UTF8String].toString
    if (part != QUERY || key == null || !key.isValid) {
      // return a null columnvector
      return GpuColumnVector.columnVectorFromNull(col.getRowCount.toInt, StringType)
    }
    val keyStr = key.getValue.asInstanceOf[UTF8String].toString
    ParseURI.parseURIQueryWithLiteral(col.getBase, keyStr)
  }

  def doColumnar(col: GpuColumnVector, partToExtract: GpuScalar, 
      key: GpuColumnVector): ColumnVector = {
    val part = partToExtract.getValue.asInstanceOf[UTF8String].toString
    if (part != QUERY) {
      // return a null columnvector
      return GpuColumnVector.columnVectorFromNull(col.getRowCount.toInt, StringType)
    }
    ParseURI.parseURIQueryWithColumn(col.getBase, key.getBase)
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    if (children.size == 2) {
      val Seq(url, partToExtract) = children
      withResourceIfAllowed(url.columnarEval(batch)) { urls =>
        withResourceIfAllowed(partToExtract.columnarEvalAny(batch)) { parts =>
          parts match {
            case partScalar: GpuScalar =>
              GpuColumnVector.from(doColumnar(urls, partScalar), dataType)
            case _ =>
              throw new UnsupportedOperationException(
                s"Cannot columnar evaluate expression: $this")
          }
        }
      }
    } else {
      // 3-arg, i.e. QUERY with key
      assert(children.size == 3)
      val Seq(url, partToExtract, key) = children
      withResourceIfAllowed(url.columnarEval(batch)) { urls =>
        withResourceIfAllowed(partToExtract.columnarEvalAny(batch)) { parts =>
          withResourceIfAllowed(key.columnarEvalAny(batch)) { keys =>
            (urls, parts, keys) match {
              case (urlCv: GpuColumnVector, partScalar: GpuScalar, keyScalar: GpuScalar) =>
                GpuColumnVector.from(doColumnar(urlCv, partScalar, keyScalar), dataType)
              case (urlCv: GpuColumnVector, partScalar: GpuScalar, keyCv: GpuColumnVector) =>
                GpuColumnVector.from(doColumnar(urlCv, partScalar, keyCv), dataType)
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
