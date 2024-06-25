/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{BinaryOp, ColumnVector, ColumnView}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuExpression, GpuProjectExec, GpuUnaryExpression}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.Hash
import com.nvidia.spark.rapids.shims.{HashUtils, ShimExpression}

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuMd5(child: Expression)
  extends GpuUnaryExpression with ImplicitCastInputTypes with NullIntolerant {
  override def toString: String = s"md5($child)"
  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)
  override def dataType: DataType = StringType

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    withResource(HashUtils.normalizeInput(input.getBase)) { normalized =>
      withResource(ColumnVector.md5Hash(normalized)) { fullResult =>
        fullResult.mergeAndSetValidity(BinaryOp.BITWISE_AND, normalized)
      }
    }
  }
}

abstract class GpuHashExpression extends GpuExpression with ShimExpression {
  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = false

  private def hasMapType(dt: DataType): Boolean = {
    dt.existsRecursively(_.isInstanceOf[MapType])
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length < 1) {
      TypeCheckResult.TypeCheckFailure(
        s"input to function $prettyName requires at least one argument")
    } else if (children.exists(child => hasMapType(child.dataType)) &&
      !SQLConf.get.getConf(SQLConf.LEGACY_ALLOW_HASH_ON_MAPTYPE)) {
      TypeCheckResult.TypeCheckFailure(
        s"input to function $prettyName cannot contain elements of MapType. In Spark, same maps " +
          "may have different hashcode, thus hash expressions are prohibited on MapType elements." +
          s" To restore previous behavior set ${SQLConf.LEGACY_ALLOW_HASH_ON_MAPTYPE.key} " +
          "to true.")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }
}

object GpuMurmur3Hash {
  def compute(batch: ColumnarBatch,
      boundExpr: Seq[Expression],
      seed: Int = 42): ColumnVector = {
    withResource(GpuProjectExec.project(batch, boundExpr)) { args =>
      val bases = GpuColumnVector.extractBases(args)
      val normalized = bases.safeMap { cv =>
        HashUtils.normalizeInput(cv).asInstanceOf[ColumnView]
      }
      withResource(normalized) { _ =>
        Hash.murmurHash32(seed, normalized)
      }
    }
  }
}

case class GpuMurmur3Hash(children: Seq[Expression], seed: Int) extends GpuHashExpression {
  override def dataType: DataType = IntegerType

  override def prettyName: String = "hash"

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector =
    GpuColumnVector.from(GpuMurmur3Hash.compute(batch, children, seed), dataType)
}


case class GpuXxHash64(children: Seq[Expression], seed: Long) extends GpuHashExpression {
  override def dataType: DataType = LongType

  override def prettyName: String = "xxhash64"

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResource(children.safeMap(_.columnarEval(batch))) { childCols =>
      val cudfCols = childCols.map(_.getBase.asInstanceOf[ColumnView]).toArray
      GpuColumnVector.from(Hash.xxhash64(seed, cudfCols), dataType)
    }
  }
}
