/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.rapids.test.CpuGetJsonObject
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

case class GpuGetJsonObject(
    json: Expression,
    path: Expression,
    verifyBetweenCpuAndGpu: Boolean,
    savePathForVerify: String,
    saveRowsForVerify: Int)
    extends GpuBinaryExpressionArgsAnyScalar
        with ExpectsInputTypes {
  override def left: Expression = json
  override def right: Expression = path
  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)
  override def nullable: Boolean = true
  override def prettyName: String = "get_json_object"

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val fromGpu = lhs.getBase().getJSONObject(rhs.getBase)

    // Below is only for testing purpose
    if (verifyBetweenCpuAndGpu) { // enable verify diff between Cpu and Gpu
      val path = rhs.getValue.asInstanceOf[UTF8String]
      withResource(CpuGetJsonObject.getJsonObjectOnCpu(lhs, path)) { fromCpu =>
        // verify result, save diffs if have
        CpuGetJsonObject.verify(
          lhs.getBase, path, fromGpu, fromCpu, savePathForVerify, saveRowsForVerify)
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
