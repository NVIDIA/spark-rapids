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

package com.nvidia.spark.rapids.delta

import ai.rapids.cudf.{ColumnVector, Scalar, Table}
import ai.rapids.cudf.Table.DuplicateKeepOption
import com.nvidia.spark.RapidsUDF
import com.nvidia.spark.rapids.Arm

import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.util.AccumulatorV2

class GpuDeltaRecordTouchedFileNameUDF(accum: AccumulatorV2[String, java.util.Set[String]])
    extends Function[String, Int] with RapidsUDF with Arm with Serializable {

  override def apply(fileName: String): Int = {
    accum.add(fileName)
    1
  }

  override def evaluateColumnar(numRows: Int, args: ColumnVector*): ColumnVector = {
    require(args.length == 1, s"Expected one argument, received $numRows")
    val input = args.head
    require(numRows == input.getRowCount, s"Expected $numRows rows, received ${input.getRowCount}")
    withResource(new Table(input)) { t =>
      val hostData = withResource(t.dropDuplicates(Array(0), DuplicateKeepOption.KEEP_ANY, true)) {
        _.getColumn(0).copyToHost()
      }
      withResource(hostData) { _ =>
        (0 until hostData.getRowCount.toInt).foreach { i =>
          val str = if (hostData.isNull(i)) {
            null
          } else {
            hostData.getJavaString(i)
          }
          accum.add(str)
        }
      }
      withResource(Scalar.fromInt(1)) { one =>
        ColumnVector.fromScalar(one, input.getRowCount.toInt)
      }
    }
  }
}

class GpuDeltaMetricUpdateUDF(metric: SQLMetric)
    extends Function0[Boolean] with RapidsUDF with Arm with Serializable {

  override def apply(): Boolean = {
    metric += 1
    true
  }

  override def evaluateColumnar(numRows: Int, args: ColumnVector*): ColumnVector = {
    require(args.isEmpty)
    metric += numRows
    withResource(Scalar.fromBool(true)) { s =>
      ColumnVector.fromScalar(s, numRows)
    }
  }
}
