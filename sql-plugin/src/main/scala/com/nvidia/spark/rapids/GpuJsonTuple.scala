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

package com.nvidia.spark.rapids

import ai.rapids.cudf.Scalar
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuJsonTuple(children: Seq[Expression]) extends GpuGenerator
  with ShimExpression {
  override def nullable: Boolean = false // a row is always returned
  @transient private lazy val fieldExpressions: Seq[Expression] = children.tail

  override def elementSchema: StructType = StructType(fieldExpressions.zipWithIndex.map {
    case (_, idx) => StructField(s"c$idx", StringType, nullable = true)
  })

  override def prettyName: String = "json_tuple"

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length < 2) {
      TypeCheckResult.TypeCheckFailure(
        s"$prettyName has wrong number of auguments: expected > 1, but found ${children.length}"
      )
    } else if (children.forall(child => child.dataType == StringType)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(s"$prettyName only supports string type input")
    }
  }

  override def generate(inputBatch: ColumnarBatch,
      generatorOffset: Int,
      outer: Boolean): ColumnarBatch = {

    val json = inputBatch.column(generatorOffset).asInstanceOf[GpuColumnVector].getBase
    val schema = Array.fill[DataType](fieldExpressions.length)(StringType)

    val fieldScalars = fieldExpressions.safeMap { field =>
      withResourceIfAllowed(field.columnarEval(inputBatch)) { fieldVal =>
        fieldVal match {
          case fieldScalar: GpuScalar =>
            // Specials characters like '.', '[', ']' are not supported in field names
            Scalar.fromString("$." + fieldScalar.getBase.getJavaString)
          case _ => throw new UnsupportedOperationException(s"JSON field must be a scalar value")
        }
      }
    }

    withResource(fieldScalars) { fieldScalars =>
      withResource(fieldScalars.safeMap(field => json.getJSONObject(field))) { resultCols =>
        val generatorCols = resultCols.safeMap(_.incRefCount).zip(schema).safeMap {
          case (col, dataType) => GpuColumnVector.from(col, dataType)
        }
        val nonGeneratorCols = (0 until generatorOffset).safeMap { i =>
          inputBatch.column(i).asInstanceOf[GpuColumnVector].incRefCount
        }
        new ColumnarBatch((nonGeneratorCols ++ generatorCols).toArray, inputBatch.numRows)
      }
    }
  }

  override def inputSplitIndices(inputBatch: ColumnarBatch,
      generatorOffset: Int,
      outer: Boolean,
      targetSizeBytes: Long,
      maxRows: Int = Int.MaxValue): Array[Int] = {

    val inputRows = inputBatch.numRows
    // if the number of input rows is 1 or less, cannot split
    if (inputRows <= 1) return Array()

    val outputRows = inputRows
    val json = inputBatch.column(generatorOffset).asInstanceOf[GpuColumnVector].getBase

    // we know we are going to output at most this much
    val estimatedOutputSizeBytes = (json.getDeviceMemorySize * fieldExpressions.length).toDouble

    val numSplitsForTargetSize =
      math.min(inputRows, math.ceil(estimatedOutputSizeBytes / targetSizeBytes).toInt)
    val splitIndices =
      GpuBatchUtils.generateSplitIndices(inputRows, numSplitsForTargetSize).distinct

    // how many splits will we need to keep the output rows under max value
    val numSplitsForTargetRow = math.ceil(outputRows / maxRows).toInt

    // If the number of splits needed to keep the row limits for cuDF is higher than
    // the splits we found by size, we need to use the row-based splits.
    // Note, given skewed input, we could be left with batches split at bad places,
    // e.g. all of the non nulls are in a single split. So we may need to re-split
    // that row-based slice using the size approach.
    if (numSplitsForTargetRow > splitIndices.length) {
      GpuBatchUtils.generateSplitIndices(inputRows, numSplitsForTargetRow)
    } else {
      splitIndices
    }
  }
}
