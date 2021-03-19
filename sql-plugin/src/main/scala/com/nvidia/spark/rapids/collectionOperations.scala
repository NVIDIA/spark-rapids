/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._

case class GpuSize(child: Expression, legacySizeOfNull: Boolean)
  extends GpuUnaryExpression {

  require(child.dataType.isInstanceOf[ArrayType] || child.dataType.isInstanceOf[MapType],
    s"The size function doesn't support the operand type ${child.dataType}")

  override def dataType: DataType = IntegerType
  override def nullable: Boolean = if (legacySizeOfNull) false else super.nullable

  override protected def doColumnar(input: GpuColumnVector): ColumnVector = {
    val inputBase = input.getBase
    if (inputBase.getRowCount == 0) {
      return GpuColumnVector.from(GpuScalar.from(0), 0, IntegerType).getBase
    }

    // Compute sizes of cuDF.ListType to get sizes of each ArrayData or MapData, considering
    // MapData is represented as List of Struct in terms of cuDF.
    // We compute list size via subtracting the offset of next element(row) to the current offset.
    val collectionSize = {
      // Here is a hack: using index -1 to fetch the offset column of list.
      // In terms of cuDF native, the offset is the first (index 0) child of list_column_view.
      // In JNI layer, we add 1 to the child index when fetching child column of ListType to keep
      // alignment.
      // So, in JVM layer, we have to use -1 as index to fetch the real first child of list_column.
      withResource(inputBase.getChildColumnView(-1)) { offset =>
        withResource(offset.subVector(1)) { upBound =>
          withResource(offset.subVector(0, offset.getRowCount.toInt - 1)) { lowBound =>
            upBound.sub(lowBound)
          }
        }
      }
    }

    val nullScalar = if (legacySizeOfNull) {
      GpuScalar.from(-1)
    } else {
      GpuScalar.from(null, IntegerType)
    }

    withResource(collectionSize) { collectionSize =>
      withResource(nullScalar) { nullScalar =>
        withResource(inputBase.isNull) { inputIsNull =>
          inputIsNull.ifElse(nullScalar, collectionSize)
        }
      }
    }
  }
}
