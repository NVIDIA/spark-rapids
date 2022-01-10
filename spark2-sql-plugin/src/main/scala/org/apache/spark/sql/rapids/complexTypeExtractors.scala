/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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
import com.nvidia.spark.rapids.{BinaryExprMeta, DataFromReplacementRule, DataTypeUtils, GpuOverrides, RapidsConf, RapidsMeta}
import com.nvidia.spark.rapids.shims.v2.ShimUnaryExpression

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExtractValue, GetArrayItem, GetMapValue, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.catalyst.util.{quoteIdentifier, TypeUtils}
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, ArrayType, BooleanType, DataType, IntegralType, MapType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

class GpuGetArrayItemMeta(
    expr: GetArrayItem,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
    extends BinaryExprMeta[GetArrayItem](expr, conf, parent, rule) {
  import GpuOverrides._

  override def tagExprForGpu(): Unit = {
    extractLit(expr.ordinal).foreach { litOrd =>
      // Once literal array/struct types are supported this can go away
      val ord = litOrd.value
      if ((ord == null || ord.asInstanceOf[Int] < 0) && DataTypeUtils.isNestedType(expr.dataType)) {
        willNotWorkOnGpu("negative and null indexes are not supported for nested types")
      }
    }
  }
}

class GpuGetMapValueMeta(
  expr: GetMapValue,
  conf: RapidsConf,
  parent: Option[RapidsMeta[_, _]],
  rule: DataFromReplacementRule)
  extends BinaryExprMeta[GetMapValue](expr, conf, parent, rule) {

}

