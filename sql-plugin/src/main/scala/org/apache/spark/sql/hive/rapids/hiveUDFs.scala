/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

package org.apache.spark.sql.hive.rapids

import com.nvidia.spark.{RapidsUDAF, RapidsUDF}
import com.nvidia.spark.rapids.GpuUserDefinedFunction
import org.apache.hadoop.hive.ql.exec.{UDAF, UDF}
import org.apache.hadoop.hive.ql.udf.generic.{AbstractGenericUDAFResolver, GenericUDF}

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.rapids.aggregate.GpuUDAFFunctionBase
import org.apache.spark.sql.types.DataType

/** Common implementation across Hive UDFs */
trait GpuHiveUDFBase extends GpuUserDefinedFunction {
  val funcWrapper: HiveFunctionWrapper

  override def nullable: Boolean = true

  override def foldable: Boolean = udfDeterministic && children.forall(_.foldable)

  override def toString: String = {
    s"$nodeName#${funcWrapper.functionClassName}(${children.mkString(",")})"
  }

  override def prettyName: String = name
}

/** GPU-accelerated version of Spark's `HiveSimpleUDF` */
case class GpuHiveSimpleUDF(
    name: String,
    funcWrapper: HiveFunctionWrapper,
    children: Seq[Expression],
    dataType: DataType,
    udfDeterministic: Boolean) extends GpuHiveUDFBase {
  @scala.annotation.nowarn(
    "msg=class UDF in package exec is deprecated"
  )
  @transient
  override lazy val function: RapidsUDF = funcWrapper.createFunction[UDF]().asInstanceOf[RapidsUDF]

  override def sql: String = s"$name(${children.map(_.sql).mkString(", ")})"
}

/** GPU-accelerated version of Spark's `HiveGenericUDF` */
case class GpuHiveGenericUDF(
    name: String,
    funcWrapper: HiveFunctionWrapper,
    children: Seq[Expression],
    dataType: DataType,
    udfDeterministic: Boolean,
    override val foldable: Boolean) extends GpuHiveUDFBase {
  @transient
  override lazy val function: RapidsUDF = funcWrapper.createFunction[GenericUDF]()
      .asInstanceOf[RapidsUDF]
}

case class GpuHiveUDAFFunction(
    name: String,
    funcWrapper: HiveFunctionWrapper,
    children: Seq[Expression],
    nullable: Boolean,
    dataType: DataType,
    isUDAFBridgeRequired: Boolean) extends GpuUDAFFunctionBase {

  @scala.annotation.nowarn("msg=class UDAF in package exec is deprecated")
  @transient
  override lazy val function: RapidsUDAF = if (isUDAFBridgeRequired) {
    funcWrapper.createFunction[UDAF]().asInstanceOf[RapidsUDAF]
  } else {
    funcWrapper.createFunction[AbstractGenericUDAFResolver]().asInstanceOf[RapidsUDAF]
  }

  override lazy val aggBufferAttributes: Seq[AttributeReference] = {
    // TODO make it compatible with the Spark one by leveraging TypedImperativeAggExprMeta.
    // The Spark HiveUDAFFunction returns only a BinaryType column as the aggregate buffer,
    // so the current implementation is not compatible with the Spark one.
    aggBufferTypes.zipWithIndex.map { case (dt, id) =>
      AttributeReference(s"${name}_$id", dt)()
    }
  }
}