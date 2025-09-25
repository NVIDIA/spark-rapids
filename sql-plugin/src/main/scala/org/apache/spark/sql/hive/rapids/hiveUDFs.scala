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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, SafeProjection, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.hive.HiveUDAFFunction
import org.apache.spark.sql.rapids.aggregate.{CpuToGpuBufferTransition, GpuToCpuBufferTransition, GpuTypedUDAFFunctionBase}
import org.apache.spark.sql.types.{DataType, StructType}

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
    isUDAFBridgeRequired: Boolean) extends GpuTypedUDAFFunctionBase {

  @scala.annotation.nowarn("msg=is deprecated")
  @transient
  override lazy val function: RapidsUDAF = if (isUDAFBridgeRequired) {
    funcWrapper.createFunction[UDAF]().asInstanceOf[RapidsUDAF]
  } else {
    funcWrapper.createFunction[AbstractGenericUDAFResolver]().asInstanceOf[RapidsUDAF]
  }
}

object HiveUDAFUtils {
  private[rapids] def cpuAggBufferType(hiveUDAF: HiveUDAFFunction): DataType = {
    try {
      // 'partialResultDataType' is private, so have to get it via the reflection.
      val pdtMethod = hiveUDAF.getClass.getMethod(
        "org$apache$spark$sql$hive$HiveUDAFFunction$$partialResultDataType")
      pdtMethod.invoke(hiveUDAF).asInstanceOf[DataType]
    } catch {
      case t: Throwable => throw new IllegalStateException("Can not get the aggregate " +
        "buffer type via 'partialResultDataType' from CPU HiveUDAFFunction", t)
    }
  }
}

case class G2cHiveUDAFBufferTransition(
    child: Expression,
    cpuBufType: DataType) extends GpuToCpuBufferTransition {
  private lazy val unsafeProj = if (cpuBufType.isInstanceOf[StructType]) {
    // GPU always uses a struct type for agg buffer, but CPU does not, depending on
    // the users implementation. So if a struct is used by CPU, then no need to
    // flatten it here.
    UnsafeProjection.create(Array(child.dataType))
  } else {
    UnsafeProjection.create(child.dataType.asInstanceOf[StructType].map(_.dataType).toArray)
  }

  private lazy val wrapRow: InternalRow => InternalRow =
    if (cpuBufType.isInstanceOf[StructType]) {
      // CPU expects a single struct column
      val wrappedRow = new GenericInternalRow(1)
      inputRow => {
        wrappedRow.update(0, inputRow)
        wrappedRow
      }
    } else {
      identity[InternalRow]
    }

  override protected def nullSafeEval(input: Any): Array[Byte] = {
    unsafeProj(wrapRow(input.asInstanceOf[InternalRow])).getBytes
  }
}

case class C2gHiveUDAFBufferTransition(
    child: Expression,
    cpuBufType: DataType,
    gpuType: DataType) extends CpuToGpuBufferTransition {
  override val dataType: DataType = gpuType

  // GPU always uses a struct type for agg buffer, but CPU does not, depending on
  // the users implementation. So if a struct is used by CPU, then no need to
  // flatten it here.
  private lazy val projTypes = if (cpuBufType.isInstanceOf[StructType]) {
    Array(gpuType)
  } else {
    gpuType.asInstanceOf[StructType].map(_.dataType).toArray
  }
  private lazy val row = new UnsafeRow(projTypes.length)
  private lazy val objectProj: InternalRow => InternalRow =
    if (cpuBufType.isInstanceOf[StructType]) {
      inputRow =>
        SafeProjection.create(projTypes)(inputRow).get(0, gpuType).asInstanceOf[InternalRow]
    } else {
      inputRow => SafeProjection.create(projTypes)(inputRow)
    }

  override protected def nullSafeEval(input: Any): InternalRow = {
    val bytes = input.asInstanceOf[Array[Byte]]
    row.pointTo(bytes, bytes.length)
    objectProj(row)
  }
}
