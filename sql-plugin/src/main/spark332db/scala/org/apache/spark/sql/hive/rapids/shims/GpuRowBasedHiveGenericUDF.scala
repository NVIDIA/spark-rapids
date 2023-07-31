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

/*** spark-rapids-shim-json-lines
{"spark": "332db"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.hive.rapids.shims

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.hive.DeferredObjectAdapter
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.hive.rapids.GpuRowBasedHiveUDFBase
import org.apache.spark.sql.types.DataType

/** Row-based version of Spark's `HiveGenericUDF` running in a GPU operation */
case class GpuRowBasedHiveGenericUDF(
    name: String,
    funcWrapper: HiveFunctionWrapper,
    children: Seq[Expression]) extends GpuRowBasedHiveUDFBase {

  @transient
  override lazy val function: GenericUDF = funcWrapper.createFunction[GenericUDF]()

  @transient
  private lazy val returnInspector =
    function.initializeAndFoldConstants(argumentInspectors.toArray)

  @transient
  private lazy val deferredObjects = argumentInspectors.zip(children).map {
    case (inspect, child) => new DeferredObjectAdapter(inspect, child.dataType)
  }.toArray

  @transient
  private lazy val unwrapper = unwrapperFor(returnInspector)

  override protected def evaluateRow(childrenRow: InternalRow): Any = {
    returnInspector // Make sure initialized.

    var i = 0
    val length = children.length
    while (i < length) {
      val idx = i
      // deferred execution is no longer supported
      deferredObjects(i).set(childRowAccessors(idx)(childrenRow))
      i += 1
    }
    unwrapper(function.evaluate(deferredObjects.asInstanceOf[Array[DeferredObject]]))
  }

  override lazy val dataType: DataType = inspectorToDataType(returnInspector)

  override def foldable: Boolean =
    udfDeterministic && returnInspector.isInstanceOf[ConstantObjectInspector]
}