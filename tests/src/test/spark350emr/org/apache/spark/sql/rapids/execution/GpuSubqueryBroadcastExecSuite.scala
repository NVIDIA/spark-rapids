/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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
{"spark": "350emr"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

import com.nvidia.spark.rapids.SparkSessionHolder
import org.mockito.Mockito
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkException
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuSubqueryBroadcastExecSuite extends AnyFunSuite with BeforeAndAfter with Matchers {

  before {
    SparkSessionHolder // Ensure we have an active SparkSession
  }

  test("executeCollect") {
    val batch1 = createLongBatch(1, 2, 3)
    val batch2 = createLongBatch(4, 5, 6)
    val subqueryBroadcast = createIntGpuSubqueryBroadcast(batch1, batch2)
    subqueryBroadcast.executeCollect().map(_.getInt(0)) shouldBe Seq(1, 2, 3, 4, 5, 6)
  }

  test("materialize") {
    val batch1 = createLongBatch(1, 2, 3)
    val subqueryBroadcast = createIntGpuSubqueryBroadcast(batch1)

    val future = subqueryBroadcast.materialize()
    val result = Await.result(future, Duration.Inf)
    result shouldBe an [Array[InternalRow]]
    result.asInstanceOf[Array[InternalRow]].map(_.getInt(0)) shouldBe Seq(1, 2, 3)
  }

  test("cancel") {
    val batch1 = createLongBatch(1, 2, 3)
    val subqueryBroadcast = createIntGpuSubqueryBroadcast(batch1)

    subqueryBroadcast.cancel()
    val future = subqueryBroadcast.materialize()
    val thrown = the [SparkException] thrownBy Await.result(future, Duration.Inf)
    thrown.getMessage should include ("was cancelled")
  }

  private def createIntGpuSubqueryBroadcast(batches: ColumnarBatch*): GpuSubqueryBroadcastExec = {
    val output = Seq('key.int)
    val broadcast = MockGpuBroadcastExchangeExec(output, batches)
    GpuSubqueryBroadcastExec("ddp", 0, output, broadcast)(None)
  }

  private def createLongBatch(values: Long*): ColumnarBatch = {
    val vector = new OnHeapColumnVector(values.length, LongType)
    vector.putLongs(0, values.length, values.toArray, 0)
    new ColumnarBatch(Array(vector), values.length)
  }
}

case class MockGpuBroadcastExchangeExec(
    output: Seq[Attribute], data: Seq[ColumnarBatch]) extends LeafExecNode {

  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException

  override protected[sql] def doExecuteBroadcast[T](): Broadcast[T] = {
    val batch = MockitoSugar.mock[SerializeConcatHostBuffersDeserializeBatch]
    Mockito.doReturn(data.toArray, Nil: _*).when(batch).hostBatch
    new MockBroadcast[SerializeConcatHostBuffersDeserializeBatch](id = 0, batch)
        .asInstanceOf[Broadcast[T]]
  }
}

class MockBroadcast[T: ClassTag](id: Long, value: T) extends Broadcast[T](id) {

  override protected def getValue(): T = value

  override protected def doUnpersist(blocking: Boolean): Unit =
    throw new UnsupportedOperationException

  override protected def doDestroy(blocking: Boolean): Unit =
    throw new UnsupportedOperationException
}