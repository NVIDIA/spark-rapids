/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, GetStructField, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.execution.{LeafExecNode, ProjectExec, SparkPlan}
import org.apache.spark.sql.types.{BinaryType, IntegerType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class ProtobufBatchMergeSuite extends AnyFunSuite {

  private case class DummyColumnarLeaf(output: Seq[AttributeReference]) extends LeafExecNode {
    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("not needed for unit test")

    override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
      throw new UnsupportedOperationException("not needed for unit test")

    override def supportsColumnar: Boolean = true
  }

  private case class FakeProtobufDataToCatalyst(child: Expression) extends UnaryExpression {
    override def dataType: StructType = StructType(Seq(
      StructField("search_id", IntegerType, nullable = true),
      StructField("nested", StructType(Seq(StructField("value", IntegerType, nullable = true))),
        nullable = true)))
    override def nullable: Boolean = true
    override protected def withNewChildInternal(newChild: Expression): Expression = copy(newChild)
    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
      throw new UnsupportedOperationException("codegen not needed in unit test")
  }

  test("project meta detects extractor project over protobuf child project") {
    val binAttr = AttributeReference("bin", BinaryType)()
    val childScan = DummyColumnarLeaf(Seq(binAttr))
    val innerProject = ProjectExec(
      Seq(Alias(FakeProtobufDataToCatalyst(binAttr), "decoded")()),
      childScan)
    val decodedAttr = innerProject.output.head.toAttribute
    val outerProject = ProjectExec(Seq(
      Alias(GetStructField(decodedAttr, 0, None), "search_id")(),
      Alias(GetStructField(GetStructField(decodedAttr, 1, None), 0, None), "value")()),
      innerProject)

    assert(GpuProjectExecMeta.shouldCoalesceAfterProject(outerProject))
    assert(!GpuProjectExecMeta.shouldCoalesceAfterProject(innerProject))
  }

  test("protobuf batch merge config defaults off and can be enabled") {
    val enabledConf = new RapidsConf(Map(
      RapidsConf.ENABLE_PROTOBUF_BATCH_MERGE_AFTER_PROJECT.key -> "true"))

    assert(!new RapidsConf(Map.empty[String, String]).isProtobufBatchMergeAfterProjectEnabled)
    assert(enabledConf.isProtobufBatchMergeAfterProjectEnabled)
  }

  test("flagged gpu project drops output batching guarantee for post-project merge") {
    val childAttr = AttributeReference("value", IntegerType)()
    val child: SparkPlan = DummyColumnarLeaf(Seq(childAttr))

    val unflaggedProject = GpuProjectExec(
      projectList = child.output.map(a => Alias(a, a.name)()).toList,
      child = child,
      enablePreSplit = true,
      forcePostProjectCoalesce = false)
    val flaggedProject = GpuProjectExec(
      projectList = child.output.map(a => Alias(a, a.name)()).toList,
      child = child,
      enablePreSplit = true,
      forcePostProjectCoalesce = true)

    assert(!unflaggedProject.coalesceAfter)
    assert(flaggedProject.coalesceAfter)
    assert(unflaggedProject.outputBatching.isInstanceOf[TargetSize])
    assert(flaggedProject.outputBatching == null)
    assert(CoalesceGoal.satisfies(unflaggedProject.outputBatching, TargetSize(1L)))
    assert(!CoalesceGoal.satisfies(flaggedProject.outputBatching, TargetSize(1L)))
  }
}
