/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

package ai.rapids.spark

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, ExprId, NamedExpression}
import org.apache.spark.sql.types.{DataType, Metadata}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuAlias(child: GpuExpression, name: String)(
    override val exprId: ExprId = NamedExpression.newExprId,
    override val qualifier: Seq[String] = Seq.empty,
    override val explicitMetadata: Option[Metadata] = None)
  extends Alias(child, name)(exprId, qualifier, explicitMetadata)
    with GpuExpression {

  override def columnarEval(batch: ColumnarBatch): Any = child.columnarEval(batch)
}


class GpuAttributeReference(
    name: String,
    dataType: DataType,
    nullable: Boolean = true,
    override val metadata: Metadata = Metadata.empty)(
    override val exprId: ExprId = NamedExpression.newExprId,
    override val qualifier: Seq[String] = Seq.empty[String])
  extends AttributeReference(name, dataType, nullable, metadata)(exprId, qualifier)
    with GpuExpression {

  override def columnarEval(batch: ColumnarBatch): Any =
    throw new IllegalStateException("Attribute executed without being bound")
}
