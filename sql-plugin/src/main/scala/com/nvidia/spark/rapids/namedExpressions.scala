/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

import java.util.Objects

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, ExprId, Generator, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.types.{DataType, Metadata}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuAlias(child: GpuExpression, name: String)(
    val exprId: ExprId = NamedExpression.newExprId,
    val qualifier: Seq[String] = Seq.empty,
    val explicitMetadata: Option[Metadata] = None)
  extends GpuUnaryExpression with NamedExpression {

  // Alias(Generator, xx) need to be transformed into Generate(generator, ...)
  override lazy val resolved =
    childrenResolved && checkInputDataTypes().isSuccess && !child.isInstanceOf[Generator]

  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable
  override def metadata: Metadata = {
    explicitMetadata.getOrElse {
      child match {
        case named: NamedExpression => named.metadata
        case _ => Metadata.empty
      }
    }
  }

  def newInstance(): NamedExpression =
    GpuAlias(child, name)(qualifier = qualifier, explicitMetadata = explicitMetadata)

  override def toAttribute: Attribute = {
    if (resolved) {
      AttributeReference(name, child.dataType, child.nullable, metadata)(exprId, qualifier)
    } else {
      UnresolvedAttribute(name)
    }
  }

  /** Used to signal the column used to calculate an eventTime watermark (e.g. a#1-T{delayMs}) */
  private def delaySuffix = if (metadata.contains(EventTimeWatermark.delayKey)) {
    s"-T${metadata.getLong(EventTimeWatermark.delayKey)}ms"
  } else {
    ""
  }

  override def toString: String = s"$child AS $name#${exprId.id}$typeSuffix$delaySuffix"

  override protected final def otherCopyArgs: Seq[AnyRef] = {
    exprId :: qualifier :: explicitMetadata :: Nil
  }

  override def hashCode(): Int = {
    val state = Seq(name, exprId, child, qualifier, explicitMetadata)
    state.map(Objects.hashCode).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def equals(other: Any): Boolean = other match {
    case a: GpuAlias =>
      name == a.name && exprId == a.exprId && child == a.child && qualifier == a.qualifier &&
        explicitMetadata == a.explicitMetadata
    case _ => false
  }

  override def sql: String = {
    val qualifierPrefix = if (qualifier.nonEmpty) qualifier.mkString(".") + "." else ""
    s"${child.sql} AS $qualifierPrefix${quoteIdentifier(name)}"
  }

  override def columnarEval(batch: ColumnarBatch): Any = child.columnarEval(batch)

  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    throw new IllegalStateException("GpuAlias should never have doColumnar called")
}

object GpuAttributeReference {
  def from(attr: Attribute): GpuAttributeReference = attr match {
    case attr: GpuAttributeReference => attr
    case attr: AttributeReference =>
      GpuAttributeReference(
        attr.name, attr.dataType, attr.nullable, attr.metadata)(attr.exprId, attr.qualifier)
    case attr => throw new IllegalStateException(s"Unexpected attribute $attr")
  }
}

case class GpuAttributeReference(
    name: String,
    dataType: DataType,
    nullable: Boolean = true,
    override val metadata: Metadata = Metadata.empty)(
    override val exprId: ExprId = NamedExpression.newExprId,
    override val qualifier: Seq[String] = Seq.empty[String])
  extends Attribute with GpuExpression {

  /**
   * Returns true iff the expression id is the same for both attributes.
   */
  def sameRef(other: GpuAttributeReference): Boolean = this.exprId == other.exprId

  override def equals(other: Any): Boolean = other match {
    case ar: GpuAttributeReference =>
      name == ar.name && dataType == ar.dataType && nullable == ar.nullable &&
        metadata == ar.metadata && exprId == ar.exprId && qualifier == ar.qualifier
    case _ => false
  }

  override def semanticEquals(other: Expression): Boolean = other match {
    case ar: GpuAttributeReference => sameRef(ar)
    case _ => false
  }

  override def semanticHash(): Int = {
    this.exprId.hashCode()
  }

  override def hashCode(): Int = {
    // See http://stackoverflow.com/questions/113511/hash-code-implementation
    var h = 17
    h = h * 37 + name.hashCode()
    h = h * 37 + dataType.hashCode()
    h = h * 37 + nullable.hashCode()
    h = h * 37 + metadata.hashCode()
    h = h * 37 + exprId.hashCode()
    h = h * 37 + qualifier.hashCode()
    h
  }

  override def newInstance(): GpuAttributeReference =
    GpuAttributeReference(name, dataType, nullable, metadata)(qualifier = qualifier)

  /**
   * Returns a copy of this [[GpuAttributeReference]] with changed nullability.
   */
  override def withNullability(newNullability: Boolean): GpuAttributeReference = {
    if (nullable == newNullability) {
      this
    } else {
      GpuAttributeReference(name, dataType, newNullability, metadata)(exprId, qualifier)
    }
  }

  override def withName(newName: String): GpuAttributeReference = {
    if (name == newName) {
      this
    } else {
      GpuAttributeReference(newName, dataType, nullable, metadata)(exprId, qualifier)
    }
  }

  /**
   * Returns a copy of this [[GpuAttributeReference]] with new qualifier.
   */
  override def withQualifier(newQualifier: Seq[String]): GpuAttributeReference = {
    if (newQualifier == qualifier) {
      this
    } else {
      GpuAttributeReference(name, dataType, nullable, metadata)(exprId, newQualifier)
    }
  }

  override def withExprId(newExprId: ExprId): GpuAttributeReference = {
    if (exprId == newExprId) {
      this
    } else {
      GpuAttributeReference(name, dataType, nullable, metadata)(newExprId, qualifier)
    }
  }

  override def withMetadata(newMetadata: Metadata): GpuAttributeReference = {
    GpuAttributeReference(name, dataType, nullable, newMetadata)(exprId, qualifier)
  }

  override protected final def otherCopyArgs: Seq[AnyRef] = {
    exprId :: qualifier :: Nil
  }

  /** Used to signal the column used to calculate an eventTime watermark (e.g. a#1-T{delayMs}) */
  private def delaySuffix = if (metadata.contains(EventTimeWatermark.delayKey)) {
    s"-T${metadata.getLong(EventTimeWatermark.delayKey)}ms"
  } else {
    ""
  }

  override def toString: String = s"$name#${exprId.id}$typeSuffix$delaySuffix"

  // Since the expression id is not in the first constructor it is missing from the default
  // tree string.
  override def simpleString(maxFields: Int): String = {
    s"$name#${exprId.id}: ${dataType.simpleString(maxFields)}"
  }

  override def sql: String = {
    val qualifierPrefix = if (qualifier.nonEmpty) qualifier.mkString(".") + "." else ""
    s"$qualifierPrefix${quoteIdentifier(name)}"
  }

  override def toAttribute: Attribute =
    AttributeReference(name, dataType, nullable, metadata)(exprId, qualifier)

  override def columnarEval(batch: ColumnarBatch): Any =
    throw new IllegalStateException("Attribute executed without being bound")
}
