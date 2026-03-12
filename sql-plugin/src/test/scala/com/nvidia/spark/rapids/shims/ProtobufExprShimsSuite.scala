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

package com.nvidia.spark.rapids.shims

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.rapids.GpuFromProtobuf
import org.apache.spark.sql.rapids.protobuf._
import org.apache.spark.sql.types._

class ProtobufExprShimsSuite extends AnyFunSuite {
  private val outputSchema = StructType(Seq(
    StructField("id", IntegerType, nullable = true),
    StructField("name", StringType, nullable = true)))

  private case class FakeExprChild() extends Expression {
    override def children: Seq[Expression] = Nil
    override def nullable: Boolean = true
    override def dataType: DataType = BinaryType
    override def eval(input: org.apache.spark.sql.catalyst.InternalRow): Any = null
    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
      throw new UnsupportedOperationException("not needed")
    override protected def withNewChildrenInternal(
        newChildren: IndexedSeq[Expression]): Expression = {
      assert(newChildren.isEmpty)
      this
    }
  }

  private abstract class FakeBaseProtobufExpr(childExpr: Expression) extends UnaryExpression {
    override def child: Expression = childExpr
    override def nullable: Boolean = true
    override def dataType: DataType = outputSchema
    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
      throw new UnsupportedOperationException("not needed")
    override protected def withNewChildInternal(newChild: Expression): Expression = this
  }

  private case class FakePathProtobufExpr(override val child: Expression)
      extends FakeBaseProtobufExpr(child) {
    def messageName: String = "test.Message"
    def descFilePath: Option[String] = Some("/tmp/test.desc")
    def options: scala.collection.Map[String, String] = Map("mode" -> "FAILFAST")
  }

  private case class FakeBytesProtobufExpr(override val child: Expression)
      extends FakeBaseProtobufExpr(child) {
    def messageName: String = "test.Message"
    def binaryDescriptorSet: Array[Byte] = Array[Byte](1, 2, 3)
    def options: scala.collection.Map[String, String] =
      Map("mode" -> "PERMISSIVE", "enums.as.ints" -> "true")
  }

  private case class FakeMissingOptionsExpr(override val child: Expression)
      extends FakeBaseProtobufExpr(child) {
    def messageName: String = "test.Message"
    def descFilePath: Option[String] = Some("/tmp/test.desc")
  }

  private case class FakeMessageDescriptor(
      syntax: String,
      fields: Map[String, ProtobufFieldDescriptor]) extends ProtobufMessageDescriptor {
    override def findField(name: String): Option[ProtobufFieldDescriptor] = fields.get(name)
  }

  private case class FakeFieldDescriptor(
      name: String,
      fieldNumber: Int,
      protoTypeName: String,
      isRepeated: Boolean = false,
      isRequired: Boolean = false,
      defaultValue: Option[ProtobufDefaultValue] = None,
      enumMetadata: Option[ProtobufEnumMetadata] = None,
      messageDescriptor: Option[ProtobufMessageDescriptor] = None) extends ProtobufFieldDescriptor

  test("compat extracts descriptor path and options from legacy expression") {
    val exprInfo = SparkProtobufCompat.extractExprInfo(FakePathProtobufExpr(FakeExprChild()))
    assert(exprInfo.isRight)
    val info = exprInfo.toOption.get
    assert(info.messageName == "test.Message")
    assert(info.options == Map("mode" -> "FAILFAST"))
    assert(info.descriptorSource ==
      ProtobufDescriptorSource.DescriptorPath("/tmp/test.desc"))
  }

  test("compat extracts binary descriptor source and planner options") {
    val exprInfo = SparkProtobufCompat.extractExprInfo(FakeBytesProtobufExpr(FakeExprChild()))
    assert(exprInfo.isRight)
    val info = exprInfo.toOption.get
    info.descriptorSource match {
      case ProtobufDescriptorSource.DescriptorBytes(bytes) =>
        assert(bytes.sameElements(Array[Byte](1, 2, 3)))
      case other =>
        fail(s"Unexpected descriptor source: $other")
    }
    val plannerOptions = SparkProtobufCompat.parsePlannerOptions(info.options)
    assert(plannerOptions ==
      Right(ProtobufPlannerOptions(enumsAsInts = true, failOnErrors = false)))
  }

  test("compat reports missing options accessor as cpu fallback reason") {
    val exprInfo = SparkProtobufCompat.extractExprInfo(FakeMissingOptionsExpr(FakeExprChild()))
    assert(exprInfo.left.toOption.exists(
      _.contains("Cannot read from_protobuf options via reflection")))
  }

  test("compat detects unsupported options and proto3 syntax") {
    assert(SparkProtobufCompat.unsupportedOptions(Map("mode" -> "FAILFAST", "foo" -> "bar")) ==
      Seq("foo"))
    assert(!SparkProtobufCompat.isGpuSupportedProtoSyntax("PROTO3"))
    assert(!SparkProtobufCompat.isGpuSupportedProtoSyntax("EDITIONS"))
    assert(!SparkProtobufCompat.isGpuSupportedProtoSyntax(""))
    assert(SparkProtobufCompat.isGpuSupportedProtoSyntax("PROTO2"))
  }

  test("extractor preserves typed enum defaults") {
    val enumMeta = ProtobufEnumMetadata(Seq(
      ProtobufEnumValue(0, "UNKNOWN"),
      ProtobufEnumValue(1, "EN"),
      ProtobufEnumValue(2, "ZH")))
    val msgDesc = FakeMessageDescriptor(
      syntax = "PROTO2",
      fields = Map(
        "language" -> FakeFieldDescriptor(
          name = "language",
          fieldNumber = 1,
          protoTypeName = "ENUM",
          defaultValue = Some(ProtobufDefaultValue.EnumValue(1, "EN")),
          enumMetadata = Some(enumMeta))))
    val schema = StructType(Seq(StructField("language", StringType, nullable = true)))

    val infos = ProtobufSchemaExtractor.analyzeAllFields(
      schema, msgDesc, enumsAsInts = false, "test.Message")

    assert(infos.isRight)
    assert(infos.toOption.get("language").defaultValue.contains(
      ProtobufDefaultValue.EnumValue(1, "EN")))
  }

  test("validator encodes enum-string defaults into both numeric and string payloads") {
    val enumMeta = ProtobufEnumMetadata(Seq(
      ProtobufEnumValue(0, "UNKNOWN"),
      ProtobufEnumValue(1, "EN")))
    val info = ProtobufFieldInfo(
      fieldNumber = 2,
      protoTypeName = "ENUM",
      sparkType = StringType,
      encoding = GpuFromProtobuf.ENC_ENUM_STRING,
      isSupported = true,
      unsupportedReason = None,
      isRequired = false,
      defaultValue = Some(ProtobufDefaultValue.EnumValue(1, "EN")),
      enumMetadata = Some(enumMeta),
      isRepeated = false)

    val flat = ProtobufSchemaValidator.toFlattenedFieldDescriptor(
      path = "common.language",
      field = StructField("language", StringType, nullable = true),
      fieldInfo = info,
      parentIdx = 0,
      depth = 1,
      outputTypeId = 6)

    assert(flat.isRight)
    assert(flat.toOption.get.defaultInt == 1L)
    assert(new String(flat.toOption.get.defaultString, "UTF-8") == "EN")
    assert(flat.toOption.get.enumValidValues.sameElements(Array(0, 1)))
    assert(flat.toOption.get.enumNames
      .map(new String(_, "UTF-8"))
      .sameElements(Array("UNKNOWN", "EN")))
  }

  test("validator rejects enum-string field without enum metadata") {
    val info = ProtobufFieldInfo(
      fieldNumber = 2,
      protoTypeName = "ENUM",
      sparkType = StringType,
      encoding = GpuFromProtobuf.ENC_ENUM_STRING,
      isSupported = true,
      unsupportedReason = None,
      isRequired = false,
      defaultValue = Some(ProtobufDefaultValue.EnumValue(1, "EN")),
      enumMetadata = None,
      isRepeated = false)

    val flat = ProtobufSchemaValidator.toFlattenedFieldDescriptor(
      path = "common.language",
      field = StructField("language", StringType, nullable = true),
      fieldInfo = info,
      parentIdx = 0,
      depth = 1,
      outputTypeId = 6)

    assert(flat.left.toOption.exists(_.contains("missing enum metadata")))
  }

  test("GpuFromProtobuf semantic equality is content-based for schema arrays") {
    def emptyEnumNames: Array[Array[Byte]] = Array.empty[Array[Byte]]

    val expr1 = GpuFromProtobuf(
      decodedSchema = outputSchema,
      fieldNumbers = Array(1, 2),
      parentIndices = Array(-1, -1),
      depthLevels = Array(0, 0),
      wireTypes = Array(0, 2),
      outputTypeIds = Array(3, 6),
      encodings = Array(0, 0),
      isRepeated = Array(false, false),
      isRequired = Array(false, false),
      hasDefaultValue = Array(false, false),
      defaultInts = Array(0L, 0L),
      defaultFloats = Array(0.0, 0.0),
      defaultBools = Array(false, false),
      defaultStrings = Array(Array.emptyByteArray, Array.emptyByteArray),
      enumValidValues = Array(Array.emptyIntArray, Array.emptyIntArray),
      enumNames = Array(emptyEnumNames, emptyEnumNames),
      failOnErrors = true,
      child = FakeExprChild())

    val expr2 = GpuFromProtobuf(
      decodedSchema = outputSchema,
      fieldNumbers = Array(1, 2),
      parentIndices = Array(-1, -1),
      depthLevels = Array(0, 0),
      wireTypes = Array(0, 2),
      outputTypeIds = Array(3, 6),
      encodings = Array(0, 0),
      isRepeated = Array(false, false),
      isRequired = Array(false, false),
      hasDefaultValue = Array(false, false),
      defaultInts = Array(0L, 0L),
      defaultFloats = Array(0.0, 0.0),
      defaultBools = Array(false, false),
      defaultStrings = Array(Array.emptyByteArray, Array.emptyByteArray),
      enumValidValues = Array(Array.emptyIntArray, Array.emptyIntArray),
      enumNames = Array(emptyEnumNames.map(identity), emptyEnumNames.map(identity)),
      failOnErrors = true,
      child = FakeExprChild())

    assert(expr1.semanticEquals(expr2))
    assert(expr1.semanticHash() == expr2.semanticHash())
  }
}
