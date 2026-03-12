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
import org.apache.spark.sql.catalyst.expressions.GetArrayStructFields
import org.apache.spark.sql.rapids.{
  GpuFromProtobuf,
  GpuGetArrayStructFieldsMeta,
  GpuStructFieldOrdinalTag
}
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

  private case class FakeDifferentMessageExpr(override val child: Expression)
      extends FakeBaseProtobufExpr(child) {
    def messageName: String = "test.OtherMessage"
    def descFilePath: Option[String] = Some("/tmp/test.desc")
    def options: scala.collection.Map[String, String] = Map("mode" -> "FAILFAST")
  }

  private case class FakeDifferentDescriptorExpr(override val child: Expression)
      extends FakeBaseProtobufExpr(child) {
    def messageName: String = "test.Message"
    def descFilePath: Option[String] = Some("/tmp/other.desc")
    def options: scala.collection.Map[String, String] = Map("mode" -> "FAILFAST")
  }

  private case class FakeDifferentOptionsExpr(override val child: Expression)
      extends FakeBaseProtobufExpr(child) {
    def messageName: String = "test.Message"
    def descFilePath: Option[String] = Some("/tmp/test.desc")
    def options: scala.collection.Map[String, String] = Map("mode" -> "PERMISSIVE")
  }

  private case class FakeTypedUnaryExpr(
      dt: DataType,
      override val child: Expression = FakeExprChild()) extends UnaryExpression {
    override def nullable: Boolean = true
    override def dataType: DataType = dt
    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
      throw new UnsupportedOperationException("not needed")
    override protected def withNewChildInternal(newChild: Expression): Expression = copy(child =
      newChild)
  }

  private object FakeSpark34ProtobufUtils {
    def buildDescriptor(messageName: String, descFilePath: Option[String]): String =
      s"$messageName:${descFilePath.getOrElse("none")}"
  }

  private object FakeSpark35ProtobufUtils {
    def buildDescriptor(messageName: String, binaryFileDescriptorSet: Option[Array[Byte]]): String =
      s"$messageName:${binaryFileDescriptorSet.map(_.mkString(",")).getOrElse("none")}"
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
      defaultValueError: Option[String] = None,
      enumMetadata: Option[ProtobufEnumMetadata] = None,
      messageDescriptor: Option[ProtobufMessageDescriptor] = None) extends ProtobufFieldDescriptor {
    override lazy val defaultValueResult: Either[String, Option[ProtobufDefaultValue]] =
      defaultValueError match {
        case Some(reason) => Left(reason)
        case None => Right(defaultValue)
      }
  }

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

  test("compat invokes Spark 3.4 descriptor builder with descriptor path") {
    val buildMethod = FakeSpark34ProtobufUtils.getClass.getMethod(
      "buildDescriptor", classOf[String], classOf[scala.Option[_]])

    val result = SparkProtobufCompat.invokeBuildDescriptor(
      buildMethod,
      FakeSpark34ProtobufUtils,
      "test.Message",
      ProtobufDescriptorSource.DescriptorPath("/tmp/test.desc"),
      _ => fail("path-to-bytes fallback should not be needed for Spark 3.4"))

    assert(result == "test.Message:/tmp/test.desc")
  }

  test("compat retries descriptor path as bytes for Spark 3.5 descriptor builder") {
    val buildMethod = FakeSpark35ProtobufUtils.getClass.getMethod(
      "buildDescriptor", classOf[String], classOf[scala.Option[_]])
    var readCalls = 0

    val result = SparkProtobufCompat.invokeBuildDescriptor(
      buildMethod,
      FakeSpark35ProtobufUtils,
      "test.Message",
      ProtobufDescriptorSource.DescriptorPath("/tmp/test.desc"),
      _ => {
        readCalls += 1
        Array[Byte](1, 2, 3)
      })

    assert(readCalls == 1)
    assert(result == "test.Message:1,2,3")
  }

  test("compat passes bytes directly to Spark 3.5 descriptor builder") {
    val buildMethod = FakeSpark35ProtobufUtils.getClass.getMethod(
      "buildDescriptor", classOf[String], classOf[scala.Option[_]])

    val result = SparkProtobufCompat.invokeBuildDescriptor(
      buildMethod,
      FakeSpark35ProtobufUtils,
      "test.Message",
      ProtobufDescriptorSource.DescriptorBytes(Array[Byte](4, 5, 6)),
      _ => fail("binary descriptor source should not read a file"))

    assert(result == "test.Message:4,5,6")
  }

  test("compat distinguishes decode semantics across message descriptor and options") {
    val child = FakeExprChild()

    assert(SparkProtobufCompat.sameDecodeSemantics(
      FakePathProtobufExpr(child), FakePathProtobufExpr(child)))
    assert(SparkProtobufCompat.sameDecodeSemantics(
      FakeBytesProtobufExpr(child), FakeBytesProtobufExpr(child)))
    assert(!SparkProtobufCompat.sameDecodeSemantics(
      FakePathProtobufExpr(child), FakeDifferentMessageExpr(child)))
    assert(!SparkProtobufCompat.sameDecodeSemantics(
      FakePathProtobufExpr(child), FakeDifferentDescriptorExpr(child)))
    assert(!SparkProtobufCompat.sameDecodeSemantics(
      FakePathProtobufExpr(child), FakeDifferentOptionsExpr(child)))
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

  test("extractor reports default value reflection failures as cpu fallback reason") {
    val msgDesc = FakeMessageDescriptor(
      syntax = "PROTO2",
      fields = Map(
        "id" -> FakeFieldDescriptor(
          name = "id",
          fieldNumber = 1,
          protoTypeName = "INT32",
          defaultValueError =
            Some("Failed to read protobuf default value for field 'id': unsupported type"))))
    val schema = StructType(Seq(StructField("id", IntegerType, nullable = true)))

    val infos = ProtobufSchemaExtractor.analyzeAllFields(
      schema, msgDesc, enumsAsInts = true, "test.Message")

    assert(infos.left.toOption.exists(
      _.contains("Failed to read protobuf default value for field 'id'")))
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

  test("validator returns Left for incompatible default type instead of throwing") {
    val info = ProtobufFieldInfo(
      fieldNumber = 3,
      protoTypeName = "FLOAT",
      sparkType = DoubleType,
      encoding = GpuFromProtobuf.ENC_DEFAULT,
      isSupported = true,
      unsupportedReason = None,
      isRequired = false,
      defaultValue = Some(ProtobufDefaultValue.FloatValue(1.5f)),
      enumMetadata = None,
      isRepeated = false)

    val flat = ProtobufSchemaValidator.toFlattenedFieldDescriptor(
      path = "common.score",
      field = StructField("score", DoubleType, nullable = true),
      fieldInfo = info,
      parentIdx = 0,
      depth = 1,
      outputTypeId = 6)

    assert(flat.left.toOption.exists(
      _.contains("Incompatible default value for protobuf field 'common.score'")))
  }

  test("array struct field meta uses pruned child field count after ordinal remap") {
    val originalStruct = StructType(Seq(
      StructField("a", IntegerType, nullable = true),
      StructField("b", IntegerType, nullable = true),
      StructField("c", IntegerType, nullable = true)))
    val prunedStruct = StructType(Seq(StructField("b", IntegerType, nullable = true)))
    val originalChild = FakeTypedUnaryExpr(ArrayType(originalStruct, containsNull = true))
    val sparkExpr = GetArrayStructFields(
      child = originalChild,
      field = originalStruct.fields(1),
      ordinal = 1,
      numFields = originalStruct.fields.length,
      containsNull = true)
    sparkExpr.setTagValue(GpuStructFieldOrdinalTag.PRUNED_ORDINAL_TAG, 0)

    val prunedChild = FakeTypedUnaryExpr(ArrayType(prunedStruct, containsNull = true))
    val runtimeOrd = sparkExpr.getTagValue(GpuStructFieldOrdinalTag.PRUNED_ORDINAL_TAG).get

    assert(runtimeOrd == 0)
    assert(
      GpuGetArrayStructFieldsMeta.effectiveNumFields(prunedChild, sparkExpr, runtimeOrd) == 1)
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
