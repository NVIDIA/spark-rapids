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

package org.apache.spark.sql.rapids.protobuf

import org.apache.spark.sql.types.DataType

sealed trait ProtobufDescriptorSource

object ProtobufDescriptorSource {
  final case class DescriptorPath(path: String) extends ProtobufDescriptorSource
  final case class DescriptorBytes(bytes: Array[Byte]) extends ProtobufDescriptorSource
}

final case class ProtobufExprInfo(
    messageName: String,
    descriptorSource: ProtobufDescriptorSource,
    options: Map[String, String])

final case class ProtobufPlannerOptions(
    enumsAsInts: Boolean,
    failOnErrors: Boolean)

sealed trait ProtobufDefaultValue

object ProtobufDefaultValue {
  final case class BoolValue(value: Boolean) extends ProtobufDefaultValue
  final case class IntValue(value: Long) extends ProtobufDefaultValue
  final case class FloatValue(value: Float) extends ProtobufDefaultValue
  final case class DoubleValue(value: Double) extends ProtobufDefaultValue
  final case class StringValue(value: String) extends ProtobufDefaultValue
  final case class BinaryValue(value: Array[Byte]) extends ProtobufDefaultValue
  final case class EnumValue(number: Int, name: String) extends ProtobufDefaultValue
}

final case class ProtobufEnumValue(number: Int, name: String)

final case class ProtobufEnumMetadata(values: Seq[ProtobufEnumValue]) {
  lazy val validValues: Array[Int] = values.map(_.number).toArray
  lazy val orderedNames: Array[Array[Byte]] = values.map(_.name.getBytes("UTF-8")).toArray
  lazy val namesByNumber: Map[Int, String] = values.map(v => v.number -> v.name).toMap

  def enumDefault(number: Int): ProtobufDefaultValue.EnumValue = {
    val name = namesByNumber.getOrElse(number, s"$number")
    ProtobufDefaultValue.EnumValue(number, name)
  }
}

trait ProtobufMessageDescriptor {
  def syntax: String
  def findField(name: String): Option[ProtobufFieldDescriptor]
}

trait ProtobufFieldDescriptor {
  def name: String
  def fieldNumber: Int
  def protoTypeName: String
  def isRepeated: Boolean
  def isRequired: Boolean
  def defaultValue: Option[ProtobufDefaultValue]
  def enumMetadata: Option[ProtobufEnumMetadata]
  def messageDescriptor: Option[ProtobufMessageDescriptor]
}

final case class ProtobufFieldInfo(
    fieldNumber: Int,
    protoTypeName: String,
    sparkType: DataType,
    encoding: Int,
    isSupported: Boolean,
    unsupportedReason: Option[String],
    isRequired: Boolean,
    defaultValue: Option[ProtobufDefaultValue],
    enumMetadata: Option[ProtobufEnumMetadata],
    isRepeated: Boolean = false) {
  def hasDefaultValue: Boolean = defaultValue.isDefined
}

final case class FlattenedFieldDescriptor(
    fieldNumber: Int,
    parentIdx: Int,
    depth: Int,
    wireType: Int,
    outputTypeId: Int,
    encoding: Int,
    isRepeated: Boolean,
    isRequired: Boolean,
    hasDefaultValue: Boolean,
    defaultInt: Long,
    defaultFloat: Double,
    defaultBool: Boolean,
    defaultString: Array[Byte],
    enumValidValues: Array[Int],
    enumNames: Array[Array[Byte]]
)

final case class FlattenedSchemaArrays(
    fieldNumbers: Array[Int],
    parentIndices: Array[Int],
    depthLevels: Array[Int],
    wireTypes: Array[Int],
    outputTypeIds: Array[Int],
    encodings: Array[Int],
    isRepeated: Array[Boolean],
    isRequired: Array[Boolean],
    hasDefaultValue: Array[Boolean],
    defaultInts: Array[Long],
    defaultFloats: Array[Double],
    defaultBools: Array[Boolean],
    defaultStrings: Array[Array[Byte]],
    enumValidValues: Array[Array[Int]],
    enumNames: Array[Array[Array[Byte]]]
)
