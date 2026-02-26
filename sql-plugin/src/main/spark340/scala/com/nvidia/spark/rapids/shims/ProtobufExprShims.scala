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

/*** spark-rapids-shim-json-lines
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "400"}
{"spark": "401"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import java.lang.ReflectiveOperationException

import scala.collection.mutable
import scala.util.Try

import ai.rapids.cudf.DType
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.jni.Protobuf.{WT_32BIT, WT_64BIT, WT_LEN, WT_VARINT}

import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference, Expression, GetArrayStructFields, GetStructField, UnaryExpression
}
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.rapids.GpuFromProtobuf
import org.apache.spark.sql.types._

/**
 * Information about a protobuf field for schema projection support.
 */
private[shims] case class ProtobufFieldInfo(
    fieldNumber: Int,
    protoTypeName: String,
    sparkType: DataType,
    encoding: Int,
    isSupported: Boolean,
    unsupportedReason: Option[String],
    isRequired: Boolean,
    hasDefaultValue: Boolean,
    defaultValue: Option[Any],  // Stored as protobuf-java type, will be converted for JNI
    // Valid enum values for ENUM fields (used for validation)
    enumValues: Option[Set[Int]] = None,
    // Enum value-name mapping for ENUM -> STRING decoding (enumsAsInts=false)
    enumNames: Option[Map[Int, String]] = None,
    isRepeated: Boolean = false  // Whether this is a repeated field
)

/**
 * Flattened field descriptor for nested protobuf schemas.
 * Used to represent a hierarchical schema as a linear array for GPU processing.
 */
private[shims] case class FlattenedFieldDescriptor(
    fieldNumber: Int,
    parentIdx: Int,          // Index of parent field in flattened array (-1 for top-level)
    depth: Int,              // Nesting depth (0 for top-level)
    wireType: Int,           // Protobuf wire type
    outputTypeId: Int,       // cudf type id for the output (element type for repeated)
    encoding: Int,           // Encoding (default/fixed/zigzag)
    isRepeated: Boolean,     // Whether this is a repeated field
    isRequired: Boolean,     // Whether this field is required (proto2)
    hasDefaultValue: Boolean,
    defaultInt: Long,
    defaultFloat: Double,
    defaultBool: Boolean,
    defaultString: Array[Byte],
    enumValidValues: Array[Int],
    enumNames: Array[Array[Byte]]
)

/**
 * Spark 3.4+ optional integration for spark-protobuf expressions.
 *
 * spark-protobuf is an external module, so these rules must be registered by reflection.
 */
object ProtobufExprShims {
  private[this] val protobufDataToCatalystClassName =
    "org.apache.spark.sql.protobuf.ProtobufDataToCatalyst"

  private[this] val sparkProtobufUtilsObjectClassName =
    "org.apache.spark.sql.protobuf.utils.ProtobufUtils$"

  def exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    try {
      val clazz = ShimReflectionUtils.loadClass(protobufDataToCatalystClassName)
        .asInstanceOf[Class[_ <: UnaryExpression]]
      Map(clazz.asInstanceOf[Class[_ <: Expression]] -> fromProtobufRule)
    } catch {
      case _: ClassNotFoundException => Map.empty
    }
  }

  private def fromProtobufRule: ExprRule[_ <: Expression] = {
    GpuOverrides.expr[UnaryExpression](
      "Decode a BinaryType column (protobuf) into a Spark SQL struct",
      ExprChecks.unaryProject(
        // Use TypeSig.all here because schema projection determines which fields
        // actually need GPU support. Detailed type checking is done in tagExprForGpu.
        TypeSig.all,
        TypeSig.all,
        TypeSig.BINARY,
        TypeSig.BINARY),
      (e, conf, p, r) => new UnaryExprMeta[UnaryExpression](e, conf, p, r) {

        // Full schema from the expression (must match original dataType for compatibility)
        private var fullSchema: StructType = _
        private var failOnErrors: Boolean = _

        // Flattened schema variables for GPU decoding
        private var flatFieldNumbers: Array[Int] = _
        private var flatParentIndices: Array[Int] = _
        private var flatDepthLevels: Array[Int] = _
        private var flatWireTypes: Array[Int] = _
        private var flatOutputTypeIds: Array[Int] = _
        private var flatEncodings: Array[Int] = _
        private var flatIsRepeated: Array[Boolean] = _
        private var flatIsRequired: Array[Boolean] = _
        private var flatHasDefaultValue: Array[Boolean] = _
        private var flatDefaultInts: Array[Long] = _
        private var flatDefaultFloats: Array[Double] = _
        private var flatDefaultBools: Array[Boolean] = _
        private var flatDefaultStrings: Array[Array[Byte]] = _
        private var flatEnumValidValues: Array[Array[Int]] = _
        private var flatEnumNames: Array[Array[Array[Byte]]] = _
        // Indices in fullSchema for top-level fields that were decoded (for schema projection)
        private var decodedTopLevelIndices: Array[Int] = _

        override def tagExprForGpu(): Unit = {
          fullSchema = e.dataType match {
            case st: StructType => st
            case other =>
              willNotWorkOnGpu(
                s"Only StructType output is supported for from_protobuf, got $other")
              return
          }

          val options = getOptionsMap(e)
          val supportedOptions = Set("enums.as.ints", "mode")
          val unsupportedOptions = options.keys.filterNot(supportedOptions.contains)
          if (unsupportedOptions.nonEmpty) {
            val keys = unsupportedOptions.mkString(",")
            willNotWorkOnGpu(
              s"from_protobuf options are not supported yet on GPU: $keys")
            return
          }

          val enumsAsInts = options.getOrElse("enums.as.ints", "false").toBoolean
          failOnErrors = options.getOrElse("mode", "PERMISSIVE").equalsIgnoreCase("FAILFAST")
          val messageName = getMessageName(e)

          // Try to get descriptor: either file path (Spark 3.4.x) or binary bytes (Spark 3.5+)
          val descFilePathOrBytes: Option[Either[String, Array[Byte]]] =
            getDescFilePath(e).map(Left(_)).orElse {
              getDescriptorBytes(e).map(Right(_))
            }

          if (descFilePathOrBytes.isEmpty) {
            willNotWorkOnGpu(
              "from_protobuf requires a descriptor set " +
                "(descFilePath or binaryFileDescriptorSet)")
            return
          }

          val msgDesc = try {
            // Spark 3.4.x: buildDescriptor(messageName, descFilePath: Option[String])
            // Spark 3.5+:  buildDescriptor(messageName, binaryFileDescriptorSet)
            buildMessageDescriptorWithSparkProtobuf(messageName, descFilePathOrBytes.get)
          } catch {
            case t: Throwable =>
              willNotWorkOnGpu(
                s"Failed to resolve protobuf descriptor for message '$messageName': " +
                  s"${t.getMessage}")
              return
          }

          // Reject proto3 descriptors — GPU decoder only supports proto2 semantics.
          // proto3 has different null/default-value behavior that the GPU path doesn't handle.
          val protoSyntax = Try {
            val fileDesc = invoke0[AnyRef](msgDesc, "getFile")
            val syntaxObj = invoke0[AnyRef](fileDesc, "getSyntax")
            typeName(syntaxObj)
          }.getOrElse("")
          if (protoSyntax == "PROTO3") {
            willNotWorkOnGpu(
              "proto3 syntax is not supported by the GPU protobuf decoder; " +
                "only proto2 is supported. The query will fall back to CPU.")
            return
          }

          // Step 1: Analyze all fields and build field info map
          val allFieldsInfo = analyzeAllFields(fullSchema, msgDesc, enumsAsInts, messageName)
          if (allFieldsInfo.isEmpty) {
            // Error was already reported in analyzeAllFields
            return
          }
          val fieldsInfoMap = allFieldsInfo.get

          // Step 2: Determine which fields are actually required by downstream operations
          val requiredFieldNames = analyzeRequiredFields(fieldsInfoMap.keySet)

          // Step 3: Check if all required fields are supported
          val unsupportedRequired = requiredFieldNames.filter { name =>
            fieldsInfoMap.get(name).exists(!_.isSupported)
          }

          if (unsupportedRequired.nonEmpty) {
            val reasons = unsupportedRequired.map { name =>
              val info = fieldsInfoMap(name)
              s"${name}: ${info.unsupportedReason.getOrElse("unknown reason")}"
            }
            willNotWorkOnGpu(
              s"Required fields not supported for from_protobuf: ${reasons.mkString(", ")}")
            return
          }

          // Step 4: Identify which fields in fullSchema need to be decoded
          // These are fields that are required AND supported
          val indicesToDecode = fullSchema.fields.zipWithIndex.collect {
            case (sf, idx) if requiredFieldNames.contains(sf.name) => idx
          }

          // Verify all fields to be decoded are actually supported
          // (This catches edge cases where field analysis might have issues)
          val unsupportedInDecode = indicesToDecode.filter { idx =>
            val sf = fullSchema.fields(idx)
            fieldsInfoMap.get(sf.name).exists(!_.isSupported)
          }
          if (unsupportedInDecode.nonEmpty) {
            val reasons = unsupportedInDecode.map { idx =>
              val sf = fullSchema.fields(idx)
              val info = fieldsInfoMap(sf.name)
              s"${sf.name}: ${info.unsupportedReason.getOrElse("unknown reason")}"
            }
            willNotWorkOnGpu(
              s"Fields not supported for from_protobuf: ${reasons.mkString(", ")}")
            return
          }

          // Step 5: Build flattened schema for GPU decoding.
          // The flattened schema represents nested fields with parent indices.
          // For pure scalar schemas, all fields are top-level (parentIdx == -1, depth == 0).
          {
            val flatFields = mutable.ArrayBuffer[FlattenedFieldDescriptor]()

            // Helper to add a field and its children recursively
            def addFieldWithChildren(
                sf: StructField,
                info: ProtobufFieldInfo,
                parentIdx: Int,
                depth: Int,
                nestedMsgDesc: AnyRef): Unit = {

              val currentIdx = flatFields.size

              val outputType = sf.dataType match {
                case ArrayType(elemType, _) =>
                  elemType match {
                    case _: StructType =>
                      // Repeated message field: ArrayType(StructType) - element type is STRUCT
                      DType.STRUCT.getTypeId.getNativeId
                    case other =>
                      GpuFromProtobuf.sparkTypeToCudfIdOpt(other)
                        .getOrElse(DType.INT8.getTypeId.getNativeId)
                  }
                case _: StructType =>
                  DType.STRUCT.getTypeId.getNativeId
                case other =>
                  GpuFromProtobuf.sparkTypeToCudfIdOpt(other)
                    .getOrElse(DType.INT8.getTypeId.getNativeId)
              }

              val wireType = getWireType(info.protoTypeName, info.encoding)

              val hasDefault = info.hasDefaultValue && info.defaultValue.isDefined
              val (defInt, defFloat, defBool, defString) = if (hasDefault) {
                val defVal = info.defaultValue.get
                sf.dataType match {
                  case BooleanType =>
                    val b = defVal.asInstanceOf[java.lang.Boolean].booleanValue()
                    (0L, 0.0, b, null: Array[Byte])
                  case IntegerType | LongType =>
                    val intVal = defVal match {
                      case i: java.lang.Integer => i.longValue()
                      case l: java.lang.Long => l.longValue()
                      case _ => 0L
                    }
                    (intVal, 0.0, false, null: Array[Byte])
                  case FloatType =>
                    val f = defVal.asInstanceOf[java.lang.Float].doubleValue()
                    (0L, f, false, null: Array[Byte])
                  case DoubleType =>
                    val d = defVal.asInstanceOf[java.lang.Double].doubleValue()
                    (0L, d, false, null: Array[Byte])
                  case StringType =>
                    val str = defVal.asInstanceOf[String]
                    val bytes = if (str != null) str.getBytes("UTF-8") else null
                    (0L, 0.0, false, bytes)
                  case _ => (0L, 0.0, false, null: Array[Byte])
                }
              } else {
                (0L, 0.0, false, null: Array[Byte])
              }

              val enumValsArr = info.enumValues.map(_.toArray.sorted).orNull
              val enumNamesArr = info.enumNames.map { nameMap =>
                val sorted = nameMap.toSeq.sortBy(_._1)
                sorted.map { case (_, enumName) => enumName.getBytes("UTF-8") }.toArray
              }.orNull

              flatFields += FlattenedFieldDescriptor(
                fieldNumber = info.fieldNumber,
                parentIdx = parentIdx,
                depth = depth,
                wireType = wireType,
                outputTypeId = outputType,
                encoding = info.encoding,
                isRepeated = info.isRepeated,
                isRequired = info.isRequired,
                hasDefaultValue = info.hasDefaultValue,
                defaultInt = defInt,
                defaultFloat = defFloat,
                defaultBool = defBool,
                defaultString = defString,
                enumValidValues = enumValsArr,
                enumNames = enumNamesArr
              )

              // For nested struct types (including repeated message = ArrayType(StructType)), 
              // add child fields
              sf.dataType match {
                case st: StructType if nestedMsgDesc != null =>
                  // Non-repeated nested message
                  addChildFieldsFromStruct(st, nestedMsgDesc, sf.name, currentIdx, depth)
                  
                case ArrayType(st: StructType, _) if nestedMsgDesc != null =>
                  // Repeated message field (pruned via Option A ordinal remapping)
                  addChildFieldsFromStruct(st, nestedMsgDesc, sf.name, currentIdx, depth)
                  
                case _ => // Not a struct, no children to add
              }
            }
            
            // Helper to add child fields from a struct type.
            // Applies nested schema pruning for ALL struct types (both repeated and non-repeated).
            // For repeated message fields (ArrayType(StructType)), the pruned output is handled
            // by ordinal remapping in GpuGetArrayStructFieldsMeta (Option A), not by expansion.
            def addChildFieldsFromStruct(
                st: StructType,
                parentMsgDesc: AnyRef,
                fieldName: String,
                parentIdx: Int,
                parentDepth: Int): Unit = {
              val fd = invoke1[AnyRef](
                parentMsgDesc, "findFieldByName", classOf[String], fieldName)
              if (fd != null) {
                try {
                  val childMsgDesc = invoke0[AnyRef](fd, "getMessageType")
                  // Filter children based on nested schema projection requirements.
                  val requiredChildren = nestedFieldRequirements.get(fieldName)
                  val filteredFields = requiredChildren match {
                    case Some(Some(childNames)) =>
                      // Only add required children (nested schema projection)
                      st.fields.filter(f => childNames.contains(f.name))
                    case _ =>
                      // None (field not in map) or Some(None) (whole field needed)
                      st.fields
                  }
                  filteredFields.foreach { childSf =>
                    val childFd = invoke1[AnyRef](
                      childMsgDesc, "findFieldByName", classOf[String], childSf.name)
                    if (childFd != null) {
                      val childProtoType = invoke0[AnyRef](childFd, "getType")
                      val childProtoTypeName = typeName(childProtoType)
                      val childFieldNumber = invoke0[java.lang.Integer](
                        childFd, "getNumber").intValue()
                      val childIsRepeated = Try {
                        invoke0[java.lang.Boolean](childFd, "isRepeated").booleanValue()
                      }.getOrElse(false)
                      val childIsRequired = Try {
                        invoke0[java.lang.Boolean](childFd, "isRequired").booleanValue()
                      }.getOrElse(false)
                      val childHasDefault = Try {
                        invoke0[java.lang.Boolean](childFd, "hasDefaultValue").booleanValue()
                      }.getOrElse(false)
                      val (_, _, childEncoding) = checkFieldSupport(
                        childSf.dataType, childProtoTypeName, childIsRepeated, enumsAsInts)

                      val (childEnumVals, childEnumNameMap): (Option[Set[Int]],
                        Option[Map[Int, String]]) =
                        if (childProtoTypeName == "ENUM") {
                          Try {
                            val enumType = invoke0[AnyRef](childFd, "getEnumType")
                            val values = invoke0[java.util.List[_]](enumType, "getValues")
                            import scala.collection.JavaConverters._
                            val pairs = values.asScala.map { v =>
                              val ev = v.asInstanceOf[AnyRef]
                              val num = invoke0[java.lang.Integer](ev, "getNumber").intValue()
                              val name = invoke0[String](ev, "getName")
                              (num, name)
                            }
                            if (enumsAsInts) {
                              (Some(pairs.map(_._1).toSet), None)
                            } else {
                              (Some(pairs.map(_._1).toSet), Some(pairs.toMap))
                            }
                          }.getOrElse((None, None))
                        } else {
                          (None, None)
                        }

                      val childInfo = ProtobufFieldInfo(
                        fieldNumber = childFieldNumber,
                        protoTypeName = childProtoTypeName,
                        sparkType = childSf.dataType,
                        encoding = childEncoding,
                        isSupported = true,
                        unsupportedReason = None,
                        isRequired = childIsRequired,
                        hasDefaultValue = childHasDefault,
                        defaultValue = None,
                        enumValues = childEnumVals,
                        enumNames = childEnumNameMap,
                        isRepeated = childIsRepeated
                      )

                      addFieldWithChildren(
                        childSf, childInfo, parentIdx, parentDepth + 1, childMsgDesc)
                    }
                  }
                } catch {
                  case _: ReflectiveOperationException =>
                  // Ignore reflection failures and let remaining fields continue.
                }
              }
            }

            // Only add top-level fields that are actually required (schema projection).
            // This significantly reduces GPU memory and computation for schemas with many
            // fields when only a few are needed. The Scala layer will post-process the
            // output to insert null columns for non-decoded fields.
            decodedTopLevelIndices = indicesToDecode
            indicesToDecode.foreach { schemaIdx =>
              val sf = fullSchema.fields(schemaIdx)
              val info = fieldsInfoMap(sf.name)
              addFieldWithChildren(sf, info, -1, 0, msgDesc)
            }

            // Populate flattened schema variables
            val flat = flatFields.toArray
            flatFieldNumbers = flat.map(_.fieldNumber)
            flatParentIndices = flat.map(_.parentIdx)
            flatDepthLevels = flat.map(_.depth)
            flatWireTypes = flat.map(_.wireType)
            flatOutputTypeIds = flat.map(_.outputTypeId)
            flatEncodings = flat.map(_.encoding)
            flatIsRepeated = flat.map(_.isRepeated)
            flatIsRequired = flat.map(_.isRequired)
            flatHasDefaultValue = flat.map(_.hasDefaultValue)
            flatDefaultInts = flat.map(_.defaultInt)
            flatDefaultFloats = flat.map(_.defaultFloat)
            flatDefaultBools = flat.map(_.defaultBool)
            flatDefaultStrings = flat.map(_.defaultString)
            flatEnumValidValues = flat.map(_.enumValidValues)
            flatEnumNames = flat.map(_.enumNames)
          }
        }

        /**
         * Analyze all fields in the schema and build a map of field name to ProtobufFieldInfo.
         * Returns None if there's an error that should abort processing.
         */
        private def analyzeAllFields(
            schema: StructType,
            msgDesc: AnyRef,
            enumsAsInts: Boolean,
            messageName: String): Option[Map[String, ProtobufFieldInfo]] = {
          val result = mutable.Map[String, ProtobufFieldInfo]()

          for (sf <- schema.fields) {
            val fd = invoke1[AnyRef](msgDesc, "findFieldByName", classOf[String], sf.name)
            if (fd == null) {
              willNotWorkOnGpu(
                s"Protobuf field '${sf.name}' not found in message '$messageName'")
              return None
            }

            val isRepeated = Try {
              invoke0[java.lang.Boolean](fd, "isRepeated").booleanValue()
            }.getOrElse(false)

            // Check if field is required (proto2 required fields)
            val isFieldRequired = Try {
              invoke0[java.lang.Boolean](fd, "isRequired").booleanValue()
            }.getOrElse(false)

            // Check if field has a default value (proto2 [default = xxx])
            val hasDefault = Try {
              invoke0[java.lang.Boolean](fd, "hasDefaultValue").booleanValue()
            }.getOrElse(false)

            // Get the default value if it exists
            val defaultVal = if (hasDefault) {
              Try(Some(invoke0[AnyRef](fd, "getDefaultValue"))).getOrElse(None)
            } else {
              None
            }

            val protoType = invoke0[AnyRef](fd, "getType")
            val protoTypeName = typeName(protoType)
            val fieldNumber = invoke0[java.lang.Integer](fd, "getNumber").intValue()

            // Check field support and determine encoding
            val (isSupported, unsupportedReason, encoding) =
              checkFieldSupport(sf.dataType, protoTypeName, isRepeated, enumsAsInts)

            // Extract enum values and (for enumsAsInts=false) value-name mapping.
            val (enumVals, enumNameMap): (Option[Set[Int]], Option[Map[Int, String]]) =
              if (protoTypeName == "ENUM") {
                Try {
                  val enumType = invoke0[AnyRef](fd, "getEnumType")
                  val values = invoke0[java.util.List[_]](enumType, "getValues")
                  import scala.collection.JavaConverters._
                  val pairs = values.asScala.map { v =>
                    val ev = v.asInstanceOf[AnyRef]
                    val num = invoke0[java.lang.Integer](ev, "getNumber").intValue()
                    val name = invoke0[String](ev, "getName")
                    (num, name)
                  }
                  if (enumsAsInts) {
                    (Some(pairs.map(_._1).toSet), None)
                  } else {
                    (Some(pairs.map(_._1).toSet), Some(pairs.toMap))
                  }
                }.getOrElse((None, None))
              } else {
                (None, None)
              }

            result(sf.name) = ProtobufFieldInfo(
              fieldNumber = fieldNumber,
              protoTypeName = protoTypeName,
              sparkType = sf.dataType,
              encoding = encoding,
              isSupported = isSupported,
              unsupportedReason = unsupportedReason,
              isRequired = isFieldRequired,
              hasDefaultValue = hasDefault,
              defaultValue = defaultVal,
              enumValues = enumVals,
              enumNames = enumNameMap,
              isRepeated = isRepeated
            )
          }

          Some(result.toMap)
        }

        /**
         * Check if a field type is supported and return encoding information.
         * @return (isSupported, unsupportedReason, encoding)
         */
        private def checkFieldSupport(
            sparkType: DataType,
            protoTypeName: String,
            isRepeated: Boolean,
            enumsAsInts: Boolean): (Boolean, Option[String], Int) = {

          // Handle repeated fields (arrays)
          if (isRepeated) {
            sparkType match {
              case ArrayType(elementType, _) =>
                // Check if element type is supported
                elementType match {
                  case BooleanType | IntegerType | LongType | FloatType | DoubleType |
                       StringType | BinaryType =>
                    // Supported repeated scalar - determine encoding from proto type
                    return checkScalarEncoding(elementType, protoTypeName, enumsAsInts)
                  case _: StructType =>
                    // Repeated nested message (array of structs) - supported on GPU
                    return (true, None, GpuFromProtobuf.ENC_DEFAULT)
                  case _ =>
                    return (false, Some(s"unsupported repeated element type: $elementType"),
                      GpuFromProtobuf.ENC_DEFAULT)
                }
              case _ =>
                return (false, Some(s"repeated field should map to ArrayType, got: $sparkType"),
                  GpuFromProtobuf.ENC_DEFAULT)
            }
          }

          // Handle nested messages (non-repeated)
          if (protoTypeName == "MESSAGE") {
            sparkType match {
              case _: StructType =>
                return (true, None, GpuFromProtobuf.ENC_DEFAULT)
              case _ =>
                return (false, Some(s"nested message should map to StructType, got: $sparkType"),
                  GpuFromProtobuf.ENC_DEFAULT)
            }
          }

          // Check Spark type is one of the supported simple types
          sparkType match {
            case BooleanType | IntegerType | LongType | FloatType | DoubleType |
                 StringType | BinaryType =>
              // Supported Spark type, continue to check encoding
            case other =>
              return (false, Some(s"unsupported Spark type: $other"), GpuFromProtobuf.ENC_DEFAULT)
          }

          checkScalarEncoding(sparkType, protoTypeName, enumsAsInts)
        }

        /**
         * Determine encoding for scalar types.
         */
        private def checkScalarEncoding(
            sparkType: DataType,
            protoTypeName: String,
            enumsAsInts: Boolean): (Boolean, Option[String], Int) = {

          // Determine encoding based on Spark type and proto type combination
          val encoding = (sparkType, protoTypeName) match {
            case (BooleanType, "BOOL") => Some(GpuFromProtobuf.ENC_DEFAULT)
            case (IntegerType, "INT32" | "UINT32") => Some(GpuFromProtobuf.ENC_DEFAULT)
            case (IntegerType, "SINT32") => Some(GpuFromProtobuf.ENC_ZIGZAG)
            case (IntegerType, "FIXED32" | "SFIXED32") => Some(GpuFromProtobuf.ENC_FIXED)
            case (LongType, "INT64" | "UINT64") => Some(GpuFromProtobuf.ENC_DEFAULT)
            case (LongType, "SINT64") => Some(GpuFromProtobuf.ENC_ZIGZAG)
            case (LongType, "FIXED64" | "SFIXED64") => Some(GpuFromProtobuf.ENC_FIXED)
            // Spark may upcast smaller integers to LongType
            case (LongType, "INT32" | "UINT32" | "SINT32" | "FIXED32" | "SFIXED32") =>
              val enc = protoTypeName match {
                case "SINT32" => GpuFromProtobuf.ENC_ZIGZAG
                case "FIXED32" | "SFIXED32" => GpuFromProtobuf.ENC_FIXED
                case _ => GpuFromProtobuf.ENC_DEFAULT
              }
              Some(enc)
            case (FloatType, "FLOAT") => Some(GpuFromProtobuf.ENC_DEFAULT)
            case (DoubleType, "DOUBLE") => Some(GpuFromProtobuf.ENC_DEFAULT)
            case (StringType, "STRING") => Some(GpuFromProtobuf.ENC_DEFAULT)
            case (BinaryType, "BYTES") => Some(GpuFromProtobuf.ENC_DEFAULT)
            case (IntegerType, "ENUM") if enumsAsInts => Some(GpuFromProtobuf.ENC_DEFAULT)
            case (StringType, "ENUM") if !enumsAsInts => Some(GpuFromProtobuf.ENC_ENUM_STRING)
            case _ => None
          }

          encoding match {
            case Some(enc) => (true, None, enc)
            case None =>
              (false,
                Some(s"type mismatch: Spark $sparkType vs Protobuf $protoTypeName"),
                GpuFromProtobuf.ENC_DEFAULT)
          }
        }

        /**
         * Get wire type constant for a given protobuf type name and encoding.
         */
        private def getWireType(protoTypeName: String, encoding: Int): Int = {
          protoTypeName match {
            case "BOOL" | "INT32" | "UINT32" | "SINT32" | "INT64" | "UINT64" | "SINT64" | "ENUM" =>
              if (encoding == GpuFromProtobuf.ENC_FIXED) {
                if (protoTypeName.contains("64")) WT_64BIT else WT_32BIT
              } else {
                WT_VARINT
              }
            case "FIXED32" | "SFIXED32" | "FLOAT" => WT_32BIT
            case "FIXED64" | "SFIXED64" | "DOUBLE" => WT_64BIT
            case "STRING" | "BYTES" | "MESSAGE" => WT_LEN
            case other =>
              throw new IllegalStateException(
                s"Unknown protobuf type name '$other' - cannot determine wire type")
          }
        }

        /**
         * Analyze which fields are actually required by downstream operations.
         * Currently supports analyzing parent Project expressions.
         *
         * @param allFieldNames All field names in the full schema
         * @return Set of field names that are actually required
         */
        private def analyzeRequiredFields(allFieldNames: Set[String]): Set[String] = {
          val parentPlanOpt = findParentPlanMeta()

          parentPlanOpt match {
            case Some(planMeta) =>
              analyzeDownstreamProject(planMeta) match {
                case Some(fields) if fields.nonEmpty =>
                  fields
                case _ =>
                  planMeta.parent match {
                    case Some(grandParentMeta: SparkPlanMeta[_]) =>
                      analyzeDownstreamProject(grandParentMeta) match {
                        case Some(fields) if fields.nonEmpty => fields
                        case _ => allFieldNames
                      }
                    case _ => allFieldNames
                  }
              }
            case None =>
              allFieldNames
          }
        }

        /**
         * Find the parent SparkPlanMeta by traversing up the parent chain.
         */
        private def findParentPlanMeta(): Option[SparkPlanMeta[_]] = {
          def traverse(meta: Option[RapidsMeta[_, _, _]]): Option[SparkPlanMeta[_]] = {
            meta match {
              case Some(p: SparkPlanMeta[_]) => Some(p)
              case Some(p: RapidsMeta[_, _, _]) => traverse(p.parent)
              case _ => None
            }
          }
          traverse(parent)
        }

        /**
         * Nested field requirements: for each top-level field, what children are needed?
         * - None means the whole field is needed (all children)
         * - Some(Set("a","b")) means only children a and b are needed
         */
        // Populated during analyzeDownstreamProject, used by addChildFieldsFromStruct
        private var nestedFieldRequirements: Map[String, Option[Set[String]]] = Map.empty

        /**
         * Analyze a Project plan to find which struct fields are actually used.
         * This looks for GetStructField expressions that reference our protobuf output.
         * Also detects nested field access (e.g., decoded.ad_info.winfoid) for nested
         * schema projection.
         */
        private def analyzeDownstreamProject(planMeta: SparkPlanMeta[_]): Option[Set[String]] = {
          planMeta.wrapped match {
            case p: ProjectExec =>
              // Collect field references with nested child tracking
              // Key = top-level field name
              // Value = None (need whole field) or Some(Set(...)) (only need specific children)
              val fieldReqs = mutable.Map[String, Option[Set[String]]]()
              var hasDirectStructRef = false

              p.projectList.foreach { expr =>
                collectStructFieldReferences(expr, fieldReqs, hasDirectStructRefHolder = () => {
                  hasDirectStructRef = true
                })
              }

              if (hasDirectStructRef) {
                None
              } else if (fieldReqs.nonEmpty) {
                nestedFieldRequirements = fieldReqs.toMap
                Some(fieldReqs.keySet.toSet)
              } else {
                None
              }
            case _ =>
              None
          }
        }

        /**
         * Get the field name from a GetStructField expression using its ordinal and schema.
         */
        private def getFieldName(ordinal: Int, nameOpt: Option[String],
            schema: StructType): String = {
          nameOpt.getOrElse {
            if (ordinal < schema.fields.length) schema.fields(ordinal).name
            else s"_$ordinal"
          }
        }

        /**
         * Recursively collect field names and nested child requirements from
         * GetStructField expressions. Detects patterns like:
         *   - decoded.field_name         -> field_name: None (whole field)
         *   - decoded.ad_info.winfoid    -> ad_info: Some({winfoid})
         *   - decoded.ad_info            -> ad_info: None (whole field)
         *
         * When both whole-field and sub-field access exist, whole-field wins (None).
         */
        /**
         * Helper: record a nested child field requirement for a parent field.
         * Merges with existing requirements (whole-field wins over sub-field).
         */
        private def addNestedFieldReq(
            fieldReqs: mutable.Map[String, Option[Set[String]]],
            parentName: String,
            childName: String): Unit = {
          fieldReqs.get(parentName) match {
            case Some(None) => // Already need whole field, keep it
            case Some(Some(existing)) =>
              fieldReqs(parentName) = Some(existing + childName)
            case None =>
              fieldReqs(parentName) = Some(Set(childName))
          }
        }

        private def collectStructFieldReferences(
            expr: Expression,
            fieldReqs: mutable.Map[String, Option[Set[String]]],
            hasDirectStructRefHolder: () => Unit): Unit = {
          expr match {
            // Pattern: decoded.parent_struct.child_field (non-array struct)
            case GetStructField(child, ordinal, nameOpt) =>
              child match {
                case GetStructField(innerChild, innerOrdinal, innerNameOpt)
                    if isProtobufStructReference(innerChild) =>
                  val parentName = getFieldName(innerOrdinal, innerNameOpt, fullSchema)
                  val parentType = fullSchema.fields(innerOrdinal).dataType
                  val childSchema = parentType match {
                    case st: StructType => st
                    case ArrayType(st: StructType, _) => st
                    case _ => null
                  }
                  if (childSchema != null) {
                    val childName = getFieldName(ordinal, nameOpt, childSchema)
                    addNestedFieldReq(fieldReqs, parentName, childName)
                  } else {
                    fieldReqs(parentName) = None
                  }

                case _ if isProtobufStructReference(child) =>
                  // Direct top-level access: decoded.field_name (whole field)
                  val fieldName = getFieldName(ordinal, nameOpt, fullSchema)
                  fieldReqs(fieldName) = None

                case _ =>
                  collectStructFieldReferences(child, fieldReqs, hasDirectStructRefHolder)
              }

            // Pattern: decoded.ad_info.winfoid where ad_info is ArrayType(StructType)
            // Spark generates: GetArrayStructFields(GetStructField(decoded, ad_info_ord), field)
            case gasf: GetArrayStructFields =>
              gasf.child match {
                case GetStructField(innerChild, innerOrdinal, innerNameOpt)
                    if isProtobufStructReference(innerChild) =>
                  // Nested array-struct access: decoded.array_field.child_field
                  val parentName = getFieldName(innerOrdinal, innerNameOpt, fullSchema)
                  val childName = gasf.field.name
                  addNestedFieldReq(fieldReqs, parentName, childName)

                case _ =>
                  // Not a direct protobuf reference, recurse into children
                  gasf.children.foreach { child =>
                    collectStructFieldReferences(child, fieldReqs, hasDirectStructRefHolder)
                  }
              }

            case _ =>
              if (isProtobufStructReference(expr)) {
                hasDirectStructRefHolder()
              }
              expr.children.foreach { child =>
                collectStructFieldReferences(child, fieldReqs, hasDirectStructRefHolder)
              }
          }
        }

        /**
         * Check if an expression references the output of a protobuf decode expression.
         * This can be either:
         * 1. The ProtobufDataToCatalyst expression itself
         * 2. An AttributeReference that references the output of ProtobufDataToCatalyst
         *    (when accessing from a downstream ProjectExec)
         */
        private def isProtobufStructReference(expr: Expression): Boolean = {
          // Check if expr is a ProtobufDataToCatalyst expression
          if (expr.getClass.getName.contains("ProtobufDataToCatalyst")) {
            return true
          }

          // Prefer exact attribute identity. Avoid matching by schema shape because unrelated
          // struct columns can share the same field names/types.
          val protobufOutputExprId: Option[org.apache.spark.sql.catalyst.expressions.ExprId] =
            parent.flatMap { meta =>
              meta.wrapped match {
                case alias: org.apache.spark.sql.catalyst.expressions.Alias
                    if alias.child eq e =>
                  Some(alias.exprId)
                case _ => None
              }
            }

          expr match {
            case attr: AttributeReference =>
              protobufOutputExprId.exists(_ == attr.exprId)
            case _ => false
          }
        }

        override def convertToGpu(child: Expression): GpuExpression = {
          // Ensure thread-local ordinal mapping doesn't leak across query conversions.
          GpuFromProtobuf.clearPrunedFields()

          // Build pruned fields map for ALL struct types (both repeated and non-repeated).
          // For repeated message fields (ArrayType(StructType)), pruning is handled via
          // ordinal remapping in GpuGetArrayStructFieldsMeta (Option A) instead of expansion.
          val prunedFieldsMap: Map[String, Seq[String]] = nestedFieldRequirements.collect {
            case (fieldName, Some(childNames)) =>
              val fieldIdx = fullSchema.fieldIndex(fieldName)
              val childSchema = fullSchema.fields(fieldIdx).dataType match {
                case st: StructType => st
                case ArrayType(st: StructType, _) => st
                case _ => null
              }
              if (childSchema != null) {
                // Preserve the original field order
                val orderedNames = childSchema.fields
                  .map(_.name)
                  .filter(childNames.contains)
                  .toSeq
                fieldName -> orderedNames
              } else {
                fieldName -> childNames.toSeq
              }
          }

          // Register pruned field ordinal mappings for GpuGetArrayStructFieldsMeta.
          // For each pruned ArrayType(StructType) field, map child field names to their
          // ordinal in the pruned struct so runtime column access uses correct indices.
          val ordinalMappings = prunedFieldsMap.flatMap { case (parentName, childNames) =>
            val fieldIdx = fullSchema.fieldIndex(parentName)
            fullSchema.fields(fieldIdx).dataType match {
              case ArrayType(_: StructType, _) =>
                childNames.zipWithIndex.map { case (name, idx) => name -> idx }
              case _ => Seq.empty
            }
          }
          if (ordinalMappings.nonEmpty) {
            GpuFromProtobuf.registerPrunedFields(ordinalMappings)
          }

          GpuFromProtobuf(
            fullSchema, decodedTopLevelIndices, flatFieldNumbers, flatParentIndices,
            flatDepthLevels, flatWireTypes, flatOutputTypeIds, flatEncodings,
            flatIsRepeated, flatIsRequired, flatHasDefaultValue, flatDefaultInts,
            flatDefaultFloats, flatDefaultBools, flatDefaultStrings, flatEnumValidValues,
            flatEnumNames,
            prunedFieldsMap, failOnErrors, child)
        }
      }
    )
  }

  private def getMessageName(e: Expression): String =
    invoke0[String](e, "messageName")

  /**
   * Newer Spark versions may carry an in-expression descriptor set payload.
   * - Spark 3.5.x: binaryFileDescriptorSet: Option[Array[Byte]]
   * - Spark 4.x: binaryDescriptorSet (may be Array[Byte] or Option[Array[Byte]])
   * Spark 3.4.x does not have this, so callers should fall back to descFilePath().
   */
  private def getDescriptorBytes(e: Expression): Option[Array[Byte]] = {
    // Spark 3.5.x: binaryFileDescriptorSet (note: "File" in the name)
    val spark35Result = Try(invoke0[Option[Array[Byte]]](e, "binaryFileDescriptorSet"))
      .toOption.flatten
    spark35Result.orElse {
      // Spark 4.x: binaryDescriptorSet - may be Array[Byte] or Option[Array[Byte]]
      val direct = Try(invoke0[Array[Byte]](e, "binaryDescriptorSet")).toOption
      direct.orElse {
        Try(invoke0[Option[Array[Byte]]](e, "binaryDescriptorSet")).toOption.flatten
      }
    }
  }

  /**
   * Get descriptor file path from expression.
   * Only available in Spark 3.4.x. Spark 3.5+ uses binaryFileDescriptorSet instead.
   */
  private def getDescFilePath(e: Expression): Option[String] =
    Try(invoke0[Option[String]](e, "descFilePath")).toOption.flatten

  /**
   * Build message descriptor using Spark's ProtobufUtils.
   * Supports both Spark 3.4.x (descFilePath: Option[String]) and
   * Spark 3.5+ (binaryFileDescriptorSet: Option[Array[Byte]]).
   *
   * @param messageName The protobuf message name
   * @param descFilePathOrBytes Either a file path (String) or binary descriptor bytes (Array[Byte])
   */
  private def buildMessageDescriptorWithSparkProtobuf(
      messageName: String,
      descFilePathOrBytes: Either[String, Array[Byte]]): AnyRef = {
    val cls = ShimReflectionUtils.loadClass(sparkProtobufUtilsObjectClassName)
    val module = cls.getField("MODULE$").get(null)

    descFilePathOrBytes match {
      case Left(filePath) =>
        // Spark 3.4.x: buildDescriptor(messageName: String, descFilePath: Option[String])
        val m = cls.getMethod("buildDescriptor", classOf[String], classOf[scala.Option[_]])
        m.invoke(module, messageName, Some(filePath)).asInstanceOf[AnyRef]
      case Right(bytes) =>
        // Spark 3.5+: buildDescriptor(messageName, binaryFileDescriptorSet)
        val m = cls.getMethod("buildDescriptor", classOf[String], classOf[scala.Option[_]])
        m.invoke(module, messageName, Some(bytes)).asInstanceOf[AnyRef]
    }
  }

  private def typeName(t: AnyRef): String = {
    if (t == null) {
      "null"
    } else {
      // Prefer Enum.name() when available; fall back to toString.
      Try(invoke0[String](t, "name")).getOrElse(t.toString)
    }
  }

  private def getOptionsMap(e: Expression): Map[String, String] = {
    val opt = Try(invoke0[scala.collection.Map[String, String]](e, "options")).toOption
    opt.map(_.toMap).getOrElse(Map.empty)
  }

  private def invoke0[T](obj: AnyRef, method: String): T =
    obj.getClass.getMethod(method).invoke(obj).asInstanceOf[T]

  private def invoke1[T](obj: AnyRef, method: String, arg0Cls: Class[_], arg0: AnyRef): T =
    obj.getClass.getMethod(method, arg0Cls).invoke(obj, arg0).asInstanceOf[T]
}
