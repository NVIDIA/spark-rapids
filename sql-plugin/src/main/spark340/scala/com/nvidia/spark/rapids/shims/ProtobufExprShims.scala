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
{"spark": "402"}
{"spark": "411"}
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

  val PRUNED_ORDINAL_TAG =
    org.apache.spark.sql.rapids.GpuStructFieldOrdinalTag.PRUNED_ORDINAL_TAG

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

          val options = getOptionsMap(e) match {
            case Some(m) => m
            case None =>
              willNotWorkOnGpu(
                "Cannot read from_protobuf options via reflection; falling back to CPU")
              return
          }
          val supportedOptions = Set("enums.as.ints", "mode")
          val unsupportedOptions = options.keys.filterNot(supportedOptions.contains)
          if (unsupportedOptions.nonEmpty) {
            val keys = unsupportedOptions.mkString(",")
            willNotWorkOnGpu(
              s"from_protobuf options are not supported yet on GPU: $keys")
            return
          }

          val enumsAsInts = options.getOrElse("enums.as.ints", "false").toBoolean
          failOnErrors = options.getOrElse("mode", "FAILFAST").equalsIgnoreCase("FAILFAST")
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
          val protoSyntax = PbReflect.getFileSyntax(msgDesc)
          if (protoSyntax == "PROTO3" || protoSyntax == "EDITIONS" || protoSyntax.isEmpty) {
            willNotWorkOnGpu(
              "proto3/editions syntax is not supported by the GPU protobuf decoder; " +
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

            // Helper to add a field and its children recursively.
            // pathPrefix is the dot-path of ancestor fields (empty for top-level).
            def addFieldWithChildren(
                sf: StructField,
                info: ProtobufFieldInfo,
                parentIdx: Int,
                depth: Int,
                nestedMsgDesc: AnyRef,
                pathPrefix: String = ""): Unit = {

              val currentIdx = flatFields.size

              if (depth >= 10) {
                willNotWorkOnGpu("Protobuf nesting depth exceeds maximum supported depth of 10")
                return
              }

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
                  case BinaryType =>
                    val bytes = Try {
                      val toByteArray = defVal.getClass.getMethod("toByteArray")
                      toByteArray.invoke(defVal).asInstanceOf[Array[Byte]]
                    }.getOrElse(null: Array[Byte])
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
                hasDefaultValue = hasDefault,
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
                  addChildFieldsFromStruct(
                    st, nestedMsgDesc, sf.name, currentIdx, depth, pathPrefix)
                  
                case ArrayType(st: StructType, _) if nestedMsgDesc != null =>
                  addChildFieldsFromStruct(
                    st, nestedMsgDesc, sf.name, currentIdx, depth, pathPrefix)
                  
                case _ => // Not a struct, no children to add
              }
            }
            
            // Helper to add child fields from a struct type.
            // Applies nested schema pruning at arbitrary depth using path-based
            // lookup into nestedFieldRequirements.
            def addChildFieldsFromStruct(
                st: StructType,
                parentMsgDesc: AnyRef,
                fieldName: String,
                parentIdx: Int,
                parentDepth: Int,
                pathPrefix: String): Unit = {
              val path = if (pathPrefix.isEmpty) fieldName else s"$pathPrefix.$fieldName"
              val fd = PbReflect.findFieldByName(parentMsgDesc, fieldName)
              if (fd != null) {
                try {
                  val childMsgDesc = PbReflect.getMessageType(fd)
                  val requiredChildren = nestedFieldRequirements.get(path)
                  val filteredFields = requiredChildren match {
                    case Some(Some(childNames)) =>
                      st.fields.filter(f => childNames.contains(f.name))
                    case _ =>
                      st.fields
                  }
                  filteredFields.foreach { childSf =>
                    val childFd = PbReflect.findFieldByName(childMsgDesc, childSf.name)
                    if (childFd != null) {
                      val childProtoTypeName = typeName(PbReflect.getFieldType(childFd))
                      val childFieldNumber = PbReflect.getFieldNumber(childFd)
                      val childIsRepeated = PbReflect.isRepeated(childFd)
                      val childIsRequired = PbReflect.isRequired(childFd)
                      val childHasDefault = PbReflect.hasDefaultValue(childFd)
                      val (childIsSupported, childUnsupportedReason, childEncoding) =
                        checkFieldSupport(
                          childSf.dataType, childProtoTypeName, childIsRepeated, enumsAsInts)

                      if (!childIsSupported) {
                        willNotWorkOnGpu(
                          s"Nested field '${childSf.name}' at '$path': " +
                            childUnsupportedReason.getOrElse("unsupported type"))
                      } else {
                        val (childEnumVals, childEnumNameMap): (Option[Set[Int]],
                          Option[Map[Int, String]]) =
                          if (childProtoTypeName == "ENUM") {
                            Try {
                              val pairs = PbReflect.getEnumValues(
                                PbReflect.getEnumType(childFd))
                              if (enumsAsInts) {
                                (Some(pairs.map(_._1).toSet), None)
                              } else {
                                (Some(pairs.map(_._1).toSet), Some(pairs.toMap))
                              }
                            }.getOrElse((None, None))
                          } else {
                            (None, None)
                          }

                        val childDefaultVal =
                          if (childHasDefault) PbReflect.getDefaultValue(childFd) else None

                        val childInfo = ProtobufFieldInfo(
                          fieldNumber = childFieldNumber,
                          protoTypeName = childProtoTypeName,
                          sparkType = childSf.dataType,
                          encoding = childEncoding,
                          isSupported = true,
                          unsupportedReason = None,
                          isRequired = childIsRequired,
                          hasDefaultValue = childHasDefault,
                          defaultValue = childDefaultVal,
                          enumValues = childEnumVals,
                          enumNames = childEnumNameMap,
                          isRepeated = childIsRepeated
                        )

                        addFieldWithChildren(
                          childSf, childInfo, parentIdx, parentDepth + 1, childMsgDesc, path)
                      }
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
            val fd = PbReflect.findFieldByName(msgDesc, sf.name)
            if (fd == null) {
              willNotWorkOnGpu(
                s"Protobuf field '${sf.name}' not found in message '$messageName'")
              return None
            }

            val isRepeated = PbReflect.isRepeated(fd)
            val isFieldRequired = PbReflect.isRequired(fd)
            val hasDefault = PbReflect.hasDefaultValue(fd)
            val defaultVal = if (hasDefault) PbReflect.getDefaultValue(fd) else None

            val protoTypeName = typeName(PbReflect.getFieldType(fd))
            val fieldNumber = PbReflect.getFieldNumber(fd)

            val (isSupported, unsupportedReason, encoding) =
              checkFieldSupport(sf.dataType, protoTypeName, isRepeated, enumsAsInts)

            val (enumVals, enumNameMap): (Option[Set[Int]], Option[Map[Int, String]]) =
              if (protoTypeName == "ENUM") {
                Try {
                  val pairs = PbReflect.getEnumValues(PbReflect.getEnumType(fd))
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
              willNotWorkOnGpu(
                s"Unknown protobuf type name '$other' - cannot determine wire type; " +
                  "falling back to CPU")
              WT_VARINT
          }
        }

        /**
         * Analyze which fields are actually required by downstream operations.
         * Currently supports analyzing parent Project expressions.
         *
         * @param allFieldNames All field names in the full schema
         * @return Set of field names that are actually required
         */
        private var targetExprsToRemap: Seq[Expression] = Seq.empty

        private def analyzeRequiredFields(allFieldNames: Set[String]): Set[String] = {
          val fieldReqs = mutable.Map[String, Option[Set[String]]]()
          var hasDirectStructRef = false
          val holder = () => { hasDirectStructRef = true }

          var currentMeta: Option[SparkPlanMeta[_]] = findParentPlanMeta()
          var foundProject = false
          var safeToPrune = true
          val collectedExprs = mutable.ArrayBuffer[Expression]()

          while (currentMeta.isDefined && !foundProject && safeToPrune) {
            currentMeta.get.wrapped match {
              case p: ProjectExec =>
                collectedExprs ++= p.projectList
                p.projectList.foreach(collectStructFieldReferences(_, fieldReqs, holder))
                foundProject = true
              case f: org.apache.spark.sql.execution.FilterExec =>
                collectedExprs += f.condition
                collectStructFieldReferences(f.condition, fieldReqs, holder)
                currentMeta = currentMeta.get.parent match {
                  case Some(pm: SparkPlanMeta[_]) => Some(pm)
                  case _ => None
                }
              case _ =>
                safeToPrune = false
            }
          }

          if (!safeToPrune || !foundProject || hasDirectStructRef || fieldReqs.isEmpty) {
            targetExprsToRemap = Seq.empty
            allFieldNames
          } else {
            nestedFieldRequirements = fieldReqs.toMap
            targetExprsToRemap = collectedExprs.toSeq
            fieldReqs.keySet.toSet
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
         * Nested field requirements: maps a field path to child requirements.
         * Keys are dot-separated paths from the protobuf root:
         *   - "level1" -> Some(Set("level2"))          (top-level struct pruning)
         *   - "level1.level2" -> Some(Set("level3"))   (deep nested pruning)
         *   - "field" -> None                           (whole field needed)
         *
         * Top-level names (keys without dots) also determine which fields are decoded.
         */
        private var nestedFieldRequirements: Map[String, Option[Set[String]]] = Map.empty

        private def getFieldName(ordinal: Int, nameOpt: Option[String],
            schema: StructType): String = {
          nameOpt.getOrElse {
            if (ordinal < schema.fields.length) schema.fields(ordinal).name
            else s"_$ordinal"
          }
        }

        /**
         * Navigate the Spark schema tree by following a dot-separated path of
         * field names. Returns the StructType at the end of the path, unwrapping
         * ArrayType(StructType) along the way, or null if the path is invalid.
         */
        private def resolveSchemaAtPath(root: StructType, path: Seq[String]): StructType = {
          var current: StructType = root
          for (name <- path) {
            val field = current.fields.find(_.name == name).orNull
            if (field == null) return null
            field.dataType match {
              case st: StructType => current = st
              case ArrayType(st: StructType, _) => current = st
              case _ => return null
            }
          }
          current
        }

        /**
         * Walk a GetStructField chain upward until it reaches the protobuf
         * reference expression, returning the sequence of field names forming
         * the access path. Returns None if the chain does not terminate at a
         * protobuf reference.
         *
         * Example: for `GetStructField(GetStructField(decoded, a_ord), b_ord)`
         *   → Some(Seq("a", "b"))
         */
        private def resolveFieldAccessChain(
            expr: Expression): Option[Seq[String]] = {
          expr match {
            case GetStructField(child, ordinal, nameOpt) =>
              if (isProtobufStructReference(child)) {
                Some(Seq(getFieldName(ordinal, nameOpt, fullSchema)))
              } else {
                resolveFieldAccessChain(child).flatMap { parentPath =>
                  val parentSchema = if (parentPath.isEmpty) fullSchema
                                     else resolveSchemaAtPath(fullSchema, parentPath)
                  if (parentSchema != null) {
                    Some(parentPath :+ getFieldName(ordinal, nameOpt, parentSchema))
                  } else {
                    None
                  }
                }
              }
            case _ if isProtobufStructReference(expr) =>
              Some(Seq.empty)
            case _ =>
              None
          }
        }

        private def addNestedFieldReq(
            fieldReqs: mutable.Map[String, Option[Set[String]]],
            parentKey: String,
            childName: String): Unit = {
          fieldReqs.get(parentKey) match {
            case Some(None) => // Already need whole field, keep it
            case Some(Some(existing)) =>
              fieldReqs(parentKey) = Some(existing + childName)
            case None =>
              fieldReqs(parentKey) = Some(Set(childName))
          }
        }

        /**
         * Register pruning requirements at every level of a field access path.
         * For path = ["a", "b"] with leafName = "c":
         *   "a" -> needs child "b"
         *   "a.b" -> needs child "c"
         */
        private def registerPathRequirements(
            fieldReqs: mutable.Map[String, Option[Set[String]]],
            path: Seq[String],
            leafName: String): Unit = {
          for (i <- path.indices) {
            val pathKey = path.take(i + 1).mkString(".")
            val childName = if (i < path.length - 1) path(i + 1) else leafName
            addNestedFieldReq(fieldReqs, pathKey, childName)
          }
        }

        private def collectStructFieldReferences(
            expr: Expression,
            fieldReqs: mutable.Map[String, Option[Set[String]]],
            hasDirectStructRefHolder: () => Unit): Unit = {
          expr match {
            case GetStructField(child, ordinal, nameOpt) =>
              resolveFieldAccessChain(child) match {
                case Some(parentPath) =>
                  val parentSchema = if (parentPath.isEmpty) fullSchema
                                     else resolveSchemaAtPath(fullSchema, parentPath)
                  if (parentSchema != null) {
                    val fieldName = getFieldName(ordinal, nameOpt, parentSchema)
                    if (parentPath.isEmpty) {
                      // Direct top-level access: decoded.field_name (whole field)
                      fieldReqs(fieldName) = None
                    } else {
                      registerPathRequirements(fieldReqs, parentPath, fieldName)
                    }
                  } else {
                    collectStructFieldReferences(child, fieldReqs, hasDirectStructRefHolder)
                  }
                case None =>
                  collectStructFieldReferences(child, fieldReqs, hasDirectStructRefHolder)
              }

            case gasf: GetArrayStructFields =>
              resolveFieldAccessChain(gasf.child) match {
                case Some(parentPath) if parentPath.nonEmpty =>
                  registerPathRequirements(fieldReqs, parentPath, gasf.field.name)
                case _ =>
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
          if (expr eq e) {
            return true
          }

          // Catalyst may create duplicate ProtobufDataToCatalyst
          // instances for each GetStructField access. Match copies
          // by class + identical input child so that
          // analyzeRequiredFields detects all field accesses in one
          // pass, keeping schema projection correct.
          //
          // Known limitation: if two different from_protobuf calls
          // (different messageName / descriptor) share the same binary
          // input column within one ProjectExec, this check will
          // incorrectly treat them as the same expression, potentially
          // mixing up field requirements and ordinal remapping. This is
          // acceptable because decoding the same bytes as two different
          // message types is not a meaningful query.
          if (expr.getClass == e.getClass &&
              expr.children.nonEmpty &&
              e.children.nonEmpty &&
              ((expr.children.head eq e.children.head) ||
                expr.children.head.semanticEquals(
                  e.children.head))) {
            return true
          }

          val protobufOutputExprId
            : Option[org.apache.spark.sql.catalyst.expressions.ExprId] =
            parent.flatMap { meta =>
              meta.wrapped match {
                case alias: org.apache.spark.sql.catalyst.expressions
                      .Alias if alias.child eq e =>
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
          val prunedFieldsMap: Map[String, Seq[String]] = nestedFieldRequirements.collect {
            case (pathKey, Some(childNames)) =>
              val pathParts = pathKey.split("\\.").toSeq
              val childSchema = resolveSchemaAtPath(fullSchema, pathParts)
              if (childSchema != null) {
                val orderedNames = childSchema.fields
                  .map(_.name)
                  .filter(childNames.contains)
                  .toSeq
                pathKey -> orderedNames
              } else {
                pathKey -> childNames.toSeq
              }
          }

          def registerExprs(expr: Expression): Unit = {
            expr match {
              case gsf @ GetStructField(childExpr, ordinal, nameOpt) =>
                resolveFieldAccessChain(childExpr) match {
                  case Some(parentPath) if parentPath.nonEmpty =>
                    val parentSchema = resolveSchemaAtPath(fullSchema, parentPath)
                    if (parentSchema != null) {
                      val pathKey = parentPath.mkString(".")
                      val childName = getFieldName(ordinal, nameOpt, parentSchema)
                      prunedFieldsMap.get(pathKey).foreach { orderedChildren =>
                        val runtimeOrd = orderedChildren.indexOf(childName)
                        if (runtimeOrd >= 0) {
                          gsf.setTagValue(ProtobufExprShims.PRUNED_ORDINAL_TAG, runtimeOrd)
                        }
                      }
                    }
                  case Some(parentPath) if parentPath.isEmpty =>
                    val runtimeOrd = decodedTopLevelIndices.indexOf(ordinal)
                    if (runtimeOrd >= 0) {
                      gsf.setTagValue(ProtobufExprShims.PRUNED_ORDINAL_TAG, runtimeOrd)
                    }
                  case _ =>
                }

              case gasf @ GetArrayStructFields(childExpr, field, _, _, _) =>
                resolveFieldAccessChain(childExpr) match {
                  case Some(parentPath) if parentPath.nonEmpty =>
                    val pathKey = parentPath.mkString(".")
                    prunedFieldsMap.get(pathKey).foreach { orderedChildren =>
                      val runtimeOrd = orderedChildren.indexOf(field.name)
                      if (runtimeOrd >= 0) {
                        gasf.setTagValue(ProtobufExprShims.PRUNED_ORDINAL_TAG, runtimeOrd)
                      }
                    }
                  case _ =>
                }
              case _ =>
            }
            expr.children.foreach(registerExprs)
          }

          targetExprsToRemap.foreach(registerExprs)

          val decodedSchema = {
            def applyPruning(field: StructField, prefix: String): StructField = {
              val path = if (prefix.isEmpty) field.name else s"$prefix.${field.name}"
              prunedFieldsMap.get(path) match {
                case Some(childNames) =>
                  field.dataType match {
                    case ArrayType(st: StructType, cn) =>
                      val pruned = StructType(
                        st.fields.filter(f => childNames.contains(f.name))
                          .map(f => applyPruning(f, path)))
                      field.copy(dataType = ArrayType(pruned, cn))
                    case st: StructType =>
                      val pruned = StructType(
                        st.fields.filter(f => childNames.contains(f.name))
                          .map(f => applyPruning(f, path)))
                      field.copy(dataType = pruned)
                    case _ => field
                  }
                case None =>
                  field.dataType match {
                    case ArrayType(st: StructType, cn) =>
                      val recursed = StructType(st.fields.map(f => applyPruning(f, path)))
                      if (recursed != st) field.copy(dataType = ArrayType(recursed, cn))
                      else field
                    case st: StructType =>
                      val recursed = StructType(st.fields.map(f => applyPruning(f, path)))
                      if (recursed != st) field.copy(dataType = recursed)
                      else field
                    case _ => field
                  }
              }
            }

            val decodedFields = decodedTopLevelIndices.map { idx =>
              applyPruning(fullSchema.fields(idx), "")
            }
            StructType(decodedFields.map(f =>
              f.copy(nullable = true)))
          }

          GpuFromProtobuf(
            decodedSchema,
            flatFieldNumbers, flatParentIndices,
            flatDepthLevels, flatWireTypes, flatOutputTypeIds, flatEncodings,
            flatIsRepeated, flatIsRequired, flatHasDefaultValue, flatDefaultInts,
            flatDefaultFloats, flatDefaultBools, flatDefaultStrings, flatEnumValidValues,
            flatEnumNames, failOnErrors, child)
        }
      }
    )
  }

  private def getMessageName(e: Expression): String =
    PbReflect.invoke0[String](e, "messageName")

  private def getDescriptorBytes(e: Expression): Option[Array[Byte]] = {
    val spark35Result = Try(PbReflect.invoke0[Option[Array[Byte]]](e, "binaryFileDescriptorSet"))
      .toOption.flatten
    spark35Result.orElse {
      val direct = Try(PbReflect.invoke0[Array[Byte]](e, "binaryDescriptorSet")).toOption
      direct.orElse {
        Try(PbReflect.invoke0[Option[Array[Byte]]](e, "binaryDescriptorSet")).toOption.flatten
      }
    }
  }

  private def getDescFilePath(e: Expression): Option[String] =
    Try(PbReflect.invoke0[Option[String]](e, "descFilePath")).toOption.flatten

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
      Try(PbReflect.invoke0[String](t, "name")).getOrElse(t.toString)
    }
  }

  private def getOptionsMap(e: Expression): Option[Map[String, String]] = {
    Try(PbReflect.invoke0[scala.collection.Map[String, String]](e, "options"))
      .map(_.toMap)
      .toOption
  }

  /**
   * Cached reflection helper for protobuf-java descriptor APIs.
   *
   * All protobuf descriptor method calls go through this object so that:
   *  1. java.lang.reflect.Method objects are cached (ConcurrentHashMap)
   *  2. Missing methods produce a clear UnsupportedOperationException with the
   *     class name and loaded protobuf-java version, instead of a raw
   *     NoSuchMethodException.
   */
  private[shims] object PbReflect {
    import java.lang.reflect.Method
    private val cache = new java.util.concurrent.ConcurrentHashMap[String, Method]()

    private def protobufJavaVersion: String = Try {
      val rtCls = Class.forName("com.google.protobuf.RuntimeVersion")
      val domain = rtCls.getField("DOMAIN").get(null)
      val major = rtCls.getField("MAJOR").get(null)
      val minor = rtCls.getField("MINOR").get(null)
      val patch = rtCls.getField("PATCH").get(null)
      s"$domain-$major.$minor.$patch"
    }.getOrElse("unknown")

    private def cached(cls: Class[_], name: String, paramTypes: Class[_]*): Method = {
      val key = s"${cls.getName}#$name(${paramTypes.map(_.getName).mkString(",")})"
      cache.computeIfAbsent(key, _ => {
        try {
          cls.getMethod(name, paramTypes: _*)
        } catch {
          case ex: NoSuchMethodException =>
            throw new UnsupportedOperationException(
              s"protobuf-java method not found: ${cls.getSimpleName}.$name " +
                s"(protobuf-java version: $protobufJavaVersion). " +
                s"This may indicate an incompatible protobuf-java library version.",
              ex)
        }
      })
    }

    def invoke0[T](obj: AnyRef, method: String): T =
      cached(obj.getClass, method).invoke(obj).asInstanceOf[T]

    def invoke1[T](obj: AnyRef, method: String, arg0Cls: Class[_], arg0: AnyRef): T =
      cached(obj.getClass, method, arg0Cls).invoke(obj, arg0).asInstanceOf[T]

    // ---- Typed helpers for common Descriptor operations ----

    def findFieldByName(msgDesc: AnyRef, name: String): AnyRef =
      invoke1[AnyRef](msgDesc, "findFieldByName", classOf[String], name)

    def getFieldNumber(fd: AnyRef): Int =
      invoke0[java.lang.Integer](fd, "getNumber").intValue()

    def getFieldType(fd: AnyRef): AnyRef = invoke0[AnyRef](fd, "getType")

    def isRepeated(fd: AnyRef): Boolean =
      Try(invoke0[java.lang.Boolean](fd, "isRepeated").booleanValue()).getOrElse(false)

    def isRequired(fd: AnyRef): Boolean =
      Try(invoke0[java.lang.Boolean](fd, "isRequired").booleanValue()).getOrElse(false)

    def hasDefaultValue(fd: AnyRef): Boolean =
      Try(invoke0[java.lang.Boolean](fd, "hasDefaultValue").booleanValue()).getOrElse(false)

    def getDefaultValue(fd: AnyRef): Option[AnyRef] =
      Try(Some(invoke0[AnyRef](fd, "getDefaultValue"))).getOrElse(None)

    def getMessageType(fd: AnyRef): AnyRef = invoke0[AnyRef](fd, "getMessageType")

    def getEnumType(fd: AnyRef): AnyRef = invoke0[AnyRef](fd, "getEnumType")

    def getEnumValues(enumType: AnyRef): Seq[(Int, String)] = {
      import scala.collection.JavaConverters._
      val values = invoke0[java.util.List[_]](enumType, "getValues")
      values.asScala.map { v =>
        val ev = v.asInstanceOf[AnyRef]
        val num = invoke0[java.lang.Integer](ev, "getNumber").intValue()
        val name = invoke0[String](ev, "getName")
        (num, name)
      }.toSeq
    }

    def getFileSyntax(msgDesc: AnyRef): String = Try {
      val fileDesc = invoke0[AnyRef](msgDesc, "getFile")
      val syntaxObj = invoke0[AnyRef](fileDesc, "getSyntax")
      typeName(syntaxObj)
    }.getOrElse("")
  }
}
