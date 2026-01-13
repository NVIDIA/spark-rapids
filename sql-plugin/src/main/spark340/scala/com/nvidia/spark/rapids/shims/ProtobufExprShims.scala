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

import java.nio.file.{Files, Path}

import scala.collection.mutable
import scala.util.Try

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, GetStructField, UnaryExpression}
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
    unsupportedReason: Option[String]
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
        // Indices into fullSchema for fields that will be decoded by GPU
        private var decodedFieldIndices: Array[Int] = _
        private var fieldNumbers: Array[Int] = _
        private var cudfTypeIds: Array[Int] = _
        private var cudfTypeScales: Array[Int] = _
        private var failOnErrors: Boolean = _

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
          val descFilePathOpt = getDescFilePath(e).orElse {
            // Newer Spark may embed a descriptor set (binaryDescriptorSet). Write it to a temp file
            // so we can reuse Spark's ProtobufUtils (and its shaded protobuf classes) to resolve
            // the descriptor.
            getDescriptorBytes(e).map(writeTempDescFile)
          }
          if (descFilePathOpt.isEmpty) {
            willNotWorkOnGpu(
              "from_protobuf requires a descriptor set " +
                "(descFilePath or binaryDescriptorSet)")
            return
          }

          val msgDesc = try {
            // Spark 3.4.x builds the descriptor as:
            // ProtobufUtils.buildDescriptor(messageName, descFilePathOpt)
            buildMessageDescriptorWithSparkProtobuf(messageName, descFilePathOpt)
          } catch {
            case t: Throwable =>
              willNotWorkOnGpu(
                s"Failed to resolve protobuf descriptor for message '$messageName': " +
                  s"${t.getMessage}")
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
          decodedFieldIndices = indicesToDecode

          // Step 5: Build arrays for the fields to decode (parallel to decodedFieldIndices)
          val fnums = new Array[Int](indicesToDecode.length)
          val typeIds = new Array[Int](indicesToDecode.length)
          val scales = new Array[Int](indicesToDecode.length)

          indicesToDecode.zipWithIndex.foreach { case (schemaIdx, arrIdx) =>
            val sf = fullSchema.fields(schemaIdx)
            val info = fieldsInfoMap(sf.name)
            fnums(arrIdx) = info.fieldNumber
            typeIds(arrIdx) = GpuFromProtobuf.sparkTypeToCudfId(sf.dataType)
            scales(arrIdx) = info.encoding
          }

          fieldNumbers = fnums
          cudfTypeIds = typeIds
          cudfTypeScales = scales
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

            val protoType = invoke0[AnyRef](fd, "getType")
            val protoTypeName = typeName(protoType)
            val fieldNumber = invoke0[java.lang.Integer](fd, "getNumber").intValue()

            // Check field support and determine encoding
            val (isSupported, unsupportedReason, encoding) =
              checkFieldSupport(sf.dataType, protoTypeName, isRepeated, enumsAsInts)

            result(sf.name) = ProtobufFieldInfo(
              fieldNumber = fieldNumber,
              protoTypeName = protoTypeName,
              sparkType = sf.dataType,
              encoding = encoding,
              isSupported = isSupported,
              unsupportedReason = unsupportedReason
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

          if (isRepeated) {
            return (false, Some("repeated fields are not supported"), GpuFromProtobuf.ENC_DEFAULT)
          }

          // Check Spark type is one of the supported simple types
          sparkType match {
            case BooleanType | IntegerType | LongType | FloatType | DoubleType |
                 StringType | BinaryType =>
              // Supported Spark type, continue to check encoding
            case other =>
              return (false, Some(s"unsupported Spark type: $other"), GpuFromProtobuf.ENC_DEFAULT)
          }

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
         * Analyze which fields are actually required by downstream operations.
         * Currently supports analyzing parent Project expressions.
         *
         * @param allFieldNames All field names in the full schema
         * @return Set of field names that are actually required
         */
        private def analyzeRequiredFields(allFieldNames: Set[String]): Set[String] = {
          // Try to find parent SparkPlanMeta and analyze downstream Project
          val parentPlanOpt = findParentPlanMeta()

          parentPlanOpt match {
            case Some(planMeta) =>
              // First, try to analyze the immediate parent
              analyzeDownstreamProject(planMeta) match {
                case Some(fields) if fields.nonEmpty =>
                  // Successfully identified required fields via schema projection
                  fields
                case _ =>
                  // The immediate parent might be a ProjectExec that just aliases the output.
                  // Try to look at its parent (the grandparent) for GetStructField references.
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
              // No parent SparkPlanMeta found in the meta tree, assume all fields are needed
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
         * Analyze a Project plan to find which struct fields are actually used.
         * This looks for GetStructField expressions that reference our protobuf output.
         */
        private def analyzeDownstreamProject(planMeta: SparkPlanMeta[_]): Option[Set[String]] = {
          planMeta.wrapped match {
            case p: ProjectExec =>
              // Collect all GetStructField references from the project list
              val fieldRefs = mutable.Set[String]()
              var hasDirectStructRef = false

              p.projectList.foreach { expr =>
                collectStructFieldReferences(expr, fieldRefs, hasDirectStructRefHolder = () => {
                  hasDirectStructRef = true
                })
              }

              if (hasDirectStructRef) {
                // If the entire struct is referenced directly (not via GetStructField),
                // we need all fields
                None
              } else if (fieldRefs.nonEmpty) {
                Some(fieldRefs.toSet)
              } else {
                // No GetStructField found - this shouldn't happen for valid plans
                // where from_protobuf is followed by field access
                None
              }
            case _ =>
              // Not a ProjectExec, cannot analyze schema projection
              None
          }
        }

        /**
         * Recursively collect field names from GetStructField expressions.
         * Also tracks if the struct is used directly without field extraction.
         */
        private def collectStructFieldReferences(
            expr: Expression,
            fieldRefs: mutable.Set[String],
            hasDirectStructRefHolder: () => Unit): Unit = {
          expr match {
            case GetStructField(child, ordinal, nameOpt) =>
              // Check if this GetStructField extracts from our protobuf struct
              if (isProtobufStructReference(child)) {
                // Get field name from the schema using ordinal
                val fieldName = nameOpt.getOrElse {
                  if (ordinal < fullSchema.fields.length) {
                    fullSchema.fields(ordinal).name
                  } else {
                    s"_$ordinal"
                  }
                }
                fieldRefs += fieldName
                // Don't recurse into child - we've handled this protobuf reference
              } else {
                // Child is not a protobuf struct, recurse to check for nested access
                collectStructFieldReferences(child, fieldRefs, hasDirectStructRefHolder)
              }

            case _ =>
              // Check if this expression directly references our protobuf struct
              // without extracting a field (e.g., passing the whole struct to a function)
              if (isProtobufStructReference(expr)) {
                hasDirectStructRefHolder()
              }
              // Recursively check children
              expr.children.foreach { child =>
                collectStructFieldReferences(child, fieldRefs, hasDirectStructRefHolder)
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
          
          // Check if expr is an AttributeReference with the same schema as our protobuf output
          // This handles the case where GetStructField references a column from a parent Project
          expr match {
            case attr: AttributeReference =>
              // Check if the data type matches our full schema (struct type from protobuf)
              attr.dataType match {
                case st: StructType => 
                  // Compare field names and types only. We intentionally do not compare
                  // nullable flags because schema transformations (like projections or
                  // certain optimizations) may change nullability while the underlying
                  // schema structure remains the same. For schema projection detection,
                  // matching names and types is sufficient to identify protobuf output.
                  st.fields.length == fullSchema.fields.length &&
                    st.fields.zip(fullSchema.fields).forall { case (a, b) =>
                      a.name == b.name && a.dataType == b.dataType
                    }
                case _ => false
              }
            case _ => false
          }
        }

        override def convertToGpu(child: Expression): GpuExpression = {
          GpuFromProtobuf(
            fullSchema, decodedFieldIndices, fieldNumbers, cudfTypeIds, cudfTypeScales,
            failOnErrors, child)
        }
      }
    )
  }

  private def getMessageName(e: Expression): String =
    invoke0[String](e, "messageName")

  /**
   * Newer Spark versions may carry an in-expression descriptor set payload
   * (e.g. binaryDescriptorSet).
   * Spark 3.4.x does not, so callers should fall back to descFilePath().
   */
  private def getDescriptorBytes(e: Expression): Option[Array[Byte]] = {
    // Spark 4.x/3.5+ (depending on the API): may be Array[Byte] or Option[Array[Byte]].
    val direct = Try(invoke0[Array[Byte]](e, "binaryDescriptorSet")).toOption
    direct.orElse {
      Try(invoke0[Option[Array[Byte]]](e, "binaryDescriptorSet")).toOption.flatten
    }
  }

  private def getDescFilePath(e: Expression): Option[String] =
    Try(invoke0[Option[String]](e, "descFilePath")).toOption.flatten

  private def writeTempDescFile(descBytes: Array[Byte]): String = {
    val tmp: Path = Files.createTempFile("spark-rapids-protobuf-desc-", ".desc")
    Files.write(tmp, descBytes)
    // deleteOnExit() is not guaranteed to run on abnormal JVM termination, but these
    // descriptor files are small (typically < 10KB) and only created when using
    // binaryDescriptorSet (Spark 4.0+). The risk of temporary file accumulation is
    // acceptable for this use case.
    tmp.toFile.deleteOnExit()
    tmp.toString
  }

  private def buildMessageDescriptorWithSparkProtobuf(
      messageName: String,
      descFilePathOpt: Option[String]): AnyRef = {
    val cls = ShimReflectionUtils.loadClass(sparkProtobufUtilsObjectClassName)
    val module = cls.getField("MODULE$").get(null)
    // buildDescriptor(messageName: String, descFilePath: Option[String])
    val m = cls.getMethod("buildDescriptor", classOf[String], classOf[scala.Option[_]])
    m.invoke(module, messageName, descFilePathOpt).asInstanceOf[AnyRef]
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
