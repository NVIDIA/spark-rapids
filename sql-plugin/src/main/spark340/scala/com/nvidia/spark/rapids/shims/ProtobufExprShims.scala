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
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "359"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "410db183"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
{"spark": "500"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import scala.collection.mutable
import scala.reflect.ClassTag

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.rapids.GpuFromProtobuf
import org.apache.spark.sql.rapids.protobuf.{FlattenedFieldDescriptor, ProtobufSchemaExtractor,
  ProtobufSchemaValidator}
import org.apache.spark.sql.types._

/**
 * Spark 3.4+ optional integration for spark-protobuf expressions.
 *
 * The module can be external or runtime-provided, so these rules must be registered by reflection.
 */
object ProtobufExprShims extends org.apache.spark.internal.Logging {
  private[this] val protobufDataToCatalystClassName =
    "org.apache.spark.sql.protobuf.ProtobufDataToCatalyst"

  /** GPU expression rules, or an empty map when the optional spark-protobuf APIs cannot load. */
  def exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    try {
      val clazz = ShimReflectionUtils.loadClass(protobufDataToCatalystClassName)
        .asInstanceOf[Class[_ <: UnaryExpression]]
      Map(clazz.asInstanceOf[Class[_ <: Expression]] -> fromProtobufRule(clazz))
    } catch {
      case _: ClassNotFoundException => Map.empty
      case e: Exception =>
        logWarning(s"Failed to load $protobufDataToCatalystClassName: ${e.getMessage}")
        Map.empty
      case e: Error =>
        logWarning(
          s"JVM error while loading $protobufDataToCatalystClassName: ${e.getMessage}")
        Map.empty
    }
  }

  private def fromProtobufRule(clazz: Class[_ <: UnaryExpression]): ExprRule[_ <: Expression] = {
    // Name the rule config and docs after ProtobufDataToCatalyst instead of UnaryExpression.
    implicit val tag: ClassTag[UnaryExpression] = ClassTag(clazz)
    GpuOverrides.expr[UnaryExpression](
      "Decode a BinaryType column (protobuf) into a Spark SQL struct",
      ExprChecks.unaryProject(
        // Output support depends on the protobuf descriptor, so tagExprForGpu checks each field.
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
        private var flatIsOutput: Array[Boolean] = _
        private var flatDefaultInts: Array[Long] = _
        private var flatDefaultFloats: Array[Double] = _
        private var flatDefaultBools: Array[Boolean] = _
        private var flatDefaultStrings: Array[Array[Byte]] = _
        private var flatEnumValidValues: Array[Array[Int]] = _
        private var flatEnumNames: Array[Array[Array[Byte]]] = _

        override def tagExprForGpu(): Unit = {
          fullSchema = e.dataType match {
            case st: StructType => st
            case other =>
              willNotWorkOnGpu(
                s"Only StructType output is supported for from_protobuf, got $other")
              return
          }

          val exprInfo = SparkProtobufCompat.extractExprInfo(e) match {
            case Right(info) => info
            case Left(reason) =>
              willNotWorkOnGpu(reason)
              return
          }
          val plannerOptions = SparkProtobufCompat.parsePlannerOptions(exprInfo.options) match {
            case Right(opts) => opts
            case Left(reason) =>
              willNotWorkOnGpu(reason)
              return
          }
          val enumsAsInts = plannerOptions.enumsAsInts
          failOnErrors = plannerOptions.failOnErrors
          val unsupportedOptions = SparkProtobufCompat.unsupportedOptions(exprInfo.options)
          if (unsupportedOptions.nonEmpty) {
            val keys = unsupportedOptions.mkString(",")
            willNotWorkOnGpu(
              s"from_protobuf options are not supported yet on GPU: $keys")
            return
          }
          val messageName = exprInfo.messageName

          val msgDesc = SparkProtobufCompat.resolveMessageDescriptor(exprInfo) match {
            case Right(desc) => desc
            case Left(reason) =>
              willNotWorkOnGpu(reason)
              return
          }

          // Step 1: Analyze all fields and build field info map
          val fieldsInfoMap =
            ProtobufSchemaExtractor.analyzeAllFields(fullSchema, msgDesc, enumsAsInts, messageName)
              .fold({ reason =>
                willNotWorkOnGpu(reason)
                return
              }, identity)

          // Step 2: Every field is decoded and returned until schema projection is supported.
          val unsupportedFields = fullSchema.fields.filter { sf =>
            fieldsInfoMap.get(sf.name).exists(!_.isSupported)
          }
          if (unsupportedFields.nonEmpty) {
            val reasons = unsupportedFields.map { sf =>
              val info = fieldsInfoMap(sf.name)
              s"${sf.name}: ${info.unsupportedReason.getOrElse("unknown reason")}"
            }
            willNotWorkOnGpu(
              s"Fields not supported for from_protobuf: ${reasons.mkString(", ")}")
            return
          }

          // Step 3: Build flattened schema for GPU decoding.
          // Only top-level fields are supported, so parentIdx == -1 and depth == 0.
          {
            val flatFields = mutable.ArrayBuffer[FlattenedFieldDescriptor]()
            var step5Failed = false

            def failStep5(reason: String): Unit = {
              step5Failed = true
              willNotWorkOnGpu(reason)
            }

            fullSchema.fields.foreach { sf =>
              if (!step5Failed) {
                val outputTypeOpt = sf.dataType match {
                  case ArrayType(elemType, _) => GpuFromProtobuf.sparkTypeToCudfIdOpt(elemType)
                  case other => GpuFromProtobuf.sparkTypeToCudfIdOpt(other)
                }
                outputTypeOpt match {
                  case None =>
                    failStep5(
                      s"Unsupported Spark type for protobuf field '${sf.name}': ${sf.dataType}")
                  case Some(outputType) =>
                    ProtobufSchemaValidator.toFlattenedFieldDescriptor(
                      sf.name,
                      sf,
                      fieldsInfoMap(sf.name),
                      -1,
                      0,
                      outputType,
                      isOutput = true) match {
                      case Right(flatField) => flatFields += flatField
                      case Left(reason) => failStep5(reason)
                    }
                }
              }
            }

            if (step5Failed) {
              return
            }

            // Populate flattened schema variables
            val flat = flatFields.toArray
            ProtobufSchemaValidator.validateFlattenedSchema(flat).fold({ reason =>
              failStep5(reason)
              return
            }, identity)
            val arrays = ProtobufSchemaValidator.toFlattenedSchemaArrays(flat)
            flatFieldNumbers = arrays.fieldNumbers
            flatParentIndices = arrays.parentIndices
            flatDepthLevels = arrays.depthLevels
            flatWireTypes = arrays.wireTypes
            flatOutputTypeIds = arrays.outputTypeIds
            flatEncodings = arrays.encodings
            flatIsRepeated = arrays.isRepeated
            flatIsRequired = arrays.isRequired
            flatHasDefaultValue = arrays.hasDefaultValue
            flatIsOutput = arrays.isOutput
            flatDefaultInts = arrays.defaultInts
            flatDefaultFloats = arrays.defaultFloats
            flatDefaultBools = arrays.defaultBools
            flatDefaultStrings = arrays.defaultStrings
            flatEnumValidValues = arrays.enumValidValues
            flatEnumNames = arrays.enumNames

            overrideDataType(decodedSchema)
          }
        }

        private def decodedSchema: StructType =
          StructType(fullSchema.fields.map(f => f.copy(nullable = true)))

        override def convertToGpu(child: Expression): GpuExpression = {
          GpuFromProtobuf(
            decodedSchema,
            flatFieldNumbers, flatParentIndices,
            flatDepthLevels, flatWireTypes, flatOutputTypeIds, flatEncodings,
            flatIsRepeated, flatIsRequired, flatHasDefaultValue, flatIsOutput, flatDefaultInts,
            flatDefaultFloats, flatDefaultBools, flatDefaultStrings, flatEnumValidValues,
            flatEnumNames, failOnErrors, child)
        }
      }
    )
  }

}
