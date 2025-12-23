/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import scala.util.Try

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.rapids.GpuFromProtobufSimple
import org.apache.spark.sql.types._

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
      "Decode a BinaryType column (protobuf) into a Spark SQL struct (simple types only)",
      ExprChecks.unaryProject(
        // Output is a struct; the rule does detailed checks in tagExprForGpu.
        TypeSig.STRUCT.nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRING),
        TypeSig.all,
        TypeSig.BINARY,
        TypeSig.BINARY),
      (e, conf, p, r) => new UnaryExprMeta[UnaryExpression](e, conf, p, r) {

        private var schema: StructType = _
        private var fieldNumbers: Array[Int] = _
        private var cudfTypeIds: Array[Int] = _
        private var cudfTypeScales: Array[Int] = _

        override def tagExprForGpu(): Unit = {
          schema = e.dataType match {
            case st: StructType => st
            case other =>
              willNotWorkOnGpu(
                s"Only StructType output is supported for from_protobuf(simple), got $other")
              return
          }

          val options = getOptionsMap(e)
          if (options.nonEmpty) {
            val keys = options.keys.mkString(",")
            willNotWorkOnGpu(
              s"from_protobuf options are not supported yet on GPU: $keys")
            return
          }

          val messageName = getMessageName(e)
          val descFilePathOpt = getDescFilePath(e).orElse {
            // Newer Spark may embed a descriptor set (binaryDescriptorSet). Write it to a temp file
            // so we can reuse Spark's ProtobufUtils (and its shaded protobuf classes) to resolve
            // the descriptor.
            getDescriptorBytes(e).map(writeTempDescFile)
          }
          if (descFilePathOpt.isEmpty) {
            willNotWorkOnGpu(
              "from_protobuf(simple) requires a descriptor set " +
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

          val fields = schema.fields
          val fnums = new Array[Int](fields.length)
          val typeIds = new Array[Int](fields.length)
          val scales = new Array[Int](fields.length)

          fields.zipWithIndex.foreach { case (sf, idx) =>
            sf.dataType match {
              case BooleanType | IntegerType | LongType | FloatType | DoubleType | StringType =>
              case other =>
                willNotWorkOnGpu(
                  s"Unsupported field type for from_protobuf(simple): ${sf.name}: $other")
                return
            }

            val fd = invoke1[AnyRef](msgDesc, "findFieldByName", classOf[String], sf.name)
            if (fd == null) {
              willNotWorkOnGpu(s"Protobuf field '${sf.name}' not found in message '$messageName'")
              return
            }

            val isRepeated = Try {
              invoke0[java.lang.Boolean](fd, "isRepeated").booleanValue()
            }.getOrElse(false)
            if (isRepeated) {
              willNotWorkOnGpu(
                s"Repeated fields are not supported for from_protobuf(simple): ${sf.name}")
              return
            }

            val protoType = invoke0[AnyRef](fd, "getType")
            val protoTypeName = typeName(protoType)
            val ok = (sf.dataType, protoTypeName) match {
              case (BooleanType, "BOOL") => true
              case (IntegerType, "INT32") => true
              case (LongType, "INT64") => true
              case (FloatType, "FLOAT") => true
              case (DoubleType, "DOUBLE") => true
              case (StringType, "STRING") => true
              case _ => false
            }
            if (!ok) {
              willNotWorkOnGpu(
                s"Field type mismatch for '${sf.name}': Spark ${sf.dataType} vs " +
                  s"Protobuf $protoTypeName")
              return
            }

            fnums(idx) = invoke0[java.lang.Integer](fd, "getNumber").intValue()
            val (tid, scale) = GpuFromProtobufSimple.sparkTypeToCudfId(sf.dataType)
            typeIds(idx) = tid
            scales(idx) = scale
          }

          fieldNumbers = fnums
          cudfTypeIds = typeIds
          cudfTypeScales = scales
        }

        override def convertToGpu(child: Expression): GpuExpression = {
          GpuFromProtobufSimple(schema, fieldNumbers, cudfTypeIds, cudfTypeScales, child)
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


