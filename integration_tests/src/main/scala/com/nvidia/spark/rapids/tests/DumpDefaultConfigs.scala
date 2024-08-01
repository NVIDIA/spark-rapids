/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tests

import java.io.{BufferedOutputStream, DataOutputStream, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.util.Locale

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.ShimReflectionUtils
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.writePretty

/**
 * Dump all spark-rapids configs with their defaults.
 */
object DumpDefaultConfigs {

  object Format extends Enumeration {
    type Format = Value
    val PLAIN, JSON = Value
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(s"Usage: ${this.getClass.getCanonicalName} {format} {output_path}")
      System.exit(1)
    }

    val format = Format.withName(args(0).toUpperCase(Locale.US))
    val outputPath = args(1)

    println(s"Dumping all spark-rapids configs and their defaults at ${outputPath}")

    // We use the reflection as RapidsConf should be accessed via the shim layer.
    val clazz = ShimReflectionUtils.loadClass("com.nvidia.spark.rapids.RapidsConf")
    val m = clazz.getDeclaredMethod("getAllConfigsWithDefault")
    val allConfs: Map[String, Any] = m.invoke(null).asInstanceOf[Map[String, Any]]
    withResource(new FileOutputStream(outputPath)) { fos =>
      withResource(new BufferedOutputStream(fos)) { bos =>
        format match {
          case Format.PLAIN =>
            withResource(new DataOutputStream(bos)) { dos =>
              allConfs.foreach( { case (k, v) =>
                val valStr = v match {
                  case some: Some[_] => some.getOrElse("")
                  case _ =>
                    if (v == null) {
                      ""
                    } else {
                      v.toString
                    }
                }
                dos.writeUTF(s"'${k}': '${valStr}',")
              })
            }
          case Format.JSON =>
            implicit val formats = DefaultFormats
            bos.write(writePretty(allConfs).getBytes(StandardCharsets.UTF_8))
          case _ =>
            System.err.println(s"Unknown format: ${format}")
        }
      }
    }
  }
}
