/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.tool.ui

import java.nio.file
import java.nio.file.{Files, FileSystems, Paths}

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.qualification.QualificationSummaryInfo
import org.apache.spark.util.Utils

class QualificationReportGenerator(outputDir: String,
    sumArr: Seq[QualificationSummaryInfo]) extends Logging {

  import QualificationReportGenerator._
  implicit val formats = DefaultFormats

  val outputWorkPath = new Path(outputDir)
  val fs = Some(FileSystem.get(outputWorkPath.toUri, new Configuration()))

  def launch(): Unit = {
    val uiRootPath = getPathForResource(RAPIDS_UI_ASSETS_DIR)
    logDebug(s"Generating UI files into... ${outputWorkPath.toUri}")
    copyAssetFolderRecursively(uiRootPath, outputWorkPath)
  }

  def copyAssetFolderRecursively(srcFolderPath: java.nio.file.Path, dstPath: Path): Unit = {
    logDebug(s"UI code generator: Copying ... ${srcFolderPath.toUri}")
    if (Files.isDirectory(srcFolderPath)) {
      val destinationPath = new Path(dstPath, srcFolderPath.getFileName.toString)
      fs.map { dstFileSys =>
        dstFileSys.mkdirs(destinationPath)
        Files.list(srcFolderPath).forEach { childPath =>
          if (Files.isDirectory(childPath)) {
            copyAssetFolderRecursively(childPath, destinationPath)
          } else {
            tryCopyAssetFile(childPath, new Path(destinationPath, childPath.getFileName.toString))
          }
        }
      }
    }
  }

  def generateJSFiles(): Unit = {
    // Serializing the entire list of sums may stress the memory.
    // Serializing one record at a time is slower but it would reduce the memory peak consumption.
    val outputPath = new Path(outputWorkPath, RAPIDS_UI_JS_DATA)
    val mainIndexPath =new Path(outputWorkPath,  RAPIDS_UI_INDEX_PATH)
    logInfo(s"Generating UI data in ${mainIndexPath.toUri}")
    val fileHeader =
      s"""
        |let qualificationRecords = [
       """.stripMargin
    val fileFooter =
      s"""|];
       """.stripMargin
    fs.foreach { dfs =>
      val outFile = dfs.create(outputPath)
      Utils.tryWithSafeFinally {
        outFile.writeBytes(fileHeader)
        if (sumArr.nonEmpty) {
          if (sumArr.size > 1) {
            for (ind <- 0.until(sumArr.size - 1)) {
              writeAppRecord(sumArr(ind), outFile)
            }
          }
          writeAppRecord(sumArr.last, outFile, "")
        }
        outFile.writeBytes(fileFooter)
      } {
        outFile.flush()
        outFile.close()
      }
    }
  }

  private def writeAppRecord(appRec: QualificationSummaryInfo,
      outStream: FSDataOutputStream, sep: String =","): Unit = {
    val sumRec =
      s"""|\t${Serialization.write(appRec)}$sep
       """.stripMargin
    outStream.writeBytes(sumRec)
  }

  def tryCopyAssetFile(srcFilePath: java.nio.file.Path, dstPath: Path) : Unit = {
    logDebug(s"Copying UI assets: ${srcFilePath.toUri.toString} to ${dstPath.toUri.toString}")
    fs.foreach { dstFileSys =>
      Utils.tryWithResource(Files.newInputStream(srcFilePath)) { in =>
        val out = dstFileSys.create(dstPath)
        Utils.tryWithSafeFinally {
          val buffer = new Array[Byte](130 * 1024)
          Iterator.continually(in.read(buffer)).takeWhile(_ != -1).foreach { bCount =>
            out.write(buffer, 0, bCount)
          }
        } {
          out.flush()
          out.close()
        }
      }
    }
  }

  def close(): Unit = {
    jarFS.foreach { jFS =>
      jFS.close()
    }
  }
}

object QualificationReportGenerator extends Logging {
  val RAPIDS_UI_ASSETS_DIR = "/ui"
  val RAPIDS_UI_JS_DATA = s"ui/js/data-output.js"
  val RAPIDS_UI_INDEX_PATH = s"ui/html/index.html"
  var jarFS : Option[file.FileSystem] = None

  private def getPathForResource(filename: String): java.nio.file.Path = {
    val url = getClass.getResource(filename)
    if (url.getPath.contains("jar")) { // this is a jar resource
      val jFs = jarFS.getOrElse(setJarFileSystem(filename))
      jFs.getPath(filename)
    } else {
      Paths.get(url.toURI)
    }
  }

  private def setJarFileSystem(fileName: String): file.FileSystem = {
    val jFileSys = FileSystems.newFileSystem(getClass.getResource(fileName).toURI,
      Map[String, String]().asJava)
    jarFS = Some(jFileSys)
    jFileSys
  }

  def generateDashBoard(outDir: String, sumArr: Seq[QualificationSummaryInfo]) : Unit = {
    // TODO - I assume we want the EstimatedSummaryInfo here as well
    val generatorOp = Some(new QualificationReportGenerator(outDir, sumArr))
    var startTime = 0L;
    generatorOp.foreach { generator =>
      Utils.tryWithSafeFinally {
        startTime = System.currentTimeMillis()
        generator.launch()
        generator.generateJSFiles()
      } {
        generator.close()
        val endTime = System.currentTimeMillis()
        logInfo(s"Took ${endTime - startTime}ms to process ")
      }
    }
  }
}
