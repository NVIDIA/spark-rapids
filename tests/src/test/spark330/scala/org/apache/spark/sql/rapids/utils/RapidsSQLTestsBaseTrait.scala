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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.utils

import java.net.JarURLConnection
import java.util.{Locale, TimeZone}

import org.apache.hadoop.fs.FileUtil
import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.rapids.utils.RapidsTestConstants.RAPIDS_TEST
import org.apache.spark.sql.test.SharedSparkSession


/** Basic trait for Rapids SQL test cases. */
trait RapidsSQLTestsBaseTrait extends SharedSparkSession with RapidsTestsBaseTrait {

  val sparkTestResourcesDir: java.nio.file.Path =
    java.nio.file.Files.createTempDirectory(getClass.getSimpleName)

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    val sparkTestClassResource = "/" + getClass.getSuperclass.getName.replace(".", "/") + ".class"
    val jarURLConnection = getClass.getResource(sparkTestClassResource)
      .openConnection()
      .asInstanceOf[JarURLConnection]
    val jarFile = new java.io.File(jarURLConnection.getJarFile.getName)
    FileUtil.unZip(jarFile, sparkTestResourcesDir.toFile)
  }

  protected override def afterAll(): Unit = {
    // SparkFunSuite will set this to true, and forget to reset to false
    System.clearProperty(IS_TESTING.key)
    FileUtil.fullyDelete(sparkTestResourcesDir.toFile)
    super.afterAll()
  }

  override protected def testFile(fileName: String): String = {
    java.nio.file.Paths.get(sparkTestResourcesDir.toString, fileName)
      .toString
  }

  protected def testRapids(testName: String, testTag: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    test(RAPIDS_TEST + testName, testTag: _*)(testFun)
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    if (shouldRun(testName)) {
      super.test(testName, testTags: _*)(testFun)
    } else {
      super.ignore(testName, testTags: _*)(testFun)
    }
  }

  override def sparkConf: SparkConf = {
    RapidsSQLTestsBaseTrait.nativeSparkConf(super.sparkConf, warehouse)
  }

  /**
   * Get all the children plan of plans.
   *
   * @param plans
   * : the input plans.
   * @return
   */
  private def getChildrenPlan(plans: Seq[SparkPlan]): Seq[SparkPlan] = {
    if (plans.isEmpty) {
      return Seq()
    }

    val inputPlans: Seq[SparkPlan] = plans.map {
      case stage: ShuffleQueryStageExec => stage.plan
      case plan => plan
    }

    var newChildren: Seq[SparkPlan] = Seq()
    inputPlans.foreach {
      plan =>
        newChildren = newChildren ++ getChildrenPlan(plan.children)
        // To avoid duplication of WholeStageCodegenXXX and its children.
        if (!plan.nodeName.startsWith("WholeStageCodegen")) {
          newChildren = newChildren :+ plan
        }
    }
    newChildren
  }

  /**
   * Get the executed plan of a data frame.
   *
   * @param df
   * : dataframe.
   * @return
   * A sequence of executed plans.
   */
  def getExecutedPlan(df: DataFrame): Seq[SparkPlan] = {
    df.queryExecution.executedPlan match {
      case exec: AdaptiveSparkPlanExec =>
        getChildrenPlan(Seq(exec.executedPlan))
      case plan =>
        getChildrenPlan(Seq(plan))
    }
  }
}

object RapidsSQLTestsBaseTrait {
  def nativeSparkConf(origin: SparkConf, warehouse: String): SparkConf = {
    // Timezone is fixed to UTC to allow timestamps to work by default
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    // Add Locale setting
    Locale.setDefault(Locale.US)

    val conf = origin
      .set("spark.rapids.sql.enabled", "true")
      .set("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .set("spark.sql.queryExecutionListeners",
        "org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback")
      .set("spark.sql.warehouse.dir", warehouse)
      .set("spark.sql.cache.serializer", "com.nvidia.spark.ParquetCachedBatchSerializer")
      // TODO: remove hard coded UTC https://github.com/NVIDIA/spark-rapids/issues/10874
      .set("spark.sql.session.timeZone", "UTC")
      .set("spark.rapids.sql.explain", "ALL")
      // uncomment below config to run `strict mode`, where fallback to CPU is treated as fail
      // .set("spark.rapids.sql.test.enabled", "true")
      // .set("spark.rapids.sql.test.allowedNonGpu",
      // "SerializeFromObjectExec,DeserializeToObjectExec,ExternalRDDScanExec")
      .setAppName("rapids spark plugin running Vanilla Spark UT")

    conf
  }
}
