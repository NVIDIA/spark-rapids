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
package org.apache.spark.sql.rapids.suites

import java.io.File
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.reflect.runtime.universe.TypeTag
import org.scalactic.Equality
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.parquet.ParquetSchemaPruningSuite
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsBaseTrait
import org.apache.spark.sql.types.StructType

class RapidsParquetSchemaPruningSuite
  extends ParquetSchemaPruningSuite
  with RapidsSQLTestsBaseTrait {

  override protected def checkScanSchemata(df: DataFrame,
                                           expectedSchemaCatalogStrings: String*): Unit = {
    val fileSourceScanSchemata =
      collect(df.queryExecution.executedPlan) {
        case scan: FileSourceScanExec => scan.requiredSchema
        case gpuScan: GpuFileSourceScanExec => gpuScan.requiredSchema
      }
    // Print the full execution plan
    println("Full Execution Plan:")
    println(df.queryExecution.executedPlan.treeString)
    assert(fileSourceScanSchemata.size === expectedSchemaCatalogStrings.size,
      s"Found ${fileSourceScanSchemata.size} file sources in dataframe, " +
        s"but expected $expectedSchemaCatalogStrings")
    fileSourceScanSchemata.zip(expectedSchemaCatalogStrings).foreach {
      case (scanSchema, expectedScanSchemaCatalogString) =>
        val expectedScanSchema = CatalystSqlParser.parseDataType(expectedScanSchemaCatalogString)
        implicit val equality = schemaEquality
        assert(scanSchema === expectedScanSchema)
    }
    println("hello I am here")
  }


  testSchemaPruning("select a single complex field 2") {
    //val query = sql("select name.middle from contacts")
    val query = sql("select name.middle, address from contacts where p=2")
    query.show()
    //checkScan2(query, "struct<name:struct<middle:string>>")
    //checkAnswer(query.orderBy("id"), Row("X.") :: Row("Y.") :: Row(null) :: Row(null) :: Nil)
  }

  protected val schemaEquality2 = new Equality[StructType] {
    override def areEqual(a: StructType, b: Any): Boolean =
      b match {
        case otherType: StructType => a.sameType(otherType)
        case _ => false
      }
  }

  protected def checkScan2(df: DataFrame, expectedSchemaCatalogStrings: String*): Unit = {
    //checkScanSchemata(df, expectedSchemaCatalogStrings: _*)
    // We check here that we can execute the query without throwing an exception. The results
    // themselves are irrelevant, and should be checked elsewhere as needed
    //df.collect()
  }

  protected def makeDataSourceFile2[T <: Product : ClassTag : TypeTag]
  (data: Seq[T], path: File): Unit = {
    spark.createDataFrame(data).write.mode(SaveMode.Overwrite).format(dataSourceName)
      .save(path.getCanonicalPath)
  }


  test("makeDataSourceFile"){

    val path = "/home/fejiang/Desktop"

    val briefContacts =
      BriefContact(2, Name("Janet", "Jones"), "567 Maple Drive") ::
        BriefContact(3, Name("Jim", "Jones"), "6242 Ash Street") :: Nil

    makeDataSourceFile2(contacts, new File(path + "/contacts/p=1"))
    makeDataSourceFile2(briefContacts, new File(path + "/contacts/p=2"))
    makeDataSourceFile2(departments, new File(path + "/departments"))

    val schema = "`id` INT,`name` STRUCT<`first`: STRING, `middle`: STRING, `last`: STRING>, " +
      "`address` STRING,`pets` INT,`friends` ARRAY<STRUCT<`first`: STRING, `middle`: STRING, " +
      "`last`: STRING>>,`relatives` MAP<STRING, STRUCT<`first`: STRING, `middle`: STRING, " +
      "`last`: STRING>>,`employer` STRUCT<`id`: INT, `company`: STRUCT<`name`: STRING, " +
      "`address`: STRING>>,`relations` MAP<STRUCT<`first`: STRING, `middle`: STRING, " +
      "`last`: STRING>,STRING>,`p` INT"

    spark.read.format(dataSourceName).schema(schema).load(path + "/contacts")
      .createOrReplaceTempView("contacts")

    val departmentSchema = "`depId` INT,`depName` STRING,`contactId` INT, " +
      "`employer` STRUCT<`id`: INT, `company`: STRUCT<`name`: STRING, `address`: STRING>>"
    spark.read.format(dataSourceName).schema(departmentSchema).load(path + "/departments")
      .createOrReplaceTempView("departments")

//    val query = sql("select name.middle from contacts")
//
//    query.show()

    val configs = Seq((false, true))

    configs.foreach { case (nestedPruning, nestedPruningOnExpr) =>
      withSQLConf(
        SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> nestedPruning.toString,
        SQLConf.NESTED_PRUNING_ON_EXPRESSIONS.key -> nestedPruningOnExpr.toString) {
        val query1 = spark.table("contacts")
          .select(explode(col("friends.first")))

        query1.collect()
//        checkAnswer(query1, Row("Susan") :: Nil)

      }
    }

  }

  def printType[T: TypeTag](value: T): Unit = {
    val tpe = typeOf[T]
    println(s"The type of the value is: $tpe")
  }

  test("test TypeTag"){
    printType(42)
    printType("Hello")
    printType(List(1, 2, 3))

    val contactsTypeTag: TypeTag[Seq[Contact]] = typeTag[Seq[Contact]]

    // Print the type
    println(s"TypeTag for contacts: ${contactsTypeTag.tpe}")

  }


  testSchemaPruning("select explode of nested field of array of struct2") {
    // Config combinations
    val configs = Seq( (false, true))

    configs.foreach { case (nestedPruning, nestedPruningOnExpr) =>
      withSQLConf(
        SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> nestedPruning.toString,
        SQLConf.NESTED_PRUNING_ON_EXPRESSIONS.key -> nestedPruningOnExpr.toString) {
        val query1 = spark.table("contacts")
          .select(explode(col("friends.first")))
        query1.collect()

      }
    }
  }


  testSchemaPruning("empty schema intersection2") {
    val query = sql("select name.middle from contacts where p=2")
    checkScan(query, "struct<name:struct<middle:string>>")
    checkAnswer(query.orderBy("id"),
      Row(null) :: Row(null) :: Nil)
  }
}
