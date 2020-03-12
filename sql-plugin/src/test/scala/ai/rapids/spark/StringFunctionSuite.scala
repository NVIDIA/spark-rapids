/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

package ai.rapids.spark

import org.apache.spark.sql.functions._
import org.scalatest.Ignore

import org.scalatest.FunSuite
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._

object incompatibleUtf8Chars {
  val uppercaseIncompatibleChars = List(223, 329, 453, 456, 459, 496, 498, 604, 609, 618, 620, 647,
                                        669, 670, 912, 944, 1011, 1321, 1323, 1325, 1327, 1415, 5112,
                                        5113, 5114, 5115, 5116, 5117, 7296, 7297, 7298, 7299, 7300,
                                        7301, 7302, 7303, 7304, 7830, 7831, 7832, 7833, 7834, 8016,
                                        8018, 8020, 8022, 8064, 8065, 8066, 8067, 8068, 8069, 8070,
                                        8071, 8072, 8073, 8074, 8075, 8076, 8077, 8078, 8079, 8080,
                                        8081, 8082, 8083, 8084, 8085, 8086, 8087, 8088, 8089, 8090,
                                        8091, 8092, 8093, 8094, 8095, 8096, 8097, 8098, 8099, 8100,
                                        8101, 8102, 8103, 8104, 8105, 8106, 8107, 8108, 8109, 8110,
                                        8111, 8114, 8115, 8116, 8118, 8119, 8124, 8130, 8131, 8132,
                                        8134, 8135, 8140, 8146, 8147, 8150, 8151, 8162, 8163, 8164,
                                        8166, 8167, 8178, 8179, 8180, 8182, 8183, 8188, 42649, 42651,
                                        42903, 42905, 42907, 42909, 42911, 42933, 42935, 43859, 43888,
                                        43889, 43890, 43891, 43892, 43893, 43894, 43895, 43896, 43897,
                                        43898, 43899, 43900, 43901, 43902, 43903, 43904, 43905, 43906,
                                        43907, 43908, 43909, 43910, 43911, 43912, 43913, 43914, 43915,
                                        43916, 43917, 43918, 43919, 43920, 43921, 43922, 43923, 43924,
                                        43925, 43926, 43927, 43928, 43929, 43930, 43931, 43932, 43933,
                                        43934, 43935, 43936, 43937, 43938, 43939, 43940, 43941, 43942,
                                        43943, 43944, 43945, 43946, 43947, 43948, 43949, 43950, 43951,
                                        43952, 43953, 43954, 43955, 43956, 43957, 43958, 43959, 43960,
                                        43961, 43962, 43963, 43964, 43965, 43966, 43967, 64256, 64257,
                                        64258, 64259, 64260, 64261, 64262, 64275, 64276, 64277, 64278,
                                        64279)

  val lowercaseIncompatibleChars = List(304, 453, 456, 459, 498, 895, 1320, 1322, 1324, 1326, 5024,
                                        5025, 5026, 5027, 5028, 5029, 5030, 5031, 5032, 5033, 5034,
                                        5035, 5036, 5037, 5038, 5039, 5040, 5041, 5042, 5043, 5044,
                                        5045, 5046, 5047, 5048, 5049, 5050, 5051, 5052, 5053, 5054,
                                        5055, 5056, 5057, 5058, 5059, 5060, 5061, 5062, 5063, 5064,
                                        5065, 5066, 5067, 5068, 5069, 5070, 5071, 5072, 5073, 5074,
                                        5075, 5076, 5077, 5078, 5079, 5080, 5081, 5082, 5083, 5084,
                                        5085, 5086, 5087, 5088, 5089, 5090, 5091, 5092, 5093, 5094,
                                        5095, 5096, 5097, 5098, 5099, 5100, 5101, 5102, 5103, 5104,
                                        5105, 5106, 5107, 5108, 5109, 8072, 8073, 8074, 8075, 8076,
                                        8077, 8078, 8079, 8088, 8089, 8090, 8091, 8092, 8093, 8094,
                                        8095, 8104, 8105, 8106, 8107, 8108, 8109, 8110, 8111, 8124,
                                        8140, 8188, 42648, 42650, 42902, 42904, 42906, 42908, 42910,
                                        42923, 42924, 42925, 42926, 42928, 42929, 42930, 42931,
                                        42932, 42934)
}

class StringOperatorsSuite extends SparkQueryCompareTestSuite {
  def uppercaseCompatibleUtf8Df(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    var utf8Chars = ((0 until 65536) diff incompatibleUtf8Chars.uppercaseIncompatibleChars).map(i => (i.toChar.toString, i))
    utf8Chars = utf8Chars
    utf8Chars.toDF("strings", "ints")
  }

  def lowercaseCompatibleUtf8Df(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    var utf8Chars = ((0 until 65536) diff incompatibleUtf8Chars.lowercaseIncompatibleChars).map(i => (i.toChar.toString, i))
    utf8Chars = utf8Chars
    utf8Chars.toDF("strings", "ints")
  }

  INCOMPAT_testSparkResultsAreEqual("Test compatible values upper case modifier", uppercaseCompatibleUtf8Df) {
    frame => frame.select(upper(col("strings")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test compatible values lower case modifier", lowercaseCompatibleUtf8Df) {
    frame => frame.select(lower(col("strings")))
  }

  testSparkResultsAreEqual("Substring location function", nullableStringsFromCsv) {
    frame => frame.selectExpr("POSITION('r' IN strings)")
  }

  testSparkResultsAreEqual("Substring location function with offset", nullableStringsFromCsv) {
    frame => frame.selectExpr("locate('o', strings, 3)")
  }

  testSparkResultsAreEqual("Substring location function miss", nullableStringsFromCsv) {
    frame => frame.selectExpr("locate('t', strings, 1000)")
  }

  testSparkResultsAreEqual("Substring location function null offset", nullableStringsFromCsv) {
    frame => frame.selectExpr("locate('t', strings, null)")
  }

  testSparkResultsAreEqual("Substring location function null substring", nullableStringsFromCsv) {
    frame => frame.selectExpr("locate(null, strings, 1)")
  }

  testSparkResultsAreEqual("Substring location function empty string", nullableStringsFromCsv) {
    frame => frame.selectExpr("POSITION('' IN strings)")
  }

  testSparkResultsAreEqual("Substring location function invalid offset", nullableStringsFromCsv) {
    frame => frame.selectExpr("locate('', strings, 0)")
  }

  testSparkResultsAreEqual("String StartsWith", nullableStringsFromCsv) {
    frame => frame.filter(col("strings").startsWith("F"))
  }

  ALLOW_NON_GPU_testSparkResultsAreEqual("String StartsWith Col", nullableStringsFromCsv) {
    frame => frame.filter(col("strings").startsWith(col("more_strings")))
  }

  testSparkResultsAreEqual("String StartsWith Empty", nullableStringsFromCsv) {
    frame => frame.filter(col("strings").startsWith(""))
  }

  testSparkResultsAreEqual("String StartsWith Null", nullableStringsFromCsv) {
    val str : String = null
    frame => frame.filter(col("strings").startsWith(str))
  }

  testSparkResultsAreEqual("String EndsWith", nullableStringsFromCsv) {
    frame => frame.filter(col("strings").endsWith("oo"))
  }

  ALLOW_NON_GPU_testSparkResultsAreEqual("String EndsWith Col", nullableStringsFromCsv) {
    frame => frame.filter(col("strings").endsWith(col("more_strings")))
  }

  testSparkResultsAreEqual("String EndWith Empty", nullableStringsFromCsv) {
    frame => frame.filter(col("strings").endsWith(""))
  }

  testSparkResultsAreEqual("String EndWith Null", nullableStringsFromCsv) {
            val str : String = null
    frame => frame.filter(col("strings").endsWith(str))
  }

  testSparkResultsAreEqual("String Like to Contains", nullableStringsFromCsv) {
    frame => frame.filter(col("strings").like("%o%"))
  }

  testSparkResultsAreEqual("String Like to Contains Empty", nullableStringsFromCsv) {
    frame => frame.filter(col("strings").like(""))
  }
}

/*
 * The underlying cuDF functions have missing functionality for some characters. Either the mappings do not match
 * between CPU and GPU or the upper/lower case version of the character maps to a multiple character set. This
 * issue can be tracked at github.com/rapidsai/cudf/issues/3132
 */
@Ignore
class StringOperatorsSuiteProblematicCharacters extends SparkQueryCompareTestSuite {
  def uppercaseIncompatibleUtf8Df(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    var utf8Chars = incompatibleUtf8Chars.uppercaseIncompatibleChars.map(i => (i.toChar.toString, i))
    utf8Chars = utf8Chars
    utf8Chars.toDF("strings", "ints")
  }

  def lowercaseIncompatibleUtf8Df(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    var utf8Chars = incompatibleUtf8Chars.lowercaseIncompatibleChars.map(i => (i.toChar.toString, i))
    utf8Chars = utf8Chars
    utf8Chars.toDF("strings", "ints")
  }

  INCOMPAT_testSparkResultsAreEqual("Test incompatible values upper case modifier", uppercaseIncompatibleUtf8Df) {
    frame => frame.select(upper(col("strings")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test incompatible values lower case modifier", lowercaseIncompatibleUtf8Df) {
    frame => frame.select(lower(col("strings")))
  }
}
