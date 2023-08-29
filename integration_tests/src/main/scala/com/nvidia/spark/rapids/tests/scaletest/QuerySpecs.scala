/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tests.scaletest

import com.nvidia.spark.rapids.tests.scaletest.ScaleTest.Config

import org.apache.spark.sql.SparkSession

case class TestQuery(name: String, content: String, iterations: Int, timeout: Long,
  description: String)

class QuerySpecs(config: Config, spark: SparkSession) {
  private val baseInputPath = s"${config.inputDir}/" +
    "SCALE_" +
    s"${config.scaleFactor}_" +
    s"${config.complexity}_" +
    s"${config.format}_" +
    s"${config.version}_" +
    s"${config.seed}"
  private val tables = Seq("a_facts",
    "b_data",
    "c_data",
    "d_data",
    "e_data",
    "f_facts",
    "g_data")

  /**
   *
   * @param prefix     column name prefix e.g. "b_data"
   * @param startIndex start column index e.g. 1
   * @param endIndex   end column index e.g. 10
   * @return an expanded string for all columns with comma separated
   */
  private def expandDataColumnWithRange(prefix: String, startIndex: Int, endIndex: Int): String = {
    (startIndex to endIndex).map(i => s"${prefix}_$i").mkString(",")
  }

  /**
   * Read data and initialize temp views in Spark
   */
  def initViews(): Unit = {

    for (table <- tables) {
      spark.read.format(config.format).load(s"$baseInputPath/$table")
        .createOrReplaceTempView(table)
    }
  }

  def getCandidateQueries(): Map[String, TestQuery] = {
    /*
    All queries are defined here
     */
    val allQueries = Map[String, TestQuery](
      "q1" -> TestQuery("q1",
        "SELECT a_facts.*, " +
          expandDataColumnWithRange("b_data", 1, 10) +
          " FROM b_data JOIN a_facts WHERE " +
          "primary_a = b_foreign_a",
        config.iterations,
        config.timeout,
        "Inner join with lots of ride along columns"),

      "q2" -> TestQuery("q2",
        "SELECT a_facts.*," +
          expandDataColumnWithRange("b_data", 1, 10) +
          " FROM b_data FULL OUTER JOIN a_facts WHERE primary_a = b_foreign_a",
        config.iterations,
        config.timeout,
        "Full outer join with lots of ride along columns"),

      "q3" -> TestQuery("q3",
        "SELECT a_facts.*," +
          expandDataColumnWithRange("b_data", 1, 10) +
          " FROM b_data LEFT OUTER JOIN a_facts WHERE primary_a = b_foreign_a",
        config.iterations,
        config.timeout,
        "Left outer join with lots of ride along columns")
    )
    if (config.queries.isEmpty) {
      allQueries
    } else {
      config.queries.map(q => {
        (q, allQueries(q))
      }).toMap
    }
  }
}
