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

import scala.collection.mutable
import scala.util.Random

import com.nvidia.spark.rapids.tests.scaletest.ScaleTest.Config

import org.apache.spark.sql.SparkSession

case class TestQuery(name: String, content: String, iterations: Int, timeout: Long,
    description: String, shufflePartitions: Int = 200)


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

  private val random = new Random(config.seed)


  /**
   * To get numeric columns in a dataframe.
   * Now numeric columns are limited to [byte, int, long, decimal]
   *
   * @param table table name
   * @return numeric type column names
   */
  private def getNumericColumns(table: String): Seq[String] = {
    assert(tables.contains(table), s"Invalid table: $table, candidate tables are ${
      tables
        .mkString(",")
    }")
    val df = spark.read.format(config.format).load(s"$baseInputPath/$table")
    // Get the data types of all columns
    val columnDataTypes = df.dtypes

    // Filter columns that are numeric
    val numericColumns = columnDataTypes.filter {
      case (_, dataType) =>
        dataType == "ByteType" || dataType == "IntegerType" || dataType == "LongType" || dataType
          .startsWith("DecimalType")
    }.map {
      case (columnName, _) => columnName
    }
    numericColumns
  }

  /**
   *
   * @param prefix     column name prefix e.g. "b_data"
   * @param startIndex start column index e.g. 1
   * @param endIndex   end column index e.g. 10
   * @return an expanded string for all columns with comma separated
   */
  private def expandColumnWithRange(prefix: String, startIndex: Int, endIndex: Int): String = {
    assert(startIndex <= endIndex, s"Invalid Data Column index range: $startIndex to $endIndex")
    (startIndex to endIndex).map(i => s"${prefix}_$i").mkString(",")
  }

  /**
   * expand the key column names by complexity. e.g. c_key2_* when complexity is 5:
   * "c_key2_1, c_key2_2, c_key2_3, c_key2_4, c_key2_5"
   * This is for Key Group 2
   *
   * @param prefix     column name prefix
   * @param complexity indicates the number of columns
   * @return expanded columns names
   */
  private def expandKeyColumnByComplexity(prefix: String, complexity: Int): String = {
    (1 to complexity).map(i => s"${prefix}_$i").mkString(",")
  }

  /**
   * expand the key column names to 3. e.g. b_key3_*:
   * "b_key3_1, b_key3_2, b_key3_3"
   * This is only for Key Group 3
   *
   * @param prefix prefix
   * @return expanded column names
   */
  private def expandKeyGroup3(prefix: String): String = {
    (1 to 3).map(i => s"${prefix}_$i").mkString(",")
  }

  /**
   * expand the columns for aggregations functions by complexity. e.g. MIN(b_data_*) when
   * complexity is 3:
   * "MIN(b_data_1), MIN(b_data_2), MIN(b_data_3), MIN(b_data_4), MIN(b_data_5)"
   *
   * @param prefix     column name prefix
   * @param aggFunc    aggregate function name
   * @param complexity indicates the number of columns
   * @return
   */
  private def expandAggColumnByComplexity(prefix: String, aggFunc: String, complexity: Int): String
  = {
    (1 to complexity).map(i => s"${aggFunc}(${prefix}_$i)").mkString(",")
  }

  /**
   * expand the where clause with AND condition after join by complexity. e.g. c_key2_* = d_key2_*
   * with complexity 3:
   * "c_key2_1 = d_key2_1 AND c_key2_2 = d_key2_2 AND c_key2_3 = d_key2_3"
   *
   * @param lhsPrefix  left hand side column name prefix
   * @param rhsPrefix  right hand side column name prefix
   * @param complexity indicates the number of columns
   * @return expanded WHERE clauses.
   */
  private def expandWhereClauseWithAndByComplexity(lhsPrefix: String, rhsPrefix: String,
      complexity: Int): String = {
    (1 to complexity).map(i => s"${lhsPrefix}_$i = ${rhsPrefix}_$i").mkString(" AND ")
  }

  /**
   * expand where clause with AND condition for Key Group 3 that only has 3 key columns.
   *
   * @param lhsPrefix left hand side column name prefix
   * @param rhsPrefix right hand side column name prefix
   * @return expanded WHERE clauses.
   */
  private def expandWhereClauseWithAndKeyGroup3(lhsPrefix: String, rhsPrefix: String): String = {
    expandWhereClauseWithAndByComplexity(lhsPrefix, rhsPrefix, 3)
  }

  /** expand data columns for A table */
  private def expandADataColumns(): String = {
    "a_data_low_unique_1, a_data_low_unique_len_1, " +
      (1 to config.complexity).map(i => s"a_data_$i").mkString(", ")
  }

  /**
   * expand columns names for B table.
   * B table contains 10 data columns for random types
   * and 10 data columns for decimal type.
   *
   * @return
   */
  private def expandBDataColumns(): String = {
    (1 to 10).map(i => s"b_data_$i").mkString(", ") + "," +
      (1 to 10).map(i => s"b_data_small_value_range_$i").mkString(", ")
  }


  /**
   * generate a string that contains multiple SUM oprations for numeric data columns
   * e.g. when table b contains numeric columns as ["b_1", "b_2", "b_3", "b_4"]
   * it will produce "SUM(b_1 * b_2), SUM(b_1 * b3), SUM(b_1 * b_4)....." according to complexity.
   * Note it's not only limited to 2 columns multiplied, when complexity goes large, it may contains
   * more columns.
   *
   * @param table
   * @param complexity
   * @return
   */
  private def expandSumForDataColumnsByComplexity(table: String, complexity: Int)
  : String = {
    val numericColumns = getNumericColumns(table)
    (2 until numericColumns.length).map(numericColumns.combinations).reduce(_ ++ _).take(
      complexity).map(f => f.mkString("SUM(", " * ", ")")).mkString(", ")
  }

  private def randomMinMax(random: Random): String = {
    if (random.nextBoolean()) {
      "MAX"
    } else {
      "MIN"
    }
  }

  private def expandMinMaxForDataColumnsByComplexity(table: String, complexity: Int): String = {
    val numericColumns = getNumericColumns(table)
    numericColumns.take(complexity).map(i => i.mkString(s"${randomMinMax(random)}(", "", ")"))
      .mkString(",")
  }

  private def MixedSumMinMaxColumnsByComplexity(table: String, complexity: Int): String = {
    val numSums = random.nextInt(complexity)
    val numMinMax = complexity - numSums
    Seq(expandSumForDataColumnsByComplexity(table, numSums),
      expandMinMaxForDataColumnsByComplexity(table, numMinMax)).filter(_.nonEmpty).mkString(",")
  }

  private def WindowMixedSumMinMaxColumnsByComplexity(table: String, complexity: Int): String = {
    val withoutWindow = MixedSumMinMaxColumnsByComplexity(table, complexity)
    withoutWindow.strip().split(",").filter(_.nonEmpty)
      .map(i => s"$i over w").mkString(", ")
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

  def getCandidateQueries(): mutable.LinkedHashMap[String, TestQuery] = {
    /*
    All queries are defined here
     */
    val allQueries = mutable.LinkedHashMap[String, TestQuery](
      "q1" -> TestQuery("q1",
        "SELECT a_facts.*, " +
          expandColumnWithRange("b_data", 1, 10) +
          " FROM b_data JOIN a_facts WHERE " +
          "primary_a = b_foreign_a",
        config.iterations,
        config.timeout,
        "Inner join with lots of ride along columns"),

      "q2" -> TestQuery("q2",
        "SELECT a_facts.*," +
          expandColumnWithRange("b_data", 1, 10) +
          " FROM b_data FULL OUTER JOIN a_facts WHERE primary_a = b_foreign_a",
        config.iterations,
        config.timeout,
        "Full outer join with lots of ride along columns"),

      "q3" -> TestQuery("q3",
        "SELECT a_facts.*," +
          expandColumnWithRange("b_data", 1, 10) +
          " FROM b_data LEFT OUTER JOIN a_facts WHERE primary_a = b_foreign_a",
        config.iterations,
        config.timeout,
        "Left outer join with lots of ride along columns"),

      "q4" -> TestQuery("q4",
        "SELECT c_data.* FROM c_data LEFT ANTI JOIN a_facts WHERE primary_a = c_foreign_a",
        config.iterations,
        config.timeout,
        "Left anti-join lots of ride along columns."),
      "q5" -> TestQuery("q5",
        "SELECT c_data.* FROM c_data LEFT SEMI JOIN a_facts WHERE primary_a = c_foreign_a",
        config.iterations,
        config.timeout,
        "Left semi-join lots of ride along columns."),
      "q6" -> TestQuery("q6",
        s"SELECT " +
          s"${expandKeyColumnByComplexity("c_key2", config.complexity)}, COUNT(1), " +
          s"${expandAggColumnByComplexity("c_data", "MIN", config.complexity)}," +
          s"${expandAggColumnByComplexity("d_data", "MAX", config.complexity)} " +
          s"FROM c_data JOIN d_data WHERE " +
          s"${
            expandWhereClauseWithAndByComplexity(
              "c_key2", "d_key2", config.complexity)
          } GROUP BY " +
          s"${expandKeyColumnByComplexity("c_key2", config.complexity)}",
        config.iterations,
        config.timeout,
        "Exploding inner large key count equi-join followed by min/max agg."),
      "q7" -> TestQuery("q7",
        s"SELECT " +
          s"${expandKeyColumnByComplexity("c_key2", config.complexity)}, COUNT(1), " +
          s"${expandAggColumnByComplexity("c_data", "MIN", config.complexity)}," +
          s"${expandAggColumnByComplexity("d_data", "MAX", config.complexity)} " +
          s"FROM c_data FULL OUTER JOIN d_data WHERE " +
          s"${
            expandWhereClauseWithAndByComplexity(
              "c_key2", "d_key2", config.complexity)
          } GROUP BY " +
          s"${expandKeyColumnByComplexity("c_key2", config.complexity)}",
        config.iterations,
        config.timeout,
        "Exploding full outer large key count equi-join followed by min/max agg."),
      "q8" -> TestQuery("q8",
        s"SELECT " +
          s"${expandKeyColumnByComplexity("c_key2", config.complexity)}, COUNT(1), " +
          s"${expandAggColumnByComplexity("c_data", "MIN", config.complexity)}," +
          s"${expandAggColumnByComplexity("d_data", "MAX", config.complexity)} " +
          s"FROM c_data LEFT OUTER JOIN d_data WHERE " +
          s"${
            expandWhereClauseWithAndByComplexity(
              "c_key2", "d_key2", config.complexity)
          } " +
          s"GROUP BY " +
          s"${expandKeyColumnByComplexity("c_key2", config.complexity)}",
        config.iterations,
        config.timeout,
        "Exploding left outer large key count equi-join followed by min/max agg."),
      "q9" -> TestQuery("q9",
        s"SELECT " +
          s"${expandKeyColumnByComplexity("c_key2", config.complexity)}, " +
          s"COUNT(1), " +
          s"${expandAggColumnByComplexity("MIN", "c_data", config.complexity)} " +
          s"FROM c_data LEFT SEMI JOIN d_data WHERE " +
          s"${
            expandWhereClauseWithAndByComplexity(
              "c_key2", "d_key2", config.complexity)
          }" +
          s" GROUP BY " +
          s"${expandKeyColumnByComplexity("c_key2", config.complexity)}",
        config.iterations,
        config.timeout,
        "Left semi large key count equi-join followed by min/max agg."),
      "q10" -> TestQuery("q10",
        s"SELECT " +
          s"${expandKeyColumnByComplexity("c_key2", config.complexity)}," +
          s" COUNT(1)," +
          s" ${expandAggColumnByComplexity("c_data", "MIN", config.complexity)}" +
          s" FROM c_data LEFT ANTI JOIN d_data WHERE " +
          s"${
            expandWhereClauseWithAndByComplexity(
              "c_key2", "d_key2", config.complexity)
          } " +
          s"GROUP BY " +
          s"${expandKeyColumnByComplexity("c_key2", config.complexity)}",
        config.iterations,
        config.timeout,
        "Left anti large key count equi-join followed by min/max agg."),
      "q11" -> TestQuery("q11",
        s"SELECT " +
          s"${expandKeyGroup3("b_key3")}, " +
          s"${expandColumnWithRange("e_data", 1, 10)}, " +
          s"${expandBDataColumns()}" +
          s"FROM b_data JOIN e_data WHERE " +
          s"${expandWhereClauseWithAndKeyGroup3("b_key3", "e_key3")}",
        config.iterations,
        config.timeout,
        "No obvious build side inner equi-join. (Shuffle partitions should be set to 10)",
        shufflePartitions = 10),
      "q12" -> TestQuery("q12",
        s"SELECT " +
          s"${expandKeyGroup3("b_key3")}, " +
          s"${expandColumnWithRange("e_data", 1, 10)}, " +
          s"${expandBDataColumns()} " +
          s"FROM b_data FULL OUTER JOIN e_data WHERE " +
          s"${expandWhereClauseWithAndKeyGroup3("b_key3", "e_key3")}",
        config.iterations,
        config.timeout,
        " No obvious build side full outer equi-join. (Shuffle partitions should be set to 10)",
        shufflePartitions = 10),
      "q13" -> TestQuery("q13",
        s"SELECT " +
          s"${expandKeyGroup3("b_key3")}, " +
          s"${expandColumnWithRange("e_data", 1, 10)}, " +
          s"${expandBDataColumns()} " +
          s"FROM b_data LEFT OUTER JOIN e_data WHERE " +
          s"${expandWhereClauseWithAndKeyGroup3("b_key3", "e_key3")}",
        config.iterations,
        config.timeout,
        "No obvious build side left outer equi-join. (Shuffle partitions should be set to 10)",
        shufflePartitions = 10),
      "q14" -> TestQuery("q14",
        s"SELECT " +
          s"${expandKeyGroup3("b_key3")}, " +
          s"${expandBDataColumns()} " +
          s"FROM " +
          s"b_data LEFT SEMI JOIN e_data WHERE " +
          s"${expandWhereClauseWithAndKeyGroup3("b_key3", "e_key3")}",
        config.iterations,
        config.timeout,
        "both sides large left semi equi-join. (Shuffle partitions should be set to 10)",
        shufflePartitions = 10),
      "q15" -> TestQuery("q15",
        s"SELECT " +
          s"${expandKeyGroup3("b_key3")}, " +
          s"${expandBDataColumns()} " +
          s"FROM " +
          s"b_data LEFT ANTI JOIN e_data WHERE " +
          s"${expandWhereClauseWithAndKeyGroup3("b_key3", "e_key3")}",
        config.iterations,
        config.timeout,
        "Both sides large left anti equi-join. (Shuffle partitions should be set to 10)",
        shufflePartitions = 10),
      "q16" -> TestQuery("q16",
        s"SELECT a_key4_1, " +
          s"${
            expandColumnWithRange("a_data",
              1,
              Math.round(config.complexity / 2.0).toInt)
          }, " +
          s"${
            expandColumnWithRange("f_data",
              1,
              Math.round(config.complexity / 2.0).toInt)
          } " +
          s"FROM a_facts JOIN f_facts " +
          s"WHERE a_key4_1 = f_key4_1 && (a_data_low_unique_1 + f_data_low_unique_1) = 2",
        config.iterations,
        config.timeout,
        "Extreme skew conditional AST inner join."),
      "q17" -> TestQuery("q17",
        s"SELECT a_key4_1, " +
          s"${
            expandColumnWithRange("a_data",
              1,
              Math.round(config.complexity / 2.0).toInt)
          }, " +
          s"${
            expandColumnWithRange("f_data",
              1,
              Math.round(config.complexity / 2.0).toInt)
          } " +
          s"FROM a_facts FULL OUTER JOIN f_facts " +
          s"WHERE a_key4_1 = f_key4_1 && (a_data_low_unique_1 + f_data_low_unique_1) = 2",
        config.iterations,
        config.timeout,
        "Extreme skew conditional AST full outer join."),
      "q18" -> TestQuery("q18",
        s"SELECT a_key4_1, " +
          s"${
            expandColumnWithRange("a_data",
              1,
              Math.round(config.complexity / 2.0).toInt)
          }, " +
          s"${
            expandColumnWithRange("f_data",
              1,
              Math.round(config.complexity / 2.0).toInt)
          } " +
          s"FROM a_facts LEFT OUTER JOIN f_facts " +
          s"WHERE a_key4_1 = f_key4_1 && (a_data_low_unique_1 + f_data_low_unique_1) = 2",
        config.iterations,
        config.timeout,
        "Extreme skew conditional AST left anti join."),
      "q19" -> TestQuery("q19",
        s"SELECT " +
          s"a_key4_1, " +
          s"${expandADataColumns()}, " +
          s"FROM a_fact " +
          s"LEFT ANTI JOIN f_fact WHERE " +
          s"a_key4_1 = f_key4_1 && (a_data_low_unique_1 + f_data_low_unique_1) != 2",
        config.iterations,
        config.timeout,
        "Extreme skew conditional AST left anti join."),
      "q20" -> TestQuery("q20",
        s"SELECT " +
          s"a_key4_1, " +
          s"${expandADataColumns()} " +
          s"FROM a_fact LEFT SEMI JOIN f_fact WHERE " +
          s"a_key4_1 = f_key4_1 && (a_data_low_unique_1 + f_data_low_unique_1) = 2",
        config.iterations,
        config.timeout,
        "Extreme skew conditional AST left semi join."),
      "q21" -> TestQuery("q21",
        s"SELECT " +
          s"a_key4_1, " +
          s"${
            expandColumnWithRange("a_data",
              1,
              Math.round(config.complexity / 2.0).toInt)
          }, " +
          s"${
            expandColumnWithRange("f_data",
              1,
              Math.round(config.complexity / 2.0).toInt)
          } " +
          s"FROM a_fact JOIN f_fact " +
          s"WHERE a_key4_1 = f_key4_1 && " +
          s"(length(concat(a_data_low_unique_len_1, f_data_low_unique_len_1))) = 2",
        config.iterations,
        config.timeout,
        "Extreme skew conditional NON-AST inner join."),
      "q22" -> TestQuery("q22",
        s"SELECT ${expandKeyGroup3("b_key3")}, " +
          s"${MixedSumMinMaxColumnsByComplexity("b_data", config.complexity)} " +
          s"FROM b_data GROUP BY " +
          s"${expandKeyGroup3("b_key3")}",
        config.iterations,
        config.timeout,
        "Group by aggregation, not a lot of combining, but lots of aggregations, " +
          "and CUDF does sort agg internally."),
      "q23" -> TestQuery("q23",
        s"SELECT ${MixedSumMinMaxColumnsByComplexity("b_data", config.complexity)} " +
          s"FROM b_data.",
        config.iterations,
        config.timeout,
        "Reduction with with lots of aggregations"),
      "q24" -> TestQuery("q24",
        s"SELECT ${expandKeyGroup3("g_key3")}, " +
          s"${MixedSumMinMaxColumnsByComplexity("b_data", config.complexity)} " +
          s"FROM g_data " +
          s"GROUP BY ${expandKeyGroup3("g_key3")}",
        config.iterations,
        config.timeout,
        "Group by aggregation with lots of combining, lots of aggs, and CUDF does hash agg" +
          " internally"),
      "q25" -> TestQuery("q25",
        s"select ${expandKeyGroup3("g_key3")}, " +
          s"collect_set(g_data_enum_1) FROM g_data GROUP BY " +
          s"${expandKeyGroup3("g_key3")}",
        config.iterations,
        config.timeout,
        "collect set group by agg"),
      "q26" -> TestQuery("q26",
        s"select b_foreign_a, collect_list(b_data_1) FROM b_data GROUP BY b_foreign_a",
        config.iterations,
        config.timeout,
        "collect list group by agg with some hope of succeeding."),
      "q27" -> TestQuery("q27",
        s"select " +
          s"min(g_data_byte_1) over w, " +
          s"max(g_data_byte_1) over w, " +
          s"sum(g_data_byte_2) over w, " +
          s"count(g_data_byte_3) over w, " +
          s"avg(g_data_byte_4) over w, " +
          s"row_number() over w " +
          s"from g_data " +
          s"WINDOW w AS (PARTITION BY ${expandKeyGroup3("g_key3")} ORDER BY " +
          s"g_data_row_num_1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
        config.iterations,
        config.timeout,
        "Running Window with skewed partition by columns, and single order by column with small " +
          "number of basic window ops (min, max, sum, count, average, row_number)"),
      "q28" -> TestQuery("q28",
        s"select min(g_data_byte_1) over w, " +
          s"max(g_data_byte_1) over w, " +
          s"sum(g_data_byte_2) over w, " +
          s"count(g_data_byte_3) over w, " +
          s"avg(g_data_byte_4) over w " +
          "from g_data " +
          s"WINDOW w AS (PARTITION BY ${expandKeyGroup3("g_key3")} " +
          s"ORDER BY g_data_row_num_1 RANGE BETWEEN 1000 * ${config.scaleFactor}" +
          s" PRECEDING AND 5000 * ${config.scaleFactor} " +
          s"FOLLOWING)",
        config.iterations,
        config.timeout,
        "Ranged Window with large range (lots of rows preceding and following) skewed partition" +
          " by columns and single order by column with small number of basic window ops (min, " +
          "max, sum, count, average)"),
      "q29" -> TestQuery("q29",
        s"select min(g_data_byte_1) over w, " +
          s"max(g_data_byte_1) over w, " +
          s"sum(g_data_byte_2) over w, " +
          s"count(g_data_byte_3) over w, " +
          s"avg(g_data_byte_4) over w " +
          s"from g_data " +
          s"WINDOW w AS (PARTITION BY g_key3_* ORDER BY g_data_row_num_1 ROWS BETWEEN " +
          s"UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
        config.iterations,
        config.timeout,
        "unbounded preceding and following window with skewed partition by columns and single " +
          "order by column with small number of basic window op (min, max, sum, count, average)"),
      "q30" -> TestQuery("q30",
        s"select " +
          s"min(g_data_byte_1) over w, " +
          s"max(g_data_byte_1) over w, " +
          s"sum(g_data_byte_2) over w, " +
          s"count(g_data_byte_3) over w, " +
          s"avg(g_data_byte_4) over w " +
          s"from g_data " +
          s"WINDOW w AS (ORDER BY g_data_row_num_1 ROWS BETWEEN UNBOUNDED PRECEDING AND " +
          s"CURRENT ROW)",
        config.iterations,
        config.timeout,
        " running window with no partition by columns and single order by column with small " +
          "number of basic window ops (min, max, sum, count, average, row_number)"),
      "q31" -> TestQuery("q31",
        s"select min(g_data_byte_1) over w, " +
          s"max(g_data_byte_1) over w, " +
          s"sum(g_data_byte_2) over w, " +
          s"count(g_data_byte_3) over w, " +
          s"avg(g_data_byte_4) over w " +
          s"from g_data " +
          s"WINDOW w AS (ORDER BY g_data_row_num_1 RANGE BETWEEN 1000 * ${config.scaleFactor}" +
          s" PRECEDING AND 5000 * ${config.scaleFactor} FOLLOWING)",
        config.iterations,
        config.timeout,
        " ranged window with large range (lots of rows preceding and following) no partition by " +
          "columns and single order by column with small number of basic window ops (min, max, " +
          "sum, count, average)"),
      "q32" -> TestQuery("q32",
        s"select  min(g_data_byte_1) over w, " +
          s"max(g_data_byte_1) over w, " +
          s"sum(g_data_byte_2) over w, " +
          s"count(g_data_byte_3) over w, " +
          s"avg(g_data_byte_4) over w " +
          s"from g_data " +
          s"WINDOW w AS (ORDER BY g_row_num_1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED " +
          s"FOLLOWING)",
        config.iterations,
        config.timeout,
        "unbounded preceding and following window with no partition by columns and single order" +
          " by column with small number of basic window ops (min, max, sum, count, average)"),
      "q33" -> TestQuery("q33",
        s"select lag(g_data_byte_1, 10 * ${config.scaleFactor}) OVER w, " +
          s"lead(g_data_byte_2, 10 * ${config.scaleFactor}) OVER w " +
          s"from g_data " +
          s"WINDOW w as (PARTITION BY ${expandKeyGroup3("g_key3")} " +
          s"ORDER BY g_data_row_num_1)",
        config.iterations,
        config.timeout,
        "Lead/Lag window with skewed partition by columns and single order by column"),
      "q34" -> TestQuery("q34",
        s"select lag(g_data_1, 10 * ${config.scaleFactor}) over w, " +
          s"lead(g_data_2, 10 * ${config.scaleFactor}) over w " +
          s"from g_data " +
          s"WINDOW w as (ORDER BY g_data_row_num_1)",
        config.iterations,
        config.timeout,
        "Lead/Lag window with no partition by columns and single order by column."),
      "q35" -> TestQuery("q35",
        s"select min(c_data_numeric_1) over w, " +
          s"max(c_data_numeric_2) over w " +
          s"from c_data " +
          s"WINDOW w AS " +
          s"(ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW " +
          s"PARTITION BY ${
            expandColumnWithRange("c_key2",
              1, Math.round(config.complexity / 2.0).toInt)
          } " +
          s"ORDER BY ${
            expandColumnWithRange("c_key2",
              1, Math.round(config.complexity / 2.0).toInt)
          }",
        config.iterations,
        config.timeout,
        "Running window with complexity/2 in partition by columns and complexity/2 in order" +
          " by columns."),
      "q36" -> TestQuery("q36",
        s"select min(c_data_numeric_1) over w, max(c_data_numeric_2) over w from c_data " +
          s"WINDOW w AS " +
          s"(PARTITION BY ${
            expandColumnWithRange("c_key2",
              1, Math.round(config.complexity / 2.0).toInt)
          }" +
          s" ORDER BY ${
            expandColumnWithRange("c_key2",
              Math.round(config.complexity / 2.0).toInt, config.complexity)
          } " +
          s"ROWS BETWEEN UNBOUNDED PRECEDING " +
          s"AND UNBOUNDED FOLLOWING)",
        config.iterations,
        config.timeout,
        "unbounded to unbounded window with complexity/2 in partition by columns and complexity/2" +
          " in order by columns."),
            "q37" -> TestQuery("q37",
              s"select ${WindowMixedSumMinMaxColumnsByComplexity("c_data",
                config.complexity)} " +
                s" from c_data " +
                s"WINDOW w AS (PARTITION BY c_foreign_a ORDER BY c_data_row_num_1 ROWS BETWEEN " +
                s"UNBOUNDED PRECEDING AND CURRENT ROW",
              config.iterations,
              config.timeout,
              "Running window with simple partition by and order by columns, but complexity " +
                "window operations as combinations of a few input columns"),
            "q38" -> TestQuery("q38",
              s"select ${
                WindowMixedSumMinMaxColumnsByComplexity("c_data", config.complexity)} " +
                s"from c_data " +
                s"WINDOW w AS (PARTITION BY c_foreign_a ORDER BY c_data_row_num_1 RANGE BETWEEN " +
                s"10 PRECEDING AND 10 FOLLOWING",
              config.iterations,
              config.timeout,
              "Ranged window with simple partition by and order by columns, but complexity " +
                "window operations as combinations of a few input columns"),
            "q39" -> TestQuery("q39",
              s"select ${
                WindowMixedSumMinMaxColumnsByComplexity("c_data", config.complexity)} " +
                s"from c_data " +
                s"WINDOW w AS (PARTITION BY c_foreign_a ORDER BY c_data_row_num_1 ROWS BETWEEN " +
                s"UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )",
              config.iterations,
              config.timeout,
              "unbounded window with simple partition by and order by columns, but complexity " +
                "window operations as combinations of a few input columns"),
      "q40" -> TestQuery("q40",
        s"select " +
          s"array_sort(collect_set(f_data_low_unique_1)) " +
          s"OVER (PARTITION BY f_key4_1 order by f_data_row_num_1 ROWS BETWEEN UNBOUNDED " +
          s"PRECEDING AND CURRENT ROW)",
        config.iterations,
        config.timeout,
        "COLLECT SET WINDOW (We may never really be able to do this well)"),
      "q41" -> TestQuery("q41",
        s"select " +
          s"collect_list(f_data_low_unique_1) " +
          s"OVER (PARTITION BY f_key4_1 order by f_data_row_num_1 " +
          s"ROWS BETWEEN ${config.complexity} PRECEDING and CURRENT ROW )",
        config.iterations,
        config.timeout,
        "COLLECT LIST WINDOW (We may never really be able to do this well)")
    )
    if (config.queries.isEmpty) {
      allQueries
    } else {
      val selected = allQueries.filter {
        case (k, _) =>
          config.queries.contains(k)
      }
      selected
    }
  }
}
