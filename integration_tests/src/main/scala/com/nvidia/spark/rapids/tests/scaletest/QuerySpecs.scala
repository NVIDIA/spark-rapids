package com.nvidia.spark.rapids.tests.scaletest

import scala.io.Source

import com.nvidia.spark.rapids.tests.scaletest.ScaleTest.Config

import org.apache.spark.sql.SparkSession

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
   *  Read data and initialize temp views in Spark
   */
  def initViews(): Unit = {

    for (table <- tables) {
      spark.read.format(config.format).load(s"$baseInputPath/$table")
        .createOrReplaceTempView(table)
    }
  }

  /**
   * Parse one query template to get the real query string that Spark SQL accepts
   * @param queryTemplate template query
   * @return Spark SQL compatible query
   */
  private def parseQueryTemplate(queryTemplate: String): String = {
    // pattern for example: "..., b_data_{1-10}, ..."
    val placeholderRegex = "([a-zA-Z0-9_]+)\\{([0-9]+)-([0-9]+)\\}".r
    // Function to replace placeholders with actual values
    def replacePlaceholder(matched: scala.util.matching.Regex.Match): String = {
      val prefix = matched.group(1)
      val start = matched.group(2).toInt
      val end = matched.group(3).toInt
      (start to end).map(i => s"${prefix}$i").mkString(", ")
    }
    // Replace placeholders in the template string
    placeholderRegex.replaceAllIn(queryTemplate, replacePlaceholder _)
  }

  /**
   * Read the query template file extract the template queries inside, Produce the real queries
   * @param queryFilePath
   * @return
   */
  def processQueryFile(queryFilePath: String): Map[String, String] = {
    var queryMap: Map[String, String] = Map()
    val source = Source.fromFile(queryFilePath)
    for (line <- source.getLines()) {
      val parts = line.split(":")
      if (parts.length == 2) {
        val queryName = parts(0).trim
        // TODO: error when execution time > timeout
//        val timeout = parts(1).trim
        val queryContent = parts(1).trim
        // Add the query to the map
        queryMap += (queryName -> parseQueryTemplate(queryContent))
      }
      else throw new IllegalArgumentException(s"Invalid query line format: $line. Expected " +
        s"'queryName: queryContent' format")
    }
    source.close()
    queryMap
  }
}
