package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.types.StructType

object DbPartitioningUtils {

  def inferPartitioning(
    sparkSession: SparkSession,
    leafDirs: Seq[Path],
    basePaths: Set[Path],
    parameters: Map[String, String],
    userSpecifiedSchema: Option[StructType]): PartitionSpec = {

    val caseInsensitiveOptions = CaseInsensitiveMap(parameters)
    val timeZoneId = caseInsensitiveOptions.get(DateTimeUtils.TIMEZONE_OPTION)
      .getOrElse(sparkSession.sessionState.conf.sessionLocalTimeZone)

    PartitioningUtils.parsePartitions(
      leafDirs,
      typeInference = sparkSession.sessionState.conf.partitionColumnTypeInferenceEnabled,
      basePaths = basePaths,
      userSpecifiedSchema = userSpecifiedSchema,
      caseSensitive = sparkSession.sqlContext.conf.caseSensitiveAnalysis,
      validatePartitionColumns = sparkSession.sqlContext.conf.validatePartitionColumns,
      timeZoneId = timeZoneId)
  }
}
