package org.apache.spark.sql.hive.execution

import java.util.Locale

import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.{QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, DataSource, FileFormat, FileIndex, HadoopFsRelation, InMemoryFileIndex, LogicalRelation}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.internal.SQLConf.HiveCaseSensitiveInferenceMode.{INFER_AND_SAVE, NEVER_INFER}
import org.apache.spark.sql.types.StructType

class HiveMetastoreCatalogUtils(sparkSession: SparkSession) extends Logging {
  // these are def_s and not val/lazy val since the latter would introduce circular references
  private def sessionState = sparkSession.sessionState
  private def catalogProxy = sparkSession.sessionState.catalog
  import org.apache.spark.sql.hive.HiveMetastoreCatalog._

  /** These locks guard against multiple attempts to instantiate a table, which wastes memory. */
//  private val tableCreationLocks = Striped.lazyWeakLock(100)

  /** Acquires a lock on the table cache for the duration of `f`. */
  private def withTableCreationLock[A](tableName: QualifiedTableName, f: => A): A = {
//    val lock = tableCreationLocks.get(tableName)
//    lock.lock()
    try f finally {
//      lock.unlock()
    }
  }

  private def getCached(
    tableIdentifier: QualifiedTableName,
    pathsInMetastore: Seq[Path],
    schemaInMetastore: StructType,
    expectedFileFormat: Class[_ <: FileFormat],
    partitionSchema: Option[StructType]): Option[LogicalRelation] = {

    catalogProxy.getCachedTable(tableIdentifier) match {
      case null => None // Cache miss
      case logical @ LogicalRelation(relation: HadoopFsRelation, _, _, _) =>
        val cachedRelationFileFormatClass = relation.fileFormat.getClass

        expectedFileFormat match {
          case `cachedRelationFileFormatClass` =>
            // If we have the same paths, same schema, and same partition spec,
            // we will use the cached relation.
            val useCached =
              relation.location.rootPaths.toSet == pathsInMetastore.toSet &&
                logical.schema.sameType(schemaInMetastore) &&
                // We don't support hive bucketed tables. This function `getCached` is only used for
                // converting supported Hive tables to data source tables.
                relation.bucketSpec.isEmpty &&
                relation.partitionSchema == partitionSchema.getOrElse(StructType(Nil))

            if (useCached) {
              Some(logical)
            } else {
              // If the cached relation is not updated, we invalidate it right away.
              catalogProxy.invalidateCachedTable(tableIdentifier)
              None
            }
          case _ =>
            logWarning(s"Table $tableIdentifier should be stored as $expectedFileFormat. " +
              s"However, we are getting a ${relation.fileFormat} from the metastore cache. " +
              "This cached entry will be invalidated.")
            catalogProxy.invalidateCachedTable(tableIdentifier)
            None
        }
      case other =>
        logWarning(s"Table $tableIdentifier should be stored as $expectedFileFormat. " +
          s"However, we are getting a $other from the metastore cache. " +
          "This cached entry will be invalidated.")
        catalogProxy.invalidateCachedTable(tableIdentifier)
        None
    }
  }

  def convert(relation: HiveTableRelation): LogicalRelation = {
    val serde = relation.tableMeta.storage.serde.getOrElse("").toLowerCase(Locale.ROOT)

    // Consider table and storage properties. For properties existing in both sides, storage
    // properties will supersede table properties.
    val options = relation.tableMeta.storage.properties
    convertToLogicalRelation(relation, options, classOf[CSVFileFormat], "csv")
  }

  private def convertToLogicalRelation(
    relation: HiveTableRelation,
    options: Map[String, String],
    fileFormatClass: Class[_ <: FileFormat],
    fileType: String): LogicalRelation = {
    val metastoreSchema = relation.tableMeta.schema
    val tableIdentifier =
      QualifiedTableName(relation.tableMeta.database, relation.tableMeta.identifier.table)

    val lazyPruningEnabled = sparkSession.sqlContext.conf.manageFilesourcePartitions
    val tablePath = new Path(relation.tableMeta.location)
    val fileFormat = fileFormatClass.getConstructor().newInstance()

    val result = if (relation.isPartitioned) {
      val partitionSchema = relation.tableMeta.partitionSchema
      val rootPaths: Seq[Path] = if (lazyPruningEnabled) {
        Seq(tablePath)
      } else {
        // By convention (for example, see CatalogFileIndex), the definition of a
        // partitioned table's paths depends on whether that table has any actual partitions.
        // Partitioned tables without partitions use the location of the table's base path.
        // Partitioned tables with partitions use the locations of those partitions' data
        // locations,_omitting_ the table's base path.
        val paths = sparkSession.sharedState.externalCatalog
          .listPartitions(tableIdentifier.database, tableIdentifier.name)
          .map(p => new Path(p.storage.locationUri.get))

        if (paths.isEmpty) {
          Seq(tablePath)
        } else {
          paths
        }
      }

      withTableCreationLock(tableIdentifier, {
        val cached = getCached(
          tableIdentifier,
          rootPaths,
          metastoreSchema,
          fileFormatClass,
          Some(partitionSchema))

        val logicalRelation = cached.getOrElse {
          val sizeInBytes = relation.stats.sizeInBytes.toLong
          val fileIndex = {
            val index = new CatalogFileIndex(sparkSession, relation.tableMeta, sizeInBytes)
            if (lazyPruningEnabled) {
              index
            } else {
              index.filterPartitions(Nil)  // materialize all the partitions in memory
            }
          }

          val updatedTable = inferIfNeeded(relation, options, fileFormat, Option(fileIndex))

          // Spark SQL's data source table now support static and dynamic partition insert. Source
          // table converted from Hive table should always use dynamic.
          val enableDynamicPartition = options.updated("partitionOverwriteMode", "dynamic")
          val fsRelation = HadoopFsRelation(
            location = fileIndex,
            partitionSchema = partitionSchema,
            dataSchema = updatedTable.dataSchema,
            bucketSpec = None,
            fileFormat = fileFormat,
            options = enableDynamicPartition)(sparkSession = sparkSession)
          val created = LogicalRelation(fsRelation, updatedTable)
          catalogProxy.cacheTable(tableIdentifier, created)
          created
        }

        logicalRelation
      })
    } else {
      val rootPath = tablePath
      withTableCreationLock(tableIdentifier, {
        val cached = getCached(
          tableIdentifier,
          Seq(rootPath),
          metastoreSchema,
          fileFormatClass,
          None)
        val logicalRelation = cached.getOrElse {
          val updatedTable = inferIfNeeded(relation, options, fileFormat)
          val created =
            LogicalRelation(
              DataSource(
                sparkSession = sparkSession,
                paths = rootPath.toString :: Nil,
                userSpecifiedSchema = Option(updatedTable.dataSchema),
                bucketSpec = None,
                options = options,
                className = fileType).resolveRelation(),
              table = updatedTable)

          catalogProxy.cacheTable(tableIdentifier, created)
          created
        }

        logicalRelation
      })
    }
    // The inferred schema may have different field names as the table schema, we should respect
    // it, but also respect the exprId in table relation output.
    if (result.output.length != relation.output.length) {
      throw new AnalysisException(
        s"Converted table has ${result.output.length} columns, " +
          s"but source Hive table has ${relation.output.length} columns. " +
          s"Set ${HiveUtils.CONVERT_METASTORE_PARQUET.key} to false, " +
          s"or recreate table ${relation.tableMeta.identifier} to workaround.")
    }
    if (!result.output.zip(relation.output).forall {
      case (a1, a2) => a1.dataType == a2.dataType }) {
      throw new AnalysisException(
        s"Column in converted table has different data type with source Hive table's. " +
          s"Set ${HiveUtils.CONVERT_METASTORE_PARQUET.key} to false, " +
          s"or recreate table ${relation.tableMeta.identifier} to workaround.")
    }
    val newOutput = result.output.zip(relation.output).map {
      case (a1, a2) => a1.withExprId(a2.exprId)
    }
    result.copy(output = newOutput)
  }

  private def inferIfNeeded(
    relation: HiveTableRelation,
    options: Map[String, String],
    fileFormat: FileFormat,
    fileIndexOpt: Option[FileIndex] = None): CatalogTable = {
    val inferenceMode = sparkSession.sessionState.conf.caseSensitiveInferenceMode
    val shouldInfer = (inferenceMode != NEVER_INFER) && !relation.tableMeta.schemaPreservesCase
    val tableName = relation.tableMeta.identifier.unquotedString
    if (shouldInfer) {
      logInfo(s"Inferring case-sensitive schema for table $tableName (inference mode: " +
        s"$inferenceMode)")
      val fileIndex = fileIndexOpt.getOrElse {
        val rootPath = new Path(relation.tableMeta.location)
        new InMemoryFileIndex(sparkSession, Seq(rootPath), options, None)
      }

      val inferredSchema = fileFormat
        .inferSchema(
          sparkSession,
          options,
          fileIndex.listFiles(Nil, Nil).flatMap(_.files))
        .map(mergeWithMetastoreSchema(relation.tableMeta.dataSchema, _))

      inferredSchema match {
        case Some(dataSchema) =>
          if (inferenceMode == INFER_AND_SAVE) {
            updateDataSchema(relation.tableMeta.identifier, dataSchema)
          }
          val newSchema = StructType(dataSchema ++ relation.tableMeta.partitionSchema)
          relation.tableMeta.copy(schema = newSchema)
        case None =>
          logWarning(s"Unable to infer schema for table $tableName from file format " +
            s"$fileFormat (inference mode: $inferenceMode). Using metastore schema.")
          relation.tableMeta
      }
    } else {
      relation.tableMeta
    }
  }

  private def updateDataSchema(identifier: TableIdentifier, newDataSchema: StructType): Unit = try {
    logInfo(s"Saving case-sensitive schema for table ${identifier.unquotedString}")
    sparkSession.sessionState.catalog.alterTableDataSchema(identifier, newDataSchema)
  } catch {
    case NonFatal(ex) =>
      logWarning(s"Unable to save case-sensitive schema for table ${identifier.unquotedString}", ex)
  }
}
