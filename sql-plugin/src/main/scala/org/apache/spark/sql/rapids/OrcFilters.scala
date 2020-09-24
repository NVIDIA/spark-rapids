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
package org.apache.spark.sql.rapids

import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.ql.io.sarg.{PredicateLeaf, SearchArgument}
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory.newBuilder
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable

import org.apache.spark.SparkException
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.quoteIfNeeded
import org.apache.spark.sql.execution.datasources.orc.OrcFiltersBase
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._

// This is derived from Apache Spark's OrcFilters code to avoid calling the
// Spark version.  Spark's version can potentially create a search argument
// applier object that is incompatible with the orc:nohive jar that has been
// shaded as part of this project.
//
// The use of Spark's OrcFiltersBase is safe since it is explicitly designed
// to be reusable across different ORC jar classifiers.
object OrcFilters extends OrcFiltersBase {

  /**
   * Create ORC filter as a SearchArgument instance.
   *
   * NOTE: These filters should be pre-filtered by Spark to only contain the
   *       filters convertible to ORC, so checking what is convertible is
   *       not necessary here.
   */
  def createFilter(schema: StructType, filters: Seq[Filter]): Option[SearchArgument] = {
    val dataTypeMap = schema.map(f => f.name -> f.dataType).toMap
    // Combines all filters using `And` to produce a single conjunction
    val conjunctionOptional = buildTree(filters)
    conjunctionOptional.map { conjunction =>
      // Then tries to build a single ORC `SearchArgument` for the conjunction predicate.
      // The input predicate is fully convertible. There should not be any empty result in the
      // following recursive method call `buildSearchArgument`.
      buildSearchArgument(dataTypeMap, conjunction, newBuilder).build()
    }
  }

  /**
   * Get PredicateLeafType which is corresponding to the given DataType.
   */
  private def getPredicateLeafType(dataType: DataType) = dataType match {
    case BooleanType => PredicateLeaf.Type.BOOLEAN
    case ByteType | ShortType | IntegerType | LongType => PredicateLeaf.Type.LONG
    case FloatType | DoubleType => PredicateLeaf.Type.FLOAT
    case StringType => PredicateLeaf.Type.STRING
    case DateType => PredicateLeaf.Type.DATE
    case TimestampType => PredicateLeaf.Type.TIMESTAMP
    case _: DecimalType => PredicateLeaf.Type.DECIMAL
    case _ => throw new UnsupportedOperationException(s"DataType: ${dataType.catalogString}")
  }

  /**
   * Cast literal values for filters.
   *
   * We need to cast to long because ORC raises exceptions
   * at 'checkLiteralType' of SearchArgumentImpl.java.
   */
  private def castLiteralValue(value: Any, dataType: DataType): Any = dataType match {
    case ByteType | ShortType | IntegerType | LongType =>
      value.asInstanceOf[Number].longValue
    case FloatType | DoubleType =>
      value.asInstanceOf[Number].doubleValue()
    case _: DecimalType =>
      new HiveDecimalWritable(HiveDecimal.create(value.asInstanceOf[java.math.BigDecimal]))
    case _ => value
  }

  /**
   * Build a SearchArgument and return the builder so far.
   *
   * @param dataTypeMap a map from the attribute name to its data type.
   * @param expression the input predicates, which should be fully convertible to SearchArgument.
   * @param builder the input SearchArgument.Builder.
   * @return the builder so far.
   */
  private def buildSearchArgument(
      dataTypeMap: Map[String, DataType],
      expression: Filter,
      builder: Builder): Builder = {
    import org.apache.spark.sql.sources._

    expression match {
      case And(left, right) =>
        val lhs = buildSearchArgument(dataTypeMap, left, builder.startAnd())
        val rhs = buildSearchArgument(dataTypeMap, right, lhs)
        rhs.end()

      case Or(left, right) =>
        val lhs = buildSearchArgument(dataTypeMap, left, builder.startOr())
        val rhs = buildSearchArgument(dataTypeMap, right, lhs)
        rhs.end()

      case Not(child) =>
        buildSearchArgument(dataTypeMap, child, builder.startNot()).end()

      case other =>
        buildLeafSearchArgument(dataTypeMap, other, builder).getOrElse {
          throw new SparkException(
            "The input filter of OrcFilters.buildSearchArgument should be fully convertible.")
        }
    }
  }

  /**
   * Return true if this is a searchable type in ORC.
   * Both CharType and VarcharType are cleaned at AstBuilder.
   *
   * Copied from Spark because scope got changed.
   */
  private def isSearchableTypeLocal(dataType: DataType) = dataType match {
    case BinaryType => false
    case _: AtomicType => true
    case _ => false
  }

  /**
   * Build a SearchArgument for a leaf predicate and return the builder so far.
   *
   * @param dataTypeMap a map from the attribute name to its data type.
   * @param expression the input filter predicates.
   * @param builder the input SearchArgument.Builder.
   * @return the builder so far.
   */
  private def buildLeafSearchArgument(
      dataTypeMap: Map[String, DataType],
      expression: Filter,
      builder: Builder): Option[Builder] = {
    def getType(attribute: String): PredicateLeaf.Type =
      getPredicateLeafType(dataTypeMap(attribute))

    import org.apache.spark.sql.sources._

    // NOTE: For all case branches dealing with leaf predicates below, the additional `startAnd()`
    // call is mandatory. ORC `SearchArgument` builder requires that all leaf predicates must be
    // wrapped by a "parent" predicate (`And`, `Or`, or `Not`).
    // Since ORC 1.5.0 (ORC-323), we need to quote for column names with `.` characters
    // in order to distinguish predicate pushdown for nested columns.
    expression match {
      case EqualTo(attribute, value) if isSearchableTypeLocal(dataTypeMap(attribute)) =>
        val quotedName = quoteIfNeeded(attribute)
        val castedValue = castLiteralValue(value, dataTypeMap(attribute))
        Some(builder.startAnd().equals(quotedName, getType(attribute), castedValue).end())

      case EqualNullSafe(attribute, value) if isSearchableTypeLocal(dataTypeMap(attribute)) =>
        val quotedName = quoteIfNeeded(attribute)
        val castedValue = castLiteralValue(value, dataTypeMap(attribute))
        Some(builder.startAnd().nullSafeEquals(quotedName, getType(attribute), castedValue).end())

      case LessThan(attribute, value) if isSearchableTypeLocal(dataTypeMap(attribute)) =>
        val quotedName = quoteIfNeeded(attribute)
        val castedValue = castLiteralValue(value, dataTypeMap(attribute))
        Some(builder.startAnd().lessThan(quotedName, getType(attribute), castedValue).end())

      case LessThanOrEqual(attribute, value) if isSearchableTypeLocal(dataTypeMap(attribute)) =>
        val quotedName = quoteIfNeeded(attribute)
        val castedValue = castLiteralValue(value, dataTypeMap(attribute))
        Some(builder.startAnd().lessThanEquals(quotedName, getType(attribute), castedValue).end())

      case GreaterThan(attribute, value) if isSearchableTypeLocal(dataTypeMap(attribute)) =>
        val quotedName = quoteIfNeeded(attribute)
        val castedValue = castLiteralValue(value, dataTypeMap(attribute))
        Some(builder.startNot().lessThanEquals(quotedName, getType(attribute), castedValue).end())

      case GreaterThanOrEqual(attribute, value) if isSearchableTypeLocal(dataTypeMap(attribute)) =>
        val quotedName = quoteIfNeeded(attribute)
        val castedValue = castLiteralValue(value, dataTypeMap(attribute))
        Some(builder.startNot().lessThan(quotedName, getType(attribute), castedValue).end())

      case IsNull(attribute) if isSearchableTypeLocal(dataTypeMap(attribute)) =>
        val quotedName = quoteIfNeeded(attribute)
        Some(builder.startAnd().isNull(quotedName, getType(attribute)).end())

      case IsNotNull(attribute) if isSearchableTypeLocal(dataTypeMap(attribute)) =>
        val quotedName = quoteIfNeeded(attribute)
        Some(builder.startNot().isNull(quotedName, getType(attribute)).end())

      case In(attribute, values) if isSearchableTypeLocal(dataTypeMap(attribute)) =>
        val quotedName = quoteIfNeeded(attribute)
        val castedValues = values.map(v => castLiteralValue(v, dataTypeMap(attribute)))
        Some(builder.startAnd().in(quotedName, getType(attribute),
          castedValues.map(_.asInstanceOf[AnyRef]): _*).end())

      case _ => None
    }
  }
}
