/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.iceberg.spark;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.io.BaseEncoding;
import org.apache.iceberg.transforms.PartitionSpecVisitor;
import org.apache.iceberg.util.ByteBuffers;

import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;

/** Derived from Apache Iceberg's Spark3Util class. */
public class Spark3Util {

  private Spark3Util() {
  }

  public static NamedReference toNamedReference(String name) {
    return Expressions.column(name);
  }

  public static String describe(List<org.apache.iceberg.expressions.Expression> exprs) {
    return exprs.stream().map(Spark3Util::describe).collect(Collectors.joining(", "));
  }

  public static String describe(org.apache.iceberg.expressions.Expression expr) {
    return ExpressionVisitors.visit(expr, DescribeExpressionVisitor.INSTANCE);
  }

  public static Transform[] toTransforms(Schema schema, List<PartitionField> fields) {
    SpecTransformToSparkTransform visitor = new SpecTransformToSparkTransform(schema);

    List<Transform> transforms = Lists.newArrayList();

    for (PartitionField field : fields) {
      Transform transform = PartitionSpecVisitor.visit(schema, field, visitor);
      if (transform != null) {
        transforms.add(transform);
      }
    }

    return transforms.toArray(new Transform[0]);
  }

  private static class DescribeExpressionVisitor extends ExpressionVisitors.ExpressionVisitor<String> {
    private static final DescribeExpressionVisitor INSTANCE = new DescribeExpressionVisitor();

    private DescribeExpressionVisitor() {
    }

    @Override
    public String alwaysTrue() {
      return "true";
    }

    @Override
    public String alwaysFalse() {
      return "false";
    }

    @Override
    public String not(String result) {
      return "NOT (" + result + ")";
    }

    @Override
    public String and(String leftResult, String rightResult) {
      return "(" + leftResult + " AND " + rightResult + ")";
    }

    @Override
    public String or(String leftResult, String rightResult) {
      return "(" + leftResult + " OR " + rightResult + ")";
    }

    @Override
    public <T> String predicate(BoundPredicate<T> pred) {
      throw new UnsupportedOperationException("Cannot convert bound predicates to SQL");
    }

    @Override
    public <T> String predicate(UnboundPredicate<T> pred) {
      switch (pred.op()) {
        case IS_NULL:
          return pred.ref().name() + " IS NULL";
        case NOT_NULL:
          return pred.ref().name() + " IS NOT NULL";
        case IS_NAN:
          return "is_nan(" + pred.ref().name() + ")";
        case NOT_NAN:
          return "not_nan(" + pred.ref().name() + ")";
        case LT:
          return pred.ref().name() + " < " + sqlString(pred.literal());
        case LT_EQ:
          return pred.ref().name() + " <= " + sqlString(pred.literal());
        case GT:
          return pred.ref().name() + " > " + sqlString(pred.literal());
        case GT_EQ:
          return pred.ref().name() + " >= " + sqlString(pred.literal());
        case EQ:
          return pred.ref().name() + " = " + sqlString(pred.literal());
        case NOT_EQ:
          return pred.ref().name() + " != " + sqlString(pred.literal());
        case STARTS_WITH:
          return pred.ref().name() + " LIKE '" + pred.literal() + "%'";
        case NOT_STARTS_WITH:
          return pred.ref().name() + " NOT LIKE '" + pred.literal() + "%'";
        case IN:
          return pred.ref().name() + " IN (" + sqlString(pred.literals()) + ")";
        case NOT_IN:
          return pred.ref().name() + " NOT IN (" + sqlString(pred.literals()) + ")";
        default:
          throw new UnsupportedOperationException("Cannot convert predicate to SQL: " + pred);
      }
    }

    private static <T> String sqlString(List<Literal<T>> literals) {
      return literals.stream().map(DescribeExpressionVisitor::sqlString).collect(Collectors.joining(", "));
    }

    private static String sqlString(org.apache.iceberg.expressions.Literal<?> lit) {
      if (lit.value() instanceof String) {
        return "'" + lit.value() + "'";
      } else if (lit.value() instanceof ByteBuffer) {
        byte[] bytes = ByteBuffers.toByteArray((ByteBuffer) lit.value());
        return "X'" + BaseEncoding.base16().encode(bytes) + "'";
      } else {
        return lit.value().toString();
      }
    }
  }

  private static class SpecTransformToSparkTransform implements PartitionSpecVisitor<Transform> {
    private final Map<Integer, String> quotedNameById;

    SpecTransformToSparkTransform(Schema schema) {
      this.quotedNameById = SparkSchemaUtil.indexQuotedNameById(schema);
    }

    @Override
    public Transform identity(String sourceName, int sourceId) {
      return Expressions.identity(quotedName(sourceId));
    }

    @Override
    public Transform bucket(String sourceName, int sourceId, int numBuckets) {
      return Expressions.bucket(numBuckets, quotedName(sourceId));
    }

    @Override
    public Transform truncate(String sourceName, int sourceId, int width) {
      NamedReference column = Expressions.column(quotedName(sourceId));
      return Expressions.apply("truncate", Expressions.literal(width), column);
    }

    @Override
    public Transform year(String sourceName, int sourceId) {
      return Expressions.years(quotedName(sourceId));
    }

    @Override
    public Transform month(String sourceName, int sourceId) {
      return Expressions.months(quotedName(sourceId));
    }

    @Override
    public Transform day(String sourceName, int sourceId) {
      return Expressions.days(quotedName(sourceId));
    }

    @Override
    public Transform hour(String sourceName, int sourceId) {
      return Expressions.hours(quotedName(sourceId));
    }

    @Override
    public Transform alwaysNull(int fieldId, String sourceName, int sourceId) {
      // do nothing for alwaysNull, it doesn't need to be converted to a transform
      return null;
    }

    @Override
    public Transform unknown(int fieldId, String sourceName, int sourceId, String transform) {
      return Expressions.apply(transform, Expressions.column(quotedName(sourceId)));
    }

    private String quotedName(int id) {
      return quotedNameById.get(id);
    }
  }
}
