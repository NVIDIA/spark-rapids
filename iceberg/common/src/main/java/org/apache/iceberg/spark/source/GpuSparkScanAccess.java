/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package org.apache.iceberg.spark.source;

import java.lang.reflect.Field;
import java.util.List;

import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;

/**
 * Package-local access to Iceberg Spark scan classes.
 *
 * <p>Iceberg keeps SparkScan, SparkBatchQueryScan, SparkCopyOnWriteScan, SparkBatch, and
 * SparkInputPartition package-private. Keep direct references to those classes in this helper so
 * callers can use Spark's public scan interfaces.
 */
public final class GpuSparkScanAccess {
  private GpuSparkScanAccess() {
  }

  public static boolean supports(Scan scan) {
    return scan instanceof SparkBatchQueryScan || scan instanceof SparkCopyOnWriteScan;
  }

  public static boolean isBatchQueryScan(Scan scan) {
    return scan instanceof SparkBatchQueryScan;
  }

  public static boolean isCopyOnWriteScan(Scan scan) {
    return scan instanceof SparkCopyOnWriteScan;
  }

  public static boolean isMetadataScan(Scan scan) {
    return sparkScan(scan).table() instanceof BaseMetadataTable;
  }

  public static SparkReadConf readConf(Scan scan) {
    return readField(sparkScan(scan), "readConf", SparkReadConf.class);
  }

  public static Table table(Scan scan) {
    return sparkScan(scan).table();
  }

  public static String branch(Scan scan) {
    return sparkScan(scan).branch();
  }

  public static boolean caseSensitive(Scan scan) {
    return sparkScan(scan).caseSensitive();
  }

  public static Schema expectedSchema(Scan scan) {
    return sparkScan(scan).expectedSchema();
  }

  public static Statistics estimateStatistics(Scan scan) {
    return sparkScan(scan).estimateStatistics();
  }

  public static List<Expression> filterExpressions(Scan scan) {
    return sparkScan(scan).filterExpressions();
  }

  public static Types.StructType groupingKeyType(Scan scan) {
    return sparkScan(scan).groupingKeyType();
  }

  public static List<? extends ScanTaskGroup<?>> taskGroups(Scan scan) {
    return sparkScan(scan).taskGroups();
  }

  public static Batch toBatch(Scan scan) {
    return sparkScan(scan).toBatch();
  }

  @SuppressWarnings("unchecked")
  public static List<Expression> runtimeFilterExpressions(Scan scan) {
    return (List<Expression>) readField(scan, "runtimeFilterExpressions", List.class);
  }

  public static Schema expectedSchema(Batch batch) {
    return readField(batch, "expectedSchema", Schema.class);
  }

  public static Table table(InputPartition partition) {
    return sparkInputPartition(partition).table();
  }

  public static boolean isCaseSensitive(InputPartition partition) {
    return sparkInputPartition(partition).isCaseSensitive();
  }

  public static ScanTaskGroup<ScanTask> taskGroup(InputPartition partition) {
    return sparkInputPartition(partition).taskGroup();
  }

  private static SparkScan sparkScan(Scan scan) {
    return (SparkScan) scan;
  }

  private static SparkInputPartition sparkInputPartition(InputPartition partition) {
    return (SparkInputPartition) partition;
  }

  private static <T> T readField(Object target, String fieldName, Class<T> fieldType) {
    try {
      Field field = findField(target.getClass(), fieldName);
      field.setAccessible(true);
      return fieldType.cast(field.get(target));
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(
          "Unable to read " + fieldName + " from " + target.getClass().getName(), e);
    }
  }

  private static Field findField(Class<?> targetClass, String fieldName) {
    Class<?> current = targetClass;
    while (current != null) {
      try {
        return current.getDeclaredField(fieldName);
      } catch (NoSuchFieldException e) {
        current = current.getSuperclass();
      }
    }
    throw new IllegalStateException("No field " + fieldName + " in " + targetClass.getName());
  }
}
