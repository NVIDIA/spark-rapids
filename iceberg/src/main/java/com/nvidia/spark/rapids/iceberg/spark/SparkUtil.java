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

import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkEnv;
import org.apache.spark.scheduler.ExecutorCacheTaskLocation;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.storage.BlockManagerMaster;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.List;
import java.util.stream.Collectors;

/** Derived from Apache Iceberg's SparkUtil class. */
public class SparkUtil {

  public static final String TIMESTAMP_WITHOUT_TIMEZONE_ERROR = String.format("Cannot handle timestamp without" +
      " timezone fields in Spark. Spark does not natively support this type but if you would like to handle all" +
      " timestamps as timestamp with timezone set '%s' to true. This will not change the underlying values stored" +
      " but will change their displayed values in Spark. For more information please see" +
      " https://docs.databricks.com/spark/latest/dataframes-datasets/dates-timestamps.html#ansi-sql-and" +
      "-spark-sql-timestamps", SparkSQLProperties.HANDLE_TIMESTAMP_WITHOUT_TIMEZONE);

  private static final Joiner DOT = Joiner.on(".");

  private SparkUtil() {
  }

  /**
   * Responsible for checking if the table schema has a timestamp without timezone column
   * @param schema table schema to check if it contains a timestamp without timezone column
   * @return boolean indicating if the schema passed in has a timestamp field without a timezone
   */
  public static boolean hasTimestampWithoutZone(Schema schema) {
    return TypeUtil.find(schema, t -> Types.TimestampType.withoutZone().equals(t)) != null;
  }

  public static List<String> executorLocations() {
    BlockManager driverBlockManager = SparkEnv.get().blockManager();
    List<BlockManagerId> executorBlockManagerIds = fetchPeers(driverBlockManager);
    return executorBlockManagerIds.stream()
        .map(SparkUtil::toExecutorLocation)
        .sorted()
        .collect(Collectors.toList());
  }

  private static List<BlockManagerId> fetchPeers(BlockManager blockManager) {
    BlockManagerMaster master = blockManager.master();
    BlockManagerId id = blockManager.blockManagerId();
    return toJavaList(master.getPeers(id));
  }

  private static <T> List<T> toJavaList(Seq<T> seq) {
    return JavaConverters.seqAsJavaListConverter(seq).asJava();
  }

  private static String toExecutorLocation(BlockManagerId id) {
    return ExecutorCacheTaskLocation.apply(id.host(), id.executorId()).toString();
  }

  public static String toColumnName(NamedReference ref) {
    return DOT.join(ref.fieldNames());
  }
}
