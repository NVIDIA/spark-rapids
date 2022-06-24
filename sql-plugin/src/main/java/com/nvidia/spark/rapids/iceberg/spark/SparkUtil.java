/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

public class SparkUtil {

  public static final String TIMESTAMP_WITHOUT_TIMEZONE_ERROR = String.format("Cannot handle timestamp without" +
      " timezone fields in Spark. Spark does not natively support this type but if you would like to handle all" +
      " timestamps as timestamp with timezone set '%s' to true. This will not change the underlying values stored" +
      " but will change their displayed values in Spark. For more information please see" +
      " https://docs.databricks.com/spark/latest/dataframes-datasets/dates-timestamps.html#ansi-sql-and" +
      "-spark-sql-timestamps", SparkSQLProperties.HANDLE_TIMESTAMP_WITHOUT_TIMEZONE);

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
}
