# Copyright (c) 2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import tempfile
import logging
from itertools import combinations
from types import MappingProxyType
from typing import Callable, List, Dict, Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, DoubleType, BinaryType

from data_gen import byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen, string_gen, \
    boolean_gen, DataGen, gen_df, date_gen, timestamp_gen, binary_gen, decimal_gen_32bit, \
    decimal_gen_64bit, decimal_gen_128bit
from spark_session import with_cpu_session

# iceberg supported types
iceberg_table_gen = MappingProxyType({
    '_c0': byte_gen, '_c1': short_gen, '_c2': int_gen, '_c3': long_gen, '_c4': float_gen,
    '_c5': double_gen, '_c6': string_gen, '_c7': boolean_gen, '_c8': date_gen, '_c9': timestamp_gen,
    '_c10': decimal_gen_32bit, '_c11': decimal_gen_64bit, '_c12': decimal_gen_128bit,
    '_c13': binary_gen
})
iceberg_base_table_cols = list(iceberg_table_gen.keys())
iceberg_gens_list = [iceberg_table_gen[col] for col in iceberg_base_table_cols]
rapids_reader_types = ['PERFILE', 'MULTITHREADED', 'COALESCING']


def can_be_eq_delete_col(data_gen: DataGen) -> bool:
    return (not isinstance(data_gen.data_type, FloatType) and
            not isinstance(data_gen.data_type, DoubleType) and
            # See https://github.com/NVIDIA/spark-rapids/issues/12469, iceberg spec doesn't prevent
            # binary type as eq delete column, but it seems there are bugs in iceberg equality
            # loader, we should remove this after the bug is fixed.
            not isinstance(data_gen.data_type, BinaryType))

def _eq_column_combinations(all_columns: List[str],
                           all_types: List[DataGen],
                           n: int) -> List[List[str]]:
    # In primitive types, float, double can't be used in eq deletes
    cols = [col for (col, data_gen) in list(zip(all_columns, all_types))
            if can_be_eq_delete_col(data_gen)]
    return list(combinations(cols, n))

all_eq_column_combinations = _eq_column_combinations(iceberg_base_table_cols, iceberg_gens_list, 3)

def setup_base_iceberg_table(spark_tmp_table_factory,
                             seed: Optional[int] = None,
                             table_prop: Optional[Dict[str, str]] = None) -> str:

    gen_list = list(zip(iceberg_base_table_cols, iceberg_gens_list))
    table_name = get_full_table_name(spark_tmp_table_factory)
    tmp_view_name = spark_tmp_table_factory.get()

    if table_prop is None:
        table_prop = {'format-version':'2', 'write.delete.mode': 'merge-on-read'}
    else:
        table_prop = {**table_prop, 'format-version': '2', 'write.delete.mode': 'merge-on-read'}

    table_prop_sql = ", ".join([f"'{k}' = '{v}'" for k, v in table_prop.items()])

    def set_iceberg_table(spark: SparkSession):
        df = gen_df(spark, gen_list, seed=seed)
        df.createOrReplaceTempView(tmp_view_name)
        spark.sql(f"CREATE TABLE {table_name} USING ICEBERG "
                  f"TBLPROPERTIES ({table_prop_sql}) "
                  f"PARTITIONED BY (bucket(16, _c2), bucket(16, _c3)) "
                  f"AS SELECT * FROM {tmp_view_name}")

    with_cpu_session(set_iceberg_table)
    return table_name


def _add_eq_deletes(spark: SparkSession, eq_delete_cols: List[str], row_count: int, table_name: str,
                    spark_tmp_path):
    for eq_delete_col in eq_delete_cols:
        assert can_be_eq_delete_col(iceberg_table_gen[eq_delete_col]), \
            f"{eq_delete_col} can't be used as eq delete column"


    spark_warehouse_dir = spark.conf.get("spark.sql.catalog.spark_catalog.warehouse")

    temp_dir = tempfile.mkdtemp(dir=spark_tmp_path)
    deletes = (spark.table(table_name)
               .select(eq_delete_cols + ["_partition"])
               .distinct()
               .orderBy(eq_delete_cols)
               .limit(row_count)
               .repartition(1))
    deletes.write.parquet(temp_dir, mode='overwrite')
    parquet_files = [f for f in os.listdir(temp_dir) if f.endswith(".parquet")]
    assert len(parquet_files) == 1, "Only one delete parquet file should be created"
    delete_parquet_file_path = os.path.join(temp_dir, parquet_files[0])
    spark.sql(f"select iceberg_add_eq_deletes('{spark_warehouse_dir}', '{table_name}', "
              f"'{delete_parquet_file_path}')").collect()
    spark.sql(f"REFRESH TABLE {table_name}")


def _change_table(table_name, table_func: Callable[[SparkSession], None], message: str):
    def change_table(spark: SparkSession):
        before_count = spark.table(table_name).count()
        table_func(spark)
        spark.sql(f"REFRESH TABLE {table_name}")
        after_count = spark.table(table_name).count()
        if before_count == after_count:
            logging.warning(message)

    with_cpu_session(change_table,
                     conf = {"spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
                         "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED"})

def get_full_table_name(spark_tmp_table_factory):
    return f"default.{spark_tmp_table_factory.get()}"

def schema_to_ddl(spark, schema):
    return spark.sparkContext._jvm.org.apache.spark.sql.types.DataType.fromJson(schema.json()).toDDL()

def create_iceberg_table(table_name: str,
                         partition_col_sql: Optional[str] = None,
                         table_prop: Optional[Dict[str, str]] = None) -> str:
    if table_prop is None:
        table_prop = {'format-version':'1'}

    table_prop_sql = ", ".join([f"'{k}' = '{v}'" for k, v in table_prop.items()])

    def set_iceberg_table(spark: SparkSession):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)), seed=42)
        ddl = schema_to_ddl(spark, df.schema)

        if partition_col_sql is None:
            sql = (f"CREATE TABLE {table_name} "
                   f"({ddl}) "
                   f"USING ICEBERG "
                   f"TBLPROPERTIES ({table_prop_sql})")
        else:
            sql = (f"CREATE TABLE {table_name} "
                   f"({ddl}) "
                   f"USING ICEBERG "
                   f"PARTITIONED BY ({partition_col_sql}) "
                   f"TBLPROPERTIES ({table_prop_sql})")

        spark.sql(sql)

    with_cpu_session(set_iceberg_table)
    return table_name