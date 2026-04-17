# Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import FloatType, DoubleType, BinaryType

import pytest

from data_gen import *
from spark_session import is_iceberg_supported_spark, with_cpu_session

iceberg_unsupported_mark = pytest.mark.skipif(
    not is_iceberg_supported_spark(),
    reason="Iceberg acceleration requires Spark 3.5.x or 4.0.x")

# iceberg supported types
iceberg_table_gen = MappingProxyType({
    '_c0': byte_gen, '_c1': short_gen, '_c2': int_gen,
    '_c3': long_gen, '_c4': float_gen, '_c5': double_gen,
    '_c6': string_gen,
    '_c7': boolean_gen, '_c8': date_gen, '_c9': timestamp_gen, '_c10': decimal_gen_32bit,
    '_c11': decimal_gen_64bit, '_c12': decimal_gen_128bit,
    # Add columns c13, c14, c15 to avoid Iceberg known issue:
    # https://github.com/NVIDIA/spark-rapids-jni/issues/4016
    # Disable special cases to avoid generating overflow values for truncate transform.
    # when this issue is fixed, we can remove these extra columns.
    '_c13': DecimalGen(precision=7, scale=3, special_cases=[]),
    '_c14': DecimalGen(precision=12, scale=2, special_cases=[]),
    '_c15': DecimalGen(precision=20, scale=2, special_cases=[]),
})
iceberg_base_table_cols = list(iceberg_table_gen.keys())
iceberg_gens_list = [iceberg_table_gen[col] for col in iceberg_base_table_cols]

rapids_reader_types = ['PERFILE', 'MULTITHREADED', 'COALESCING']

# All data types of iceberg, not all of them are supported by spark-rapids for now
iceberg_map_gens = [MapGen(f(nullable=False), f()) for f in [
    BooleanGen, ByteGen, ShortGen, IntegerGen, LongGen, FloatGen, DoubleGen, DateGen, TimestampGen ]] + \
                   [simple_string_to_string_map_gen,
                    MapGen(StringGen(pattern='key_[0-9]', nullable=False), ArrayGen(string_gen), max_length=10),
                    MapGen(RepeatSeqGen(IntegerGen(nullable=False), 10), long_gen, max_length=10),
                    MapGen(StringGen(pattern='key_[0-9]', nullable=False), simple_string_to_string_map_gen)]

# Broad Iceberg end-to-end coverage including nested types
iceberg_full_gens_list = ([byte_gen, short_gen, IntegerGen(nullable=False), LongGen(nullable=False),
                          float_gen, double_gen, string_gen, boolean_gen, date_gen,
                          timestamp_gen, binary_gen, ArrayGen(binary_gen), ArrayGen(byte_gen),
                          ArrayGen(long_gen), ArrayGen(string_gen), ArrayGen(date_gen),
                          ArrayGen(timestamp_gen), ArrayGen(decimal_gen_64bit),
                          ArrayGen(ArrayGen(byte_gen)),
                          StructGen([['child0', ArrayGen(byte_gen)], ['child1', byte_gen],
                                     ['child2', float_gen], ['child3', decimal_gen_64bit]]),
                          ArrayGen(StructGen([['child0', string_gen], ['child1', double_gen],
                                              ['child2', int_gen]]))] +
                          iceberg_map_gens + decimal_gens)

# Scalar top-level column generators.
#
# Top-level columns are non-nullable so rows are always present (row-level null coverage lives in
# `iceberg_full_gens_list`). Float/double allow NaN (no_nans=False default) so NaN handling is
# exercised at the column level as well as inside nested containers.
iceberg_nested_write_float_gen = FloatGen(nullable=False)
iceberg_nested_write_double_gen = DoubleGen(nullable=False)
iceberg_nested_write_string_gen = StringGen(nullable=False)
iceberg_nested_write_boolean_gen = BooleanGen(nullable=False)
iceberg_nested_write_date_gen = DateGen(nullable=False)
iceberg_nested_write_timestamp_gen = TimestampGen(nullable=False)
iceberg_nested_write_decimal32_gen = DecimalGen(precision=7, scale=3, nullable=False,
                                                special_cases=[])
iceberg_nested_write_decimal64_gen = DecimalGen(precision=12, scale=2, nullable=False,
                                                special_cases=[])
iceberg_nested_write_decimal128_gen = DecimalGen(precision=20, scale=2, nullable=False,
                                                 special_cases=[])
iceberg_nested_write_binary_gen = BinaryGen(nullable=False)

# Nested container generators.
#
# Outer containers stay non-nullable so the column is always present, but inner elements/values
# use default nullable=True gens so nulls appear inside arrays/maps/structs. Using the default
# min_length=0 lets arrays/maps be empty. Float/double inner gens keep no_nans=False so NaN is
# covered inside containers too.
iceberg_nested_write_string_array_gen = ArrayGen(StringGen(), max_length=10, nullable=False)
iceberg_nested_write_long_array_gen = ArrayGen(LongGen(), max_length=10, nullable=False)
iceberg_nested_write_binary_array_gen = ArrayGen(BinaryGen(), max_length=10, nullable=False)
iceberg_nested_write_date_array_gen = ArrayGen(DateGen(), max_length=10, nullable=False)
iceberg_nested_write_timestamp_array_gen = ArrayGen(TimestampGen(), max_length=10, nullable=False)
iceberg_nested_write_decimal64_array_gen = ArrayGen(
    DecimalGen(precision=12, scale=2, special_cases=[]),
    max_length=10, nullable=False)
iceberg_nested_write_nested_int_array_gen = ArrayGen(
    ArrayGen(IntegerGen(), max_length=10),
    max_length=10, nullable=False)
iceberg_nested_write_struct_gen = StructGen(
    [['child0', ArrayGen(IntegerGen(), max_length=10)],
     ['child1', IntegerGen()],
     ['child2', FloatGen()],
     ['child3', DecimalGen(precision=12, scale=2, special_cases=[])]],
    nullable=False)
iceberg_nested_write_struct_array_gen = ArrayGen(
    StructGen([['child0', StringGen()],
               ['child1', DoubleGen()],
               ['child2', IntegerGen()]]),
    max_length=10, nullable=False)
iceberg_nested_write_simple_string_map_gen = MapGen(
    StringGen(pattern='key_[0-9]', nullable=False),
    StringGen(),
    max_length=10, nullable=False)
iceberg_nested_write_map_gens = [
    MapGen(BooleanGen(nullable=False), BooleanGen(), max_length=10, nullable=False),
    MapGen(IntegerGen(nullable=False), IntegerGen(), max_length=10, nullable=False),
    MapGen(LongGen(nullable=False), LongGen(), max_length=10, nullable=False),
    MapGen(FloatGen(nullable=False), FloatGen(), max_length=10, nullable=False),
    MapGen(DoubleGen(nullable=False), DoubleGen(), max_length=10, nullable=False),
    MapGen(DateGen(nullable=False), DateGen(), max_length=10, nullable=False),
    MapGen(TimestampGen(nullable=False), TimestampGen(), max_length=10, nullable=False),
    iceberg_nested_write_simple_string_map_gen,
    MapGen(StringGen(pattern='key_[0-9]', nullable=False),
           ArrayGen(StringGen(), max_length=10),
           max_length=10, nullable=False),
    MapGen(RepeatSeqGen(IntegerGen(nullable=False), 10), LongGen(),
           max_length=10, nullable=False),
    MapGen(StringGen(pattern='key_[0-9]', nullable=False),
           iceberg_nested_write_simple_string_map_gen,
           max_length=10, nullable=False)
]

# Nested types intended for focused positive GPU Iceberg tests.
#
# Covers nullable nested fields, empty arrays/maps, and NaN/Inf floats/doubles inside nested
# containers so merge/update/delete/append/CTAS/RTAS/overwrite nested_types tests exercise the
# same edge cases that `iceberg_full_gens_list`-based `_all_cols` tests do. `_c0` and `_c1` stay
# strict non-nullable IntegerGen/LongGen so merge/update/delete join keys and predicates don't
# get trivialized by nulls.
iceberg_nested_write_gens_list = ([IntegerGen(nullable=False), LongGen(nullable=False),
                                   iceberg_nested_write_float_gen,
                                   iceberg_nested_write_double_gen,
                                   iceberg_nested_write_string_gen,
                                   iceberg_nested_write_boolean_gen,
                                   iceberg_nested_write_date_gen,
                                   iceberg_nested_write_timestamp_gen,
                                   iceberg_nested_write_binary_gen,
                                   iceberg_nested_write_long_array_gen,
                                   iceberg_nested_write_binary_array_gen,
                                   iceberg_nested_write_string_array_gen,
                                   iceberg_nested_write_date_array_gen,
                                   iceberg_nested_write_timestamp_array_gen,
                                   iceberg_nested_write_decimal64_array_gen,
                                   iceberg_nested_write_nested_int_array_gen,
                                   iceberg_nested_write_struct_gen,
                                   iceberg_nested_write_struct_array_gen] +
                                  iceberg_nested_write_map_gens +
                                  [iceberg_nested_write_decimal32_gen,
                                   iceberg_nested_write_decimal64_gen,
                                   iceberg_nested_write_decimal128_gen])

iceberg_write_enabled_conf = {
    "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
    "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED",
    "spark.rapids.sql.format.iceberg.enabled": "true",
    "spark.rapids.sql.format.iceberg.write.enabled": "true",
    # WriteDeltaExec is disabled by default as it's experimental, but we need it enabled
    # for merge-on-read (MOR) DML operations (UPDATE/DELETE/MERGE with write.*.mode='merge-on-read')
    "spark.rapids.sql.exec.WriteDeltaExec": "true",
}

parquet_write_corrected_conf = {
    "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
    "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED",
}


def materialize_parquet_source(spark_tmp_path: str,
                               gen_list,
                               seed: Optional[int] = None,
                               length: int = 2048) -> str:
    temp_dir = tempfile.mkdtemp(dir=spark_tmp_path)

    def write_parquet(spark: SparkSession):
        gen_df(spark, gen_list, length=length, seed=seed).write.mode("overwrite").parquet(temp_dir)

    with_cpu_session(write_parquet, conf=parquet_write_corrected_conf)
    return temp_dir


def assert_no_cpu_project_exec(spark: SparkSession, df: DataFrame) -> None:
    """Assert that the Spark plan for `df` does not contain a CPU ProjectExec.

    Used by positive write tests to confirm the entire write pipeline stays on
    GPU (no Project operator is left behind on CPU).
    """
    jvm = spark.sparkContext._jvm
    jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback.assertNotContain(
        df._jdf, "ProjectExec")


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

all_eq_column_combinations = _eq_column_combinations(iceberg_base_table_cols, iceberg_gens_list, 2)

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
    table_prop = _build_tblprops(table_prop)

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

# Base table properties applied to every Iceberg test table.
# Disables the fanout writer to prevent OOM in CI.  S3TablesCatalog does not
# honor catalog-level table-default properties, so this must be set per table.
_BASE_TBLPROPS = {'write.spark.fanout.enabled': False}


def _to_tblprops_str(props: dict) -> dict:
    """Convert property values to their SQL-safe string representation."""
    return {k: str(v).lower() if isinstance(v, bool) else str(v) for k, v in props.items()}


def _build_tblprops(extra_props: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """Build table properties dict with base props merged in.
    Caller properties take precedence over base.
    Returns string-valued dict suitable for SQL interpolation."""
    if extra_props is None:
        return _to_tblprops_str(_BASE_TBLPROPS)
    return _to_tblprops_str({**_BASE_TBLPROPS, **extra_props})


# SQL fragment for raw CREATE TABLE statements that have no other TBLPROPERTIES.
_BASE_TBLPROPS_SQL = "TBLPROPERTIES (" + \
    ", ".join(f"'{k}' = '{v}'" for k, v in _build_tblprops().items()) + ")"


def create_iceberg_table(table_name: str,
                         partition_col_sql: Optional[str] = None,
                         table_prop: Optional[Dict[str, str]] = None,
                         df_gen: Optional[Callable[[SparkSession], DataFrame]] = None) -> str:
    if table_prop is None:
        table_prop = {'format-version':'1'}
    table_prop = _build_tblprops(table_prop)

    if df_gen is None:
        df_gen = lambda spark: gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))


    table_prop_sql = ", ".join([f"'{k}' = '{v}'" for k, v in table_prop.items()])

    def set_iceberg_table(spark: SparkSession):
        df = df_gen(spark)
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
