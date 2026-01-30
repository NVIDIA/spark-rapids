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

from typing import Callable, Dict, Optional

import pytest

from asserts import assert_equal_with_local_sort, assert_gpu_fallback_collect
from conftest import is_iceberg_remote_catalog
from data_gen import gen_df, copy_and_update
from iceberg import (create_iceberg_table, iceberg_base_table_cols,
                     iceberg_gens_list,
                     get_full_table_name, iceberg_write_enabled_conf)
from marks import iceberg, ignore_order, allow_non_gpu, datagen_overrides
from spark_session import with_gpu_session, with_cpu_session, is_spark_35x

pytestmark = [
    pytest.mark.skipif(not is_spark_35x(),
                       reason="Current spark-rapids only support spark 3.5.x"),
]


def _props_to_sql(table_prop: Dict[str, str]) -> str:
    return ", ".join([f"'{k}' = '{v}'" for k, v in table_prop.items()])


def _execute_ctas(spark,
                  target_table: str,
                  spark_tmp_table_factory,
                  df_gen: Callable,
                  table_prop: Dict[str, str],
                  partition_col_sql: Optional[str] = None,
                  ret = True):
    view_name = spark_tmp_table_factory.get()
    df = df_gen(spark)
    df.createOrReplaceTempView(view_name)

    spark.sql(f"DROP TABLE IF EXISTS {target_table}")

    partition_clause = "" if partition_col_sql is None else f"PARTITIONED BY ({partition_col_sql}) "
    props_sql = _props_to_sql(table_prop)
    df = spark.sql(
        f"CREATE TABLE {target_table} USING ICEBERG {partition_clause}"
        f"TBLPROPERTIES ({props_sql}) AS SELECT * FROM {view_name}")

    if ret:
        return df
    else:
        return None


def _assert_gpu_equals_cpu_ctas(spark_tmp_table_factory,
                                df_gen: Callable,
                                table_prop: Dict[str, str],
                                partition_col_sql: Optional[str] = None,
                                conf: Optional[Dict[str, str]] = None):
    if conf is None:
        conf = iceberg_write_enabled_conf

    base_name = get_full_table_name(spark_tmp_table_factory)
    gpu_table = f"{base_name}_gpu"
    cpu_table = f"{base_name}_cpu"

    with_gpu_session(lambda spark: _execute_ctas(spark, gpu_table, spark_tmp_table_factory,
                                                 df_gen, table_prop, partition_col_sql, False),
                     conf=conf)
    with_cpu_session(lambda spark: _execute_ctas(spark, cpu_table, spark_tmp_table_factory,
                                                 df_gen, table_prop, partition_col_sql, False),
                     conf=conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
def test_ctas_unpartitioned_table(spark_tmp_table_factory):
    table_prop = {
        "format-version": "2"
    }

    df_gen = lambda spark: gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))

    _assert_gpu_equals_cpu_ctas(spark_tmp_table_factory, df_gen, table_prop)


def _do_test_ctas_partitioned_table(spark_tmp_table_factory, partition_col_sql):
    """Helper function for partitioned table CTAS tests."""
    table_prop = {
        "format-version": "2"
    }

    df_gen = lambda spark: gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))

    _assert_gpu_equals_cpu_ctas(spark_tmp_table_factory,
                                df_gen,
                                table_prop,
                                partition_col_sql=partition_col_sql)


@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param("year(_c9)", id="year(timestamp_col)"),
])
def test_ctas_partitioned_table(spark_tmp_table_factory, partition_col_sql):
    """Basic partition test - runs for all catalogs including remote."""
    _do_test_ctas_partitioned_table(spark_tmp_table_factory, partition_col_sql)


@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param("year(_c8)", id="year(date_col)"),
    pytest.param("month(_c8)", id="month(date_col)"),
    pytest.param("day(_c8)", id="day(date_col)"),
    pytest.param("month(_c9)", id="month(timestamp_col)"),
    pytest.param("day(_c9)", id="day(timestamp_col)"),
    pytest.param("hour(_c9)", id="hour(timestamp_col)"),
    pytest.param("truncate(10, _c2)", id="truncate(10, int_col)"),
    pytest.param("truncate(10, _c3)", id="truncate(10, long_col)"),
    pytest.param("truncate(5, _c6)", id="truncate(5, string_col)"),
    pytest.param("truncate(10, _c13)", id="truncate(10, decimal32_col)"),
    pytest.param("truncate(10, _c14)", id="truncate(10, decimal64_col)"),
    pytest.param("truncate(10, _c15)", id="truncate(10, decimal128_col)"),
    pytest.param("bucket(16, _c2)", id="bucket(16, int_col)"),
    pytest.param("bucket(16, _c3)", id="bucket(16, long_col)"),
    pytest.param("bucket(16, _c8)", id="bucket(16, date_col)"),
    pytest.param("bucket(16, _c9)", id="bucket(16, timestamp_col)"),
    pytest.param("bucket(16, _c6)", id="bucket(16, string_col)"),
    pytest.param("bucket(16, _c13)", id="bucket(16, decimal32_col)"),
    pytest.param("bucket(16, _c14)", id="bucket(16, decimal64_col)"),
    pytest.param("bucket(16, _c15)", id="bucket(16, decimal128_col)"),
    pytest.param("_c0", id="identity(byte)"),
    pytest.param("_c2", id="identity(int)"),
    pytest.param("_c3", id="identity(long)"),
    pytest.param("_c6", id="identity(string)"),
    pytest.param("_c8", id="identity(date)"),
    pytest.param("_c10", id="identity(decimal)"),
])
def test_ctas_partitioned_table_full_coverage(spark_tmp_table_factory, partition_col_sql):
    """Full partition coverage test - skipped for remote catalogs."""
    _do_test_ctas_partitioned_table(spark_tmp_table_factory, partition_col_sql)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('AtomicCreateTableAsSelectExec', 'AppendDataExec')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("file_format", ["orc", "avro"], ids=lambda x: f"file_format={x}")
def test_ctas_unsupported_file_format_fallback(spark_tmp_table_factory,
                                               file_format):
    table_prop = {
        "format-version": "2",
        "write.format.default": file_format
    }

    def run_ctas(spark):
        target = get_full_table_name(spark_tmp_table_factory)
        return _execute_ctas(spark,
                      target,
                      spark_tmp_table_factory,
                      lambda sp: gen_df(sp, list(zip(iceberg_base_table_cols, iceberg_gens_list))),
                      table_prop)

    assert_gpu_fallback_collect(run_ctas,
                                'AtomicCreateTableAsSelectExec',
                                conf=iceberg_write_enabled_conf)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('AtomicCreateTableAsSelectExec', 'AppendDataExec')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("conf_key", ["spark.rapids.sql.format.iceberg.enabled",
                                      "spark.rapids.sql.format.iceberg.write.enabled"],
                         ids=lambda x: f"{x}=False")
def test_ctas_fallback_when_conf_disabled(spark_tmp_table_factory,
                                          conf_key):
    table_prop = {
        "format-version": "2"
    }

    def run_ctas(spark):
        target = get_full_table_name(spark_tmp_table_factory)
        return _execute_ctas(spark,
                      target,
                      spark_tmp_table_factory,
                      lambda sp: gen_df(sp, list(zip(iceberg_base_table_cols, iceberg_gens_list))),
                      table_prop)

    updated_conf = copy_and_update(iceberg_write_enabled_conf, {conf_key: "false"})
    assert_gpu_fallback_collect(run_ctas,
                                'AtomicCreateTableAsSelectExec',
                                conf=updated_conf)


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("partition_table", [True, False], ids=lambda x: f"partition_table={x}")
@allow_non_gpu('AtomicCreateTableAsSelectExec', 'AppendDataExec', 'ShuffleExchangeExec', 'SortExec', 'ProjectExec')
def test_ctas_from_values(spark_tmp_table_factory,
                          partition_table):
    table_prop = {
        "format-version": "2"
    }

    base_name = get_full_table_name(spark_tmp_table_factory)
    gpu_table = f"{base_name}_gpu"
    cpu_table = f"{base_name}_cpu"

    def execute_ctas_from_values(spark, target_table: str):
        spark.sql(f"DROP TABLE IF EXISTS {target_table}")
        
        partition_clause = "" if not partition_table else "PARTITIONED BY (bucket(8, id)) "
        props_sql = _props_to_sql(table_prop)
        
        spark.sql(
            f"CREATE TABLE {target_table} USING ICEBERG {partition_clause}"
            f"TBLPROPERTIES ({props_sql}) AS "
            f"SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)")

    with_gpu_session(lambda spark: execute_ctas_from_values(spark, gpu_table),
                     conf=iceberg_write_enabled_conf)
    with_cpu_session(lambda spark: execute_ctas_from_values(spark, cpu_table),
                     conf=iceberg_write_enabled_conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param(None, id="unpartitioned"),
    pytest.param("year(_c9)", id="triple_datetime_transforms"),
])
def test_ctas_aqe(spark_tmp_table_factory, partition_col_sql):
    """
    Test CTAS with multiple partition transforms on the same column with AQE enabled.
    
    This test reproduces NVBUGS-5689547 where the error "ROW BASED PROCESSING IS NOT SUPPORTED"
    occurs when writing to Iceberg tables with multiple partition transforms when AQE is enabled.
    
    The issue manifests when:
    - AQE is enabled
    - Multiple partition transforms are applied (year, month, day, hour)
    - GpuShuffleCoalesceExec ends up as a child of GpuRowToColumnarExec
    """
    table_prop = {
        "format-version": "2",
    }

    df_gen = lambda spark: gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))

    # Configuration with AQE enabled (this is the key to reproducing the issue)
    conf = copy_and_update(iceberg_write_enabled_conf, {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    })

    _assert_gpu_equals_cpu_ctas(spark_tmp_table_factory,
                                df_gen,
                                table_prop,
                                partition_col_sql=partition_col_sql,
                                conf=conf)
