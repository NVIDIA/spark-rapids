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

from typing import Callable, Dict, Optional

import pytest

from asserts import assert_equal_with_local_sort, assert_gpu_fallback_collect
from conftest import is_iceberg_remote_catalog
from data_gen import gen_df, copy_and_update
from iceberg import (create_iceberg_table, iceberg_base_table_cols,
                     iceberg_gens_list, iceberg_full_gens_list,
                     get_full_table_name, iceberg_write_enabled_conf)
from marks import iceberg, ignore_order, allow_non_gpu
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
                  partition_col_sql: Optional[str] = None):
    view_name = spark_tmp_table_factory.get()
    df = df_gen(spark)
    df.createOrReplaceTempView(view_name)

    spark.sql(f"DROP TABLE IF EXISTS {target_table}")

    partition_clause = "" if partition_col_sql is None else f"PARTITIONED BY ({partition_col_sql}) "
    props_sql = _props_to_sql(table_prop)
    spark.sql(
        f"CREATE TABLE {target_table} USING ICEBERG {partition_clause}"
        f"TBLPROPERTIES ({props_sql}) AS SELECT * FROM {view_name}")


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
                                                 df_gen, table_prop, partition_col_sql),
                     conf=conf)
    with_cpu_session(lambda spark: _execute_ctas(spark, cpu_table, spark_tmp_table_factory,
                                                 df_gen, table_prop, partition_col_sql),
                     conf=conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize("format_version", ["1", "2"], ids=lambda x: f"format_version={x}")
@pytest.mark.parametrize("write_distribution_mode", ["none", "hash", "range"],
                         ids=lambda x: f"write_distribution_mode={x}")
def test_ctas_unpartitioned_table(spark_tmp_table_factory,
                                  format_version,
                                  write_distribution_mode):
    table_prop = {
        "format-version": format_version,
        "write.distribution-mode": write_distribution_mode
    }

    df_gen = lambda spark: gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))

    _assert_gpu_equals_cpu_ctas(spark_tmp_table_factory, df_gen, table_prop)


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize("format_version", ["1", "2"], ids=lambda x: f"format_version={x}")
@pytest.mark.parametrize("write_distribution_mode", ["none", "hash", "range"],
                         ids=lambda x: f"write_distribution_mode={x}")
@pytest.mark.parametrize("fanout", [True, False], ids=lambda x: f"fanout={x}")
def test_ctas_partitioned_table(spark_tmp_table_factory,
                                format_version,
                                write_distribution_mode,
                                fanout):
    table_prop = {
        "format-version": format_version,
        "write.distribution-mode": write_distribution_mode,
        "write.spark.fanout.enabled": str(fanout).lower()
    }

    df_gen = lambda spark: gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))

    _assert_gpu_equals_cpu_ctas(spark_tmp_table_factory,
                                df_gen,
                                table_prop,
                                partition_col_sql="bucket(8, _c2)")


@iceberg
@ignore_order(local=True)
@allow_non_gpu('AtomicCreateTableAsSelectExec')
@pytest.mark.parametrize("format_version", ["1", "2"], ids=lambda x: f"format_version={x}")
@pytest.mark.parametrize("file_format", ["orc", "avro"], ids=lambda x: f"file_format={x}")
@pytest.mark.parametrize("write_distribution_mode", ["none", "hash", "range"],
                         ids=lambda x: f"write_distribution_mode={x}")
def test_ctas_unsupported_file_format_fallback(spark_tmp_table_factory,
                                               format_version,
                                               write_distribution_mode,
                                               file_format):
    table_prop = {
        "format-version": format_version,
        "write.distribution-mode": write_distribution_mode,
        "write.format.default": file_format
    }

    def run_ctas(spark):
        target = get_full_table_name(spark_tmp_table_factory)
        _execute_ctas(spark,
                      target,
                      spark_tmp_table_factory,
                      lambda sp: gen_df(sp, list(zip(iceberg_base_table_cols, iceberg_gens_list))),
                      table_prop)

    assert_gpu_fallback_collect(run_ctas,
                                'AtomicCreateTableAsSelectExec',
                                conf=iceberg_write_enabled_conf)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('AtomicCreateTableAsSelectExec')
@pytest.mark.parametrize("format_version", ["1", "2"], ids=lambda x: f"format_version={x}")
@pytest.mark.parametrize("write_distribution_mode", ["none", "hash", "range"],
                         ids=lambda x: f"write_distribution_mode={x}")
@pytest.mark.parametrize("conf_key", ["spark.rapids.sql.format.iceberg.enabled",
                                      "spark.rapids.sql.format.iceberg.write.enabled"],
                         ids=lambda x: f"{x}=False")
def test_ctas_fallback_when_conf_disabled(spark_tmp_table_factory,
                                          format_version,
                                          write_distribution_mode,
                                          conf_key):
    table_prop = {
        "format-version": format_version,
        "write.distribution-mode": write_distribution_mode
    }

    def run_ctas(spark):
        target = get_full_table_name(spark_tmp_table_factory)
        _execute_ctas(spark,
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
@allow_non_gpu('AtomicCreateTableAsSelectExec', 'ShuffleExchangeExec', 'ProjectExec')
@pytest.mark.parametrize("format_version", ["1", "2"], ids=lambda x: f"format_version={x}")
@pytest.mark.parametrize("write_distribution_mode", ["none", "hash", "range"],
                         ids=lambda x: f"write_distribution_mode={x}")
def test_ctas_unpartitioned_table_all_cols_fallback(spark_tmp_table_factory,
                                                     format_version,
                                                     write_distribution_mode):
    table_prop = {
        "format-version": format_version,
        "write.distribution-mode": write_distribution_mode
    }

    def run_ctas(spark):
        cols = [f"_c{idx}" for idx, _ in enumerate(iceberg_full_gens_list)]
        target = get_full_table_name(spark_tmp_table_factory)
        _execute_ctas(spark,
                      target,
                      spark_tmp_table_factory,
                      lambda sp: gen_df(sp, list(zip(cols, iceberg_full_gens_list))),
                      table_prop)

    assert_gpu_fallback_collect(run_ctas,
                                'AtomicCreateTableAsSelectExec',
                                conf=iceberg_write_enabled_conf)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('AtomicCreateTableAsSelectExec', 'ShuffleExchangeExec', 'ProjectExec')
@pytest.mark.parametrize("format_version", ["1", "2"], ids=lambda x: f"format_version={x}")
@pytest.mark.parametrize("write_distribution_mode", ["none", "hash", "range"],
                         ids=lambda x: f"write_distribution_mode={x}")
def test_ctas_partitioned_table_all_cols_fallback(spark_tmp_table_factory,
                                                   format_version,
                                                   write_distribution_mode):
    table_prop = {
        "format-version": format_version,
        "write.distribution-mode": write_distribution_mode
    }

    def run_ctas(spark):
        cols = [f"_c{idx}" for idx, _ in enumerate(iceberg_full_gens_list)]
        target = get_full_table_name(spark_tmp_table_factory)
        _execute_ctas(spark,
                      target,
                      spark_tmp_table_factory,
                      lambda sp: gen_df(sp, list(zip(cols, iceberg_full_gens_list))),
                      table_prop,
                      partition_col_sql="bucket(16, _c2)")

    assert_gpu_fallback_collect(run_ctas,
                                'AtomicCreateTableAsSelectExec',
                                conf=iceberg_write_enabled_conf)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('AtomicCreateTableAsSelectExec', 'ShuffleExchangeExec', 'SortExec', 'ProjectExec')
@pytest.mark.parametrize("format_version", ["1", "2"], ids=lambda x: f"format_version={x}")
@pytest.mark.parametrize("write_distribution_mode", ["none", "hash", "range"],
                         ids=lambda x: f"write_distribution_mode={x}")
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param("_c2", id="identity"),
    pytest.param("truncate(5, _c6)", id="truncate"),
    pytest.param("year(_c9)", id="year"),
    pytest.param("month(_c9)", id="month"),
    pytest.param("day(_c9)", id="day"),
    pytest.param("hour(_c9)", id="hour"),
    pytest.param("bucket(8, _c6)", id="bucket_unsupported_type"),
])
def test_ctas_partitioned_table_unsupported_partition_fallback(
        spark_tmp_table_factory,
        format_version,
        write_distribution_mode,
        partition_col_sql):
    table_prop = {
        "format-version": format_version,
        "write.distribution-mode": write_distribution_mode
    }

    def run_ctas(spark):
        target = get_full_table_name(spark_tmp_table_factory)
        _execute_ctas(spark,
                      target,
                      spark_tmp_table_factory,
                      lambda sp: gen_df(sp, list(zip(iceberg_base_table_cols, iceberg_gens_list))),
                      table_prop,
                      partition_col_sql=partition_col_sql)

    assert_gpu_fallback_collect(run_ctas,
                                'AtomicCreateTableAsSelectExec',
                                conf=iceberg_write_enabled_conf)


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize("format_version", ["1", "2"], ids=lambda x: f"format_version={x}")
@pytest.mark.parametrize("write_distribution_mode", ["none", "hash", "range"],
                         ids=lambda x: f"write_distribution_mode={x}")
@pytest.mark.parametrize("partition_table", [True, False], ids=lambda x: f"partition_table={x}")
def test_ctas_from_values(spark_tmp_table_factory,
                          format_version,
                          write_distribution_mode,
                          partition_table):
    table_prop = {
        "format-version": format_version,
        "write.distribution-mode": write_distribution_mode
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
