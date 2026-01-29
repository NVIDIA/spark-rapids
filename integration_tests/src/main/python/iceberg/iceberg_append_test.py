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
from typing import Callable, Any

import pytest

from asserts import assert_equal_with_local_sort, assert_gpu_fallback_collect
from conftest import is_iceberg_remote_catalog
from data_gen import gen_df, copy_and_update
from iceberg import create_iceberg_table, iceberg_base_table_cols, iceberg_gens_list, \
    get_full_table_name, iceberg_full_gens_list, iceberg_write_enabled_conf
from marks import iceberg, ignore_order, allow_non_gpu, datagen_overrides
from spark_session import with_gpu_session, with_cpu_session, is_spark_35x

pytestmark = pytest.mark.skipif(not is_spark_35x(),
                       reason="Current spark-rapids only support spark 3.5.x")


def do_test_insert_into_table_sql(spark_tmp_table_factory,
                                  create_table_func: Callable[[str], Any]):
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"

    create_table_func(cpu_table_name)
    create_table_func(gpu_table_name)

    def insert_data(spark, table_name: str):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    with_gpu_session(lambda spark: insert_data(spark, gpu_table_name),
                     conf = iceberg_write_enabled_conf)
    with_cpu_session(lambda spark: insert_data(spark, cpu_table_name),
                     conf = iceberg_write_enabled_conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
def test_insert_into_unpartitioned_table(spark_tmp_table_factory):
    table_prop = {"format-version": "2"}

    do_test_insert_into_table_sql(
        spark_tmp_table_factory,
        lambda table_name: create_iceberg_table(table_name, table_prop=table_prop))

@iceberg
@ignore_order(local=True)
@allow_non_gpu('AppendDataExec')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("partition_table", [True, False], ids=lambda x: f"partition_table={x}")
def test_insert_into_unpartitioned_table_values(spark_tmp_table_factory,
                                                partition_table):
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"

    def create_table(spark, table_name: str):
        sql = f"""CREATE TABLE {table_name} (id int, name string) USING ICEBERG """
        if partition_table:
            sql += "PARTITIONED BY (bucket(8, id)) "

        sql += f"""TBLPROPERTIES (
        'format-version' = '2')
        """
        spark.sql(sql)

    with_cpu_session(lambda spark: create_table(spark, cpu_table_name))
    with_cpu_session(lambda spark: create_table(spark, gpu_table_name))

    def insert_data(spark, table_name: str):
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    with_gpu_session(lambda spark: insert_data(spark, gpu_table_name),
                     conf = iceberg_write_enabled_conf)
    with_cpu_session(lambda spark: insert_data(spark, cpu_table_name),
                     conf = iceberg_write_enabled_conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('AppendDataExec', 'ShuffleExchangeExec', 'ProjectExec')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_insert_into_unpartitioned_table_all_cols_fallback(spark_tmp_table_factory):
    table_prop = {"format-version": "2"}

    def this_gen_df(spark):
        cols = [ f"_c{idx}" for idx, _ in enumerate(iceberg_full_gens_list)]
        return gen_df(spark, list(zip(cols, iceberg_full_gens_list)))

    def insert_data(spark, table_name: str):
        df = this_gen_df(spark)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    table_name = get_full_table_name(spark_tmp_table_factory)
    create_iceberg_table(table_name, table_prop=table_prop, df_gen=this_gen_df)

    assert_gpu_fallback_collect(lambda spark: insert_data(spark, table_name),
                                "AppendDataExec",
                                conf = iceberg_write_enabled_conf)


def _do_test_insert_into_partitioned_table(spark_tmp_table_factory, partition_col_sql):
    """Helper function for partitioned table insert tests."""
    table_prop = {"format-version": "2"}

    def create_table_and_set_write_order(table_name: str):
        create_iceberg_table(
            table_name,
            partition_col_sql=partition_col_sql,
            table_prop=table_prop)

        sql = f"ALTER TABLE {table_name} WRITE ORDERED BY _c2, _c3, _c4"
        with_cpu_session(lambda spark: spark.sql(sql).collect())

    do_test_insert_into_table_sql(
        spark_tmp_table_factory,
        create_table_and_set_write_order)


@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param("year(_c9)", id="year(timestamp_col)"),
])
def test_insert_into_partitioned_table(spark_tmp_table_factory, partition_col_sql):
    """Basic partition test - runs for all catalogs including remote."""
    _do_test_insert_into_partitioned_table(spark_tmp_table_factory, partition_col_sql)


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
def test_insert_into_partitioned_table_full_coverage(spark_tmp_table_factory, partition_col_sql):
    """Full partition coverage test - skipped for remote catalogs."""
    _do_test_insert_into_partitioned_table(spark_tmp_table_factory, partition_col_sql)

@iceberg
@ignore_order(local=True)
@allow_non_gpu('AppendDataExec', 'ShuffleExchangeExec', 'ProjectExec')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_insert_into_partitioned_table_all_cols_fallback(spark_tmp_table_factory):
    table_prop = {"format-version": "2"}

    def this_gen_df(spark):
        cols = [ f"_c{idx}" for idx, _ in enumerate(iceberg_full_gens_list)]
        return gen_df(spark, list(zip(cols, iceberg_full_gens_list)))

    def insert_data(spark, table_name: str):
        df = this_gen_df(spark)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    table_name = get_full_table_name(spark_tmp_table_factory)
    create_iceberg_table(table_name,
                          partition_col_sql="bucket(16, _c2), bucket(16, _c3)",
                          table_prop=table_prop,
                          df_gen=this_gen_df)

    assert_gpu_fallback_collect(lambda spark: insert_data(spark, table_name),
                                "AppendDataExec",
                                conf = iceberg_write_enabled_conf)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('AppendDataExec', 'ShuffleExchangeExec', 'ProjectExec')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("file_format", ["orc", "avro"], ids=lambda x: f"file_format={x}")
def test_insert_into_table_unsupported_file_format_fallback(
        spark_tmp_table_factory, file_format):
    table_prop = {"format-version": "2",
                  "write.format.default": file_format}

    def insert_data(spark, table_name: str):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    table_name = get_full_table_name(spark_tmp_table_factory)
    create_iceberg_table(table_name, table_prop=table_prop)

    assert_gpu_fallback_collect(lambda spark: insert_data(spark, table_name),
                                "AppendDataExec",
                                conf = iceberg_write_enabled_conf)

@iceberg
@ignore_order(local=True)
@allow_non_gpu('AppendDataExec', 'ShuffleExchangeExec', 'ProjectExec')
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize("conf_key", ["spark.rapids.sql.format.iceberg.enabled",
                                      "spark.rapids.sql.format.iceberg.write.enabled"],
                         ids=lambda x: f"{x}=False")
def test_insert_into_iceberg_table_fallback_when_conf_disabled(
        spark_tmp_table_factory, conf_key):
    table_prop = {"format-version": "2"}

    def insert_data(spark, table_name: str):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    table_name = get_full_table_name(spark_tmp_table_factory)
    create_iceberg_table(table_name, table_prop=table_prop)

    updated_conf = copy_and_update(iceberg_write_enabled_conf, {conf_key: "false"})
    assert_gpu_fallback_collect(lambda spark: insert_data(spark, table_name),
                                "AppendDataExec",
                                conf = updated_conf)


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param(None, id="unpartitioned"),
    pytest.param("year(_c9)", id="year_partition"),
])
def test_insert_into_aqe(spark_tmp_table_factory, partition_col_sql):
    """
    Test INSERT INTO with AQE enabled.
    """
    table_prop = {"format-version": "2"}

    # Configuration with AQE enabled
    conf = copy_and_update(iceberg_write_enabled_conf, {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    })

    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"

    # Create tables
    with_cpu_session(lambda spark: create_iceberg_table(
        cpu_table_name, partition_col_sql, table_prop,
        lambda sp: gen_df(sp, list(zip(iceberg_base_table_cols, iceberg_gens_list)))))
    with_cpu_session(lambda spark: create_iceberg_table(
        gpu_table_name, partition_col_sql, table_prop,
        lambda sp: gen_df(sp, list(zip(iceberg_base_table_cols, iceberg_gens_list)))))

    # Insert data
    def insert_data(spark, table_name: str):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    with_gpu_session(lambda spark: insert_data(spark, gpu_table_name), conf=conf)
    with_cpu_session(lambda spark: insert_data(spark, cpu_table_name), conf=conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_insert_after_drop_partition_field(spark_tmp_table_factory):
    """Test INSERT on table after dropping a partition field (void transform).
    
    When a partition field is dropped, Iceberg creates a 'void transform' - 
    the field remains in the partition spec but no longer affects partitioning.
    This test verifies INSERT still runs correctly on GPU after partition evolution.
    """
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"
    
    table_prop = {"format-version": "2"}
    # Use two partition columns so after dropping one, we still have at least one
    partition_col_sql = "bucket(8, _c2), bucket(8, _c3)"
    
    # Create partitioned tables
    create_iceberg_table(cpu_table_name, partition_col_sql=partition_col_sql, table_prop=table_prop)
    create_iceberg_table(gpu_table_name, partition_col_sql=partition_col_sql, table_prop=table_prop)
    
    # Insert initial data before partition evolution using same seed for both tables
    def insert_initial_data(spark, table_name):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)), seed=42)
        df.writeTo(table_name).append()
    
    with_cpu_session(lambda spark: insert_initial_data(spark, cpu_table_name))
    with_cpu_session(lambda spark: insert_initial_data(spark, gpu_table_name))
    
    # Drop one partition field on both tables (creates void transform)
    def drop_partition_field(spark, table_name):
        spark.sql(f"ALTER TABLE {table_name} DROP PARTITION FIELD bucket(8, _c2)")
    
    with_cpu_session(lambda spark: drop_partition_field(spark, cpu_table_name))
    with_cpu_session(lambda spark: drop_partition_field(spark, gpu_table_name))
    
    # Insert data after partition evolution - this is the operation we're testing on GPU
    # Use same seed for both sessions to ensure identical data
    def insert_data(spark, table_name):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)), seed=43)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")
    
    with_gpu_session(lambda spark: insert_data(spark, gpu_table_name),
                     conf=iceberg_write_enabled_conf)
    with_cpu_session(lambda spark: insert_data(spark, cpu_table_name),
                     conf=iceberg_write_enabled_conf)
    
    # Compare results
    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)
