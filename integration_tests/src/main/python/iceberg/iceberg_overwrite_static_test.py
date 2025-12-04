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
from typing import Callable, Any

import pytest

from asserts import assert_equal_with_local_sort, assert_gpu_fallback_collect
from conftest import is_iceberg_remote_catalog
from data_gen import gen_df, copy_and_update, StringGen
from iceberg import create_iceberg_table, iceberg_base_table_cols, iceberg_gens_list, \
    get_full_table_name, iceberg_full_gens_list, iceberg_write_enabled_conf
from marks import iceberg, ignore_order, allow_non_gpu, datagen_overrides
from spark_session import with_gpu_session, with_cpu_session, is_spark_35x

pytestmark = pytest.mark.skipif(not is_spark_35x(),
                       reason="Current spark-rapids only support spark 3.5.x")

# Seed for initial insert to ensure different data from overwrite
INITIAL_INSERT_SEED = 7893676382

# Configuration for static overwrite tests
iceberg_static_overwrite_conf = copy_and_update(iceberg_write_enabled_conf, {
    "spark.sql.sources.partitionOverwriteMode": "STATIC"
})


def do_test_insert_overwrite_table_sql(spark_tmp_table_factory,
                                       create_table_func: Callable[[str], Any]):
    """Test INSERT OVERWRITE for Iceberg tables comparing GPU vs CPU results."""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"

    create_table_func(cpu_table_name)
    create_table_func(gpu_table_name)

    def insert_initial_data(spark, table_name: str):
        """Insert initial data into the table."""
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)), seed=INITIAL_INSERT_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    # Insert initial data on both CPU and GPU
    with_cpu_session(lambda spark: insert_initial_data(spark, cpu_table_name),
                     conf = iceberg_static_overwrite_conf)
    with_gpu_session(lambda spark: insert_initial_data(spark, gpu_table_name),
                     conf = iceberg_static_overwrite_conf)

    def overwrite_data(spark, table_name: str):
        """Overwrite the table with new data."""
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")

    # Perform overwrite on GPU and CPU
    with_gpu_session(lambda spark: overwrite_data(spark, gpu_table_name),
                     conf = iceberg_static_overwrite_conf)
    with_cpu_session(lambda spark: overwrite_data(spark, cpu_table_name),
                     conf = iceberg_static_overwrite_conf)

    # Compare results
    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@ignore_order(local=True)
def test_insert_overwrite_unpartitioned_table(spark_tmp_table_factory):
    """Test INSERT OVERWRITE on unpartitioned Iceberg tables."""
    table_prop = {"format-version": "2"}

    do_test_insert_overwrite_table_sql(
        spark_tmp_table_factory,
        lambda table_name: create_iceberg_table(table_name, table_prop=table_prop))


@iceberg
@ignore_order(local=True)
@allow_non_gpu('OverwriteByExpressionExec')
def test_insert_overwrite_unpartitioned_table_values(spark_tmp_table_factory):
    """Test INSERT OVERWRITE on unpartitioned tables with VALUES syntax."""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"

    def create_table(spark, table_name: str):
        sql = f"""CREATE TABLE {table_name} (id int, name string) USING ICEBERG 
        TBLPROPERTIES (
        'format-version' = '2')
        """
        spark.sql(sql)

    with_cpu_session(lambda spark: create_table(spark, cpu_table_name))
    with_cpu_session(lambda spark: create_table(spark, gpu_table_name))

    def insert_initial_data(spark, table_name: str):
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    def overwrite_data(spark, table_name: str):
        spark.sql(f"INSERT OVERWRITE TABLE {table_name} VALUES (4, 'd'), (5, 'e')")

    # Insert initial data
    with_cpu_session(lambda spark: insert_initial_data(spark, cpu_table_name),
                     conf = iceberg_static_overwrite_conf)
    with_gpu_session(lambda spark: insert_initial_data(spark, gpu_table_name),
                     conf = iceberg_static_overwrite_conf)

    # Overwrite
    with_gpu_session(lambda spark: overwrite_data(spark, gpu_table_name),
                     conf = iceberg_static_overwrite_conf)
    with_cpu_session(lambda spark: overwrite_data(spark, cpu_table_name),
                     conf = iceberg_static_overwrite_conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param("bucket(16, _c2), bucket(16, _c3)", id="bucket(16, int_col), bucket(16, long_col)"),
    pytest.param("year(_c8)", id="year(date_col)"),
    pytest.param("month(_c8)", id="month(date_col)"),
    pytest.param("day(_c8)", id="day(date_col)"),
    pytest.param("year(_c9)", id="year(timestamp_col)"),
    pytest.param("month(_c9)", id="month(timestamp_col)"),
    pytest.param("day(_c9)", id="day(timestamp_col)"),
    pytest.param("hour(_c9)", id="hour(timestamp_col)"),
    pytest.param("truncate(10, _c2)", id="truncate(10, int_col)"),
    pytest.param("truncate(10, _c3)", id="truncate(10, long_col)"),
    pytest.param("truncate(5, _c6)", id="truncate(5, string_col)"),
    pytest.param("truncate(10, _c13)", id="truncate(10, decimal32_col)"),
    pytest.param("truncate(10, _c14)", id="truncate(10, decimal64_col)"),
    pytest.param("truncate(10, _c15)", id="truncate(10, decimal128_col)"),
])
def test_insert_overwrite_partitioned_table(spark_tmp_table_factory, partition_col_sql):
    """Test INSERT OVERWRITE on partitioned Iceberg tables."""
    table_prop = {"format-version": "2"}

    def create_table_and_set_write_order(table_name: str):
        create_iceberg_table(
            table_name,
            partition_col_sql=partition_col_sql,
            table_prop=table_prop)

        sql = f"ALTER TABLE {table_name} WRITE ORDERED BY _c2, _c3, _c4"
        with_cpu_session(lambda spark: spark.sql(sql).collect())

    do_test_insert_overwrite_table_sql(
        spark_tmp_table_factory,
        create_table_and_set_write_order)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('OverwriteByExpressionExec', 'ShuffleExchangeExec', 'SortExec', 'ProjectExec')
def test_insert_overwrite_specific_partition_identity_fallback(spark_tmp_table_factory):
    """Test INSERT OVERWRITE with PARTITION clause for specific partitions (identity partition - falls back to CPU)."""
    # Extend base columns with a partition column that has limited values
    partition_col = '_c999'
    partition_gen = StringGen(pattern='category_[ABC]', nullable=False)  # Generates category_A, category_B, category_C
    
    extended_cols = iceberg_base_table_cols + [partition_col]
    extended_gens = iceberg_gens_list + [partition_gen]
    
    table_name = get_full_table_name(spark_tmp_table_factory)

    table_prop = {"format-version": "2"}

    # Create table with identity partition on the category column
    create_iceberg_table(
        table_name,
        partition_col_sql=partition_col,  # Identity partition
        table_prop=table_prop,
        df_gen=lambda spark: gen_df(spark, list(zip(extended_cols, extended_gens))))

    def insert_initial_data(spark, table_name: str):
        """Insert initial data with multiple partition values."""
        df = gen_df(spark, list(zip(extended_cols, extended_gens)), seed=INITIAL_INSERT_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    def overwrite_specific_partition(spark, table_name: str):
        """Overwrite only category_A partition - other partitions should remain unchanged."""
        df = gen_df(spark, list(zip(extended_cols, extended_gens)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        # Overwrite only rows where partition column equals 'category_A'
        cols_to_select = ','.join(iceberg_base_table_cols)
        sql = f"INSERT OVERWRITE TABLE {table_name} PARTITION ({partition_col} = 'category_A') " \
              f"SELECT {cols_to_select} FROM {view_name} WHERE {partition_col} = 'category_A'"
        return spark.sql(sql)

    # Insert initial data
    with_cpu_session(lambda spark: insert_initial_data(spark, table_name),
                     conf = iceberg_static_overwrite_conf)

    # Perform overwrite (should fall back because identity partition is not supported on GPU)
    assert_gpu_fallback_collect(lambda spark: overwrite_specific_partition(spark, table_name),
                                "OverwriteByExpressionExec",
                                conf = iceberg_static_overwrite_conf)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('OverwriteByExpressionExec', 'ShuffleExchangeExec', 'SortExec', 'ProjectExec')
def test_insert_overwrite_unpartitioned_table_all_cols_fallback(spark_tmp_table_factory):
    """Test INSERT OVERWRITE with all data types including unsupported ones (falls back to CPU)."""
    table_prop = {"format-version": "2"}

    def this_gen_df(spark):
        cols = [ f"_c{idx}" for idx, _ in enumerate(iceberg_full_gens_list)]
        return gen_df(spark, list(zip(cols, iceberg_full_gens_list)))

    table_name = get_full_table_name(spark_tmp_table_factory)
    create_iceberg_table(table_name, table_prop=table_prop, df_gen=this_gen_df)

    def insert_initial_data(spark, table_name: str):
        cols = [ f"_c{idx}" for idx, _ in enumerate(iceberg_full_gens_list)]
        df = gen_df(spark, list(zip(cols, iceberg_full_gens_list)), seed=INITIAL_INSERT_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    def overwrite_data(spark, table_name: str):
        df = this_gen_df(spark)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")

    # Initial insert (not tested for fallback)
    with_cpu_session(lambda spark: insert_initial_data(spark, table_name),
                     conf = iceberg_static_overwrite_conf)

    # Assert that overwrite falls back to CPU
    assert_gpu_fallback_collect(lambda spark: overwrite_data(spark, table_name),
                                "OverwriteByExpressionExec",
                                conf = iceberg_static_overwrite_conf)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('OverwriteByExpressionExec', 'ShuffleExchangeExec', 'SortExec', 'ProjectExec')
def test_insert_overwrite_partitioned_table_all_cols_fallback(spark_tmp_table_factory):
    """Test INSERT OVERWRITE on partitioned table with all data types including unsupported ones (falls back to CPU)."""
    table_prop = {"format-version": "2"}

    def this_gen_df(spark):
        cols = [ f"_c{idx}" for idx, _ in enumerate(iceberg_full_gens_list)]
        return gen_df(spark, list(zip(cols, iceberg_full_gens_list)))

    table_name = get_full_table_name(spark_tmp_table_factory)
    create_iceberg_table(table_name,
                         partition_col_sql="bucket(16, _c2), bucket(16, _c3)",
                         table_prop=table_prop,
                         df_gen=this_gen_df)

    def insert_initial_data(spark, table_name: str):
        cols = [ f"_c{idx}" for idx, _ in enumerate(iceberg_full_gens_list)]
        df = gen_df(spark, list(zip(cols, iceberg_full_gens_list)), seed=INITIAL_INSERT_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")

    def overwrite_data(spark, table_name: str):
        cols = [ f"_c{idx}" for idx, _ in enumerate(iceberg_full_gens_list)]
        df = gen_df(spark, list(zip(cols, iceberg_full_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")

    # Initial insert (not tested for fallback)
    with_cpu_session(lambda spark: insert_initial_data(spark, table_name),
                     conf = iceberg_static_overwrite_conf)

    # Assert that overwrite falls back to CPU
    assert_gpu_fallback_collect(lambda spark: overwrite_data(spark, table_name),
                                "OverwriteByExpressionExec",
                                conf = iceberg_static_overwrite_conf)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('OverwriteByExpressionExec', 'ShuffleExchangeExec', 'SortExec', 'ProjectExec')
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param("_c2", id="identity"),
    pytest.param("bucket(8, _c6)", id="bucket_unsupported_type"),
])
def test_insert_overwrite_partitioned_table_unsupported_partition_fallback(
        spark_tmp_table_factory, partition_col_sql):
    """Test that unsupported partition transforms fall back to CPU."""
    table_prop = {"format-version": "2"}

    def insert_initial_data(spark, table_name: str):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)), seed=INITIAL_INSERT_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")
    
    def overwrite_data(spark, table_name: str):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")

    table_name = get_full_table_name(spark_tmp_table_factory)
    create_iceberg_table(table_name,
                         partition_col_sql=partition_col_sql,
                         table_prop=table_prop)

    # Initial insert (not tested for fallback)
    with_cpu_session(lambda spark: insert_initial_data(spark, table_name),
                     conf = iceberg_static_overwrite_conf)

    # Assert that overwrite falls back to CPU
    assert_gpu_fallback_collect(lambda spark: overwrite_data(spark, table_name),
                                "OverwriteByExpressionExec",
                                conf = iceberg_static_overwrite_conf)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('OverwriteByExpressionExec', 'ShuffleExchangeExec', 'SortExec', 'ProjectExec')
@pytest.mark.parametrize("file_format", ["orc", "avro"], ids=lambda x: f"file_format={x}")
def test_insert_overwrite_table_unsupported_file_format_fallback(
        spark_tmp_table_factory, file_format):
    """Test that unsupported file formats fall back to CPU."""
    table_prop = {"format-version": "2",
                  "write.format.default": file_format}

    def insert_initial_data(spark, table_name: str):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)), seed=INITIAL_INSERT_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")
    
    def overwrite_data(spark, table_name: str):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")

    table_name = get_full_table_name(spark_tmp_table_factory)
    create_iceberg_table(table_name, table_prop=table_prop)

    # Initial insert (not tested for fallback)
    with_cpu_session(lambda spark: insert_initial_data(spark, table_name),
                     conf = iceberg_static_overwrite_conf)

    # Assert that overwrite falls back to CPU
    assert_gpu_fallback_collect(lambda spark: overwrite_data(spark, table_name),
                                "OverwriteByExpressionExec",
                                conf = iceberg_static_overwrite_conf)


@iceberg
@ignore_order(local=True)
@allow_non_gpu('OverwriteByExpressionExec', 'ShuffleExchangeExec', 'SortExec', 'ProjectExec')
@pytest.mark.parametrize("conf_key", ["spark.rapids.sql.format.iceberg.enabled",
                                      "spark.rapids.sql.format.iceberg.write.enabled"],
                         ids=lambda x: f"{x}=False")
def test_insert_overwrite_iceberg_table_fallback_when_conf_disabled(
        spark_tmp_table_factory, conf_key):
    """Test that overwrite falls back to CPU when Iceberg write is disabled."""
    table_prop = {"format-version": "2"}

    def insert_initial_data(spark, table_name: str):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)), seed=INITIAL_INSERT_SEED)
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {view_name}")
    
    def overwrite_data(spark, table_name: str):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        view_name = spark_tmp_table_factory.get()
        df.createOrReplaceTempView(view_name)
        return spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")

    table_name = get_full_table_name(spark_tmp_table_factory)
    create_iceberg_table(table_name, table_prop=table_prop)

    # Initial insert (not tested for fallback)
    with_cpu_session(lambda spark: insert_initial_data(spark, table_name),
                     conf = iceberg_static_overwrite_conf)

    # Assert that overwrite falls back to CPU when config is disabled
    updated_conf = copy_and_update(iceberg_static_overwrite_conf, {conf_key: "false"})
    assert_gpu_fallback_collect(lambda spark: overwrite_data(spark, table_name),
                                "OverwriteByExpressionExec",
                                conf = updated_conf)

