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

import pytest

from asserts import assert_equal_with_local_sort, assert_gpu_fallback_write_sql
from conftest import is_iceberg_remote_catalog
from data_gen import *
from iceberg import (create_iceberg_table, get_full_table_name, iceberg_write_enabled_conf,
                     iceberg_base_table_cols, iceberg_gens_list, iceberg_full_gens_list)
from marks import allow_non_gpu, iceberg, ignore_order, datagen_overrides
from spark_session import is_spark_35x, with_cpu_session, with_gpu_session

pytestmark = pytest.mark.skipif(not is_spark_35x(),
                                reason="Current spark-rapids only support spark 3.5.x")

# Configuration for copy-on-write DELETE operations
iceberg_delete_cow_enabled_conf = copy_and_update(iceberg_write_enabled_conf, {})

# Configuration for merge-on-read DELETE operations
iceberg_delete_mor_enabled_conf = copy_and_update(iceberg_write_enabled_conf, {})

# Fixed seed for reproducible test data. Iceberg's delete test plan will be different with different data and filter. For example,
# if deleted data exactly match some data files, we could remove all files using delete metadata only operation, then the physical plan 
# would be DeleteFromTableExec.
DELETE_TEST_SEED = 42
DELETE_TEST_SEED_OVERRIDE_REASON = "Ensure reproducible test data for DELETE operations"

def create_iceberg_table_with_data(table_name: str, 
                                   partition_col_sql=None,
                                   data_gen_func=None,
                                   table_properties=None,
                                   delete_mode='copy-on-write'):
    """Helper function to create and populate an Iceberg table for DELETE tests."""
    # Default to copy-on-write mode, but allow override for merge-on-read tests
    base_props = {
        'format-version': '2',
        'write.delete.mode': delete_mode
    }
    if table_properties:
        base_props.update(table_properties)
    
    # Use standard iceberg table definition if no custom generator provided
    if data_gen_func is None:
        data_gen_func = lambda spark: gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
    
    create_iceberg_table(table_name, 
                        partition_col_sql=partition_col_sql,
                        table_prop=base_props,
                        df_gen=data_gen_func)
    
    # Insert data
    def insert_data(spark):
        df = data_gen_func(spark)
        df.writeTo(table_name).append()
    
    with_cpu_session(insert_data)

def do_delete_test(spark_tmp_table_factory, delete_sql_func, data_gen_func=None, 
                  partition_col_sql=None, table_properties=None,
                  delete_mode='copy-on-write'):
    """
    Helper function to test DELETE operations by comparing CPU and GPU results.
    
    Args:
        spark_tmp_table_factory: Factory for generating unique table names
        delete_sql_func: Function that takes (spark, table_name) and executes DELETE
        data_gen_func: Function to generate test data
        partition_col_sql: SQL for partitioning clause
        table_properties: Additional table properties
        delete_mode: 'copy-on-write' or 'merge-on-read'
    """
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"
    
    # Create identical tables for CPU and GPU
    create_iceberg_table_with_data(cpu_table_name, partition_col_sql, 
                                   data_gen_func, table_properties, delete_mode)
    create_iceberg_table_with_data(gpu_table_name, partition_col_sql, 
                                   data_gen_func, table_properties, delete_mode)
    
    # Execute DELETE on GPU
    def do_gpu_delete(spark):
        delete_sql_func(spark, gpu_table_name)
        
    with_gpu_session(do_gpu_delete, conf=iceberg_delete_cow_enabled_conf)
    
    # Execute DELETE on CPU
    def do_cpu_delete(spark):
        delete_sql_func(spark, cpu_table_name)
        
    with_cpu_session(do_cpu_delete)
    
    # Compare results
    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


# This requires reading of _partition field, which is a struct
@allow_non_gpu("ColumnarToRowExec", "BatchScanExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize('delete_mode', ['copy-on-write', 'merge-on-read'])
def test_iceberg_delete_unpartitioned_table(spark_tmp_table_factory, delete_mode):
    """Test DELETE on unpartitioned table with both copy-on-write and merge-on-read modes"""
    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"DELETE FROM {table} WHERE _c2 % 3 = 0"),
        delete_mode=delete_mode
    )

def _do_test_iceberg_delete_partitioned_table(spark_tmp_table_factory, partition_col_sql, delete_mode):
    """Helper function for partitioned table DELETE tests."""
    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"DELETE FROM {table} WHERE _c2 % 3 = 0"),
        partition_col_sql=partition_col_sql,
        delete_mode=delete_mode
    )


# This requires reading of _partition field, which is a struct
@allow_non_gpu("ColumnarToRowExec", "BatchScanExec")
@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param("year(_c9)", id="year(timestamp_col)"),
])
@pytest.mark.parametrize('delete_mode', ['copy-on-write', 'merge-on-read'])
def test_iceberg_delete_partitioned_table(spark_tmp_table_factory, partition_col_sql, delete_mode):
    """Basic partition test - runs for all catalogs including remote."""
    _do_test_iceberg_delete_partitioned_table(spark_tmp_table_factory, partition_col_sql, delete_mode)


# This requires reading of _partition field, which is a struct
@allow_non_gpu("ColumnarToRowExec", "BatchScanExec")
@iceberg
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids-jni/issues/4016')
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
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
@pytest.mark.parametrize('delete_mode', ['copy-on-write', 'merge-on-read'])
def test_iceberg_delete_partitioned_table_full_coverage(spark_tmp_table_factory, partition_col_sql, delete_mode):
    """Full partition coverage test - skipped for remote catalogs."""
    _do_test_iceberg_delete_partitioned_table(spark_tmp_table_factory, partition_col_sql, delete_mode)

@allow_non_gpu("ColumnarToRowExec", "BatchScanExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize('delete_mode', ['copy-on-write', 'merge-on-read'])
def test_iceberg_delete_with_complex_predicate(spark_tmp_table_factory, delete_mode):
    """Test DELETE with complex predicate"""
    do_delete_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(
            f"DELETE FROM {table} WHERE _c2 > 100 AND _c3 < 50 OR _c1 IS NULL"
        ),
        delete_mode=delete_mode
    )


@allow_non_gpu("ReplaceDataExec", "WriteDeltaExec", "BatchScanExec", "ShuffleExchangeExec", "SortExec", "ProjectExec", "ColumnarToRowExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize('delete_mode,fallback_exec', [
    pytest.param('copy-on-write', 'ReplaceDataExec', id='cow'),
    pytest.param('merge-on-read', 'WriteDeltaExec', id='mor')
])
def test_iceberg_delete_fallback_write_disabled(spark_tmp_table_factory, delete_mode, fallback_exec):
    """Test DELETE falls back when Iceberg write is disabled (both modes)"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    # Phase 1: Initialize tables with data (separate for CPU and GPU)
    cpu_table_name = f'{base_table_name}_cpu'
    gpu_table_name = f'{base_table_name}_gpu'
    create_iceberg_table_with_data(cpu_table_name, delete_mode=delete_mode)
    create_iceberg_table_with_data(gpu_table_name, delete_mode=delete_mode)
    
    # Phase 2: DELETE operation (to be tested with fallback)
    def write_func(spark, table_name):
        spark.sql(f"DELETE FROM {table_name} WHERE _c2 % 3 = 0")
    
    # Read function to verify results
    def read_func(spark, table_name):
        return spark.sql(f"SELECT * FROM {table_name}")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name,
        [fallback_exec],
        conf=copy_and_update(iceberg_delete_cow_enabled_conf, {
            "spark.rapids.sql.format.iceberg.write.enabled": "false"
        })
    )

@allow_non_gpu("ReplaceDataExec", "WriteDeltaExec", "BatchScanExec", "ShuffleExchangeExec", "SortExec", "ProjectExec", "ColumnarToRowExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize('delete_mode,fallback_exec', [
    pytest.param('copy-on-write', 'ReplaceDataExec', id='cow'),
    pytest.param('merge-on-read', 'WriteDeltaExec', id='mor')
])
@pytest.mark.parametrize("file_format", ["orc", "avro"], ids=lambda x: f"file_format={x}")
def test_iceberg_delete_fallback_unsupported_file_format(spark_tmp_table_factory, delete_mode, fallback_exec, file_format):
    """Test DELETE falls back with unsupported file formats (ORC, Avro) for both modes"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    def data_gen(spark):
        return gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
    
    # Phase 1: Initialize tables with data (separate for CPU and GPU)
    def init_table(table_name):
        # Step 1: Create table with parquet as default write format
        table_props = {
            'format-version': '2',
            'write.delete.mode': delete_mode,
            'write.format.default': 'parquet'
        }
        
        create_iceberg_table(table_name,
                            table_prop=table_props,
                            df_gen=data_gen)
        
        # Step 2: Insert data into the table (creates parquet files)
        def insert_data(spark):
            df = data_gen(spark)
            df.writeTo(table_name).append()
        
        with_cpu_session(insert_data)
        
        # Step 3: Change default write format to unsupported format
        def change_format(spark):
            spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('write.format.default' = '{file_format}')")
        
        with_cpu_session(change_format)
    
    # Initialize both CPU and GPU tables
    cpu_table_name = f'{base_table_name}_cpu'
    gpu_table_name = f'{base_table_name}_gpu'
    init_table(cpu_table_name)
    init_table(gpu_table_name)
    
    # Phase 2: DELETE operation (to be tested with fallback)
    def write_func(spark, table_name):
        spark.sql(f"DELETE FROM {table_name} WHERE _c2 % 3 = 0")
    
    # Read function to verify results
    def read_func(spark, table_name):
        # We should select 1 here to avoid throwing exception for unsupported file format
        return spark.sql(f"SELECT 1")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name,
        [fallback_exec],
        conf=iceberg_delete_cow_enabled_conf
    )

@allow_non_gpu("ReplaceDataExec", "WriteDeltaExec", "BatchScanExec", "ShuffleExchangeExec", "SortExec", "ProjectExec", "ColumnarToRowExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize('delete_mode,fallback_exec', [
    pytest.param('copy-on-write', 'ReplaceDataExec', id='cow'),
    pytest.param('merge-on-read', 'WriteDeltaExec', id='mor')
])
def test_iceberg_delete_fallback_nested_types(spark_tmp_table_factory, delete_mode, fallback_exec):
    """Test DELETE falls back with nested types (arrays, structs, maps) - currently unsupported"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)

    # Use table with all iceberg types including nested types (arrays, structs, maps)
    def data_gen(spark):
        cols = [f"_c{idx}" for idx, _ in enumerate(iceberg_full_gens_list)]
        return gen_df(spark, list(zip(cols, iceberg_full_gens_list)))

    # Phase 1: Initialize tables with data (separate for CPU and GPU)
    def init_table(table_name):
        table_props = {
            'format-version': '2',
            'write.delete.mode': delete_mode
        }

        create_iceberg_table(table_name,
                             table_prop=table_props,
                             df_gen=data_gen)

        def insert_data(spark):
            df = data_gen(spark)
            df.writeTo(table_name).append()

        with_cpu_session(insert_data)

    # Initialize both CPU and GPU tables
    cpu_table_name = f'{base_table_name}_cpu'
    gpu_table_name = f'{base_table_name}_gpu'
    init_table(cpu_table_name)
    init_table(gpu_table_name)

    # Phase 2: DELETE operation (to be tested with fallback)
    # Use _c2 (IntegerGen) for DELETE condition as it's a simple type
    def write_func(spark, table_name):
        spark.sql(f"DELETE FROM {table_name} WHERE _c2 % 3 = 0")

    # Read function to verify results
    def read_func(spark, table_name):
        return spark.sql(f"SELECT * FROM {table_name}")

    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name,
        fallback_exec,
        conf=iceberg_delete_cow_enabled_conf
    )


@allow_non_gpu("ReplaceDataExec", "WriteDeltaExec", "BatchScanExec", "ShuffleExchangeExec", "SortExec", "ProjectExec", "ColumnarToRowExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.parametrize('delete_mode,fallback_exec', [
    pytest.param('copy-on-write', 'ReplaceDataExec', id='cow'),
    pytest.param('merge-on-read', 'WriteDeltaExec', id='mor')
])
@pytest.mark.xfail(reason="https://github.com/NVIDIA/spark-rapids/issues/13649")
def test_iceberg_delete_fallback_iceberg_disabled(spark_tmp_table_factory, delete_mode, fallback_exec):
    """Test DELETE falls back when Iceberg is completely disabled (both modes)"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    # Phase 1: Initialize tables with data (separate for CPU and GPU)
    cpu_table_name = f'{base_table_name}_cpu'
    gpu_table_name = f'{base_table_name}_gpu'
    create_iceberg_table_with_data(cpu_table_name, delete_mode=delete_mode)
    create_iceberg_table_with_data(gpu_table_name, delete_mode=delete_mode)
    
    # Phase 2: DELETE operation (to be tested with fallback)
    def write_func(spark, table_name):
        spark.sql(f"DELETE FROM {table_name} WHERE _c2 % 3 = 0")
    
    # Read function to verify results
    def read_func(spark, table_name):
        return spark.sql(f"SELECT * from {table_name}")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name,
        [fallback_exec],
        conf=copy_and_update(iceberg_delete_cow_enabled_conf, {
            "spark.rapids.sql.format.iceberg.enabled": "false"
        })
    )

@allow_non_gpu("WriteDeltaExec", "BatchScanExec", "ShuffleExchangeExec", "SortExec", "ProjectExec", "ColumnarToRowExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
def test_iceberg_delete_mor_fallback_writedelta_disabled(spark_tmp_table_factory):
    """Test merge-on-read DELETE falls back when WriteDeltaExec is disabled
    
    This test verifies that when WriteDeltaExec is explicitly disabled (it's disabled by default
    as experimental), merge-on-read DELETE operations correctly fallback to CPU execution.
    """
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    # Phase 1: Initialize tables with data (separate for CPU and GPU)
    cpu_table_name = f'{base_table_name}_cpu'
    gpu_table_name = f'{base_table_name}_gpu'
    create_iceberg_table_with_data(cpu_table_name, delete_mode='merge-on-read')
    create_iceberg_table_with_data(gpu_table_name, delete_mode='merge-on-read')
    
    # Phase 2: DELETE operation (to be tested with fallback)
    def write_func(spark, table_name):
        spark.sql(f"DELETE FROM {table_name} WHERE _c2 % 3 = 0")
    
    # Read function to verify results
    def read_func(spark, table_name):
        return spark.sql(f"SELECT * FROM {table_name}")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name,
        ['WriteDeltaExec'],
        conf=copy_and_update(iceberg_delete_cow_enabled_conf, {
            "spark.rapids.sql.exec.WriteDeltaExec": "false"
        })
    )



@allow_non_gpu("BatchScanExec", "ColumnarToRowExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize('update_mode', ['copy-on-write', 'merge-on-read'])
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param(None, id="unpartitioned"),
    pytest.param("year(_c9)", id="year_partition"),
])
def test_delete_aqe(spark_tmp_table_factory, update_mode, partition_col_sql):
    """
    Test DELETE with AQE enabled.
    """
    table_prop = {
        'format-version': '2',
        'write.delete.mode': update_mode
    }

    # Configuration with AQE enabled
    conf = copy_and_update(iceberg_write_enabled_conf, {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    })

    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table = f"{base_table_name}_cpu"
    gpu_table = f"{base_table_name}_gpu"

    def initialize_table(table_name):
        df_gen = lambda spark: gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        create_iceberg_table(table_name, partition_col_sql, table_prop, df_gen)

    with_cpu_session(lambda spark: initialize_table(cpu_table))
    with_cpu_session(lambda spark: initialize_table(gpu_table))

    def delete_from_table(spark, table_name):
        spark.sql(f"DELETE FROM {table_name} WHERE _c2 % 3 = 0")

    with_gpu_session(lambda spark: delete_from_table(spark, gpu_table), conf=conf)
    with_cpu_session(lambda spark: delete_from_table(spark, cpu_table), conf=conf)

    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@allow_non_gpu("BatchScanExec", "ColumnarToRowExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason="Skip for remote catalog to reduce test time")
@pytest.mark.datagen_overrides(seed=DELETE_TEST_SEED, reason=DELETE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize('delete_mode', ['copy-on-write', 'merge-on-read'])
def test_iceberg_delete_after_drop_partition_field(spark_tmp_table_factory, delete_mode):
    """Test DELETE on table after dropping a partition field (void transform).
    
    When a partition field is dropped, Iceberg creates a 'void transform' - 
    the field remains in the partition spec but no longer affects partitioning.
    This test verifies DELETE still runs correctly on GPU after partition evolution.
    """
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"
    
    # Use two partition columns so after dropping one, we still have at least one
    partition_col_sql = "bucket(8, _c2), bucket(8, _c3)"
    
    # Create partitioned tables with data
    create_iceberg_table_with_data(cpu_table_name, partition_col_sql=partition_col_sql, 
                                   delete_mode=delete_mode)
    create_iceberg_table_with_data(gpu_table_name, partition_col_sql=partition_col_sql, 
                                   delete_mode=delete_mode)
    
    # Drop one partition field on both tables (creates void transform)
    def drop_partition_field(spark, table_name):
        spark.sql(f"ALTER TABLE {table_name} DROP PARTITION FIELD bucket(8, _c2)")
    
    with_cpu_session(lambda spark: drop_partition_field(spark, cpu_table_name))
    with_cpu_session(lambda spark: drop_partition_field(spark, gpu_table_name))
    
    # Execute DELETE after partition evolution - this is the operation we're testing on GPU
    def do_delete(spark, table_name):
        spark.sql(f"DELETE FROM {table_name} WHERE _c2 % 3 = 0")
    
    with_gpu_session(lambda spark: do_delete(spark, gpu_table_name), 
                     conf=iceberg_delete_cow_enabled_conf)
    with_cpu_session(lambda spark: do_delete(spark, cpu_table_name))
    
    # Compare results
    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)
