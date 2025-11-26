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

import pytest

from asserts import assert_equal_with_local_sort, assert_gpu_fallback_write_sql
from data_gen import *
from iceberg import (create_iceberg_table, get_full_table_name, iceberg_write_enabled_conf,
                     iceberg_base_table_cols, iceberg_gens_list, iceberg_full_gens_list, rapids_reader_types)
from marks import allow_non_gpu, iceberg, ignore_order
from spark_session import is_spark_35x, with_cpu_session, with_gpu_session

pytestmark = pytest.mark.skipif(not is_spark_35x(),
                                reason="Current spark-rapids only support spark 3.5.x")

# Configuration for copy-on-write UPDATE operations
iceberg_update_cow_enabled_conf = copy_and_update(iceberg_write_enabled_conf, {})

# Fixed seed for reproducible test data. Iceberg's update test plan will be different with different data and filter.
UPDATE_TEST_SEED = 42
UPDATE_TEST_SEED_OVERRIDE_REASON = "Ensure reproducible test data for UPDATE operations"

def create_iceberg_table_with_data(table_name: str, 
                                   partition_col_sql=None,
                                   data_gen_func=None,
                                   table_properties=None,
                                   update_mode='copy-on-write'):
    """Helper function to create and populate an Iceberg table for UPDATE tests.

    Args:
        table_name: Name of the table to create
        partition_col_sql: SQL for partitioning clause
        data_gen_func: Function to generate test data
        table_properties: Additional table properties
        update_mode: Update mode - 'copy-on-write' or 'merge-on-read'
    """
    base_props = {
        'format-version': '2',
        'write.update.mode': update_mode
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

def do_update_test(spark_tmp_table_factory, update_sql_func, data_gen_func=None, 
                  partition_col_sql=None, table_properties=None, reader_type='COALESCING',
                  update_mode='copy-on-write'):
    """
    Helper function to test UPDATE operations by comparing CPU and GPU results.
    
    Args:
        spark_tmp_table_factory: Factory for generating unique table names
        update_sql_func: Function that takes (spark, table_name) and executes UPDATE
        data_gen_func: Function to generate test data
        partition_col_sql: SQL for partitioning clause
        table_properties: Additional table properties
        reader_type: Rapids reader type for parquet reading
        update_mode: Update mode - 'copy-on-write' or 'merge-on-read'
    """
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    cpu_table_name = f"{base_table_name}_cpu"
    gpu_table_name = f"{base_table_name}_gpu"
    
    # Create identical tables for CPU and GPU
    create_iceberg_table_with_data(cpu_table_name, partition_col_sql, 
                                   data_gen_func, table_properties, update_mode)
    create_iceberg_table_with_data(gpu_table_name, partition_col_sql, 
                                   data_gen_func, table_properties, update_mode)
    
    # Merge reader_type into configuration
    test_conf = copy_and_update(iceberg_update_cow_enabled_conf, {
        "spark.rapids.sql.format.parquet.reader.type": reader_type
    })
    
    # Execute UPDATE on GPU
    def do_gpu_update(spark):
        update_sql_func(spark, gpu_table_name)
        
    with_gpu_session(do_gpu_update, conf=test_conf)
    
    # Execute UPDATE on CPU
    def do_cpu_update(spark):
        update_sql_func(spark, cpu_table_name)
        
    with_cpu_session(do_cpu_update)
    
    # Compare results
    cpu_data = with_cpu_session(lambda spark: spark.table(cpu_table_name).collect())
    gpu_data = with_cpu_session(lambda spark: spark.table(gpu_table_name).collect())
    assert_equal_with_local_sort(cpu_data, gpu_data)


@allow_non_gpu("BatchScanExec", "ColumnarToRowExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=UPDATE_TEST_SEED, reason=UPDATE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize('update_mode', ['copy-on-write', 'merge-on-read'])
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_update_unpartitioned_table_single_column(spark_tmp_table_factory, reader_type, update_mode):
    """Test UPDATE on unpartitioned table with single column update"""
    do_update_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"UPDATE {table} SET _c2 = _c2 + 100 WHERE _c2 % 3 = 0"),
        reader_type=reader_type,
        update_mode=update_mode
    )

@allow_non_gpu("BatchScanExec", "ColumnarToRowExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=UPDATE_TEST_SEED, reason=UPDATE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize('update_mode', ['copy-on-write', 'merge-on-read'])
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_update_unpartitioned_table_multiple_columns(spark_tmp_table_factory, reader_type, update_mode):
    """Test UPDATE on unpartitioned table with multiple column updates"""
    do_update_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"UPDATE {table} SET _c2 = _c2 + 100, _c6 = 'updated' WHERE _c2 % 3 = 0"),
        reader_type=reader_type,
        update_mode=update_mode
    )

@allow_non_gpu("BatchScanExec", "ColumnarToRowExec", "ShuffleExchangeExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=UPDATE_TEST_SEED, reason=UPDATE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize('update_mode', ['copy-on-write', 'merge-on-read'])
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param("bucket(16, _c2)", id="bucket(16, int_col)"),
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
    pytest.param("truncate(10, _c10)", id="truncate(10, decimal32_col)"),
    pytest.param("truncate(10, _c11)", id="truncate(10, decimal64_col)"),
    pytest.param("truncate(10, _c12)", id="truncate(10, decimal128_col)"),
])
def test_iceberg_update_partitioned_table_single_column(spark_tmp_table_factory, reader_type, update_mode, partition_col_sql):
    """Test UPDATE on bucket-partitioned table with single column update"""
    do_update_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"UPDATE {table} SET _c2 = _c2 + 100 WHERE _c2 % 3 = 0"),
        partition_col_sql=partition_col_sql,
        reader_type=reader_type,
        update_mode=update_mode
    )

@allow_non_gpu("BatchScanExec", "ColumnarToRowExec", "ShuffleExchangeExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=UPDATE_TEST_SEED, reason=UPDATE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize('update_mode', ['copy-on-write', 'merge-on-read'])
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_update_partitioned_table_multiple_columns(spark_tmp_table_factory, reader_type, update_mode):
    """Test UPDATE on bucket-partitioned table with multiple column updates"""
    do_update_test(
        spark_tmp_table_factory,
        lambda spark, table: spark.sql(f"UPDATE {table} SET _c2 = _c2 + 100, _c6 = 'updated' WHERE _c2 % 3 = 0"),
        partition_col_sql="bucket(16, _c2)",
        reader_type=reader_type,
        update_mode=update_mode
    )


@allow_non_gpu("ReplaceDataExec", "WriteDeltaExec", "BatchScanExec", "ColumnarToRowExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=UPDATE_TEST_SEED, reason=UPDATE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize('update_mode,fallback_exec', [
    pytest.param('copy-on-write', 'ReplaceDataExec', id='cow'),
    pytest.param('merge-on-read', 'WriteDeltaExec', id='mor')
])
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_update_fallback_write_disabled(spark_tmp_table_factory, reader_type, update_mode, fallback_exec):
    """Test UPDATE falls back when Iceberg write is disabled"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    # Phase 1: Initialize tables with data (separate for CPU and GPU)
    cpu_table_name = f'{base_table_name}_cpu'
    gpu_table_name = f'{base_table_name}_gpu'
    create_iceberg_table_with_data(cpu_table_name, update_mode=update_mode)
    create_iceberg_table_with_data(gpu_table_name, update_mode=update_mode)
    
    # Phase 2: UPDATE operation (to be tested with fallback)
    def write_func(spark, table_name):
        spark.sql(f"UPDATE {table_name} SET _c2 = _c2 + 100 WHERE _c2 % 3 = 0")
    
    # Read function to verify results
    def read_func(spark, table_name):
        return spark.sql(f"SELECT * FROM {table_name}")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name,
        [fallback_exec],
        conf=copy_and_update(iceberg_update_cow_enabled_conf, {
            "spark.rapids.sql.format.iceberg.write.enabled": "false",
            "spark.rapids.sql.format.parquet.reader.type": reader_type
        })
    )

@allow_non_gpu("ReplaceDataExec", "WriteDeltaExec", "BatchScanExec", "ColumnarToRowExec", "ShuffleExchangeExec", "SortExec", "ProjectExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=UPDATE_TEST_SEED, reason=UPDATE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize('update_mode,fallback_exec', [
    pytest.param('copy-on-write', 'ReplaceDataExec', id='cow'),
    pytest.param('merge-on-read', 'WriteDeltaExec', id='mor')
])
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("partition_col_sql", [
    pytest.param("_c2", id="identity"),
    pytest.param("bucket(8, _c6)", id="bucket_unsupported_type"),
])
def test_iceberg_update_fallback_unsupported_partition_transform(spark_tmp_table_factory, reader_type, partition_col_sql, update_mode, fallback_exec):
    """Test UPDATE falls back with unsupported partition transforms"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    def data_gen(spark):
        return gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
    
    # Phase 1: Initialize tables with data (separate for CPU and GPU)
    def init_table(table_name):
        table_props = {
            'format-version': '2',
            'write.update.mode': update_mode
        }
        
        create_iceberg_table(table_name,
                            partition_col_sql=partition_col_sql,
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
    
    # Phase 2: UPDATE operation (to be tested with fallback)
    def write_func(spark, table_name):
        spark.sql(f"UPDATE {table_name} SET _c2 = _c2 + 100 WHERE _c2 % 3 = 0")
    
    # Read function to verify results
    def read_func(spark, table_name):
        return spark.sql(f"SELECT * FROM {table_name}")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name,
        [fallback_exec],
        conf=copy_and_update(iceberg_update_cow_enabled_conf, {
            "spark.rapids.sql.format.parquet.reader.type": reader_type
        })
    )

@allow_non_gpu("ReplaceDataExec", "WriteDeltaExec", "BatchScanExec", "ColumnarToRowExec", "ShuffleExchangeExec", "ProjectExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=UPDATE_TEST_SEED, reason=UPDATE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize('update_mode,fallback_exec', [
    pytest.param('copy-on-write', 'ReplaceDataExec', id='cow'),
    pytest.param('merge-on-read', 'WriteDeltaExec', id='mor')
])
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("file_format", ["orc", "avro"], ids=lambda x: f"file_format={x}")
def test_iceberg_update_fallback_unsupported_file_format(spark_tmp_table_factory, reader_type, file_format, update_mode, fallback_exec):
    """Test UPDATE falls back with unsupported file formats (ORC, Avro)
    
    This test creates a table with parquet format, inserts data, then changes the
    default write format to an unsupported format. When UPDATE is executed, it needs
    to write new files in the unsupported format, which should trigger a fallback.
    """
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    def data_gen(spark):
        return gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
    
    # Phase 1: Initialize tables with data (separate for CPU and GPU)
    def init_table(table_name):
        # Step 1: Create table with parquet as default write format
        table_props = {
            'format-version': '2',
            'write.update.mode': update_mode,
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
    
    # Phase 2: UPDATE operation (to be tested with fallback)
    def write_func(spark, table_name):
        spark.sql(f"UPDATE {table_name} SET _c2 = _c2 + 100 WHERE _c2 % 3 = 0")
    
    # Read function to verify results
    def read_func(spark, table_name):
        # We should select 1 here to avoid throwing exception for unsupported file format
        return spark.sql(f"SELECT 1")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name,
        [fallback_exec],
        conf=copy_and_update(iceberg_update_cow_enabled_conf, {
            "spark.rapids.sql.format.parquet.reader.type": reader_type
        })
    )

@allow_non_gpu("ReplaceDataExec", "WriteDeltaExec", "BatchScanExec", "ColumnarToRowExec", "ShuffleExchangeExec", "ExpandExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=UPDATE_TEST_SEED, reason=UPDATE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize('update_mode,fallback_exec', [
    pytest.param('copy-on-write', 'ReplaceDataExec', id='cow'),
    pytest.param('merge-on-read', 'WriteDeltaExec', id='mor')
])
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_update_fallback_nested_types(spark_tmp_table_factory, reader_type, update_mode, fallback_exec):
    """Test UPDATE falls back with nested types (arrays, structs, maps) - currently unsupported"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    # Use table with all iceberg types including nested types (arrays, structs, maps)
    def data_gen(spark):
        cols = [f"_c{idx}" for idx, _ in enumerate(iceberg_full_gens_list)]
        return gen_df(spark, list(zip(cols, iceberg_full_gens_list)))
    
    # Phase 1: Initialize tables with data (separate for CPU and GPU)
    def init_table(table_name):
        table_props = {
            'format-version': '2',
            'write.update.mode': update_mode
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
    
    # Phase 2: UPDATE operation (to be tested with fallback)
    # Use _c2 (IntegerGen) for UPDATE condition as it's a simple type
    def write_func(spark, table_name):
        spark.sql(f"UPDATE {table_name} SET _c2 = _c2 + 100 WHERE _c2 % 3 = 0")
    
    # Read function to verify results
    def read_func(spark, table_name):
        return spark.sql(f"SELECT * FROM {table_name}")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name,
        [fallback_exec],
        conf=copy_and_update(iceberg_update_cow_enabled_conf, {
            "spark.rapids.sql.format.parquet.reader.type": reader_type
        })
    )

@allow_non_gpu("ReplaceDataExec", "WriteDeltaExec", "BatchScanExec", "ColumnarToRowExec")
@iceberg
@ignore_order(local=True)
@pytest.mark.datagen_overrides(seed=UPDATE_TEST_SEED, reason=UPDATE_TEST_SEED_OVERRIDE_REASON)
@pytest.mark.parametrize('update_mode,fallback_exec', [
    pytest.param('copy-on-write', 'ReplaceDataExec', id='cow'),
    pytest.param('merge-on-read', 'WriteDeltaExec', id='mor')
])
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_update_fallback_iceberg_disabled(spark_tmp_table_factory, reader_type, update_mode, fallback_exec):
    """Test UPDATE falls back when Iceberg is completely disabled"""
    base_table_name = get_full_table_name(spark_tmp_table_factory)
    
    # Phase 1: Initialize tables with data (separate for CPU and GPU)
    cpu_table_name = f'{base_table_name}_cpu'
    gpu_table_name = f'{base_table_name}_gpu'
    create_iceberg_table_with_data(cpu_table_name, update_mode=update_mode)
    create_iceberg_table_with_data(gpu_table_name, update_mode=update_mode)
    
    # Phase 2: UPDATE operation (to be tested with fallback)
    def write_func(spark, table_name):
        spark.sql(f"UPDATE {table_name} SET _c2 = _c2 + 100 WHERE _c2 % 3 = 0")
    
    # Read function to verify results
    def read_func(spark, table_name):
        return spark.sql(f"SELECT * from {table_name}")
    
    assert_gpu_fallback_write_sql(
        write_func,
        read_func,
        base_table_name,
        [fallback_exec],
        conf=copy_and_update(iceberg_update_cow_enabled_conf, {
            "spark.rapids.sql.format.iceberg.enabled": "false",
            "spark.rapids.sql.format.parquet.reader.type": reader_type
        })
    )

