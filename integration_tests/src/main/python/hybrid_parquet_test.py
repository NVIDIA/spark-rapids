# Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

from asserts import *
from data_gen import *
from marks import *
from parquet_test import rebase_write_corrected_conf
from spark_session import *
import pyspark.sql.functions as f

"""
Hybrid Scan unsupported types:
1. Decimal with negative scale is NOT supported
2. Decimal128 inside nested types is NOT supported
3. BinaryType is NOT supported
4. MapType wrapped by NestedType (Struct of Map/Array of Map/Map of Map) is NOT fully supported
"""
parquet_gens_list = [
    [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
     string_gen, boolean_gen, date_gen,
     TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc)), ArrayGen(byte_gen),
     ArrayGen(long_gen), ArrayGen(string_gen), ArrayGen(date_gen),
     ArrayGen(TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))),
     ArrayGen(decimal_gen_64bit),
     ArrayGen(ArrayGen(byte_gen)),
     StructGen([['child0', ArrayGen(byte_gen)],
                ['child1', byte_gen],
                ['child2', float_gen],
                ['child3', decimal_gen_64bit]]),
     ArrayGen(StructGen([['child0', string_gen],
                         ['child1', double_gen],
                         ['child2', int_gen]]))
     ],
    [MapGen(f(nullable=False), f()) for f in [
        BooleanGen, ByteGen, ShortGen, IntegerGen, LongGen, FloatGen, DoubleGen, DateGen,
        lambda nullable=True: TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc), nullable=nullable)]
     ],
    [simple_string_to_string_map_gen,
     MapGen(StringGen(pattern='key_[0-9]', nullable=False), ArrayGen(string_gen), max_length=10),
     MapGen(RepeatSeqGen(IntegerGen(nullable=False), 10), long_gen, max_length=10),
     ],
    decimal_gens,
]

parquet_gens_fallback_lists = [
    # Decimal128 inside nested types is NOT supported
    [MapGen(StringGen(pattern='key_[0-9]', nullable=False), decimal_gen_128bit)],
    # BinaryType is NOT supported
    [BinaryGen()],
    # MapType wrapped by NestedType is NOT fully supported
    [MapGen(StringGen(pattern='key_[0-9]', nullable=False), simple_string_to_string_map_gen)],
    [ArrayGen(simple_string_to_string_map_gen)],
    [ArrayGen(ArrayGen(simple_string_to_string_map_gen))],
    [ArrayGen(StructGen([["c0", simple_string_to_string_map_gen]]))],
    [StructGen([["c0", simple_string_to_string_map_gen]])],
    [StructGen([["c0", ArrayGen(simple_string_to_string_map_gen)]])],
    [StructGen([["c0", StructGen([["cc0", simple_string_to_string_map_gen]])]])],
    [],
]


@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@pytest.mark.parametrize('parquet_gens', parquet_gens_list, ids=idfn)
@pytest.mark.parametrize('gen_rows', [20, 100, 512, 1024, 4096], ids=idfn)
@hybrid_test
def test_hybrid_parquet_read_round_trip(spark_tmp_path, parquet_gens, gen_rows):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list, length=gen_rows).write.parquet(data_path),
        conf=rebase_write_corrected_conf)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        conf={
            'spark.sql.sources.useV1SourceList': 'parquet',
            'spark.rapids.sql.hybrid.parquet.enabled': 'true',
        })


# Creating scenarios in which CoalesceConverter will coalesce several input batches by adjusting
# reader_batch_size and coalesced_batch_size, tests if the CoalesceConverter functions correctly
# when coalescing is needed.
@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@pytest.mark.parametrize('parquet_gens', parquet_gens_list, ids=idfn)
@pytest.mark.parametrize('reader_batch_size', [512, 1024, 2048], ids=idfn)
@pytest.mark.parametrize('coalesced_batch_size', [1 << 25, 1 << 27], ids=idfn)
@pytest.mark.parametrize('gen_rows', [8192, 10000], ids=idfn)
@hybrid_test
def test_hybrid_parquet_read_round_trip_multiple_batches(spark_tmp_path,
                                                         parquet_gens,
                                                         reader_batch_size,
                                                         coalesced_batch_size,
                                                         gen_rows):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list, length=gen_rows).write.parquet(data_path),
        conf=rebase_write_corrected_conf)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        conf={
            'spark.sql.sources.useV1SourceList': 'parquet',
            'spark.rapids.sql.hybrid.parquet.enabled': 'true',
            'spark.gluten.sql.columnar.maxBatchSize': reader_batch_size,
            'spark.rapids.sql.batchSizeBytes': coalesced_batch_size,
        })


# HybridScan shall NOT be enabled over unsupported data types. Instead, fallbacks to GpuScan.
@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@pytest.mark.parametrize('parquet_gens', parquet_gens_fallback_lists, ids=idfn)
@hybrid_test
def test_hybrid_parquet_read_fallback_to_gpu(spark_tmp_path, parquet_gens):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    # check the fallback over empty schema(`SELECT COUNT(1)`) within the same case
    if len(parquet_gens) == 0:
        with_cpu_session(
            lambda spark: gen_df(spark, [('a', int_gen)], length=512).write.parquet(data_path),
            conf=rebase_write_corrected_conf)
        read_fn = lambda spark: spark.read.parquet(data_path).selectExpr('count(1)')
    else:
        gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
        with_cpu_session(
            lambda spark: gen_df(spark, gen_list, length=512).write.parquet(data_path),
            conf=rebase_write_corrected_conf)
        read_fn = lambda spark: spark.read.parquet(data_path)
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        read_fn,
        exist_classes='GpuFileSourceScanExec',
        non_exist_classes='HybridFileSourceScanExec',
        conf={
            'spark.sql.sources.useV1SourceList': 'parquet',
            'spark.rapids.sql.hybrid.parquet.enabled': 'true',
        })


# Test the preloading feature with extreme tiny target batch size (and source batch size), creating
# scenarios in which multiple target batches will be generated.
@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@pytest.mark.parametrize('coalesced_batch_size', [1 << 17, 1 << 20], ids=idfn)
@pytest.mark.parametrize('preloaded_batches', [1, 3, 5], ids=idfn)
@hybrid_test
def test_hybrid_parquet_preloading(spark_tmp_path, coalesced_batch_size, preloaded_batches):
    parquet_gens = parquet_gens_list[0].copy()
    parquet_gens.extend(parquet_gens_list[1])
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list, length=4096).write.parquet(data_path),
        conf=rebase_write_corrected_conf)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        conf={
            'spark.sql.sources.useV1SourceList': 'parquet',
            'spark.rapids.sql.hybrid.parquet.enabled': 'true',
            'spark.gluten.sql.columnar.maxBatchSize': 16,
            'spark.rapids.sql.batchSizeBytes': coalesced_batch_size,
            'spark.rapids.sql.hybrid.parquet.numPreloadedBatches': preloaded_batches,
        })


# Common configuration for filter pushdown tests
filter_split_conf = {
    'spark.sql.sources.useV1SourceList': 'parquet',
    'spark.rapids.sql.hybrid.parquet.enabled': 'true',
    'spark.rapids.sql.parquet.pushDownFiltersToHybrid': 'CPU',
    'spark.rapids.sql.expression.Ascii': False,
    'spark.rapids.sql.expression.StartsWith': False,
    'spark.rapids.sql.hybrid.whitelistExprs': 'StartsWith'
}

def check_filter_pushdown(plan, pushed_exprs, not_pushed_exprs):
    plan = str(plan)
    filter_part, scan_part = plan.split("Scan parquet")
    for expr in pushed_exprs:
        assert expr in scan_part
    for expr in not_pushed_exprs:
        assert expr in filter_part

@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@pytest.mark.parametrize('data_type,gen,filter_expr,pushed_exprs', [
    # Numeric type tests
    ('byte', byte_gen, 'a > 10', ['> 10']),
    ('short', short_gen, 'a <= 100', ['<= 100']),
    ('int', int_gen, 'a >= 1000', ['>= 1000']),
    ('long', long_gen, 'a < 50000', ['< 50000']),
    ('float', float_gen, 'a BETWEEN 10.5 AND 100.5', ['BETWEEN']),
    ('double', double_gen, 'a != 50.25', ['!=']),
    
    # String type tests
    ('string', StringGen(pattern='[a-zA-Z0-9]{5,10}'), 'a LIKE "a%"', ['LIKE']),
    ('string', StringGen(pattern='[a-zA-Z0-9]{5,10}'), 'length(a) > 6', ['length']),
    ('string', StringGen(pattern='[a-zA-Z0-9]{5,10}'), 'upper(a) = "ABCDE"', ['upper']),
    ('string', StringGen(pattern='[a-zA-Z0-9]{5,10}'), 'lower(a) = "abcde"', ['lower']),
    
    # Date type tests
    ('date', date_gen, 'a > "2020-01-01"', ['>']),
    ('date', date_gen, 'year(a) = 2022', ['year']),
    ('date', date_gen, 'month(a) > 6', ['month']),
    ('date', date_gen, 'dayofmonth(a) = 15', ['dayofmonth']),
    
    # Boolean type tests
    ('boolean', boolean_gen, 'a = true', ['=']),
    ('boolean', boolean_gen, 'a AND true', ['AND']),
    ('boolean', boolean_gen, 'a OR false', ['OR']),
    
    # Decimal type tests
    ('decimal', decimal_gen_32bit, 'a > 100.50', ['>']),
    ('decimal', decimal_gen_64bit, 'a < 1000.25', ['<']),
    
    # Complex logical expressions
    ('int', int_gen, 'a > 10 AND a < 100', ['AND', '>', '<']),
    ('int', int_gen, 'a < 10 OR a > 100', ['OR', '<', '>']),
    ('int', int_gen, 'NOT (a BETWEEN 10 AND 50)', ['NOT', 'BETWEEN']),
    
    # Math functions
    ('double', double_gen, 'abs(a) > 50.0', ['abs']),
    ('double', double_gen, 'ceil(a) = 100', ['ceil']),
    ('double', double_gen, 'floor(a) = 99', ['floor']),
    ('double', double_gen, 'round(a) < 75', ['round']),
])
@hybrid_test
def test_hybrid_parquet_filter_pushdown_datatypes(spark_tmp_path, data_type, gen, filter_expr, pushed_exprs):
    """Test filter pushdown for various data types and expressions."""
    data_path = spark_tmp_path + f'/PARQUET_DATA_{data_type}'
    with_cpu_session(
        lambda spark: gen_df(spark, [('a', gen)]).write.parquet(data_path),
        conf=rebase_write_corrected_conf)
    
    # Clone the configuration
    conf = filter_split_conf.copy()
    
    # Check that filters are correctly pushed down
    plan = with_gpu_session(
        lambda spark: spark.read.parquet(data_path).filter(filter_expr)._jdf.queryExecution().executedPlan(),
        conf=conf)
    
    # Verify filter expressions are pushed to scan
    check_filter_pushdown(plan, pushed_exprs=pushed_exprs + ['isnotnull'], not_pushed_exprs=[])
    
    # Verify results match between CPU and GPU
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).filter(filter_expr),
        conf=conf)

@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@hybrid_test
def test_hybrid_parquet_complex_types_filter_pushdown(spark_tmp_path):
    """Test filter pushdown for complex types (struct, array)."""
    data_path = spark_tmp_path + '/PARQUET_DATA_COMPLEX'
    
    # Generate complex type data
    complex_gens = [
        ('id', long_gen),
        ('arr_int', ArrayGen(int_gen)),
        ('arr_str', ArrayGen(string_gen)),
        ('struct_data', StructGen([
            ('int_field', int_gen),
            ('str_field', string_gen),
            ('date_field', date_gen)
        ]))
    ]
    
    with_cpu_session(
        lambda spark: gen_df(spark, complex_gens, length=1000).write.parquet(data_path),
        conf=rebase_write_corrected_conf)
    
    # Clone the configuration
    conf = filter_split_conf.copy()
    
    # Test array functions
    filter_tests = [
        # Array element access and functions
        ("size(arr_int) > 2", ['size']),
        ("arr_int[0] > 50", ['>']),
        ("array_contains(arr_str, 'test')", ['array_contains']),
        
        # Struct field access
        ("struct_data.int_field > 100", ['>']),
        ("struct_data.str_field LIKE 'A%'", ['LIKE']),
        ("year(struct_data.date_field) = 2022", ['year']),
        
        # Combined conditions
        ("size(arr_int) > 0 AND struct_data.int_field < 500", ['AND', 'size', '<']),
        ("id > 500 OR struct_data.str_field = 'test'", ['OR', '>', '='])
    ]
    
    for filter_expr, pushed_exprs in filter_tests:
        # Check filter pushdown plan
        plan = with_gpu_session(
            lambda spark: spark.read.parquet(data_path).filter(filter_expr)._jdf.queryExecution().executedPlan(),
            conf=conf)
        
        check_filter_pushdown(plan, pushed_exprs=pushed_exprs + ['isnotnull'], not_pushed_exprs=[])
        
        # Verify results
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark: spark.read.parquet(data_path).filter(filter_expr),
            conf=conf)

@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@hybrid_test
def test_hybrid_parquet_multiple_filter_combinations(spark_tmp_path):
    """Test various combinations of filters that should all be pushed down."""
    data_path = spark_tmp_path + '/PARQUET_DATA_MULTI'
    
    # Generate data with multiple columns of different types
    gen_list = [
        ('int_col', int_gen),
        ('long_col', long_gen),
        ('double_col', double_gen),
        ('string_col', string_gen),
        ('date_col', date_gen),
        ('bool_col', boolean_gen),
        ('decimal_col', decimal_gen_64bit)
    ]
    
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list, length=1000).write.parquet(data_path),
        conf=rebase_write_corrected_conf)
    
    # Clone the configuration
    conf = filter_split_conf.copy()
    
    # Complex filter combinations
    filter_tests = [
        # Multiple conditions with AND
        ("int_col > 100 AND string_col LIKE 'A%' AND date_col > '2022-01-01'", 
         ['AND', '>', 'LIKE']),
        
        # Multiple conditions with OR
        ("long_col < 50 OR double_col > 100.0 OR bool_col = true", 
         ['OR', '<', '>', '=']),
        
        # Mixed AND/OR
        ("(int_col > 10 AND int_col < 100) OR (long_col >= 1000 AND long_col <= 5000)", 
         ['AND', 'OR', '>', '<', '>=', '<=']),
        
        # Functions and operators together
        ("abs(double_col) > 50.0 AND upper(string_col) = 'TEST' AND year(date_col) = 2022", 
         ['AND', 'abs', 'upper', 'year']),
        
        # IN and BETWEEN
        ("int_col IN (1, 2, 3, 4, 5) AND double_col BETWEEN 10.0 AND 50.0",
         ['AND', 'IN', 'BETWEEN']),
        
        # Mathematical expressions
        ("int_col + long_col > 1000 AND double_col * 2 < 100",
         ['AND', '+', '*', '>', '<']),
        
        # CASE expressions
        ("CASE WHEN int_col > 100 THEN true WHEN int_col < 0 THEN false ELSE bool_col END",
         ['CASE']),
        
        # Complex boolean logic
        ("NOT (int_col > 10 AND string_col = 'test') OR (bool_col AND long_col < 100)",
         ['NOT', 'AND', 'OR', '>', '=', '<']),
    ]
    
    for filter_expr, expected_fragments in filter_tests:
        # Check filter pushdown plan 
        plan = with_gpu_session(
            lambda spark: spark.read.parquet(data_path).filter(filter_expr)._jdf.queryExecution().executedPlan(),
            conf=conf)
        
        check_filter_pushdown(plan, pushed_exprs=expected_fragments + ['isnotnull'], not_pushed_exprs=[])
        
        # Verify results
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark: spark.read.parquet(data_path).filter(filter_expr),
            conf=conf)

@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@hybrid_test
def test_hybrid_parquet_partial_filter_pushdown(spark_tmp_path):
    """Test scenarios where some filters can be pushed down but others cannot."""
    data_path = spark_tmp_path + '/PARQUET_DATA_PARTIAL'
    
    gen_list = [
        ('int_col', int_gen),
        ('string_col', string_gen),
        ('timestamp_col', TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc)))
    ]
    
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list, length=1000).write.parquet(data_path),
        conf=rebase_write_corrected_conf)
    
    # Register a UDF that won't be pushed down
    def custom_udf(s):
        return f"custom_{s}"
    
    with_cpu_session(lambda spark: spark.udf.register("custom_udf", custom_udf))
    
    # Clone the configuration
    conf = filter_split_conf.copy()
    
    # Partial pushdown cases
    filter_tests = [
        # Mix of pushable and non-pushable (UDF)
        ("int_col > 100 AND custom_udf(string_col) = 'custom_test'", 
         ['> 100'], ['custom_udf']),
        
        # Mix of pushable and non-pushable (timestamp)
        ("int_col < 500 AND timestamp_col > '2022-01-01'", 
         ['< 500'], ['timestamp_col']),
        
        # Complex mix
        ("(int_col BETWEEN 100 AND 500) AND (custom_udf(string_col) = 'test' OR timestamp_col < '2023-01-01')", 
         ['BETWEEN'], ['custom_udf', 'timestamp_col']),
    ]
    
    for filter_expr, pushed_exprs, not_pushed_exprs in filter_tests:
        # Check filter pushdown plan 
        plan = with_gpu_session(
            lambda spark: spark.read.parquet(data_path).filter(filter_expr)._jdf.queryExecution().executedPlan(),
            conf=conf)
        
        # Check that the correct expressions are pushed down and others remain in filter
        check_filter_pushdown(plan, pushed_exprs=pushed_exprs + ['isnotnull'], not_pushed_exprs=not_pushed_exprs)
        
        # Verify results
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark: spark.read.parquet(data_path).filter(filter_expr),
            conf=conf)

@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@hybrid_test
def test_hybrid_parquet_filter_expression_types(spark_tmp_path):
    """Test specific expression types listed in HybridExecutionUtils.scala."""
    data_path = spark_tmp_path + '/PARQUET_DATA_EXPRESSIONS'
    
    # Generate data with columns for testing different expression types
    gen_list = [
        ('int_col', int_gen),
        ('double_col', double_gen),
        ('string_col', string_gen),
        ('date_col', date_gen),
        ('array_col', ArrayGen(int_gen)),
        ('map_col', simple_string_to_string_map_gen)
    ]
    
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list, length=1000).write.parquet(data_path),
        conf=rebase_write_corrected_conf)
    
    # Clone the configuration with all expressions enabled
    conf = filter_split_conf.copy()
    conf.update({
        'spark.rapids.sql.expression.Ascii': True,  # Enable all expressions for this test
    })
    
    # Test various expression types from HybridExecutionUtils.scala
    expression_tests = [
        # Mathematical functions
        ("abs(double_col) > 50", ['abs']),
        ("ceil(double_col) = 100", ['ceil']),
        ("floor(double_col) = 99", ['floor']),
        ("round(double_col) < 75", ['round']),
        ("sqrt(abs(double_col)) > 5", ['sqrt', 'abs']),
        ("pow(double_col, 2) > 100", ['pow']),
        
        # String functions
        ("length(string_col) > 5", ['length']),
        ("upper(string_col) = 'TEST'", ['upper']),
        ("lower(string_col) = 'test'", ['lower']),
        ("substring(string_col, 1, 3) = 'abc'", ['substring']),
        ("concat(string_col, '_suffix') LIKE '%suffix'", ['concat', 'LIKE']),
        
        # Date/time functions
        ("year(date_col) = 2022", ['year']),
        ("month(date_col) = 6", ['month']),
        ("dayofmonth(date_col) = 15", ['dayofmonth']),
        ("dayofweek(date_col) = 1", ['dayofweek']),
        ("dayofyear(date_col) < 200", ['dayofyear']),
        
        # Boolean logic
        ("int_col > 10 AND int_col < 100", ['AND', '>', '<']),
        ("int_col < 10 OR int_col > 100", ['OR', '<', '>']),
        ("NOT (int_col BETWEEN 10 AND 50)", ['NOT', 'BETWEEN']),
        
        # Array functions
        ("size(array_col) > 2", ['size']),
        ("array_contains(array_col, 5)", ['array_contains']),
        
        # Map functions
        ("map_keys(map_col)[0] = 'key'", ['map_keys']),
        ("map_values(map_col)[0] = 'value'", ['map_values']),
        
        # CASE expressions
        ("CASE WHEN int_col > 100 THEN 'large' WHEN int_col > 50 THEN 'medium' ELSE 'small' END = 'large'", ['CASE']),
        
        # IN expressions
        ("int_col IN (1, 2, 3, 4, 5)", ['IN']),
        
        # Null handling
        ("int_col IS NULL", ['isnull']),
        ("int_col IS NOT NULL", ['isnotnull']),
        
        # Complex combinations
        ("(int_col > 50 AND string_col LIKE 'A%') OR (double_col < 100 AND year(date_col) = 2022)",
         ['AND', 'OR', '>', 'LIKE', '<', 'year'])
    ]
    
    for filter_expr, expected_fragments in expression_tests:
        # Check filter pushdown plan
        plan = with_gpu_session(
            lambda spark: spark.read.parquet(data_path).filter(filter_expr)._jdf.queryExecution().executedPlan(),
            conf=conf)
        
        # Verify filter expressions are pushed to scan
        check_filter_pushdown(plan, pushed_exprs=expected_fragments, not_pushed_exprs=[])
        
        # Verify results match between CPU and GPU
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark: spark.read.parquet(data_path).filter(filter_expr),
            conf=conf)

@pytest.mark.skipif(is_databricks_runtime(), reason="Hybrid feature does not support Databricks currently")
@pytest.mark.skipif(not is_hybrid_backend_loaded(), reason="HybridScan specialized tests")
@pytest.mark.parametrize('parquet_gens', parquet_gens_list, ids=idfn)
@hybrid_test
def test_hybrid_parquet_bucket_read(parquet_gens, spark_tmp_path, spark_tmp_table_factory):
    data_path = spark_tmp_path + '/PARQUET_BUCKET_DATA'
    
    gen_list = [('id', long_gen)] + [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    num_buckets = 8
    table_name = spark_tmp_table_factory.get()
    
    with_cpu_session(lambda spark: 
        gen_df(spark, gen_list, length=10000)
            .write
            .bucketBy(num_buckets, "id")
            .sortBy("id")
            .option("path", data_path)
            .saveAsTable(table_name),
        conf=rebase_write_corrected_conf)
    
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.table(table_name).filter("id > 5000"),
        conf={
            'spark.sql.sources.useV1SourceList': 'parquet',
            'spark.rapids.sql.hybrid.parquet.enabled': 'true',
            'spark.sql.sources.bucketing.enabled': 'true',
            'spark.sql.sources.bucketing.autoBucketedScan.enabled': 'false'
        })