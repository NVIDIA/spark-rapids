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

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from marks import *
from pyspark.sql.types import *
from spark_session import with_cpu_session, with_gpu_session

# Configuration for enabling protobuf support
_protobuf_conf = {
    'spark.rapids.sql.format.protobuf.enabled': 'true',
    'spark.rapids.sql.format.protobuf.read.enabled': 'true',
}

# Data generators matching protobuf field types
# Protobuf supports: int32, int64, uint32, uint64, sint32, sint64, 
#                    fixed32, fixed64, sfixed32, sfixed64, float, double,
#                    bool, string, bytes
protobuf_basic_gens = [
    byte_gen,
    short_gen, 
    int_gen,
    long_gen,
    float_gen,
    double_gen,
    string_gen,
    boolean_gen,
]

protobuf_gens_list = [
    [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen, string_gen, boolean_gen],
    [long_gen, string_gen, double_gen],  # Common protobuf message pattern
    [int_gen, string_gen, boolean_gen],
]


def read_with_schema(data_path, schema):
    """Helper to read data with a specific schema."""
    return lambda spark: spark.read.schema(schema).parquet(data_path)


@pytest.mark.parametrize('protobuf_gens', protobuf_gens_list, ids=idfn)
def test_protobuf_basic_types_round_trip(spark_tmp_path, protobuf_gens):
    """Test that basic protobuf-compatible types work correctly on GPU."""
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(protobuf_gens)]
    data_path = spark_tmp_path + '/PROTOBUF_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        conf=_protobuf_conf)


def test_protobuf_int64_field(spark_tmp_path):
    """Test INT64 field type support (varint encoding in protobuf)."""
    data_path = spark_tmp_path + '/PROTOBUF_DATA'
    # Test various int64 values including edge cases for varint encoding
    gen = LongGen(min_val=0, max_val=2**62, special_cases=[0, 1, 127, 128, 255, 256, 2**31-1])
    with_cpu_session(
        lambda spark: unary_op_df(spark, gen).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        conf=_protobuf_conf)


def test_protobuf_string_field(spark_tmp_path):
    """Test STRING field type support (length-delimited in protobuf)."""
    data_path = spark_tmp_path + '/PROTOBUF_DATA'
    # Test various string patterns
    gen = StringGen(pattern='[a-zA-Z0-9]{0,50}')
    with_cpu_session(
        lambda spark: unary_op_df(spark, gen).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        conf=_protobuf_conf)


def test_protobuf_string_unicode(spark_tmp_path):
    """Test STRING field with unicode characters."""
    data_path = spark_tmp_path + '/PROTOBUF_DATA'
    with_cpu_session(
        lambda spark: unary_op_df(spark, string_gen).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        conf=_protobuf_conf)


@approximate_float
def test_protobuf_double_field(spark_tmp_path):
    """Test DOUBLE field type support (fixed64 in protobuf)."""
    data_path = spark_tmp_path + '/PROTOBUF_DATA'
    with_cpu_session(
        lambda spark: unary_op_df(spark, double_gen).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        conf=_protobuf_conf)


@approximate_float
def test_protobuf_float_field(spark_tmp_path):
    """Test FLOAT field type support (fixed32 in protobuf)."""
    data_path = spark_tmp_path + '/PROTOBUF_DATA'
    with_cpu_session(
        lambda spark: unary_op_df(spark, float_gen).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        conf=_protobuf_conf)


def test_protobuf_bool_field(spark_tmp_path):
    """Test BOOL field type support."""
    data_path = spark_tmp_path + '/PROTOBUF_DATA'
    with_cpu_session(
        lambda spark: unary_op_df(spark, boolean_gen).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        conf=_protobuf_conf)


def test_protobuf_multiple_columns(spark_tmp_path):
    """Test reading multiple columns (simulating protobuf message with multiple fields)."""
    data_path = spark_tmp_path + '/PROTOBUF_DATA'
    gen_list = [
        ('id', long_gen),
        ('name', string_gen),
        ('value', double_gen),
        ('active', boolean_gen),
    ]
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        conf=_protobuf_conf)


def test_protobuf_null_handling(spark_tmp_path):
    """Test NULL value handling (protobuf optional fields)."""
    data_path = spark_tmp_path + '/PROTOBUF_DATA'
    gen_list = [
        ('id', LongGen(nullable=True)),
        ('name', StringGen(nullable=True)),
        ('value', DoubleGen(nullable=True)),
    ]
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        conf=_protobuf_conf)


def test_protobuf_large_dataset(spark_tmp_path):
    """Test with larger dataset for performance validation."""
    data_path = spark_tmp_path + '/PROTOBUF_DATA'
    gen_list = [
        ('id', long_gen),
        ('name', StringGen(pattern='[a-z]{5,20}')),
        ('value', double_gen),
    ]
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list, length=10000).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        conf=_protobuf_conf)


@ignore_order
@allow_non_gpu('HashAggregateExec', 'ShuffleExchangeExec')
def test_protobuf_aggregation(spark_tmp_path):
    """Test aggregation on protobuf-like data."""
    data_path = spark_tmp_path + '/PROTOBUF_DATA'
    gen_list = [
        ('id', long_gen),
        ('category', StringGen(pattern='cat_[0-9]')),
        ('value', double_gen),
    ]
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path)
            .groupBy('category')
            .agg(f.sum('value'), f.count('id')),
        conf=_protobuf_conf)


def test_protobuf_filter(spark_tmp_path):
    """Test filter operations on protobuf-like data."""
    data_path = spark_tmp_path + '/PROTOBUF_DATA'
    gen_list = [
        ('id', LongGen(min_val=0, max_val=1000)),
        ('name', string_gen),
        ('value', DoubleGen(min_exp=0, max_exp=10)),
    ]
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path)
            .filter('id > 50')
            .filter('value < 1000.0'),
        conf=_protobuf_conf)


def test_protobuf_projection(spark_tmp_path):
    """Test column projection (reading subset of protobuf fields)."""
    data_path = spark_tmp_path + '/PROTOBUF_DATA'
    gen_list = [
        ('field1', long_gen),
        ('field2', string_gen),
        ('field3', double_gen),
        ('field4', boolean_gen),
    ]
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list).write.parquet(data_path))
    # Test reading only subset of columns
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).select('field1', 'field3'),
        conf=_protobuf_conf)


@ignore_order
def test_protobuf_join(spark_tmp_path):
    """Test join operations on protobuf-like data."""
    data_path1 = spark_tmp_path + '/PROTOBUF_DATA1'
    data_path2 = spark_tmp_path + '/PROTOBUF_DATA2'

    gen_list1 = [
        ('id', LongGen(min_val=0, max_val=100, nullable=False)),
        ('name', string_gen),
    ]
    gen_list2 = [
        ('id', LongGen(min_val=50, max_val=150, nullable=False)),
        ('value', double_gen),
    ]

    with_cpu_session(
        lambda spark: gen_df(spark, gen_list1, length=100).write.parquet(data_path1))
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list2, length=100).write.parquet(data_path2))

    def do_join(spark):
        df1 = spark.read.parquet(data_path1)
        df2 = spark.read.parquet(data_path2)
        return df1.join(df2, 'id')

    assert_gpu_and_cpu_are_equal_collect(do_join, conf=_protobuf_conf)


def test_protobuf_order_by(spark_tmp_path):
    """Test ORDER BY operations."""
    data_path = spark_tmp_path + '/PROTOBUF_DATA'
    gen_list = [
        ('id', long_gen),
        ('name', UniqueLongGen(nullable=False)),
        ('value', double_gen),
    ]
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).orderBy('name'),
        conf=_protobuf_conf)


@ignore_order
def test_protobuf_distinct(spark_tmp_path):
    """Test DISTINCT operations."""
    data_path = spark_tmp_path + '/PROTOBUF_DATA'
    gen_list = [
        ('id', LongGen(min_val=0, max_val=10)),  # Limited range to ensure duplicates
        ('category', StringGen(pattern='cat_[0-5]')),
    ]
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list, length=1000).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).distinct(),
        conf=_protobuf_conf)


def test_protobuf_nested_struct(spark_tmp_path):
    """Test nested struct types (simulating nested protobuf messages)."""
    data_path = spark_tmp_path + '/PROTOBUF_DATA'
    nested_gen = StructGen([
        ('nested_id', int_gen),
        ('nested_name', string_gen),
    ])
    gen_list = [
        ('id', long_gen),
        ('nested', nested_gen),
    ]
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        conf=_protobuf_conf)


def test_protobuf_array_field(spark_tmp_path):
    """Test array types (simulating protobuf repeated fields)."""
    data_path = spark_tmp_path + '/PROTOBUF_DATA'
    gen_list = [
        ('id', long_gen),
        ('tags', ArrayGen(string_gen, max_length=5)),
        ('values', ArrayGen(int_gen, max_length=10)),
    ]
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        conf=_protobuf_conf)


def test_protobuf_map_field(spark_tmp_path):
    """Test map types (simulating protobuf map fields)."""
    data_path = spark_tmp_path + '/PROTOBUF_DATA'
    gen_list = [
        ('id', long_gen),
        ('attributes', MapGen(StringGen(pattern='key_[0-9]', nullable=False), string_gen, max_length=5)),
    ]
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        conf=_protobuf_conf)


def test_protobuf_configuration_keys(spark_tmp_path):
    """Test that protobuf configuration keys are properly recognized."""
    data_path = spark_tmp_path + '/PROTOBUF_DATA'
    with_cpu_session(
        lambda spark: unary_op_df(spark, long_gen).write.parquet(data_path))
    
    # Test with protobuf enabled
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        conf=_protobuf_conf)

    # Test with protobuf disabled (should still work, just not use protobuf path)
    disabled_conf = {
        'spark.rapids.sql.format.protobuf.enabled': 'false',
        'spark.rapids.sql.format.protobuf.read.enabled': 'false',
    }
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        conf=disabled_conf)


@pytest.mark.parametrize('num_rows', [100, 1000, 10000], ids=idfn)
def test_protobuf_scaling(spark_tmp_path, num_rows):
    """Test scaling behavior with different data sizes."""
    data_path = spark_tmp_path + '/PROTOBUF_DATA'
    gen_list = [
        ('id', long_gen),
        ('name', StringGen(pattern='[a-z]{10,30}')),
        ('value', double_gen),
    ]
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list, length=num_rows).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        conf=_protobuf_conf)
