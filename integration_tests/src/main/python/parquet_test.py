# Copyright (c) 2020, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_writes_are_equal_collect, assert_gpu_fallback_collect
from datetime import date, datetime, timezone
from data_gen import *
from marks import *
from pyspark.sql.types import *
from spark_session import with_cpu_session

def read_parquet_df(data_path):
    return lambda spark : spark.read.parquet(data_path)

def read_parquet_sql(data_path):
    return lambda spark : spark.sql('select * from parquet.`{}`'.format(data_path))

parquet_gens_list = [[byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, boolean_gen, date_gen,
    TimestampGen(start=datetime(1590, 1, 1, tzinfo=timezone.utc))],
    pytest.param([timestamp_gen], marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/132'))]

@pytest.mark.parametrize('parquet_gens', parquet_gens_list, ids=idfn)
@pytest.mark.parametrize('read_func', [read_parquet_df, read_parquet_sql])
def test_read_round_trip(spark_tmp_path, parquet_gens, read_func):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.parquet(data_path),
            conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED'})
    assert_gpu_and_cpu_are_equal_collect(
            read_func(data_path))

@allow_non_gpu('FileSourceScanExec')
@pytest.mark.parametrize('read_func', [read_parquet_df, read_parquet_sql])
@pytest.mark.parametrize('disable_conf', ['spark.rapids.sql.format.parquet.enabled', 'spark.rapids.sql.format.parquet.read.enabled'])
def test_parquet_fallback(spark_tmp_path, read_func, disable_conf):
    data_gens =[string_gen,
        byte_gen, short_gen, int_gen, long_gen, boolean_gen]
 
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(data_gens)]
    gen = StructGen(gen_list, nullable=False)
    data_path = spark_tmp_path + '/PARQUET_DATA'
    reader = read_func(data_path)
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.parquet(data_path))
    assert_gpu_fallback_collect(
            lambda spark : reader(spark).select(f.col('*'), f.col('_c2') + f.col('_c3')),
            'FileSourceScanExec',
            conf={disable_conf: 'false'})

parquet_compress_options = ['none', 'uncompressed', 'snappy', 'gzip']
# The following need extra jars 'lzo', 'lz4', 'brotli', 'zstd'
# https://github.com/NVIDIA/spark-rapids/issues/143

@pytest.mark.parametrize('compress', parquet_compress_options)
def test_compress_read_round_trip(spark_tmp_path, compress):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
            lambda spark : binary_op_df(spark, long_gen).write.parquet(data_path),
            conf={'spark.sql.parquet.compression.codec': compress})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path))

parquet_pred_push_gens = [
        byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen, boolean_gen,
        string_gen, date_gen,
        # Once https://github.com/NVIDIA/spark-rapids/issues/132 is fixed replace this with
        # timestamp_gen 
        TimestampGen(start=datetime(1590, 1, 1, tzinfo=timezone.utc))]

@pytest.mark.parametrize('parquet_gen', parquet_pred_push_gens, ids=idfn)
@pytest.mark.parametrize('read_func', [read_parquet_df, read_parquet_sql])
def test_pred_push_round_trip(spark_tmp_path, parquet_gen, read_func):
    data_path = spark_tmp_path + '/ORC_DATA'
    gen_list = [('a', RepeatSeqGen(parquet_gen, 100)), ('b', parquet_gen)]
    s0 = gen_scalar(parquet_gen, force_no_nulls=True)
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).orderBy('a').write.parquet(data_path),
            conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED'})
    rf = read_func(data_path)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: rf(spark).select(f.col('a') >= s0))

parquet_ts_write_options = ['INT96', 'TIMESTAMP_MICROS', 'TIMESTAMP_MILLIS']

@pytest.mark.parametrize('ts_write', parquet_ts_write_options)
@pytest.mark.parametrize('ts_rebase', ['CORRECTED', 'LEGACY'])
def test_ts_read_round_trip(spark_tmp_path, ts_write, ts_rebase):
    # Once https://github.com/NVIDIA/spark-rapids/issues/132 is fixed replace this with
    # timestamp_gen
    gen = TimestampGen(start=datetime(1590, 1, 1, tzinfo=timezone.utc))
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
            lambda spark : unary_op_df(spark, gen).write.parquet(data_path),
            conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': ts_rebase,
                'spark.sql.parquet.outputTimestampType': ts_write})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path))

parquet_gens_legacy_list = [[byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, boolean_gen, DateGen(start=date(1590, 1, 1)),
    TimestampGen(start=datetime(1590, 1, 1, tzinfo=timezone.utc))],
    pytest.param([timestamp_gen], marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/133')),
    pytest.param([date_gen], marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/133'))]

@pytest.mark.parametrize('parquet_gens', parquet_gens_legacy_list, ids=idfn)
def test_read_round_trip_legacy(spark_tmp_path, parquet_gens):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.parquet(data_path),
            conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'LEGACY'})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path))

def test_simple_partitioned_read(spark_tmp_path):
    # Once https://github.com/NVIDIA/spark-rapids/issues/133 and https://github.com/NVIDIA/spark-rapids/issues/132 are fixed 
    # we should go with a more standard set of generators
    parquet_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, boolean_gen, DateGen(start=date(1590, 1, 1)),
    TimestampGen(start=datetime(1590, 1, 1, tzinfo=timezone.utc))]
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    first_data_path = spark_tmp_path + '/PARQUET_DATA/key=0'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.parquet(first_data_path),
            conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'LEGACY'})
    second_data_path = spark_tmp_path + '/PARQUET_DATA/key=1'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.parquet(second_data_path),
            conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED'})
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path))

def test_read_merge_schema(spark_tmp_path):
    # Once https://github.com/NVIDIA/spark-rapids/issues/133 and https://github.com/NVIDIA/spark-rapids/issues/132 are fixed 
    # we should go with a more standard set of generators
    parquet_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, boolean_gen, DateGen(start=date(1590, 1, 1)),
    TimestampGen(start=datetime(1590, 1, 1, tzinfo=timezone.utc))]
    first_gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    first_data_path = spark_tmp_path + '/PARQUET_DATA/key=0'
    with_cpu_session(
            lambda spark : gen_df(spark, first_gen_list).write.parquet(first_data_path),
            conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'LEGACY'})
    second_gen_list = [(('_c' if i % 2 == 0 else '_b') + str(i), gen) for i, gen in enumerate(parquet_gens)]
    second_data_path = spark_tmp_path + '/PARQUET_DATA/key=1'
    with_cpu_session(
            lambda spark : gen_df(spark, second_gen_list).write.parquet(second_data_path),
            conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED'})
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.option('mergeSchema', 'true').parquet(data_path))

parquet_write_gens_list = [
        [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
            string_gen, boolean_gen, date_gen, timestamp_gen]]

@pytest.mark.parametrize('parquet_gens', parquet_write_gens_list, ids=idfn)
def test_write_round_trip(spark_tmp_path, parquet_gens):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.parquet(path),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
                'spark.sql.parquet.outputTimestampType': 'TIMESTAMP_MICROS'})

parquet_part_write_gens = [
        byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
        # Some file systems have issues with UTF8 strings so to help the test pass even there
        StringGen('(\\w| ){0,50}'),
        boolean_gen, date_gen, timestamp_gen]

# There are race conditions around when individual files are read in for partitioned data
@ignore_order
@pytest.mark.parametrize('parquet_gen', parquet_part_write_gens, ids=idfn)
def test_part_write_round_trip(spark_tmp_path, parquet_gen):
    gen_list = [('a', RepeatSeqGen(parquet_gen, 10)),
            ('b', parquet_gen)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.partitionBy('a').parquet(path),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
                'spark.sql.parquet.outputTimestampType': 'TIMESTAMP_MICROS'})

parquet_write_compress_options = ['none', 'uncompressed', 'snappy']
@pytest.mark.parametrize('compress', parquet_write_compress_options)
def test_compress_write_round_trip(spark_tmp_path, compress):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path : binary_op_df(spark, long_gen).coalesce(1).write.parquet(path),
            lambda spark, path : spark.read.parquet(path),
            data_path,
            conf={'spark.sql.parquet.compression.codec': compress})

def test_input_meta(spark_tmp_path):
    first_data_path = spark_tmp_path + '/PARQUET_DATA/key=0'
    with_cpu_session(
            lambda spark : unary_op_df(spark, long_gen).write.parquet(first_data_path))
    second_data_path = spark_tmp_path + '/PARQUET_DATA/key=1'
    with_cpu_session(
            lambda spark : unary_op_df(spark, long_gen).write.parquet(second_data_path))
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path)\
                    .filter(f.col('a') > 0)\
                    .selectExpr('a',
                        'input_file_name()',
                        'input_file_block_start()',
                        'input_file_block_length()'))
