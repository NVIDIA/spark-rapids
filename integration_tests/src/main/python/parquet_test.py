# Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

from asserts import assert_cpu_and_gpu_are_equal_collect_with_capture, assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect
from data_gen import *
from marks import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from spark_session import with_cpu_session, with_gpu_session, is_before_spark_330
from conftest import is_databricks_runtime


def read_parquet_df(data_path):
    return lambda spark : spark.read.parquet(data_path)

def read_parquet_sql(data_path):
    return lambda spark : spark.sql('select * from parquet.`{}`'.format(data_path))


# Override decimal_gens because decimal with negative scale is unsupported in parquet reading
decimal_gens = [DecimalGen(), DecimalGen(precision=7, scale=3), DecimalGen(precision=10, scale=10),
                DecimalGen(precision=9, scale=0), DecimalGen(precision=18, scale=15)]

rebase_write_corrected_conf = {
    'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
    'spark.sql.legacy.parquet.int96RebaseModeInWrite': 'CORRECTED'
}

rebase_write_legacy_conf = {
    'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'LEGACY',
    'spark.sql.legacy.parquet.int96RebaseModeInWrite': 'LEGACY'
}

# Like the standard map_gens_sample but with timestamps limited
parquet_map_gens = [MapGen(f(nullable=False), f()) for f in [
        BooleanGen, ByteGen, ShortGen, IntegerGen, LongGen, FloatGen, DoubleGen, DateGen,
        lambda nullable=True: TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc), nullable=nullable)]] +\
    [simple_string_to_string_map_gen,
     MapGen(StringGen(pattern='key_[0-9]', nullable=False), ArrayGen(string_gen), max_length=10),
     MapGen(RepeatSeqGen(IntegerGen(nullable=False), 10), long_gen, max_length=10),
     MapGen(StringGen(pattern='key_[0-9]', nullable=False), simple_string_to_string_map_gen)]

parquet_gens_list = [[byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, boolean_gen, date_gen,
    TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc)), ArrayGen(byte_gen),
    ArrayGen(long_gen), ArrayGen(string_gen), ArrayGen(date_gen),
    ArrayGen(TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))),
    ArrayGen(DecimalGen()),
    ArrayGen(ArrayGen(byte_gen)),
    StructGen([['child0', ArrayGen(byte_gen)], ['child1', byte_gen], ['child2', float_gen], ['child3', DecimalGen()]]),
    ArrayGen(StructGen([['child0', string_gen], ['child1', double_gen], ['child2', int_gen]]))] +
                     parquet_map_gens + decimal_gens,
                     pytest.param([timestamp_gen], marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/132'))]

# test with original parquet file reader, the multi-file parallel reader for cloud, and coalesce file reader for
# non-cloud
original_parquet_file_reader_conf = {'spark.rapids.sql.format.parquet.reader.type': 'PERFILE'}
multithreaded_parquet_file_reader_conf = {'spark.rapids.sql.format.parquet.reader.type': 'MULTITHREADED'}
coalesce_parquet_file_reader_conf = {'spark.rapids.sql.format.parquet.reader.type': 'COALESCING'}
reader_opt_confs = [original_parquet_file_reader_conf, multithreaded_parquet_file_reader_conf,
                    coalesce_parquet_file_reader_conf]

@pytest.mark.parametrize('parquet_gens', parquet_gens_list, ids=idfn)
@pytest.mark.parametrize('read_func', [read_parquet_df, read_parquet_sql])
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_read_round_trip(spark_tmp_path, parquet_gens, read_func, reader_confs, v1_enabled_list):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.parquet(data_path),
            conf=rebase_write_corrected_conf)
    all_confs = copy_and_update(reader_confs, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        # set the int96 rebase mode values because its LEGACY in databricks which will preclude this op from running on GPU
        'spark.sql.legacy.parquet.int96RebaseModeInRead' : 'CORRECTED',
        'spark.sql.legacy.parquet.datetimeRebaseModeInRead': 'CORRECTED'})
    # once https://github.com/NVIDIA/spark-rapids/issues/1126 is in we can remove spark.sql.legacy.parquet.datetimeRebaseModeInRead config which is a workaround
    # for nested timestamp/date support
    assert_gpu_and_cpu_are_equal_collect(read_func(data_path),
            conf=all_confs)


@allow_non_gpu('FileSourceScanExec')
@pytest.mark.parametrize('read_func', [read_parquet_df, read_parquet_sql])
@pytest.mark.parametrize('disable_conf', ['spark.rapids.sql.format.parquet.enabled', 'spark.rapids.sql.format.parquet.read.enabled'])
def test_parquet_fallback(spark_tmp_path, read_func, disable_conf):
    data_gens = [string_gen,
        byte_gen, short_gen, int_gen, long_gen, boolean_gen] + decimal_gens + decimal_128_gens_no_neg

    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(data_gens)]
    gen = StructGen(gen_list, nullable=False)
    data_path = spark_tmp_path + '/PARQUET_DATA'
    reader = read_func(data_path)
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.parquet(data_path))
    assert_gpu_fallback_collect(
            lambda spark : reader(spark).select(f.col('*'), f.col('_c2') + f.col('_c3')),
            'FileSourceScanExec',
            conf={disable_conf: 'false',
                "spark.sql.sources.useV1SourceList": "parquet"})

parquet_compress_options = ['none', 'uncompressed', 'snappy', 'gzip']
# The following need extra jars 'lzo', 'lz4', 'brotli', 'zstd'
# https://github.com/NVIDIA/spark-rapids/issues/143

@pytest.mark.parametrize('compress', parquet_compress_options)
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_compress_read_round_trip(spark_tmp_path, compress, v1_enabled_list, reader_confs):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
            lambda spark : binary_op_df(spark, long_gen).write.parquet(data_path),
            conf={'spark.sql.parquet.compression.codec': compress})
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path),
            conf=all_confs)

parquet_pred_push_gens = [
        byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen, boolean_gen,
        string_gen, date_gen,
        # Once https://github.com/NVIDIA/spark-rapids/issues/132 is fixed replace this with
        # timestamp_gen
        TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))] + decimal_gens + decimal_128_gens_no_neg

@pytest.mark.parametrize('parquet_gen', parquet_pred_push_gens, ids=idfn)
@pytest.mark.parametrize('read_func', [read_parquet_df, read_parquet_sql])
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_pred_push_round_trip(spark_tmp_path, parquet_gen, read_func, v1_enabled_list, reader_confs):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    gen_list = [('a', RepeatSeqGen(parquet_gen, 100)), ('b', parquet_gen)]
    s0 = gen_scalar(parquet_gen, force_no_nulls=True)
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).orderBy('a').write.parquet(data_path),
            conf=rebase_write_corrected_conf)
    rf = read_func(data_path)
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: rf(spark).select(f.col('a') >= s0),
            conf=all_confs)

parquet_ts_write_options = ['INT96', 'TIMESTAMP_MICROS', 'TIMESTAMP_MILLIS']


# Once https://github.com/NVIDIA/spark-rapids/issues/1126 is fixed delete this test and merge it
# into test_ts_read_round_trip nested timestamps and dates are not supported right now.
@pytest.mark.parametrize('gen', [ArrayGen(TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))),
    ArrayGen(ArrayGen(TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))))], ids=idfn)
@pytest.mark.parametrize('ts_write', parquet_ts_write_options)
@pytest.mark.parametrize('ts_rebase', ['CORRECTED', 'LEGACY'])
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/1126')
def test_parquet_ts_read_round_trip_nested(gen, spark_tmp_path, ts_write, ts_rebase, v1_enabled_list, reader_confs):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
            lambda spark : unary_op_df(spark, gen).write.parquet(data_path),
            conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': ts_rebase,
                'spark.sql.legacy.parquet.int96RebaseModeInWrite': ts_rebase,
                'spark.sql.parquet.outputTimestampType': ts_write})
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path),
            conf=all_confs)

# Once https://github.com/NVIDIA/spark-rapids/issues/132 is fixed replace this with
# timestamp_gen
@pytest.mark.parametrize('gen', [TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))], ids=idfn)
@pytest.mark.parametrize('ts_write', parquet_ts_write_options)
@pytest.mark.parametrize('ts_rebase', ['CORRECTED', 'LEGACY'])
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_ts_read_round_trip(gen, spark_tmp_path, ts_write, ts_rebase, v1_enabled_list, reader_confs):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
            lambda spark : unary_op_df(spark, gen).write.parquet(data_path),
            conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': ts_rebase,
                'spark.sql.legacy.parquet.int96RebaseModeInWrite': ts_rebase,
                'spark.sql.parquet.outputTimestampType': ts_write})
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path),
            conf=all_confs)

def readParquetCatchException(spark, data_path):
    with pytest.raises(Exception) as e_info:
        df = spark.read.parquet(data_path).collect()
    assert e_info.match(r".*SparkUpgradeException.*")

# Once https://github.com/NVIDIA/spark-rapids/issues/1126 is fixed nested timestamps and dates should be added in
# Once https://github.com/NVIDIA/spark-rapids/issues/132 is fixed replace this with
# timestamp_gen
@pytest.mark.parametrize('gen', [TimestampGen(start=datetime(1590, 1, 1, tzinfo=timezone.utc))], ids=idfn)
@pytest.mark.parametrize('ts_write', parquet_ts_write_options)
@pytest.mark.parametrize('ts_rebase', ['LEGACY'])
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_ts_read_fails_datetime_legacy(gen, spark_tmp_path, ts_write, ts_rebase, v1_enabled_list, reader_confs):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
            lambda spark : unary_op_df(spark, gen).write.parquet(data_path),
            conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': ts_rebase,
                'spark.sql.legacy.parquet.int96RebaseModeInWrite': ts_rebase,
                'spark.sql.parquet.outputTimestampType': ts_write})
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    with_gpu_session(
            lambda spark : readParquetCatchException(spark, data_path),
            conf=all_confs)

@pytest.mark.parametrize('parquet_gens', [[byte_gen, short_gen, DecimalGen(precision=7, scale=3)], decimal_gens,
                                          [ArrayGen(DecimalGen(7,2), max_length=10)],
                                          [StructGen([['child0', DecimalGen(7, 2)]])], decimal_128_gens_no_neg], ids=idfn)
@pytest.mark.parametrize('read_func', [read_parquet_df, read_parquet_sql])
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_decimal_read_legacy(spark_tmp_path, parquet_gens, read_func, reader_confs, v1_enabled_list):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.parquet(data_path),
            conf={'spark.sql.parquet.writeLegacyFormat': 'true'})
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(read_func(data_path), conf=all_confs)


parquet_gens_legacy_list = [[byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
                            string_gen, boolean_gen, DateGen(start=date(1590, 1, 1)),
                            TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))] + decimal_gens + decimal_128_gens_no_neg,
                            pytest.param([timestamp_gen], marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/133')),
                            pytest.param([date_gen], marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/133'))]

@pytest.mark.parametrize('parquet_gens', parquet_gens_legacy_list, ids=idfn)
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_read_round_trip_legacy(spark_tmp_path, parquet_gens, v1_enabled_list, reader_confs):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.parquet(data_path),
            conf=rebase_write_legacy_conf)
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path),
            conf=all_confs)

@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_simple_partitioned_read(spark_tmp_path, v1_enabled_list, reader_confs):
    # Once https://github.com/NVIDIA/spark-rapids/issues/133 and https://github.com/NVIDIA/spark-rapids/issues/132 are fixed
    # we should go with a more standard set of generators
    parquet_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, boolean_gen, DateGen(start=date(1590, 1, 1)),
    TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))] + decimal_gens + decimal_128_gens_no_neg
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    first_data_path = spark_tmp_path + '/PARQUET_DATA/key=0/key2=20'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.parquet(first_data_path),
            conf=rebase_write_legacy_conf)
    second_data_path = spark_tmp_path + '/PARQUET_DATA/key=1/key2=21'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.parquet(second_data_path),
            conf=rebase_write_corrected_conf)
    third_data_path = spark_tmp_path + '/PARQUET_DATA/key=2/key2=22'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.parquet(third_data_path),
            conf=rebase_write_corrected_conf)
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path),
            conf=all_confs)


# In this we are reading the data, but only reading the key the data was partitioned by
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_partitioned_read_just_partitions(spark_tmp_path, v1_enabled_list, reader_confs):
    parquet_gens = [byte_gen]
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    first_data_path = spark_tmp_path + '/PARQUET_DATA/key=0'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.parquet(first_data_path),
            conf=rebase_write_legacy_conf)
    second_data_path = spark_tmp_path + '/PARQUET_DATA/key=1'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.parquet(second_data_path),
            conf=rebase_write_corrected_conf)
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path).select("key"),
            conf=all_confs)

@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_read_schema_missing_cols(spark_tmp_path, v1_enabled_list, reader_confs):
    # Once https://github.com/NVIDIA/spark-rapids/issues/133 and https://github.com/NVIDIA/spark-rapids/issues/132 are fixed
    # we should go with a more standard set of generators
    parquet_gens = [byte_gen, short_gen, int_gen, long_gen]
    first_gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    first_data_path = spark_tmp_path + '/PARQUET_DATA/key=0'
    with_cpu_session(
            lambda spark : gen_df(spark, first_gen_list, 1).write.parquet(first_data_path))
    # generate with 1 column less
    second_parquet_gens = [byte_gen, short_gen, int_gen]
    second_gen_list = [('_c' + str(i), gen) for i, gen in enumerate(second_parquet_gens)]
    second_data_path = spark_tmp_path + '/PARQUET_DATA/key=1'
    with_cpu_session(
            lambda spark : gen_df(spark, second_gen_list, 1).write.parquet(second_data_path))
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs = copy_and_update(reader_confs, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.sql.files.maxPartitionBytes': '1g',
        'spark.sql.files.minPartitionNum': '1'})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path),
            conf=all_confs)

@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_read_merge_schema(spark_tmp_path, v1_enabled_list, reader_confs):
    # Once https://github.com/NVIDIA/spark-rapids/issues/133 and https://github.com/NVIDIA/spark-rapids/issues/132 are fixed
    # we should go with a more standard set of generators
    parquet_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, boolean_gen, DateGen(start=date(1590, 1, 1)),
    TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))] + decimal_gens + decimal_128_gens_no_neg
    first_gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    first_data_path = spark_tmp_path + '/PARQUET_DATA/key=0'
    with_cpu_session(
            lambda spark : gen_df(spark, first_gen_list).write.parquet(first_data_path),
            conf=rebase_write_legacy_conf)
    second_gen_list = [(('_c' if i % 2 == 0 else '_b') + str(i), gen) for i, gen in enumerate(parquet_gens)]
    second_data_path = spark_tmp_path + '/PARQUET_DATA/key=1'
    with_cpu_session(
            lambda spark : gen_df(spark, second_gen_list).write.parquet(second_data_path),
            conf=rebase_write_corrected_conf)
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.option('mergeSchema', 'true').parquet(data_path),
            conf=all_confs)

@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_read_merge_schema_from_conf(spark_tmp_path, v1_enabled_list, reader_confs):
    # Once https://github.com/NVIDIA/spark-rapids/issues/133 and https://github.com/NVIDIA/spark-rapids/issues/132 are fixed
    # we should go with a more standard set of generators
    parquet_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, boolean_gen, DateGen(start=date(1590, 1, 1)),
    TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))] + decimal_gens + decimal_128_gens_no_neg
    first_gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    first_data_path = spark_tmp_path + '/PARQUET_DATA/key=0'
    with_cpu_session(
            lambda spark : gen_df(spark, first_gen_list).write.parquet(first_data_path),
            conf=rebase_write_legacy_conf)
    second_gen_list = [(('_c' if i % 2 == 0 else '_b') + str(i), gen) for i, gen in enumerate(parquet_gens)]
    second_data_path = spark_tmp_path + '/PARQUET_DATA/key=1'
    with_cpu_session(
            lambda spark : gen_df(spark, second_gen_list).write.parquet(second_data_path),
            conf=rebase_write_corrected_conf)
    all_confs = copy_and_update(reader_confs, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.sql.parquet.mergeSchema': 'true'})
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path),
            conf=all_confs)

@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_input_meta(spark_tmp_path, v1_enabled_list, reader_confs):
    first_data_path = spark_tmp_path + '/PARQUET_DATA/key=0'
    with_cpu_session(
            lambda spark : unary_op_df(spark, long_gen).write.parquet(first_data_path))
    second_data_path = spark_tmp_path + '/PARQUET_DATA/key=1'
    with_cpu_session(
            lambda spark : unary_op_df(spark, long_gen).write.parquet(second_data_path))
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path)\
                    .filter(f.col('a') > 0)\
                    .selectExpr('a',
                        'input_file_name()',
                        'input_file_block_start()',
                        'input_file_block_length()'),
            conf=all_confs)

@allow_non_gpu('ProjectExec', 'Alias', 'InputFileName', 'InputFileBlockStart', 'InputFileBlockLength',
               'FilterExec', 'And', 'IsNotNull', 'GreaterThan', 'Literal',
               'FileSourceScanExec', 'ColumnarToRowExec',
               'BatchScanExec', 'ParquetScan')
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('disable_conf', ['spark.rapids.sql.format.parquet.enabled', 'spark.rapids.sql.format.orc.parquet.enabled'])
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_input_meta_fallback(spark_tmp_path, v1_enabled_list, reader_confs, disable_conf):
    first_data_path = spark_tmp_path + '/PARQUET_DATA/key=0'
    with_cpu_session(
            lambda spark : unary_op_df(spark, long_gen).write.parquet(first_data_path))
    second_data_path = spark_tmp_path + '/PARQUET_DATA/key=1'
    with_cpu_session(
            lambda spark : unary_op_df(spark, long_gen).write.parquet(second_data_path))
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs = copy_and_update(reader_confs, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        disable_conf: 'false'})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path)\
                    .filter(f.col('a') > 0)\
                    .selectExpr('a',
                        'input_file_name()',
                        'input_file_block_start()',
                        'input_file_block_length()'),
            conf=all_confs)

def createBucketedTableAndJoin(spark, tbl_1, tbl_2):
    spark.range(10e4).write.bucketBy(4, "id").sortBy("id").mode('overwrite').saveAsTable(tbl_1)
    spark.range(10e6).write.bucketBy(4, "id").sortBy("id").mode('overwrite').saveAsTable(tbl_2)
    bucketed_4_10e4 = spark.table(tbl_1)
    bucketed_4_10e6 = spark.table(tbl_2)
    return bucketed_4_10e4.join(bucketed_4_10e6, "id")

@ignore_order
@allow_non_gpu('DataWritingCommandExec')
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
# this test would be better if we could ensure exchanges didn't exist - ie used buckets
def test_buckets(spark_tmp_path, v1_enabled_list, reader_confs, spark_tmp_table_factory):
    all_confs = copy_and_update(reader_confs, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.sql.autoBroadcastJoinThreshold': '-1'})
    def do_it(spark):
        return createBucketedTableAndJoin(spark, spark_tmp_table_factory.get(),
                spark_tmp_table_factory.get())
    assert_gpu_and_cpu_are_equal_collect(do_it, conf=all_confs)

@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_small_file_memory(spark_tmp_path, v1_enabled_list):
    # stress the memory usage by creating a lot of small files.
    # The more files we combine the more the offsets will be different which will cause
    # footer size to change.
    # Without the addition of extraMemory in GpuParquetScan this would cause reallocations
    # of the host memory buffers.
    cols = [string_gen] * 4
    gen_list = [('_c' + str(i), gen ) for i, gen in enumerate(cols)]
    first_data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).repartition(2000).write.parquet(first_data_path),
            conf=rebase_write_corrected_conf)
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path),
            conf={'spark.rapids.sql.format.parquet.reader.type': 'COALESCING',
                  'spark.sql.sources.useV1SourceList': v1_enabled_list,
                  'spark.sql.files.maxPartitionBytes': "1g"})


_nested_pruning_schemas = [
        ([["a", StructGen([["c_1", StringGen()], ["c_2", LongGen()], ["c_3", ShortGen()]])]],
            [["a", StructGen([["c_1", StringGen()]])]]),
        ([["a", StructGen([["c_1", StringGen()], ["c_2", LongGen()], ["c_3", ShortGen()]])]],
            [["a", StructGen([["c_2", LongGen()]])]]),
        ([["a", StructGen([["c_1", StringGen()], ["c_2", LongGen()], ["c_3", ShortGen()]])]],
            [["a", StructGen([["c_3", ShortGen()]])]]),
        ([["a", StructGen([["c_1", StringGen()], ["c_2", LongGen()], ["c_3", ShortGen()]])]],
            [["a", StructGen([["c_1", StringGen()], ["c_3", ShortGen()]])]]),
        ([["a", StructGen([["c_1", StringGen()], ["c_2", LongGen()], ["c_3", ShortGen()]])]],
            [["a", StructGen([["c_3", ShortGen()], ["c_2", LongGen()], ["c_1", StringGen()]])]]),
        ([["ar", ArrayGen(StructGen([["str_1", StringGen()],["str_2", StringGen()]]))]],
            [["ar", ArrayGen(StructGen([["str_2", StringGen()]]))]]),
        ([["struct", StructGen([["c_1", StringGen()], ["case_insensitive", LongGen()], ["c_3", ShortGen()]])]],
            [["STRUCT", StructGen([["case_INSENsitive", LongGen()]])]]),
        ([["struct", StructGen([["c_1", StringGen()], ["case_insensitive", LongGen()], ["c_3", ShortGen()]])]],
            [["struct", StructGen([["CASE_INSENSITIVE", LongGen()]])]]),
        ([["struct", StructGen([["c_1", StringGen()], ["case_insensitive", LongGen()], ["c_3", ShortGen()]])]],
            [["stRUct", StructGen([["CASE_INSENSITIVE", LongGen()]])]]),
        ]
# TODO CHECK FOR DECIMAL??
@pytest.mark.parametrize('data_gen,read_schema', _nested_pruning_schemas, ids=idfn)
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
@pytest.mark.parametrize('nested_enabled', ["true", "false"])
def test_nested_pruning_and_case_insensitive(spark_tmp_path, data_gen, read_schema, reader_confs, v1_enabled_list, nested_enabled):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
            lambda spark : gen_df(spark, data_gen).write.parquet(data_path),
            conf=rebase_write_corrected_conf)
    all_confs = copy_and_update(reader_confs, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.sql.optimizer.nestedSchemaPruning.enabled': nested_enabled,
        'spark.sql.legacy.parquet.datetimeRebaseModeInRead': 'CORRECTED'})
    # This is a hack to get the type in a slightly less verbose way
    rs = StructGen(read_schema, nullable=False).data_type
    assert_gpu_and_cpu_are_equal_collect(lambda spark : spark.read.schema(rs).parquet(data_path),
            conf=all_confs)

def test_spark_32639(std_input_path):
    data_path = "%s/SPARK-32639/000.snappy.parquet" % (std_input_path)
    schema_str = 'value MAP<STRUCT<first:STRING, middle:STRING, last:STRING>, STRING>'
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(schema_str).parquet(data_path),
        conf=original_parquet_file_reader_conf)


def test_many_column_project():
    def _create_wide_data_frame(spark, num_cols):
        schema_dict = {}
        for i in range(num_cols):
            schema_dict[f"c{i}"] = i
        return spark.createDataFrame([Row(**r) for r in [schema_dict]])\
            .withColumn('out', f.col('c1') * 100)

    assert_gpu_and_cpu_are_equal_collect(
        func=lambda spark: _create_wide_data_frame(spark, 1000),
        is_cpu_first=False)

def setup_parquet_file_with_column_names(spark, table_name):
    drop_query = "DROP TABLE IF EXISTS {}".format(table_name)
    create_query = "CREATE TABLE `{}` (`a` INT, `b` ARRAY<INT>, `c` STRUCT<`c_1`: INT, `c_2`: STRING>) USING parquet"\
        .format(table_name)
    insert_query = "INSERT INTO {} VALUES(13, array(2020), named_struct('c_1', 1, 'c_2', 'hello'))".format(table_name)
    spark.sql(drop_query).collect
    spark.sql(create_query).collect
    spark.sql(insert_query).collect

@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_disorder_read_schema(spark_tmp_table_factory, reader_confs, v1_enabled_list):
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    table_name = spark_tmp_table_factory.get()
    with_cpu_session(lambda spark : setup_parquet_file_with_column_names(spark, table_name))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT a,b FROM {}".format(table_name)),
        all_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT c,a FROM {}".format(table_name)),
        all_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT c,b FROM {}".format(table_name)),
        all_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT a,c,b FROM {}".format(table_name)),
        all_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT a,b,c FROM {}".format(table_name)),
        all_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT b,c,a FROM {}".format(table_name)),
        all_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT b,c,a FROM {}".format(table_name)),
        all_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT c,a,b FROM {}".format(table_name)),
        all_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT c,b,a FROM {}".format(table_name)),
        all_confs)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT c.c_2,c.c_1,b,a FROM {}".format(table_name)),
        all_confs)


# SPARK-34859 put in a fix for handling column indexes with vectorized parquet
# This is a version of those same tests to verify that we are parsing
# the data correctly.
# These tests really only matter for Spark 3.2.0 and above, but they should run
# on any version, but might not test the exact same thing.
# Based off of ParquetColumnIndexSuite.
# Timestamp generation was modified because the original tests were written
# that to cast a long to a a timestamp the long was stored in ms, but it is
# stored in seconds, which resulted in dates/timetamps past what python can handle
# We also modified decimal generation to be at most DECIMAL64 until we can support
# DECIMAL128

filters = ["_1 = 500",
        "_1 = 500 or _1 = 1500",
        "_1 = 500 or _1 = 501 or _1 = 1500",
        "_1 = 500 or _1 = 501 or _1 = 1000 or _1 = 1500",
        "_1 >= 500 and _1 < 1000",
        "(_1 >= 500 and _1 < 1000) or (_1 >= 1500 and _1 < 1600)"]

@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@pytest.mark.parametrize('enable_dictionary', ["true", "false"], ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_reading_from_unaligned_pages_basic_filters(spark_tmp_path, reader_confs, enable_dictionary, v1_enabled_list):
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    data_path = spark_tmp_path + '/PARQUET_UNALIGNED_DATA'
    with_cpu_session(lambda spark : spark.range(0, 2000)\
            .selectExpr("id as _1", "concat(id, ':', repeat('o', id DIV 100)) as _2")\
            .coalesce(1)\
            .write\
            .option("parquet.page.size", "4096")
            .option("parquet.enable.dictionary", enable_dictionary)
            .parquet(data_path))
    for filter_str in filters:
        assert_gpu_and_cpu_are_equal_collect(
                lambda spark : spark.read.parquet(data_path).filter(filter_str),
                all_confs)

@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@pytest.mark.parametrize('enable_dictionary', ["true", "false"], ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_reading_from_unaligned_pages_all_types(spark_tmp_path, reader_confs, enable_dictionary, v1_enabled_list):
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    data_path = spark_tmp_path + '/PARQUET_UNALIGNED_DATA'
    with_cpu_session(lambda spark : spark.range(0, 2000)\
            .selectExpr("id as _1",
                "cast(id as short) as _3",
                "cast(id as int) as _4",
                "cast(id as float) as _5",
                "cast(id as double) as _6",
                # DECIMAL128 IS NOT SUPPORTED YET "cast(id as decimal(20,0)) as _7",
                "cast(id as decimal(10,0)) as _7",
                "cast(id as decimal(30,0)) as _8",
                "cast(cast(1618161925 + (id * 60 * 60 * 24) as timestamp) as date) as _9",
                "cast(1618161925 + id as timestamp) as _10")\
            .coalesce(1)\
            .write\
            .option("parquet.page.size", "4096")
            .option("parquet.enable.dictionary", enable_dictionary)
            .parquet(data_path))
    for filter_str in filters:
        assert_gpu_and_cpu_are_equal_collect(
                lambda spark : spark.read.parquet(data_path).filter(filter_str),
                all_confs)

@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@pytest.mark.parametrize('enable_dictionary', ["true", "false"], ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_reading_from_unaligned_pages_all_types_dict_optimized(spark_tmp_path, reader_confs, enable_dictionary, v1_enabled_list):
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    data_path = spark_tmp_path + '/PARQUET_UNALIGNED_DATA'
    with_cpu_session(lambda spark : spark.range(0, 2000)\
            .selectExpr("id as _1",
                "cast(id % 10 as byte) as _2",
                "cast(id % 10 as short) as _3",
                "cast(id % 10 as int) as _4",
                "cast(id % 10 as float) as _5",
                "cast(id % 10 as double) as _6",
                # DECIMAL128 IS NOT SUPPORTED YET "cast(id % 10 as decimal(20,0)) as _7",
                "cast(id % 10 as decimal(10,0)) as _7",
                "cast(id % 10 as decimal(20,0)) as _8",
                "cast(id % 2 as boolean) as _9",
                "cast(cast(1618161925 + ((id % 10) * 60 * 60 * 24) as timestamp) as date) as _10",
                "cast(1618161925 + (id % 10) as timestamp) as _11")\
            .coalesce(1)\
            .write\
            .option("parquet.page.size", "4096")
            .option("parquet.enable.dictionary", enable_dictionary)
            .parquet(data_path))
    for filter_str in filters:
        assert_gpu_and_cpu_are_equal_collect(
                lambda spark : spark.read.parquet(data_path).filter(filter_str),
                all_confs)

@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@pytest.mark.parametrize('enable_dictionary', ["true", "false"], ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_reading_from_unaligned_pages_basic_filters_with_nulls(spark_tmp_path, reader_confs, enable_dictionary, v1_enabled_list):
    # insert 50 null values in [400, 450) to verify that they are skipped during processing row
    # range [500, 1000) against the second page of col_2 [400, 800)
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    data_path = spark_tmp_path + '/PARQUET_UNALIGNED_DATA'
    with_cpu_session(lambda spark : spark.range(0, 2000)\
            .selectExpr("id as _1", "IF(id >= 400 AND id < 450, null, concat(id, ':', repeat('o', id DIV 100))) as _2")\
            .coalesce(1)\
            .write\
            .option("parquet.page.size", "4096")
            .option("parquet.enable.dictionary", enable_dictionary)
            .parquet(data_path))
    for filter_str in filters:
        assert_gpu_and_cpu_are_equal_collect(
                lambda spark : spark.read.parquet(data_path).filter(filter_str),
                all_confs)


conf_for_parquet_aggregate_pushdown = {
    "spark.sql.parquet.aggregatePushdown": "true", 
    "spark.sql.sources.useV1SourceList": ""
}

@pytest.mark.skipif(is_before_spark_330(), reason='Aggregate push down on Parquet is a new feature of Spark 330')
def test_parquet_scan_without_aggregation_pushdown_not_fallback(spark_tmp_path):
    """
    No aggregation will be pushed down in this test, so we should not fallback to CPU
    """
    data_path = spark_tmp_path + "/pushdown.parquet"

    def do_parquet_scan(spark):
        spark.range(10).selectExpr("id", "id % 3 as p").write.partitionBy("p").mode("overwrite").parquet(data_path)
        df = spark.read.parquet(data_path).selectExpr("Max(p)")
        return df

    assert_gpu_and_cpu_are_equal_collect(
        do_parquet_scan,
        conf_for_parquet_aggregate_pushdown
    )


@pytest.mark.skipif(is_before_spark_330(), reason='Aggregate push down on Parquet is a new feature of Spark 330')
@allow_non_gpu(any = True)
def test_parquet_scan_with_aggregation_pushdown_fallback(spark_tmp_path):
    """
    The aggregation will be pushed down in this test, so we should fallback to CPU
    """
    data_path = spark_tmp_path + "/pushdown.parquet"

    def do_parquet_scan(spark):
        spark.range(10).selectExpr("id", "id % 3 as p").write.partitionBy("p").mode("overwrite").parquet(data_path)
        df = spark.read.parquet(data_path).selectExpr("count(p)")
        return df

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        do_parquet_scan,
        exist_classes= "BatchScanExec",
        non_exist_classes= "GpuBatchScanExec",
        conf = conf_for_parquet_aggregate_pushdown)

@pytest.mark.skipif(is_before_spark_330(), reason='Hidden file metadata columns are a new feature of Spark 330')
@allow_non_gpu(any = True)
@pytest.mark.parametrize('metadata_column', ["file_path", "file_name", "file_size", "file_modification_time"])
def test_parquet_scan_with_hidden_metadata_fallback(spark_tmp_path, metadata_column):
    data_path = spark_tmp_path + "/hidden_metadata.parquet"
    with_cpu_session(lambda spark : spark.range(10) \
                     .selectExpr("id", "id % 3 as p") \
                     .write \
                     .partitionBy("p") \
                     .mode("overwrite") \
                     .parquet(data_path))

    def do_parquet_scan(spark):
        df = spark.read.parquet(data_path).selectExpr("id", "_metadata.{}".format(metadata_column))
        return df

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        do_parquet_scan,
        exist_classes= "FileSourceScanExec",
        non_exist_classes= "GpuBatchScanExec")


@ignore_order
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@pytest.mark.skipif(is_databricks_runtime(), reason="Databricks does not support ignoreCorruptFiles")
def test_parquet_read_with_corrupt_files(spark_tmp_path, reader_confs, v1_enabled_list):
    first_data_path = spark_tmp_path + '/PARQUET_DATA/first'
    with_cpu_session(lambda spark : spark.range(1).toDF("a").write.parquet(first_data_path))
    second_data_path = spark_tmp_path + '/PARQUET_DATA/second'
    with_cpu_session(lambda spark : spark.range(1, 2).toDF("a").write.parquet(second_data_path))
    third_data_path = spark_tmp_path + '/PARQUET_DATA/third'
    with_cpu_session(lambda spark : spark.range(2, 3).toDF("a").write.json(third_data_path))

    all_confs = copy_and_update(reader_confs,
                                {'spark.sql.files.ignoreCorruptFiles': "true",
                                 'spark.sql.sources.useV1SourceList': v1_enabled_list})

    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(first_data_path, second_data_path, third_data_path),
            conf=all_confs)