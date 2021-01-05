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
from spark_session import with_cpu_session, with_gpu_session

def read_parquet_df(data_path):
    return lambda spark : spark.read.parquet(data_path)

def read_parquet_sql(data_path):
    return lambda spark : spark.sql('select * from parquet.`{}`'.format(data_path))

parquet_gens_list = [[byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, boolean_gen, date_gen,
    TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc)), ArrayGen(byte_gen),
    ArrayGen(long_gen), ArrayGen(string_gen), ArrayGen(date_gen),
    ArrayGen(TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))),
    ArrayGen(ArrayGen(byte_gen)),
    StructGen([['child0', ArrayGen(byte_gen)], ['child1', byte_gen], ['child2', float_gen]]),
    ArrayGen(StructGen([['child0', string_gen], ['child1', double_gen], ['child2', int_gen]]))] + map_gens_sample,
    pytest.param([timestamp_gen], marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/132'))]

# test with original parquet file reader, the multi-file parallel reader for cloud, and coalesce file reader for
# non-cloud
original_parquet_file_reader_conf={'spark.rapids.sql.format.parquet.reader.type': 'PERFILE'}
multithreaded_parquet_file_reader_conf={'spark.rapids.sql.format.parquet.reader.type': 'MULTITHREADED'}
coalesce_parquet_file_reader_conf={'spark.rapids.sql.format.parquet.reader.type': 'COALESCING'}
reader_opt_confs = [original_parquet_file_reader_conf, multithreaded_parquet_file_reader_conf,
        coalesce_parquet_file_reader_conf]

@pytest.mark.parametrize('parquet_gens', parquet_gens_list, ids=idfn)
@pytest.mark.parametrize('read_func', [read_parquet_df, read_parquet_sql])
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_read_round_trip(spark_tmp_path, parquet_gens, read_func, reader_confs, v1_enabled_list):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.parquet(data_path),
            conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED'})
    all_confs = reader_confs.copy()
    all_confs.update({'spark.sql.sources.useV1SourceList': v1_enabled_list, 'spark.sql.legacy.parquet.datetimeRebaseModeInRead': 'CORRECTED'})
    # once https://github.com/NVIDIA/spark-rapids/issues/1126 is in we can remove spark.sql.legacy.parquet.datetimeRebaseModeInRead config which is a workaround
    # for nested timestamp/date support
    assert_gpu_and_cpu_are_equal_collect(read_func(data_path),
            conf=all_confs)

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
            conf={disable_conf: 'false',
                "spark.sql.sources.useV1SourceList": "parquet"})

parquet_compress_options = ['none', 'uncompressed', 'snappy', 'gzip']
# The following need extra jars 'lzo', 'lz4', 'brotli', 'zstd'
# https://github.com/NVIDIA/spark-rapids/issues/143

@pytest.mark.parametrize('compress', parquet_compress_options)
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_compress_read_round_trip(spark_tmp_path, compress, v1_enabled_list, reader_confs):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
            lambda spark : binary_op_df(spark, long_gen).write.parquet(data_path),
            conf={'spark.sql.parquet.compression.codec': compress})
    all_confs = reader_confs.copy()
    all_confs.update({'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path),
            conf=all_confs)

parquet_pred_push_gens = [
        byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen, boolean_gen,
        string_gen, date_gen,
        # Once https://github.com/NVIDIA/spark-rapids/issues/132 is fixed replace this with
        # timestamp_gen 
        TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))]

@pytest.mark.parametrize('parquet_gen', parquet_pred_push_gens, ids=idfn)
@pytest.mark.parametrize('read_func', [read_parquet_df, read_parquet_sql])
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_pred_push_round_trip(spark_tmp_path, parquet_gen, read_func, v1_enabled_list, reader_confs):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    gen_list = [('a', RepeatSeqGen(parquet_gen, 100)), ('b', parquet_gen)]
    s0 = gen_scalar(parquet_gen, force_no_nulls=True)
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).orderBy('a').write.parquet(data_path),
            conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED'})
    rf = read_func(data_path)
    all_confs = reader_confs.copy()
    all_confs.update({'spark.sql.sources.useV1SourceList': v1_enabled_list})
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
def test_ts_read_round_trip_nested(gen, spark_tmp_path, ts_write, ts_rebase, v1_enabled_list, reader_confs):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
            lambda spark : unary_op_df(spark, gen).write.parquet(data_path),
            conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': ts_rebase,
                'spark.sql.parquet.outputTimestampType': ts_write})
    all_confs = reader_confs.copy()
    all_confs.update({'spark.sql.sources.useV1SourceList': v1_enabled_list})
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
                'spark.sql.parquet.outputTimestampType': ts_write})
    all_confs = reader_confs.copy()
    all_confs.update({'spark.sql.sources.useV1SourceList': v1_enabled_list})
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
    all_confs = reader_confs.copy()
    all_confs.update({'spark.sql.sources.useV1SourceList': v1_enabled_list})
    with_gpu_session(
            lambda spark : readParquetCatchException(spark, data_path),
            conf=all_confs)

parquet_gens_legacy_list = [[byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, boolean_gen, DateGen(start=date(1590, 1, 1)),
    TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))],
    pytest.param([timestamp_gen], marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/133')),
    pytest.param([date_gen], marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/133'))]

@pytest.mark.parametrize('parquet_gens', parquet_gens_legacy_list, ids=idfn)
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_read_round_trip_legacy(spark_tmp_path, parquet_gens, v1_enabled_list, reader_confs):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.parquet(data_path),
            conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'LEGACY'})
    all_confs = reader_confs.copy()
    all_confs.update({'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path),
            conf=all_confs)

@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_simple_partitioned_read(spark_tmp_path, v1_enabled_list, reader_confs):
    # Once https://github.com/NVIDIA/spark-rapids/issues/133 and https://github.com/NVIDIA/spark-rapids/issues/132 are fixed 
    # we should go with a more standard set of generators
    parquet_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, boolean_gen, DateGen(start=date(1590, 1, 1)),
    TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))]
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
    all_confs = reader_confs.copy()
    all_confs.update({'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path),
            conf=all_confs)

# In this we are reading the data, but only reading the key the data was partitioned by
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_partitioned_read_just_partitions(spark_tmp_path, v1_enabled_list, reader_confs):
    parquet_gens = [byte_gen]
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
    all_confs = reader_confs.copy()
    all_confs.update({'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path).select("key"),
            conf=all_confs)

@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_read_schema_missing_cols(spark_tmp_path, v1_enabled_list, reader_confs):
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
    all_confs = reader_confs.copy()
    all_confs.update({'spark.sql.sources.useV1SourceList': v1_enabled_list,
          'spark.sql.files.maxPartitionBytes': "1g",
          'spark.sql.files.minPartitionNum': '1'})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path),
            conf=all_confs)

@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_read_merge_schema(spark_tmp_path, v1_enabled_list, reader_confs):
    # Once https://github.com/NVIDIA/spark-rapids/issues/133 and https://github.com/NVIDIA/spark-rapids/issues/132 are fixed 
    # we should go with a more standard set of generators
    parquet_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, boolean_gen, DateGen(start=date(1590, 1, 1)),
    TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))]
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
    all_confs = reader_confs.copy()
    all_confs.update({'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.option('mergeSchema', 'true').parquet(data_path),
            conf=all_confs)

@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_read_merge_schema_from_conf(spark_tmp_path, v1_enabled_list, reader_confs):
    # Once https://github.com/NVIDIA/spark-rapids/issues/133 and https://github.com/NVIDIA/spark-rapids/issues/132 are fixed 
    # we should go with a more standard set of generators
    parquet_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, boolean_gen, DateGen(start=date(1590, 1, 1)),
    TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))]
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
    all_confs = reader_confs.copy()
    all_confs.update({'spark.sql.sources.useV1SourceList': v1_enabled_list,
          'spark.sql.parquet.mergeSchema': "true"})
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path),
            conf=all_confs)

@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_input_meta(spark_tmp_path, v1_enabled_list, reader_confs):
    first_data_path = spark_tmp_path + '/PARQUET_DATA/key=0'
    with_cpu_session(
            lambda spark : unary_op_df(spark, long_gen).write.parquet(first_data_path))
    second_data_path = spark_tmp_path + '/PARQUET_DATA/key=1'
    with_cpu_session(
            lambda spark : unary_op_df(spark, long_gen).write.parquet(second_data_path))
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs = reader_confs.copy()
    all_confs.update({'spark.sql.sources.useV1SourceList': v1_enabled_list})
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
    all_confs = reader_confs.copy()
    all_confs.update({'spark.sql.sources.useV1SourceList': v1_enabled_list,
          "spark.sql.autoBroadcastJoinThreshold": '-1'})
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
            conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED'})
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
            [["ar", ArrayGen(StructGen([["str_2", StringGen()]]))]])
        ]

@pytest.mark.parametrize('data_gen,read_schema', _nested_pruning_schemas, ids=idfn)
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
@pytest.mark.parametrize('nested_enabled', ["true", "false"])
def test_nested_pruning(spark_tmp_path, data_gen, read_schema, reader_confs, v1_enabled_list, nested_enabled):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
            lambda spark : gen_df(spark, data_gen).write.parquet(data_path),
            conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED'})
    all_confs = reader_confs.copy()
    all_confs.update({'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.sql.optimizer.nestedSchemaPruning.enabled': nested_enabled,
        'spark.sql.legacy.parquet.datetimeRebaseModeInRead': 'CORRECTED'})
    # This is a hack to get the type in a slightly less verbose way
    rs = StructGen(read_schema, nullable=False).data_type
    assert_gpu_and_cpu_are_equal_collect(lambda spark : spark.read.schema(rs).parquet(data_path),
            conf=all_confs)

