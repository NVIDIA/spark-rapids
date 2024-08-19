# Copyright (c) 2020-2024, NVIDIA CORPORATION.
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
import os

import pytest

from asserts import *
from conftest import is_not_utc
from data_gen import *
from parquet_write_test import parquet_nested_datetime_gen, parquet_ts_write_options
from marks import *
import pyarrow as pa
import pyarrow.parquet as pa_pq
from pyspark.sql.types import *
from pyspark.sql.functions import *
from spark_init_internal import spark_version
from spark_session import *
from conftest import is_databricks_runtime, is_dataproc_runtime


def read_parquet_df(data_path):
    return lambda spark : spark.read.parquet(data_path)

def read_parquet_sql(data_path):
    return lambda spark : spark.sql('select * from parquet.`{}`'.format(data_path))


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
    ArrayGen(decimal_gen_64bit),
    ArrayGen(ArrayGen(byte_gen)),
    StructGen([['child0', ArrayGen(byte_gen)], ['child1', byte_gen], ['child2', float_gen], ['child3', decimal_gen_64bit]]),
    ArrayGen(StructGen([['child0', string_gen], ['child1', double_gen], ['child2', int_gen]]))] +
                     parquet_map_gens + decimal_gens,
                     pytest.param([timestamp_gen], marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/132'))]

# test with original parquet file reader, the multi-file parallel reader for cloud, and coalesce file reader for
# non-cloud
original_parquet_file_reader_conf = {'spark.rapids.sql.format.parquet.reader.type': 'PERFILE'}
multithreaded_parquet_file_reader_conf = {'spark.rapids.sql.format.parquet.reader.type': 'MULTITHREADED',
        'spark.rapids.sql.reader.multithreaded.combine.sizeBytes': '0',
        'spark.rapids.sql.reader.multithreaded.read.keepOrder': True}
coalesce_parquet_file_reader_conf = {'spark.rapids.sql.format.parquet.reader.type': 'COALESCING'}
coalesce_parquet_file_reader_multithread_filter_chunked_conf = {'spark.rapids.sql.format.parquet.reader.type': 'COALESCING',
        'spark.rapids.sql.coalescing.reader.numFilterParallel': '2',
        'spark.rapids.sql.reader.chunked': True,
        'spark.rapids.sql.reader.chunked.subPage': True}
coalesce_parquet_file_reader_multithread_filter_sub_not_chunked_conf = {'spark.rapids.sql.format.parquet.reader.type': 'COALESCING',
        'spark.rapids.sql.coalescing.reader.numFilterParallel': '2',
        'spark.rapids.sql.reader.chunked': True,
        'spark.rapids.sql.reader.chunked.subPage': False}
coalesce_parquet_file_reader_multithread_filter_conf = {'spark.rapids.sql.format.parquet.reader.type': 'COALESCING',
        'spark.rapids.sql.coalescing.reader.numFilterParallel': '2',
        'spark.rapids.sql.reader.chunked': False}
native_parquet_file_reader_conf = {'spark.rapids.sql.format.parquet.reader.type': 'PERFILE',
        'spark.rapids.sql.format.parquet.reader.footer.type': 'NATIVE'}
native_multithreaded_parquet_file_reader_conf = {'spark.rapids.sql.format.parquet.reader.type': 'MULTITHREADED',
        'spark.rapids.sql.format.parquet.reader.footer.type': 'NATIVE',
        'spark.rapids.sql.reader.multithreaded.combine.sizeBytes': '0',
        'spark.rapids.sql.reader.multithreaded.read.keepOrder': True}
native_coalesce_parquet_file_reader_conf = {'spark.rapids.sql.format.parquet.reader.type': 'COALESCING',
        'spark.rapids.sql.format.parquet.reader.footer.type': 'NATIVE'}
native_coalesce_parquet_file_reader_chunked_conf = {'spark.rapids.sql.format.parquet.reader.type': 'COALESCING',
        'spark.rapids.sql.format.parquet.reader.footer.type': 'NATIVE',
        'spark.rapids.sql.reader.chunked': True,
        'spark.rapids.sql.reader.chunked.subPage': True}
native_coalesce_parquet_file_reader_sub_not_chunked_conf = {'spark.rapids.sql.format.parquet.reader.type': 'COALESCING',
        'spark.rapids.sql.format.parquet.reader.footer.type': 'NATIVE',
        'spark.rapids.sql.reader.chunked': True,
        'spark.rapids.sql.reader.chunked.subPage': False}
combining_multithreaded_parquet_file_reader_conf_ordered = {'spark.rapids.sql.format.parquet.reader.type': 'MULTITHREADED',
        'spark.rapids.sql.reader.multithreaded.combine.sizeBytes': '64m',
        'spark.rapids.sql.reader.multithreaded.read.keepOrder': True}
combining_multithreaded_parquet_file_reader_conf_unordered = pytest.param({'spark.rapids.sql.format.parquet.reader.type': 'MULTITHREADED',
        'spark.rapids.sql.reader.multithreaded.combine.sizeBytes': '64m',
        'spark.rapids.sql.reader.multithreaded.read.keepOrder': False}, marks=pytest.mark.ignore_order(local=True))
combining_multithreaded_parquet_file_reader_deprecated_conf_ordered = {
        'spark.rapids.sql.format.parquet.reader.type': 'MULTITHREADED',
        'spark.rapids.sql.format.parquet.multithreaded.combine.sizeBytes': '64m',
        'spark.rapids.sql.format.parquet.multithreaded.read.keepOrder': True}


# For now the native configs are not compatible with spark.sql.parquet.writeLegacyFormat written files
# for nested types
reader_opt_confs_native = [native_parquet_file_reader_conf, native_multithreaded_parquet_file_reader_conf,
                    native_coalesce_parquet_file_reader_conf,
                    coalesce_parquet_file_reader_multithread_filter_chunked_conf,
                    coalesce_parquet_file_reader_multithread_filter_sub_not_chunked_conf,
                    native_coalesce_parquet_file_reader_chunked_conf,
                    native_coalesce_parquet_file_reader_sub_not_chunked_conf]

reader_opt_confs_no_native = [original_parquet_file_reader_conf, multithreaded_parquet_file_reader_conf,
                    coalesce_parquet_file_reader_conf, coalesce_parquet_file_reader_multithread_filter_conf,
                    combining_multithreaded_parquet_file_reader_conf_ordered,
                    combining_multithreaded_parquet_file_reader_deprecated_conf_ordered]

reader_opt_confs = reader_opt_confs_native + reader_opt_confs_no_native


@pytest.mark.parametrize('parquet_gens', [[byte_gen, short_gen, int_gen, long_gen]], ids=idfn)
@pytest.mark.parametrize('read_func', [read_parquet_df])
@pytest.mark.parametrize('reader_confs', [coalesce_parquet_file_reader_multithread_filter_conf,
    coalesce_parquet_file_reader_multithread_filter_chunked_conf,
    coalesce_parquet_file_reader_multithread_filter_sub_not_chunked_conf])
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_read_coalescing_multiple_files(spark_tmp_path, parquet_gens, read_func, reader_confs, v1_enabled_list):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
            # high number of slices so that a single task reads more than 1 file
            lambda spark : gen_df(spark, gen_list, num_slices=30).write.parquet(data_path),
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


@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_read_avoid_coalesce_incompatible_files(spark_tmp_path, v1_enabled_list):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    def setup_table(spark):
        df1 = spark.createDataFrame([(("a", "b"),)], "x: struct<y: string, z: string>")
        df1.write.parquet(data_path + "/data1")
        df2 = spark.createDataFrame([(("a",),)], "x: struct<z: string>")
        df2.write.parquet(data_path + "/data2")
    with_cpu_session(setup_table, conf=rebase_write_corrected_conf)
    # Configure confs to read as a single task
    all_confs = copy_and_update(coalesce_parquet_file_reader_multithread_filter_conf, {
        "spark.sql.sources.useV1SourceList": v1_enabled_list,
        "spark.sql.files.minPartitionNum": "1"})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read\
            .schema("x STRUCT<y: string, z: string>")\
            .option("recursiveFileLookup", "true").parquet(data_path),
        conf=all_confs)

@pytest.mark.parametrize('parquet_gens', parquet_gens_list, ids=idfn)
@pytest.mark.parametrize('read_func', [read_parquet_df, read_parquet_sql])
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
@tz_sensitive_test
@allow_non_gpu(*non_utc_allow)
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
        byte_gen, short_gen, int_gen, long_gen, boolean_gen] + decimal_gens

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

@pytest.mark.parametrize('read_func', [read_parquet_df, read_parquet_sql])
@pytest.mark.parametrize('binary_as_string', [True, False])
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@tz_sensitive_test
def test_parquet_read_round_trip_binary(std_input_path, read_func, binary_as_string, reader_confs):
    data_path = std_input_path + '/binary_as_string.parquet'

    all_confs = copy_and_update(reader_confs, {
        'spark.sql.parquet.binaryAsString': binary_as_string,
        # set the int96 rebase mode values because its LEGACY in databricks which will preclude this op from running on GPU
        'spark.sql.legacy.parquet.int96RebaseModeInRead' : 'CORRECTED',
        'spark.sql.legacy.parquet.datetimeRebaseModeInRead': 'CORRECTED'})
    # once https://github.com/NVIDIA/spark-rapids/issues/1126 is in we can remove spark.sql.legacy.parquet.datetimeRebaseModeInRead config which is a workaround
    # for nested timestamp/date support
    assert_gpu_and_cpu_are_equal_collect(read_func(data_path),
                                         conf=all_confs)

@pytest.mark.parametrize('read_func', [read_parquet_df, read_parquet_sql])
@pytest.mark.parametrize('binary_as_string', [True, False])
@pytest.mark.parametrize('data_gen', [binary_gen,
    ArrayGen(binary_gen),
    StructGen([('a_1', binary_gen), ('a_2', string_gen)]),
    StructGen([('a_1', ArrayGen(binary_gen))]),
    MapGen(ByteGen(nullable=False), binary_gen)], ids=idfn)
def test_binary_df_read(spark_tmp_path, binary_as_string, read_func, data_gen):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(lambda spark: unary_op_df(spark, data_gen).write.parquet(data_path))
    all_confs = {
        'spark.sql.parquet.binaryAsString': binary_as_string,
        # set the int96 rebase mode values because its LEGACY in databricks which will preclude this op from running on GPU
        'spark.sql.legacy.parquet.int96RebaseModeInRead': 'CORRECTED',
        'spark.sql.legacy.parquet.datetimeRebaseModeInRead': 'CORRECTED'}
    assert_gpu_and_cpu_are_equal_collect(read_func(data_path), conf=all_confs)

@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_read_forced_binary_schema(std_input_path, v1_enabled_list):
    data_path = std_input_path + '/binary_as_string.parquet'

    all_confs = copy_and_update(reader_opt_confs[0], {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        # set the int96 rebase mode values because its LEGACY in databricks which will preclude this op from running on GPU
        'spark.sql.legacy.parquet.int96RebaseModeInRead' : 'CORRECTED',
        'spark.sql.legacy.parquet.datetimeRebaseModeInRead': 'CORRECTED'})
    # once https://github.com/NVIDIA/spark-rapids/issues/1126 is in we can remove spark.sql.legacy.parquet.datetimeRebaseModeInRead config which is a workaround
    # for nested timestamp/date support

    # This forces a Binary Column to a String Column and a String Column to a Binary Column.
    schema = StructType([StructField("a", LongType()), StructField("b", StringType()), StructField("c", BinaryType())])
    assert_gpu_and_cpu_are_equal_collect(lambda spark : spark.read.schema(schema).parquet(data_path),
            conf=all_confs)

@pytest.mark.parametrize('read_func', [read_parquet_df, read_parquet_sql])
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
@tz_sensitive_test
def test_parquet_read_round_trip_binary_as_string(std_input_path, read_func, reader_confs, v1_enabled_list):
    data_path = std_input_path + '/binary_as_string.parquet'

    all_confs = copy_and_update(reader_confs, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.sql.parquet.binaryAsString': 'true',
        # set the int96 rebase mode values because its LEGACY in databricks which will preclude this op from running on GPU
        'spark.sql.legacy.parquet.int96RebaseModeInRead' : 'CORRECTED',
        'spark.sql.legacy.parquet.datetimeRebaseModeInRead': 'CORRECTED'})
    # once https://github.com/NVIDIA/spark-rapids/issues/1126 is in we can remove spark.sql.legacy.parquet.datetimeRebaseModeInRead config which is a workaround
    # for nested timestamp/date support
    assert_gpu_and_cpu_are_equal_collect(read_func(data_path),
            conf=all_confs)

parquet_compress_options = ['none', 'uncompressed', 'snappy', 'gzip']
# zstd is available in spark 3.2.0 and later.
if not is_before_spark_320():
    parquet_compress_options.append('zstd')
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
        TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))] + decimal_gens

@pytest.mark.parametrize('parquet_gen', parquet_pred_push_gens, ids=idfn)
@pytest.mark.parametrize('read_func', [read_parquet_df, read_parquet_sql])
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
@allow_non_gpu(*non_utc_allow)
def test_parquet_pred_push_round_trip(spark_tmp_path, parquet_gen, read_func, v1_enabled_list, reader_confs):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    gen_list = [('a', RepeatSeqGen(parquet_gen, 100)), ('b', parquet_gen)]
    s0 = with_cpu_session(lambda spark: gen_scalar(parquet_gen, force_no_nulls=True))
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).orderBy('a').write.parquet(data_path),
            conf=rebase_write_corrected_conf)
    rf = read_func(data_path)
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: rf(spark).select(f.col('a') >= s0),
            conf=all_confs)

@pytest.mark.skipif(is_not_utc(), reason="LEGACY datetime rebase mode is only supported for UTC timezone")
@pytest.mark.parametrize('parquet_gens', [parquet_nested_datetime_gen], ids=idfn)
@pytest.mark.parametrize('ts_type', parquet_ts_write_options)
@pytest.mark.parametrize('ts_rebase_write', [('CORRECTED', 'LEGACY'), ('LEGACY', 'CORRECTED')])
@pytest.mark.parametrize('ts_rebase_read', [('CORRECTED', 'LEGACY'), ('LEGACY', 'CORRECTED')])
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_read_roundtrip_datetime_with_legacy_rebase(spark_tmp_path, parquet_gens, ts_type,
                                                            ts_rebase_write, ts_rebase_read,
                                                            reader_confs, v1_enabled_list):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    write_confs = {'spark.sql.parquet.outputTimestampType': ts_type,
                   'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': ts_rebase_write[0],
                   'spark.sql.legacy.parquet.int96RebaseModeInWrite': ts_rebase_write[1]}
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list).write.parquet(data_path),
        conf=write_confs)
    # The rebase modes in read configs should be ignored and overridden by the same modes in write
    # configs, which are retrieved from the written files.
    read_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list,
                                                'spark.sql.legacy.parquet.datetimeRebaseModeInRead': ts_rebase_read[0],
                                                'spark.sql.legacy.parquet.int96RebaseModeInRead': ts_rebase_read[1]})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        conf=read_confs)

# This is legacy format, which is totally different from datatime legacy rebase mode.
@pytest.mark.parametrize('parquet_gens', [[byte_gen, short_gen, decimal_gen_32bit], decimal_gens,
                                          [ArrayGen(decimal_gen_32bit, max_length=10)],
                                          [StructGen([['child0', decimal_gen_32bit]])]], ids=idfn)
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

@pytest.mark.skipif(is_not_utc(), reason="LEGACY datetime rebase mode is only supported for UTC timezone")
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
@pytest.mark.parametrize('batch_size', [100, INT_MAX])
def test_parquet_simple_partitioned_read(spark_tmp_path, v1_enabled_list, reader_confs, batch_size):
    # Once https://github.com/NVIDIA/spark-rapids/issues/133 and https://github.com/NVIDIA/spark-rapids/issues/132 are fixed
    # we should go with a more standard set of generators
    parquet_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, boolean_gen, DateGen(start=date(1590, 1, 1)),
    TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))] + decimal_gens
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
    all_confs = copy_and_update(reader_confs,
            {'spark.sql.sources.useV1SourceList': v1_enabled_list,
             'spark.rapids.sql.batchSizeBytes': batch_size})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path),
            conf=all_confs)


# In this we are reading the data, but only reading the key the data was partitioned by
@pytest.mark.skipif(is_not_utc(), reason="LEGACY datetime rebase mode is only supported for UTC timezone")
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

reader_opt_confs_with_unordered = reader_opt_confs + [combining_multithreaded_parquet_file_reader_conf_unordered]
@pytest.mark.parametrize('reader_confs', reader_opt_confs_with_unordered)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_read_schema_missing_cols(spark_tmp_path, v1_enabled_list, reader_confs):
    # Once https://github.com/NVIDIA/spark-rapids/issues/133 and https://github.com/NVIDIA/spark-rapids/issues/132 are fixed
    # we should go with a more standard set of generators
    parquet_gens = [byte_gen, short_gen, int_gen, long_gen]
    first_gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    first_data_path = spark_tmp_path + '/PARQUET_DATA/key=0'
    with_cpu_session(
            lambda spark : gen_df(spark, first_gen_list, 10).write.parquet(first_data_path))
    # generate with 1 column less
    second_parquet_gens = [byte_gen, short_gen, int_gen]
    second_gen_list = [('_c' + str(i), gen) for i, gen in enumerate(second_parquet_gens)]
    second_data_path = spark_tmp_path + '/PARQUET_DATA/key=1'
    with_cpu_session(
            lambda spark : gen_df(spark, second_gen_list, 10).write.parquet(second_data_path))
    # third with same as first
    third_gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    third_data_path = spark_tmp_path + '/PARQUET_DATA/key=2'
    with_cpu_session(
            lambda spark : gen_df(spark, third_gen_list, 10).write.parquet(third_data_path))
    # fourth with same as second
    fourth_gen_list = [('_c' + str(i), gen) for i, gen in enumerate(second_parquet_gens)]
    fourth_data_path = spark_tmp_path + '/PARQUET_DATA/key=3'
    with_cpu_session(
            lambda spark : gen_df(spark, fourth_gen_list, 10).write.parquet(fourth_data_path))
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs = copy_and_update(reader_confs, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.sql.files.maxPartitionBytes': '1g',
        'spark.sql.files.minPartitionNum': '1'})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path),
            conf=all_confs)

# To test https://github.com/NVIDIA/spark-rapids/pull/7405. Without the fix in that issue this test
# throws an exception about can't allocate negative amount. To make this problem happen, we
# read a bunch of empty parquet blocks by filtering on only things in the first and last of 1000 files.
@pytest.mark.parametrize('reader_confs', [combining_multithreaded_parquet_file_reader_conf_ordered])
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_read_buffer_allocation_empty_blocks(spark_tmp_path, v1_enabled_list, reader_confs):
    data_path = spark_tmp_path + '/PARQUET_DATA/'
    with_cpu_session(
            lambda spark : spark.range(0, 1000, 1, 1000).write.parquet(data_path))
    # we want all the files to be read by a single Spark task
    all_confs = copy_and_update(reader_confs, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.sql.files.maxPartitionBytes': '2g',
        'spark.sql.files.minPartitionNum': '1',
        'spark.sql.openCostInBytes': '1'})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.parquet(data_path).filter("id < 2 or id > 990"),
            conf=all_confs)

@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
@pytest.mark.skipif(is_databricks_runtime(), reason="https://github.com/NVIDIA/spark-rapids/issues/7733")
def test_parquet_read_ignore_missing(spark_tmp_path, v1_enabled_list, reader_confs):
    data_path = spark_tmp_path + '/PARQUET_DATA/'
    data_path_tmp = spark_tmp_path + '/PARQUET_DATA_TMP/'

    # we need to create the files, get the dataframe but remove the file before we
    # actually read the file contents. Here we save the data into a second directory
    # so that when CPU runs, it can remove the file and then put the data back to run
    # on the GPU.
    def setup_data(spark):
        df = spark.range(0, 1000, 1, 2).write.parquet(data_path)
        sc = spark.sparkContext
        config = sc._jsc.hadoopConfiguration()
        src_path = sc._jvm.org.apache.hadoop.fs.Path(data_path)
        dst_path = sc._jvm.org.apache.hadoop.fs.Path(data_path_tmp)
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(config)
        sc._jvm.org.apache.hadoop.fs.FileUtil.copy(fs, src_path, fs, dst_path, False, config)
        df

    with_cpu_session(lambda spark : setup_data(spark))
    file_deleted = ""

    def read_and_remove(spark):
        sc = spark.sparkContext
        config = sc._jsc.hadoopConfiguration()
        path = sc._jvm.org.apache.hadoop.fs.Path(data_path_tmp)
        src_path = sc._jvm.org.apache.hadoop.fs.Path(data_path)
        dst_path = sc._jvm.org.apache.hadoop.fs.Path(data_path_tmp)
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(config)
        fs.delete(src_path)
        sc._jvm.org.apache.hadoop.fs.FileUtil.copy(fs, dst_path, fs, src_path, False, config)
        # input_file_name doesn't use combine so get the input file names in a different dataframe
        # that we ultimately don't return
        df = spark.read.parquet(data_path)
        df_with_file_names = df.withColumn("input_file", input_file_name())
        distinct_file_names = df_with_file_names.select("input_file").distinct().sort("input_file")
        num_files = distinct_file_names.count()
        assert(num_files == 2)
        files_to_read=[]
        for i in range(0, 2):
            files_to_read.insert(i, distinct_file_names.collect()[i][0])

        df_to_test = spark.read.parquet(files_to_read[0], files_to_read[1])
        # we do our best to try to remove the one Spark will read first but its not
        # guaranteed
        file_to_delete = files_to_read[1]
        path_to_delete = sc._jvm.org.apache.hadoop.fs.Path(file_to_delete)
        fs.delete(path_to_delete)
        df_with_file_names_after = df.withColumn("input_file", input_file_name())
        distinct_file_names_after = df_with_file_names_after.select("input_file").distinct()
        num_files_after_delete = distinct_file_names_after.count()
        assert(num_files_after_delete == 1)
        return df_to_test


    # we want all the files to be read by a single Spark task
    all_confs = copy_and_update(reader_confs, {
        'spark.sql.files.ignoreMissingFiles': 'true',
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.sql.files.maxPartitionBytes': '2g',
        'spark.sql.files.minPartitionNum': '1',
        'spark.sql.openCostInBytes': '1'})
    assert_gpu_and_cpu_row_counts_equal(
            lambda spark : read_and_remove(spark),
            conf=all_confs)

@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
@pytest.mark.skipif(is_not_utc(), reason="LEGACY datetime rebase mode is only supported for UTC timezone")
def test_parquet_read_merge_schema(spark_tmp_path, v1_enabled_list, reader_confs):
    # Once https://github.com/NVIDIA/spark-rapids/issues/133 and https://github.com/NVIDIA/spark-rapids/issues/132 are fixed
    # we should go with a more standard set of generators
    parquet_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, boolean_gen, DateGen(start=date(1590, 1, 1)),
    TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))] + decimal_gens
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
@pytest.mark.skipif(is_not_utc(), reason="LEGACY datetime rebase mode is only supported for UTC timezone")
def test_parquet_read_merge_schema_from_conf(spark_tmp_path, v1_enabled_list, reader_confs):
    # Once https://github.com/NVIDIA/spark-rapids/issues/133 and https://github.com/NVIDIA/spark-rapids/issues/132 are fixed
    # we should go with a more standard set of generators
    parquet_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, boolean_gen, DateGen(start=date(1590, 1, 1)),
    TimestampGen(start=datetime(1900, 1, 1, tzinfo=timezone.utc))] + decimal_gens
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

@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
def test_read_parquet_with_empty_clipped_schema(spark_tmp_path, v1_enabled_list, reader_confs):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, [('a', int_gen)], length=100).write.parquet(data_path))
    schema = StructType([StructField('b', IntegerType()), StructField('c', StringType())])
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(schema).parquet(data_path), conf=all_confs)

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
    all_confs = copy_and_update(reader_confs, {'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.rapids.sql.format.parquet.multithreaded.read.keepOrder': 'true'})
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
@pytest.mark.parametrize('disable_conf', ['spark.rapids.sql.format.parquet.enabled', 'spark.rapids.sql.format.parquet.read.enabled'])
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
@allow_non_gpu('DataWritingCommandExec,ExecutedCommandExec,WriteFilesExec')
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

@pytest.mark.skipif(not is_before_spark_320(), reason='Spark 3.1.x does not need special handling')
@pytest.mark.skipif(is_dataproc_runtime(), reason='https://github.com/NVIDIA/spark-rapids/issues/8074')
def test_parquet_read_nano_as_longs_31x(std_input_path):
    data_path = "%s/timestamp-nanos.parquet" % (std_input_path)
    # we correctly return timestamp_micros when running against Spark 3.1.x
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path))

@pytest.mark.skipif(is_before_spark_320(), reason='Spark 3.1.x supports reading timestamps in nanos')
def test_parquet_read_nano_as_longs_false(std_input_path):
    data_path = "%s/timestamp-nanos.parquet" % (std_input_path)
    conf = copy_and_update(original_parquet_file_reader_conf, {
            'spark.sql.legacy.parquet.nanosAsLong': False })
    def read_timestamp_nano_parquet(spark):
        spark.read.parquet(data_path).collect()
    assert_gpu_and_cpu_error(
        read_timestamp_nano_parquet,
        conf,
        error_message="Illegal Parquet type: INT64 (TIMESTAMP(NANOS,true))")

@pytest.mark.skipif(is_before_spark_320(), reason='Spark 3.1.x supports reading timestamps in nanos')
def test_parquet_read_nano_as_longs_not_configured(std_input_path):
    data_path = "%s/timestamp-nanos.parquet" % (std_input_path)
    def read_timestamp_nano_parquet(spark):
        spark.read.parquet(data_path).collect()
    assert_gpu_and_cpu_error(
        read_timestamp_nano_parquet,
        conf=original_parquet_file_reader_conf,
        error_message="Illegal Parquet type: INT64 (TIMESTAMP(NANOS,true))")

@pytest.mark.skipif(is_before_spark_320(), reason='Spark 3.1.x supports reading timestamps in nanos')
@pytest.mark.skipif(spark_version() >= '3.2.0' and spark_version() < '3.2.4', reason='New config added in 3.2.4')
@pytest.mark.skipif(spark_version() >= '3.3.0' and spark_version() < '3.3.2', reason='New config added in 3.3.2')
@pytest.mark.skipif(is_databricks_runtime() and spark_version() == '3.3.2', reason='Config not in DB 12.2')
@pytest.mark.skipif(is_databricks_runtime() and spark_version() == '3.4.1', reason='Config not in DB 13.3')
@allow_non_gpu('FileSourceScanExec, ColumnarToRowExec')
def test_parquet_read_nano_as_longs_true(std_input_path):
    data_path = "%s/timestamp-nanos.parquet" % (std_input_path)
    conf = copy_and_update(original_parquet_file_reader_conf, {
            'spark.sql.legacy.parquet.nanosAsLong': True })
    assert_gpu_fallback_collect(
        lambda spark: spark.read.parquet(data_path),
        'FileSourceScanExec',
        conf=conf)

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
@allow_non_gpu(*non_utc_allow)
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
@allow_non_gpu(*non_utc_allow)
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

def with_id(i):
    return {'parquet.field.id': i}

# Field ID test cases were re-written from:
# https://github.com/apache/spark/blob/v3.3.0-rc3/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetFieldIdIOSuite.scala
@pytest.mark.skipif(is_before_spark_330(), reason='Field ID is not supported before Spark 330')
@pytest.mark.parametrize('footer_read', ["JAVA", "NATIVE", "AUTO"], ids=idfn)
def test_parquet_read_field_id_using_correctly(spark_tmp_path, footer_read):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    write_schema = StructType([StructField("random", IntegerType(), metadata=with_id(1)),
                               StructField("name", StringType(), metadata=with_id(0))])
    write_data = [(100, 'text'), (200, 'more')]
    # write parquet with field IDs
    with_cpu_session(lambda spark: spark.createDataFrame(write_data, write_schema).repartition(1)
                     .write.mode("overwrite").parquet(data_path),
                     conf=enable_parquet_field_id_write)

    # use field IDs to specify the reading columns, then mapping the column names
    # map column `name` to `a`, map column `random` to `b`
    read_schema = StructType([
        StructField("a", StringType(), True, metadata=with_id(0)),
        StructField("b", IntegerType(), True, metadata=with_id(1)),
    ])
    conf = copy_and_update(enable_parquet_field_id_read,
                           {"spark.rapids.sql.format.parquet.reader.footer.type": footer_read})

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(read_schema).parquet(data_path),
        conf=conf)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(read_schema).parquet(data_path).where("b < 50"),
        conf=conf)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(read_schema).parquet(data_path).where("a >= 'oh'"),
        conf=conf)

    read_schema_mixed = StructType([
        StructField("name", StringType(), True),
        StructField("b", IntegerType(), True, metadata=with_id(1)),
    ])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(read_schema_mixed).parquet(data_path),
        conf=conf)

    read_schema_mixed_half_matched = StructType([
        StructField("unmatched", StringType(), True),
        StructField("b", IntegerType(), True, metadata=with_id(1)),
    ])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(read_schema_mixed_half_matched).parquet(data_path),
        conf=conf)

    # not specify schema
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).where("name >= 'oh'"),
        conf=conf)

@pytest.mark.skipif(is_before_spark_330(), reason='Field ID is not supported before Spark 330')
@pytest.mark.parametrize('footer_read', ["JAVA", "NATIVE", "AUTO"], ids=idfn)
def test_parquet_read_field_id_absence(spark_tmp_path, footer_read):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    write_schema = StructType([StructField("a", IntegerType(), metadata=with_id(3)),
                               StructField("randomName", StringType())])
    write_data = [(100, 'text'), (200, 'more')]
    # write parquet with field IDs
    with_cpu_session(lambda spark: spark.createDataFrame(write_data, write_schema).repartition(1)
                     .write.mode("overwrite").parquet(data_path),
                     conf=enable_parquet_field_id_write)

    conf = copy_and_update(enable_parquet_field_id_read,
                           {"spark.rapids.sql.format.parquet.reader.footer.type": footer_read})

    # 3 different cases for the 3 columns to read:
    #   - a: ID 1 is not found, but there is column with name `a`, still return null
    #   - b: ID 2 is not found, return null
    #   - c: ID 3 is found, read it
    read_schema = StructType([
        StructField("a", IntegerType(), True, metadata=with_id(1)),
        StructField("b", StringType(), True, metadata=with_id(2)),
        StructField("c", IntegerType(), True, metadata=with_id(3)),
    ])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(read_schema).parquet(data_path),
        conf=conf)

@pytest.mark.skipif(is_before_spark_330(), reason='Field ID is not supported before Spark 330')
@pytest.mark.parametrize('footer_read', ["JAVA", "NATIVE", "AUTO"], ids=idfn)
def test_parquet_read_multiple_field_id_matches(spark_tmp_path, footer_read):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    write_schema = StructType([
        StructField("a", IntegerType(), True, metadata=with_id(1)),  # duplicated field ID
        StructField("rand1", StringType(), True, metadata=with_id(2)),
        StructField("rand2", StringType(), True, metadata=with_id(1)),  # duplicated field ID
    ])
    write_data = [(100, 'text', 'txt'), (200, 'more', 'mr')]
    # write parquet with field IDs
    with_cpu_session(lambda spark: spark.createDataFrame(write_data, write_schema).repartition(1)
                     .write.mode("overwrite").parquet(data_path),
                     conf=enable_parquet_field_id_write)

    conf = copy_and_update(enable_parquet_field_id_read,
                           {"spark.rapids.sql.format.parquet.reader.footer.type": footer_read})

    read_schema = StructType([StructField("a", IntegerType(), True, metadata=with_id(1))])
    # Both CPU and GPU invokes `ParquetReadSupport.clipParquetSchema` which throws an exception
    assert_gpu_and_cpu_error(
        lambda spark: spark.read.schema(read_schema).parquet(data_path).collect(),
        conf=conf,
        error_message="Found duplicate field(s)")

@pytest.mark.skipif(is_before_spark_330(), reason='Field ID is not supported before Spark 330')
@pytest.mark.parametrize('footer_read', ["JAVA", "NATIVE", "AUTO"], ids=idfn)
def test_parquet_read_without_field_id(spark_tmp_path, footer_read):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    # Parquet without field ID
    write_schema = StructType([
        StructField("a", IntegerType(), True),
        StructField("rand1", StringType(), True),
        StructField("rand2", StringType(), True),
    ])
    write_data = [(100, 'text', 'txt'), (200, 'more', 'mr')]
    # write parquet with field IDs
    with_cpu_session(lambda spark: spark.createDataFrame(write_data, write_schema).repartition(1)
                     .write.mode("overwrite").parquet(data_path),
                     conf=enable_parquet_field_id_write)

    conf = copy_and_update(enable_parquet_field_id_read,
                           {"spark.rapids.sql.format.parquet.reader.footer.type": footer_read})

    read_schema = StructType([StructField("a", IntegerType(), True, metadata=with_id(1))])

    # Spark read schema expects field Ids, but Parquet file schema doesn't contain any field Ids.
    # If `spark.sql.parquet.fieldId.read.ignoreMissing` is false(default value), throws exception
    assert_gpu_and_cpu_error(
        lambda spark: spark.read.schema(read_schema).parquet(data_path).collect(),
        conf=conf,
        error_message="Parquet file schema doesn't contain any field Ids")

    # Spark read schema expects field Ids, but Parquet file schema doesn't contain any field Ids.
    # If `spark.sql.parquet.fieldId.read.ignoreMissing` is true,
    # return a column with all values are null for the unmatched field IDs
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(read_schema).parquet(data_path),
        conf=copy_and_update(conf,
                             {"spark.sql.parquet.fieldId.read.ignoreMissing": "true"}))

#  test global config: field_id_write_enable=false, field_id_read_enable=true
#  test global config: field_id_write_enable=true,  field_id_read_enable=true
@pytest.mark.skipif(is_before_spark_330(), reason='Field ID is not supported before Spark 330')
@pytest.mark.parametrize('footer_read', ["JAVA", "NATIVE", "AUTO"], ids=idfn)
def test_parquet_read_field_id_global_flags(spark_tmp_path, footer_read):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    write_schema = StructType([
        StructField("a", IntegerType(), True, metadata=with_id(1)),
        StructField("rand1", StringType(), True, metadata=with_id(2)),
        StructField("rand2", StringType(), True, metadata=with_id(3)),
    ])
    read_schema = StructType([
        StructField("some", IntegerType(), True, metadata=with_id(1)),
        StructField("other", StringType(), True, metadata=with_id(2)),
        StructField("name", StringType(), True, metadata=with_id(3)),
    ])
    write_data = [(100, "text", "txt"), (200, "more", "mr")]

    # not write field IDs into Parquet file although `write_schema` contains field IDs
    # try to read by field IDs
    with_cpu_session(lambda spark: spark.createDataFrame(write_data, write_schema).repartition(1)
                     .write.mode("overwrite").parquet(data_path),
                     conf=disable_parquet_field_id_write)

    conf = copy_and_update(enable_parquet_field_id_read,
                           {"spark.rapids.sql.format.parquet.reader.footer.type": footer_read})

    assert_gpu_and_cpu_error(
        lambda spark: spark.read.schema(read_schema).parquet(data_path).collect(),
        conf=conf,
        error_message="Parquet file schema doesn't contain any field Ids")

    # write field IDs into Parquet
    # read by field IDs
    with_cpu_session(lambda spark: spark.createDataFrame(write_data, write_schema).repartition(1)
                     .write.mode("overwrite").parquet(data_path),
                     conf=enable_parquet_field_id_write)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(read_schema).parquet(data_path),
        conf=conf)

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
def test_parquet_read_daytime_interval_cpu_file(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    gen_list = [('_c1', DayTimeIntervalGen())]
    # write DayTimeInterval with CPU
    with_cpu_session(lambda spark :gen_df(spark, gen_list).coalesce(1).write.mode("overwrite").parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: spark.read.parquet(data_path))

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
def test_parquet_read_daytime_interval_gpu_file(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    gen_list = [('_c1', DayTimeIntervalGen())]
    # write DayTimeInterval with GPU
    with_gpu_session(lambda spark :gen_df(spark, gen_list).coalesce(1).write.mode("overwrite").parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: spark.read.parquet(data_path))


@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
def test_parquet_push_down_on_interval_type(spark_tmp_path):
    gen_list = [('_c1', DayTimeIntervalGen())]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_cpu_session(lambda spark: gen_df(spark, gen_list).coalesce(1).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: spark.read.parquet(data_path),
        "testData",
        "select * from testData where _c1 > interval '10 0:0:0' day to second")


def test_parquet_read_case_insensitivity(spark_tmp_path):
    gen_list = [('one', int_gen), ('tWo', byte_gen), ('THREE', boolean_gen)]
    data_path = spark_tmp_path + '/PARQUET_DATA'

    with_cpu_session(lambda spark: gen_df(spark, gen_list).write.parquet(data_path))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path).select('one', 'two', 'three'),
        {'spark.sql.caseSensitive': 'false'}
    )


# test read INT32 as INT8/INT16/Date
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_int32_downcast(spark_tmp_path, reader_confs, v1_enabled_list):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    write_schema = [("d", date_gen), ('s', short_gen), ('b', byte_gen)]
    with_cpu_session(
        lambda spark: gen_df(spark, write_schema).selectExpr(
            "cast(d as Int) as d",
            "cast(s as Int) as s",
            "cast(b as Int) as b").write.parquet(data_path))

    read_schema = StructType([StructField("d", DateType()),
                              StructField("s", ShortType()),
                              StructField("b", ByteType())])
    conf = copy_and_update(reader_confs,
                           {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(read_schema).parquet(data_path),
        conf=conf)

@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
@pytest.mark.parametrize("types", [("byte", "short"), ("byte", "int"), ("short", "int")], ids=idfn)
def test_parquet_read_int_upcast(spark_tmp_path, reader_confs, v1_enabled_list, types):
    data_path = spark_tmp_path + "/PARQUET_DATA"
    store_type, load_type = types
    with_cpu_session(lambda spark: spark.range(10) \
                     .selectExpr(f"cast(id as {store_type})") \
                     .write.parquet(data_path))
    conf = copy_and_update(reader_confs,
                           {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(f"id {load_type}").parquet(data_path),
        conf=conf)

@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_parquet_nested_column_missing(spark_tmp_path, reader_confs, v1_enabled_list):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    write_schema = [("a", string_gen), ("b", int_gen), ("c", StructGen([("ca", long_gen)]))]
    with_cpu_session(
        lambda spark: gen_df(spark, write_schema).write.parquet(data_path))

    read_schema = StructType([StructField("a", StringType()),
                              StructField("b", IntegerType()),
                              StructField("c", StructType([
                                  StructField("ca", LongType()),
                                  StructField("cb", StringType())]))])
    conf = copy_and_update(reader_confs,
                           {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(read_schema).parquet(data_path),
        conf=conf)

def test_parquet_check_schema_compatibility(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    gen_list = [('int', int_gen), ('long', long_gen), ('dec32', decimal_gen_32bit)]
    with_cpu_session(lambda spark: gen_df(spark, gen_list).coalesce(1).write.parquet(data_path))

    read_int_as_long = StructType(
        [StructField('long', LongType()), StructField('int', LongType())])
    assert_gpu_and_cpu_error(
        lambda spark: spark.read.schema(read_int_as_long).parquet(data_path).collect(),
        conf={},
        error_message='Parquet column cannot be converted')

    read_dec32_as_dec64 = StructType(
        [StructField('int', IntegerType()), StructField('dec32', DecimalType(15, 10))])
    assert_gpu_and_cpu_error(
        lambda spark: spark.read.schema(read_dec32_as_dec64).parquet(data_path).collect(),
        conf={},
        error_message='Parquet column cannot be converted')


# For nested types, GPU throws incompatible exception with a different message from CPU.
def test_parquet_check_schema_compatibility_nested_types(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    gen_list = [('array_long', ArrayGen(long_gen)),
                ('array_array_int', ArrayGen(ArrayGen(int_gen))),
                ('struct_float', StructGen([('f', float_gen), ('d', double_gen)])),
                ('struct_array_int', StructGen([('a', ArrayGen(int_gen))])),
                ('map', map_string_string_gen[0])]
    with_cpu_session(lambda spark: gen_df(spark, gen_list).coalesce(1).write.parquet(data_path))

    read_array_long_as_int = StructType([StructField('array_long', ArrayType(IntegerType()))])
    assert_spark_exception(
        lambda: with_gpu_session(
            lambda spark: spark.read.schema(read_array_long_as_int).parquet(data_path).collect()),
        error_message='Parquet column cannot be converted')

    read_arr_arr_int_as_long = StructType(
        [StructField('array_array_int', ArrayType(ArrayType(LongType())))])
    assert_spark_exception(
        lambda: with_gpu_session(
            lambda spark: spark.read.schema(read_arr_arr_int_as_long).parquet(data_path).collect()),
        error_message='Parquet column cannot be converted')

    read_struct_flt_as_dbl = StructType([StructField(
        'struct_float', StructType([StructField('f', DoubleType())]))])
    assert_spark_exception(
        lambda: with_gpu_session(
            lambda spark: spark.read.schema(read_struct_flt_as_dbl).parquet(data_path).collect()),
        error_message='Parquet column cannot be converted')

    read_struct_arr_int_as_long = StructType([StructField(
        'struct_array_int', StructType([StructField('a', ArrayType(LongType()))]))])
    assert_spark_exception(
        lambda: with_gpu_session(
            lambda spark: spark.read.schema(read_struct_arr_int_as_long).parquet(data_path).collect()),
        error_message='Parquet column cannot be converted')

    read_map_str_str_as_str_int = StructType([StructField(
        'map', MapType(StringType(), IntegerType()))])
    assert_spark_exception(
        lambda: with_gpu_session(
            lambda spark: spark.read.schema(read_map_str_str_as_str_int).parquet(data_path).collect()),
        error_message='Parquet column cannot be converted')

@pytest.mark.skipif(is_before_spark_320() or is_spark_321cdh(), reason='Encryption is not supported before Spark 3.2.0 or Parquet < 1.12')
@pytest.mark.skipif(os.environ.get('INCLUDE_PARQUET_HADOOP_TEST_JAR', 'false') == 'false', reason='INCLUDE_PARQUET_HADOOP_TEST_JAR is disabled')
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
def test_parquet_read_encryption(spark_tmp_path, reader_confs, v1_enabled_list):

    data_path = spark_tmp_path + '/PARQUET_DATA'
    gen_list = [('one', int_gen), ('two', byte_gen), ('THREE', boolean_gen)]

    encryption_confs = {
        'parquet.encryption.kms.client.class': 'org.apache.parquet.crypto.keytools.mocks.InMemoryKMS',
        'parquet.encryption.key.list': 'keyA:AAECAwQFBgcICQoLDA0ODw== ,  keyB:AAECAAECAAECAAECAAECAA==',
        'parquet.crypto.factory.class': 'org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory'
    }

    conf = copy_and_update(reader_confs, encryption_confs)

    with_cpu_session(
        lambda spark : gen_df(spark, gen_list).write.
            option("parquet.encryption.column.keys" , "keyA:one").
            option("parquet.encryption.footer.key" , "keyB").
            parquet(data_path), conf=encryption_confs)

    # test with missing encryption conf reading encrypted file
    assert_spark_exception(
        lambda: with_gpu_session(
            lambda spark: spark.read.parquet(data_path).collect()),
        error_message='Could not read footer for file')

    assert_spark_exception(
        lambda: with_gpu_session(
            lambda spark: spark.read.parquet(data_path).collect(), conf=conf),
        error_message='The GPU does not support reading encrypted Parquet files')

def test_parquet_read_count(spark_tmp_path):
    parquet_gens = [int_gen, string_gen, double_gen]
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'

    with_cpu_session(lambda spark: gen_df(spark, gen_list).write.parquet(data_path))

    assert_gpu_and_cpu_row_counts_equal(lambda spark: spark.read.parquet(data_path))

    # assert the spark plan of the equivalent SQL query contains no column in read schema
    assert_cpu_and_gpu_are_equal_sql_with_capture(
        lambda spark: spark.read.parquet(data_path), "SELECT COUNT(*) FROM tab", "tab",
        exist_classes=r'GpuFileGpuScan parquet .* ReadSchema: struct<>')

@pytest.mark.parametrize('read_func', [read_parquet_df, read_parquet_sql])
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@pytest.mark.parametrize('col_name', ['K0', 'k0', 'K3', 'k3', 'V0', 'v0'], ids=idfn)
@ignore_order
def test_read_case_col_name(spark_tmp_path, read_func, v1_enabled_list, reader_confs, col_name):
    all_confs = copy_and_update(reader_confs, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list})
    gen_list =[('k0', LongGen(nullable=False, min_val=0, max_val=0)),
            ('k1', LongGen(nullable=False, min_val=1, max_val=1)),
            ('k2', LongGen(nullable=False, min_val=2, max_val=2)),
            ('k3', LongGen(nullable=False, min_val=3, max_val=3)),
            ('v0', LongGen()),
            ('v1', LongGen()),
            ('v2', LongGen()),
            ('v3', LongGen())]

    gen = StructGen(gen_list, nullable=False)
    data_path = spark_tmp_path + '/PAR_DATA'
    reader = read_func(data_path)
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.partitionBy('k0', 'k1', 'k2', 'k3').parquet(data_path))

    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : reader(spark).selectExpr(col_name),
            conf=all_confs)

@pytest.mark.parametrize("reader_confs", reader_opt_confs, ids=idfn)
@ignore_order
def test_parquet_column_name_with_dots(spark_tmp_path, reader_confs):
    data_path = spark_tmp_path + "/PARQUET_DATA"
    reader = read_parquet_df(data_path)
    all_confs = reader_confs
    gens = [
        ("a.b", StructGen([
            ("c.d.e", StructGen([
                ("f.g", int_gen),
                ("h", string_gen)])),
            ("i.j", long_gen)])),
        ("k", boolean_gen)]
    with_cpu_session(lambda spark: gen_df(spark, gens).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(lambda spark: reader(spark), conf=all_confs)
    assert_gpu_and_cpu_are_equal_collect(lambda spark: reader(spark).selectExpr("`a.b`"), conf=all_confs)
    assert_gpu_and_cpu_are_equal_collect(lambda spark: reader(spark).selectExpr("`a.b`.`c.d.e`.`f.g`"),
                                         conf=all_confs)

def test_parquet_partition_batch_row_count_only_splitting(spark_tmp_path):
    data_path = spark_tmp_path + "/PARQUET_DATA"
    def setup_table(spark):
        spark.range(1000).withColumn("p", f.lit("x")).coalesce(1)\
            .write\
            .partitionBy("p")\
            .parquet(data_path)
    with_cpu_session(lambda spark: setup_table(spark))
    assert_gpu_and_cpu_are_equal_collect(lambda spark: spark.read.parquet(data_path).select("p"),
                                         conf={"spark.rapids.sql.columnSizeBytes": "100"})
