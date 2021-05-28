# Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_writes_are_equal_collect, assert_gpu_fallback_write, assert_py4j_exception
from datetime import date, datetime, timezone
from data_gen import *
from marks import *
from pyspark.sql.types import *
from spark_session import with_cpu_session, with_gpu_session
import random

# test with original parquet file reader, the multi-file parallel reader for cloud, and coalesce file reader for
# non-cloud
original_parquet_file_reader_conf={'spark.rapids.sql.format.parquet.reader.type': 'PERFILE'}
multithreaded_parquet_file_reader_conf={'spark.rapids.sql.format.parquet.reader.type': 'MULTITHREADED'}
coalesce_parquet_file_reader_conf={'spark.rapids.sql.format.parquet.reader.type': 'COALESCING'}
reader_opt_confs = [original_parquet_file_reader_conf, multithreaded_parquet_file_reader_conf,
        coalesce_parquet_file_reader_conf]
parquet_decimal_gens=[decimal_gen_default, decimal_gen_scale_precision, decimal_gen_same_scale_precision, decimal_gen_64bit]
parquet_decimal_struct_gen= StructGen([['child'+str(ind), sub_gen] for ind, sub_gen in enumerate(parquet_decimal_gens)])
writer_confs={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
              'spark.sql.legacy.parquet.int96RebaseModeInWrite': 'CORRECTED'}


parquet_basic_gen =[byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
                    string_gen, boolean_gen, date_gen,
                    # we are limiting TimestampGen to avoid overflowing the INT96 value
                    # see https://github.com/rapidsai/cudf/issues/8070
                    TimestampGen(start=datetime(1677, 9, 22, tzinfo=timezone.utc), end=datetime(2262, 4, 11, tzinfo=timezone.utc))]
parquet_struct_gen = [StructGen([['child'+str(ind), sub_gen] for ind, sub_gen in enumerate(parquet_basic_gen)]),
                      StructGen([['child0', StructGen([[ 'child1', byte_gen]])]])]

parquet_write_gens_list = [parquet_basic_gen + parquet_struct_gen +
                           [decimal_gen_default,
                           decimal_gen_scale_precision, decimal_gen_same_scale_precision, decimal_gen_64bit]]

parquet_ts_write_options = ['INT96', 'TIMESTAMP_MICROS', 'TIMESTAMP_MILLIS']

@pytest.mark.parametrize('parquet_gens', parquet_write_gens_list, ids=idfn)
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
@pytest.mark.parametrize('ts_type', parquet_ts_write_options)
def test_write_round_trip(spark_tmp_path, parquet_gens, v1_enabled_list, ts_type,
                                  reader_confs):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs = reader_confs.copy()
    all_confs.update({'spark.sql.sources.useV1SourceList': v1_enabled_list,
            'spark.sql.parquet.outputTimestampType': ts_type})
    all_confs.update(writer_confs)
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.parquet(path),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            conf=all_confs)

@pytest.mark.parametrize('ts_type', parquet_ts_write_options)
@pytest.mark.parametrize('ts_rebase', ['CORRECTED'])
@ignore_order
def test_write_ts_millis(spark_tmp_path, ts_type, ts_rebase):
    # we are limiting TimestampGen to avoid overflowing the INT96 value
    # see https://github.com/rapidsai/cudf/issues/8070
    gen = TimestampGen(start=datetime(1677, 9, 22, tzinfo=timezone.utc), end=datetime(2262, 4, 11, tzinfo=timezone.utc))
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: unary_op_df(spark, gen).write.parquet(path),
        lambda spark, path: spark.read.parquet(path),
        data_path,
        conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': ts_rebase,
              'spark.sql.legacy.parquet.int96RebaseModeInWrite': ts_rebase,
              'spark.sql.parquet.outputTimestampType': ts_type})


parquet_part_write_gens = [
    byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    # Some file systems have issues with UTF8 strings so to help the test pass even there
    StringGen('(\\w| ){0,50}'),
    boolean_gen, date_gen,
    # we are limiting TimestampGen to avoid overflowing the INT96 value
    # see https://github.com/rapidsai/cudf/issues/8070
    TimestampGen(start=datetime(1677, 9, 22, tzinfo=timezone.utc), end=datetime(2262, 4, 11, tzinfo=timezone.utc))]

# There are race conditions around when individual files are read in for partitioned data
@ignore_order
@pytest.mark.parametrize('parquet_gen', parquet_part_write_gens, ids=idfn)
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
@pytest.mark.parametrize('ts_type', parquet_ts_write_options)
def test_part_write_round_trip(spark_tmp_path, parquet_gen, v1_enabled_list, ts_type, reader_confs):
    gen_list = [('a', RepeatSeqGen(parquet_gen, 10)),
            ('b', parquet_gen)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs = reader_confs.copy()
    all_confs.update({'spark.sql.sources.useV1SourceList': v1_enabled_list,
            'spark.sql.parquet.outputTimestampType': ts_type})
    all_confs.update(writer_confs)
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.partitionBy('a').parquet(path),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            conf=all_confs)

# we are limiting TimestampGen to avoid overflowing the INT96 value
# see https://github.com/rapidsai/cudf/issues/8070
@pytest.mark.parametrize('data_gen', [TimestampGen(end=datetime(1677, 9, 22, tzinfo=timezone.utc)),
                                      TimestampGen(start=datetime(2262, 4, 11, tzinfo=timezone.utc))], ids=idfn)
def test_catch_int96_overflow(spark_tmp_path, data_gen):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    confs = writer_confs.copy()
    confs.update({'spark.sql.parquet.outputTimestampType': 'INT96'})
    assert_py4j_exception(lambda: with_gpu_session(
        lambda spark: unary_op_df(spark, data_gen).coalesce(1).write.parquet(data_path), conf=confs), "org.apache.spark.SparkException: Job aborted.")

@pytest.mark.parametrize('data_gen', [TimestampGen()], ids=idfn)
@pytest.mark.allow_non_gpu("DataWritingCommandExec")
def test_int96_write_conf(spark_tmp_path, data_gen):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    confs = writer_confs.copy()
    confs.update({'spark.sql.parquet.outputTimestampType': 'INT96',
                 'spark.rapids.sql.format.parquet.writer.int96.enabled': 'false'})
    with_gpu_session(lambda spark: unary_op_df(spark, data_gen).coalesce(1).write.parquet(data_path), conf=confs)

parquet_write_compress_options = ['none', 'uncompressed', 'snappy']
@pytest.mark.parametrize('compress', parquet_write_compress_options)
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
def test_compress_write_round_trip(spark_tmp_path, compress, v1_enabled_list, reader_confs):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs = reader_confs.copy()
    all_confs.update({'spark.sql.sources.useV1SourceList': v1_enabled_list,
            'spark.sql.parquet.compression.codec': compress})
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path : binary_op_df(spark, long_gen).coalesce(1).write.parquet(path),
            lambda spark, path : spark.read.parquet(path),
            data_path,
            conf=all_confs)

@pytest.mark.parametrize('parquet_gens', parquet_write_gens_list, ids=idfn)
@pytest.mark.parametrize('ts_type', parquet_ts_write_options)
def test_write_save_table(spark_tmp_path, parquet_gens, ts_type, spark_tmp_table_factory):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs={'spark.sql.parquet.outputTimestampType': ts_type}
    all_confs.update(writer_confs)
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.format("parquet").mode('overwrite').option("path", path).saveAsTable(spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            conf=all_confs)

def write_parquet_sql_from(spark, df, data_path, write_to_table):
    tmp_view_name = 'tmp_view_{}'.format(random.randint(0, 1000000))
    df.createOrReplaceTempView(tmp_view_name)
    write_cmd = 'CREATE TABLE `{}` USING PARQUET location \'{}\' AS SELECT * from `{}`'.format(write_to_table, data_path, tmp_view_name)
    spark.sql(write_cmd)

@pytest.mark.parametrize('parquet_gens', parquet_write_gens_list, ids=idfn)
@pytest.mark.parametrize('ts_type', parquet_ts_write_options)
def test_write_sql_save_table(spark_tmp_path, parquet_gens, ts_type, spark_tmp_table_factory):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs={'spark.sql.parquet.outputTimestampType': ts_type}
    all_confs.update(writer_confs)
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: write_parquet_sql_from(spark, gen_df(spark, gen_list).coalesce(1), path, spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            conf=all_confs)

def writeParquetUpgradeCatchException(spark, df, data_path, spark_tmp_table_factory, ts_rebase, ts_write):
    spark.conf.set('spark.sql.parquet.outputTimestampType', ts_write)
    spark.conf.set('spark.sql.legacy.parquet.datetimeRebaseModeInWrite', ts_rebase)
    spark.conf.set('spark.sql.legacy.parquet.int96RebaseModeInWrite', ts_rebase) # for spark 310
    with pytest.raises(Exception) as e_info:
        df.coalesce(1).write.format("parquet").mode('overwrite').option("path", data_path).saveAsTable(spark_tmp_table_factory.get())
    assert e_info.match(r".*SparkUpgradeException.*")

# TODO - https://github.com/NVIDIA/spark-rapids/issues/1130 to handle TIMESTAMP_MILLIS
# TODO - we are limiting the INT96 values, see https://github.com/rapidsai/cudf/issues/8070
@pytest.mark.parametrize('ts_write_data_gen', [('INT96', TimestampGen(start=datetime(1677, 9, 22, tzinfo=timezone.utc), end=datetime(2262, 4, 11, tzinfo=timezone.utc))),
                                      ('TIMESTAMP_MICROS', TimestampGen(start=datetime(1, 1, 1, tzinfo=timezone.utc), end=datetime(1582, 1, 1, tzinfo=timezone.utc)))])
@pytest.mark.parametrize('ts_rebase', ['EXCEPTION'])
def test_ts_write_fails_datetime_exception(spark_tmp_path, ts_write_data_gen, ts_rebase, spark_tmp_table_factory):
    ts_write, gen = ts_write_data_gen
    data_path = spark_tmp_path + '/PARQUET_DATA'
    with_gpu_session(
            lambda spark : writeParquetUpgradeCatchException(spark, unary_op_df(spark, gen), data_path, spark_tmp_table_factory, ts_rebase, ts_write))

def writeParquetNoOverwriteCatchException(spark, df, data_path, table_name):
    with pytest.raises(Exception) as e_info:
        df.coalesce(1).write.format("parquet").option("path", data_path).saveAsTable(table_name)
    assert e_info.match(r".*already exists.*")

def test_ts_write_twice_fails_exception(spark_tmp_path, spark_tmp_table_factory):
    gen = IntegerGen()
    data_path = spark_tmp_path + '/PARQUET_DATA'
    table_name = spark_tmp_table_factory.get()
    with_gpu_session(
            lambda spark : unary_op_df(spark, gen).coalesce(1).write.format("parquet").mode('overwrite').option("path", data_path).saveAsTable(table_name))
    with_gpu_session(
            lambda spark : writeParquetNoOverwriteCatchException(spark, unary_op_df(spark, gen), data_path, table_name))

@allow_non_gpu('DataWritingCommandExec')
@pytest.mark.parametrize('ts_write', parquet_ts_write_options)
@pytest.mark.parametrize('ts_rebase', ['LEGACY'])
def test_parquet_write_legacy_fallback(spark_tmp_path, ts_write, ts_rebase, spark_tmp_table_factory):
    gen = TimestampGen(start=datetime(1590, 1, 1, tzinfo=timezone.utc))
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': ts_rebase,
            'spark.sql.legacy.parquet.int96RebaseModeInWrite': ts_rebase,
            'spark.sql.parquet.outputTimestampType': ts_write}
    assert_gpu_fallback_write(
            lambda spark, path: unary_op_df(spark, gen).coalesce(1).write.format("parquet").mode('overwrite').option("path", path).saveAsTable(spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            'DataWritingCommandExec',
            conf=all_confs)

@allow_non_gpu('DataWritingCommandExec')
# note that others should fail as well but requires you to load the libraries for them
# 'lzo', 'brotli', 'lz4', 'zstd' should all fallback
@pytest.mark.parametrize('codec', ['gzip'])
def test_parquet_write_compression_fallback(spark_tmp_path, codec, spark_tmp_table_factory):
    gen = IntegerGen()
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs={'spark.sql.parquet.compression.codec': codec}
    assert_gpu_fallback_write(
            lambda spark, path: unary_op_df(spark, gen).coalesce(1).write.format("parquet").mode('overwrite').option("path", path).saveAsTable(spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            'DataWritingCommandExec',
            conf=all_confs)

@allow_non_gpu('DataWritingCommandExec')
def test_parquet_writeLegacyFormat_fallback(spark_tmp_path, spark_tmp_table_factory):
    gen = IntegerGen()
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs={'spark.sql.parquet.writeLegacyFormat': 'true'}
    assert_gpu_fallback_write(
            lambda spark, path: unary_op_df(spark, gen).coalesce(1).write.format("parquet").mode('overwrite').option("path", path).saveAsTable(spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            'DataWritingCommandExec',
            conf=all_confs)

@ignore_order
@allow_non_gpu('DataWritingCommandExec')
def test_buckets_write_fallback(spark_tmp_path, spark_tmp_table_factory):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_fallback_write(
            lambda spark, path: spark.range(10e4).write.bucketBy(4, "id").sortBy("id").format('parquet').mode('overwrite').option("path", path).saveAsTable(spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            'DataWritingCommandExec')
