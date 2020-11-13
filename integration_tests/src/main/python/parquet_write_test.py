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

# test with original parquet file reader, the multi-file parallel reader for cloud, and coalesce file reader for
# non-cloud
original_parquet_file_reader_conf={'spark.rapids.sql.format.parquet.reader.type': 'PERFILE'}
multithreaded_parquet_file_reader_conf={'spark.rapids.sql.format.parquet.reader.type': 'MULTITHREADED'}
coalesce_parquet_file_reader_conf={'spark.rapids.sql.format.parquet.reader.type': 'COALESCING'}
reader_opt_confs = [original_parquet_file_reader_conf, multithreaded_parquet_file_reader_conf,
        coalesce_parquet_file_reader_conf]

parquet_write_gens_list = [
        [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
            string_gen, boolean_gen, date_gen, timestamp_gen]]

@pytest.mark.parametrize('parquet_gens', parquet_write_gens_list, ids=idfn)
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
@pytest.mark.parametrize('ts_type', ["TIMESTAMP_MICROS", "TIMESTAMP_MILLIS"])
def test_write_round_trip(spark_tmp_path, parquet_gens, v1_enabled_list, ts_type, reader_confs):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs = reader_confs.copy()
    all_confs.update({'spark.sql.sources.useV1SourceList': v1_enabled_list,
            'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
            'spark.sql.parquet.outputTimestampType': ts_type})
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.parquet(path),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            conf=all_confs)

@pytest.mark.parametrize('ts_type', ['TIMESTAMP_MILLIS', 'TIMESTAMP_MICROS'])
@pytest.mark.parametrize('ts_rebase', ['CORRECTED'])
@ignore_order
def test_write_ts_millis(spark_tmp_path, ts_type, ts_rebase):
    gen = TimestampGen()
    data_path = spark_tmp_path + '/PARQUET_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
        lambda spark, path: unary_op_df(spark, gen).write.parquet(path),
        lambda spark, path: spark.read.parquet(path),
        data_path,
        conf={'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': ts_rebase,
              'spark.sql.parquet.outputTimestampType': ts_type})

parquet_part_write_gens = [
        byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
        # Some file systems have issues with UTF8 strings so to help the test pass even there
        StringGen('(\\w| ){0,50}'),
        boolean_gen, date_gen, timestamp_gen]

# There are race conditions around when individual files are read in for partitioned data
@ignore_order
@pytest.mark.parametrize('parquet_gen', parquet_part_write_gens, ids=idfn)
@pytest.mark.parametrize('reader_confs', reader_opt_confs)
@pytest.mark.parametrize('v1_enabled_list', ["", "parquet"])
@pytest.mark.parametrize('ts_type', ['TIMESTAMP_MILLIS', 'TIMESTAMP_MICROS'])
def test_part_write_round_trip(spark_tmp_path, parquet_gen, v1_enabled_list, ts_type, reader_confs):
    gen_list = [('a', RepeatSeqGen(parquet_gen, 10)),
            ('b', parquet_gen)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs = reader_confs.copy()
    all_confs.update({'spark.sql.sources.useV1SourceList': v1_enabled_list,
            'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
            'spark.sql.parquet.outputTimestampType': ts_type})
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.partitionBy('a').parquet(path),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            conf=all_confs)

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
@pytest.mark.parametrize('ts_type', ["TIMESTAMP_MICROS", "TIMESTAMP_MILLIS"])
def test_write_save_table(spark_tmp_path, parquet_gens, ts_type, spark_tmp_table_factory):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(parquet_gens)]
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs={'spark.sql.sources.useV1SourceList': "parquet",
            'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
            'spark.sql.parquet.outputTimestampType': ts_type}
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.format("parquet").mode('overwrite').option("path", path).saveAsTable(spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.parquet(path),
            data_path,
            conf=all_confs)
