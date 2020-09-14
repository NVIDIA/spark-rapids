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
from spark_session import with_cpu_session, with_spark_session, is_before_spark_310

def read_orc_df(data_path):
    return lambda spark : spark.read.orc(data_path)

def read_orc_sql(data_path):
    return lambda spark : spark.sql('select * from orc.`{}`'.format(data_path))

@pytest.mark.parametrize('name', ['timestamp-date-test.orc'])
@pytest.mark.parametrize('read_func', [read_orc_df, read_orc_sql])
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
def test_basic_read(std_input_path, name, read_func, v1_enabled_list):
    assert_gpu_and_cpu_are_equal_collect(
            read_func(std_input_path + '/' + name),
            conf={'spark.sql.sources.useV1SourceList': v1_enabled_list})

orc_gens_list = [[byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, boolean_gen, DateGen(start=date(1590, 1, 1)),
    TimestampGen(start=datetime(1590, 1, 1, tzinfo=timezone.utc))],
    pytest.param([date_gen], marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/131')),
    pytest.param([timestamp_gen], marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/131'))]

@allow_non_gpu('FileSourceScanExec')
@pytest.mark.parametrize('read_func', [read_orc_df, read_orc_sql])
@pytest.mark.parametrize('disable_conf', ['spark.rapids.sql.format.orc.enabled', 'spark.rapids.sql.format.orc.read.enabled'])
def test_orc_fallback(spark_tmp_path, read_func, disable_conf):
    data_gens =[string_gen,
        byte_gen, short_gen, int_gen, long_gen, boolean_gen]
 
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(data_gens)]
    gen = StructGen(gen_list, nullable=False)
    data_path = spark_tmp_path + '/PARQUET_DATA'
    reader = read_func(data_path)
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.orc(data_path))
    assert_gpu_fallback_collect(
            lambda spark : reader(spark).select(f.col('*'), f.col('_c2') + f.col('_c3')),
            'FileSourceScanExec',
            conf={disable_conf: 'false'})

@pytest.mark.parametrize('orc_gens', orc_gens_list, ids=idfn)
@pytest.mark.parametrize('read_func', [read_orc_df, read_orc_sql])
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
def test_read_round_trip(spark_tmp_path, orc_gens, read_func, v1_enabled_list):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
    data_path = spark_tmp_path + '/ORC_DATA'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.orc(data_path))
    assert_gpu_and_cpu_are_equal_collect(
            read_func(data_path),
            conf={'spark.sql.sources.useV1SourceList': v1_enabled_list})

orc_pred_push_gens = [
        byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen, boolean_gen,
        string_gen,
        # Once https://github.com/NVIDIA/spark-rapids/issues/139 is fixed replace this with
        # date_gen
        DateGen(start=date(1590, 1, 1)),
        # Once https://github.com/NVIDIA/spark-rapids/issues/140 is fixed replace this with
        # timestamp_gen 
        TimestampGen(start=datetime(1970, 1, 1, tzinfo=timezone.utc))]

@pytest.mark.parametrize('orc_gen', orc_pred_push_gens, ids=idfn)
@pytest.mark.parametrize('read_func', [read_orc_df, read_orc_sql])
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
def test_pred_push_round_trip(spark_tmp_path, orc_gen, read_func, v1_enabled_list):
    data_path = spark_tmp_path + '/ORC_DATA'
    gen_list = [('a', RepeatSeqGen(orc_gen, 100)), ('b', orc_gen)]
    s0 = gen_scalar(orc_gen, force_no_nulls=True)
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).orderBy('a').write.orc(data_path))
    rf = read_func(data_path)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: rf(spark).select(f.col('a') >= s0),
            conf={'spark.sql.sources.useV1SourceList': v1_enabled_list})

orc_compress_options = ['none', 'uncompressed', 'snappy', 'zlib']
# The following need extra jars 'lzo'
# https://github.com/NVIDIA/spark-rapids/issues/143

@pytest.mark.parametrize('compress', orc_compress_options)
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
def test_compress_read_round_trip(spark_tmp_path, compress, v1_enabled_list):
    data_path = spark_tmp_path + '/ORC_DATA'
    with_cpu_session(
            lambda spark : binary_op_df(spark, long_gen).write.orc(data_path),
            conf={'spark.sql.orc.compression.codec': compress})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.orc(data_path),
            conf={'spark.sql.sources.useV1SourceList': v1_enabled_list})

@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
def test_simple_partitioned_read(spark_tmp_path, v1_enabled_list):
    # Once https://github.com/NVIDIA/spark-rapids/issues/131 is fixed
    # we should go with a more standard set of generators
    orc_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, boolean_gen, DateGen(start=date(1590, 1, 1)),
    TimestampGen(start=datetime(1590, 1, 1, tzinfo=timezone.utc))]
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
    first_data_path = spark_tmp_path + '/ORC_DATA/key=0'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.orc(first_data_path))
    second_data_path = spark_tmp_path + '/ORC_DATA/key=1'
    with_cpu_session(
            lambda spark : gen_df(spark, gen_list).write.orc(second_data_path))
    data_path = spark_tmp_path + '/ORC_DATA'
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.orc(data_path),
            conf={'spark.sql.sources.useV1SourceList': v1_enabled_list})

@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/135')
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
def test_merge_schema_read(spark_tmp_path, v1_enabled_list):
    # Once https://github.com/NVIDIA/spark-rapids/issues/131 is fixed
    # we should go with a more standard set of generators
    orc_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
    string_gen, boolean_gen, DateGen(start=date(1590, 1, 1)),
    TimestampGen(start=datetime(1590, 1, 1, tzinfo=timezone.utc))]
    first_gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
    first_data_path = spark_tmp_path + '/ORC_DATA/key=0'
    with_cpu_session(
            lambda spark : gen_df(spark, first_gen_list).write.orc(first_data_path))
    second_gen_list = [(('_c' if i % 2 == 0 else '_b') + str(i), gen) for i, gen in enumerate(orc_gens)]
    second_data_path = spark_tmp_path + '/ORC_DATA/key=1'
    with_cpu_session(
            lambda spark : gen_df(spark, second_gen_list).write.orc(second_data_path))
    data_path = spark_tmp_path + '/ORC_DATA'
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.option('mergeSchema', 'true').orc(data_path),
            conf={'spark.sql.sources.useV1SourceList': v1_enabled_list})

orc_write_gens_list = [
        [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
            string_gen, boolean_gen, DateGen(start=date(1590, 1, 1)),
            TimestampGen(start=datetime(1970, 1, 1, tzinfo=timezone.utc))],
        pytest.param([date_gen], marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/139')),
        pytest.param([timestamp_gen], marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/140'))]

@pytest.mark.parametrize('orc_gens', orc_write_gens_list, ids=idfn)
def test_write_round_trip(spark_tmp_path, orc_gens):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
    data_path = spark_tmp_path + '/ORC_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.orc(path),
            lambda spark, path: spark.read.orc(path),
            data_path)

orc_part_write_gens = [
        byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen, boolean_gen,
        # Some file systems have issues with UTF8 strings so to help the test pass even there
        StringGen('(\\w| ){0,50}'),
        # Once https://github.com/NVIDIA/spark-rapids/issues/139 is fixed replace this with
        # date_gen
        DateGen(start=date(1590, 1, 1)),
        # Once https://github.com/NVIDIA/spark-rapids/issues/140 is fixed replace this with
        # timestamp_gen 
        TimestampGen(start=datetime(1970, 1, 1, tzinfo=timezone.utc))]

# There are race conditions around when individual files are read in for partitioned data
@ignore_order
@pytest.mark.parametrize('orc_gen', orc_part_write_gens, ids=idfn)
def test_part_write_round_trip(spark_tmp_path, orc_gen):
    gen_list = [('a', RepeatSeqGen(orc_gen, 10)),
            ('b', orc_gen)]
    data_path = spark_tmp_path + '/ORC_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.partitionBy('a').orc(path),
            lambda spark, path: spark.read.orc(path),
            data_path)

orc_write_compress_options = ['none', 'uncompressed', 'snappy']
@pytest.mark.parametrize('compress', orc_write_compress_options)
def test_compress_write_round_trip(spark_tmp_path, compress):
    data_path = spark_tmp_path + '/ORC_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path : binary_op_df(spark, long_gen).coalesce(1).write.orc(path),
            lambda spark, path : spark.read.orc(path),
            data_path,
            conf={'spark.sql.orc.compression.codec': compress})

@pytest.mark.xfail(
    condition=not(is_before_spark_310()),
    reason='https://github.com/NVIDIA/spark-rapids/issues/576')
def test_input_meta(spark_tmp_path):
    first_data_path = spark_tmp_path + '/ORC_DATA/key=0'
    with_cpu_session(
            lambda spark : unary_op_df(spark, long_gen).write.orc(first_data_path))
    second_data_path = spark_tmp_path + '/ORC_DATA/key=1'
    with_cpu_session(
            lambda spark : unary_op_df(spark, long_gen).write.orc(second_data_path))
    data_path = spark_tmp_path + '/ORC_DATA'
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.orc(data_path)\
                    .filter(f.col('a') > 0)\
                    .selectExpr('a',
                        'input_file_name()',
                        'input_file_block_start()',
                        'input_file_block_length()'))
