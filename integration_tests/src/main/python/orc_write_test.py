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

from asserts import assert_gpu_and_cpu_writes_are_equal_collect, assert_gpu_fallback_write
from datetime import date, datetime, timezone
from data_gen import *
from marks import *
from pyspark.sql.types import *

orc_write_gens_list = [
        [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
            string_gen, boolean_gen, DateGen(start=date(1590, 1, 1)),
            TimestampGen(start=datetime(1970, 1, 1, tzinfo=timezone.utc))],
        pytest.param([date_gen], marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/139')),
        pytest.param([timestamp_gen], marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/140'))]

@pytest.mark.parametrize('orc_gens', orc_write_gens_list, ids=idfn)
@pytest.mark.parametrize('orc_impl', ["native", "hive"])
def test_write_round_trip(spark_tmp_path, orc_gens, orc_impl):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
    data_path = spark_tmp_path + '/ORC_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.orc(path),
            lambda spark, path: spark.read.orc(path),
            data_path,
            conf={'spark.sql.orc.impl': orc_impl, 'spark.rapids.sql.format.orc.write.enabled': True})

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
            data_path,
            conf = {'spark.rapids.sql.format.orc.write.enabled': True})

orc_write_compress_options = ['none', 'uncompressed', 'snappy']
@pytest.mark.parametrize('compress', orc_write_compress_options)
def test_compress_write_round_trip(spark_tmp_path, compress):
    data_path = spark_tmp_path + '/ORC_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path : binary_op_df(spark, long_gen).coalesce(1).write.orc(path),
            lambda spark, path : spark.read.orc(path),
            data_path,
            conf={'spark.sql.orc.compression.codec': compress, 'spark.rapids.sql.format.orc.write.enabled': True})

@pytest.mark.parametrize('orc_gens', orc_write_gens_list, ids=idfn)
@pytest.mark.parametrize('orc_impl', ["native", "hive"])
def test_write_save_table(spark_tmp_path, orc_gens, orc_impl, spark_tmp_table_factory):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
    data_path = spark_tmp_path + '/ORC_DATA'
    all_confs={'spark.sql.sources.useV1SourceList': "orc",
               'spark.rapids.sql.format.orc.write.enabled': True,
               "spark.sql.orc.impl": orc_impl}
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: gen_df(spark, gen_list).coalesce(1).write.format("orc").mode('overwrite').option("path", path).saveAsTable(spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.orc(path),
            data_path,
            conf=all_confs)

def write_orc_sql_from(spark, df, data_path, write_to_table):
    tmp_view_name = 'tmp_view_{}'.format(random.randint(0, 1000000))
    df.createOrReplaceTempView(tmp_view_name)
    write_cmd = 'CREATE TABLE `{}` USING ORC location \'{}\' AS SELECT * from `{}`'.format(write_to_table, data_path, tmp_view_name)
    spark.sql(write_cmd)

@pytest.mark.parametrize('orc_gens', orc_write_gens_list, ids=idfn)
@pytest.mark.parametrize('ts_type', ["TIMESTAMP_MICROS", "TIMESTAMP_MILLIS"])
@pytest.mark.parametrize('orc_impl', ["native", "hive"])
def test_write_sql_save_table(spark_tmp_path, orc_gens, ts_type, orc_impl, spark_tmp_table_factory):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(orc_gens)]
    data_path = spark_tmp_path + '/ORC_DATA'
    assert_gpu_and_cpu_writes_are_equal_collect(
            lambda spark, path: write_orc_sql_from(spark, gen_df(spark, gen_list).coalesce(1), path, spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.orc(path),
            data_path,
            conf={'spark.sql.orc.impl': orc_impl, 'spark.rapids.sql.format.orc.write.enabled': True})

@allow_non_gpu('DataWritingCommandExec')
@pytest.mark.parametrize('codec', ['zlib', 'lzo'])
def test_orc_write_compression_fallback(spark_tmp_path, codec, spark_tmp_table_factory):
    gen = TimestampGen()
    data_path = spark_tmp_path + '/PARQUET_DATA'
    all_confs={'spark.sql.orc.compression.codec': codec, 'spark.rapids.sql.format.orc.write.enabled': True}
    assert_gpu_fallback_write(
            lambda spark, path: unary_op_df(spark, gen).coalesce(1).write.format("orc").mode('overwrite').option("path", path).saveAsTable(spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.orc(path),
            data_path,
            'DataWritingCommandExec',
            conf=all_confs)

@ignore_order
@allow_non_gpu('DataWritingCommandExec')
def test_buckets_write_fallback(spark_tmp_path, spark_tmp_table_factory):
    data_path = spark_tmp_path + '/ORC_DATA'
    assert_gpu_fallback_write(
            lambda spark, path: spark.range(10e4).write.bucketBy(4, "id").sortBy("id").format('orc').mode('overwrite').option("path", path).saveAsTable(spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.orc(path),
            data_path,
            'DataWritingCommandExec',
            conf = {'spark.rapids.sql.format.orc.write.enabled': True})
