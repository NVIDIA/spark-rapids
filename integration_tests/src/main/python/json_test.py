# Copyright (c) 2021-2022, NVIDIA CORPORATION.
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
from src.main.python.marks import approximate_float, allow_non_gpu

from src.main.python.spark_session import with_cpu_session

json_supported_gens = [
    # Spark does not escape '\r' or '\n' even though it uses it to mark end of record
    # This would require multiLine reads to work correctly, so we avoid these chars
    StringGen('(\\w| |\t|\ud720){0,10}', nullable=False),
    StringGen('[aAbB ]{0,10}'),
    byte_gen, short_gen, int_gen, long_gen, boolean_gen,
    # Once https://github.com/NVIDIA/spark-rapids/issues/125 and https://github.com/NVIDIA/spark-rapids/issues/124
    # are fixed we should not have to special case float values any more.
    pytest.param(double_gen, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/125')),
    pytest.param(FloatGen(no_nans=True), marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/124')),
    pytest.param(float_gen, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/125')),
    DoubleGen(no_nans=True)
]

_enable_all_types_conf = {
    'spark.rapids.sql.format.json.enabled': 'true',
    'spark.rapids.sql.format.json.read.enabled': 'true'}

@approximate_float
@pytest.mark.parametrize('data_gen', [
    StringGen('(\\w| |\t|\ud720){0,10}', nullable=False),
    StringGen('[aAbB ]{0,10}'),
    byte_gen, short_gen, int_gen, long_gen, boolean_gen,], ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
@allow_non_gpu('FileSourceScanExec')
def test_json_infer_schema_round_trip(spark_tmp_path, data_gen, v1_enabled_list):
    gen = StructGen([('a', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/JSON_DATA'
    updated_conf = copy_and_update(_enable_all_types_conf, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.json(data_path))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.json(data_path),
            conf=updated_conf)

@approximate_float
@pytest.mark.parametrize('data_gen', json_supported_gens, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
def test_json_round_trip(spark_tmp_path, data_gen, v1_enabled_list):
    gen = StructGen([('a', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/JSON_DATA'
    schema = gen.data_type
    updated_conf = copy_and_update(_enable_all_types_conf, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.json(data_path))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.schema(schema).json(data_path),
            conf=updated_conf)

@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
def test_json_input_meta(spark_tmp_path, v1_enabled_list):
    gen = StructGen([('a', long_gen), ('b', long_gen), ('c', long_gen)], nullable=False)
    first_data_path = spark_tmp_path + '/JSON_DATA/key=0'
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.json(first_data_path))
    second_data_path = spark_tmp_path + '/JSON_DATA/key=1'
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.json(second_data_path))
    data_path = spark_tmp_path + '/JSON_DATA'
    updated_conf = copy_and_update(_enable_all_types_conf, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.schema(gen.data_type)
                    .json(data_path)
                    .filter(f.col('b') > 0)
                    .selectExpr('b',
                        'input_file_name()',
                        'input_file_block_start()',
                        'input_file_block_length()'),
            conf=updated_conf)

json_supported_date_formats = ['yyyy-MM-dd', 'yyyy/MM/dd', 'yyyy-MM', 'yyyy/MM',
        'MM-yyyy', 'MM/yyyy', 'MM-dd-yyyy', 'MM/dd/yyyy']
@pytest.mark.parametrize('date_format', json_supported_date_formats, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
def test_json_date_formats_round_trip(spark_tmp_path, date_format, v1_enabled_list):
    gen = StructGen([('a', DateGen())], nullable=False)
    data_path = spark_tmp_path + '/JSON_DATA'
    schema = gen.data_type
    updated_conf = copy_and_update(_enable_all_types_conf, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write\
                    .option('dateFormat', date_format)\
                    .json(data_path))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read\
                    .schema(schema)\
                    .option('dateFormat', date_format)\
                    .json(data_path),
            conf=updated_conf)

json_supported_ts_parts = ['', # Just the date
        "'T'HH:mm:ss.SSSXXX",
        "'T'HH:mm:ss[.SSS][XXX]",
        "'T'HH:mm:ss.SSS",
        "'T'HH:mm:ss[.SSS]",
        "'T'HH:mm:ss",
        "'T'HH:mm[:ss]",
        "'T'HH:mm"]

@pytest.mark.parametrize('ts_part', json_supported_ts_parts)
@pytest.mark.parametrize('date_format', json_supported_date_formats)
@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
def test_json_ts_formats_round_trip(spark_tmp_path, date_format, ts_part, v1_enabled_list):
    full_format = date_format + ts_part
    data_gen = TimestampGen()
    gen = StructGen([('a', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/JSON_DATA'
    schema = gen.data_type
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write\
                    .option('timestampFormat', full_format)\
                    .json(data_path))
    updated_conf = copy_and_update(_enable_all_types_conf, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read\
                    .schema(schema)\
                    .option('timestampFormat', full_format)\
                    .json(data_path),
            conf=updated_conf)