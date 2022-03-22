# Copyright (c) 2022, NVIDIA CORPORATION.
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

from spark_session import with_cpu_session
import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from marks import *
from pyspark.sql.types import *

if os.environ.get('INCLUDE_SPARK_AVRO_JAR', 'false') == 'false':
    pytestmark = pytest.mark.skip(reason=str("INCLUDE_SPARK_AVRO_JAR is disabled"))

support_gens = numeric_gens + [string_gen, boolean_gen]

_enable_all_types_conf = {
    'spark.rapids.sql.format.avro.enabled': 'true',
    'spark.rapids.sql.format.avro.read.enabled': 'true'}


@pytest.mark.parametrize('gen', support_gens)
@pytest.mark.parametrize('v1_enabled_list', ["avro", ""])
def test_basic_read(spark_tmp_path, gen, v1_enabled_list):
    data_path = spark_tmp_path + '/AVRO_DATA'
    with_cpu_session(
        lambda spark: unary_op_df(spark, gen).write.format("avro").save(data_path)
    )

    all_confs = copy_and_update(_enable_all_types_conf, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format("avro").load(data_path),
        conf=all_confs)


@pytest.mark.parametrize('v1_enabled_list', ["", "avro"])
def test_avro_simple_partitioned_read(spark_tmp_path, v1_enabled_list):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(support_gens)]
    first_data_path = spark_tmp_path + '/AVRO_DATA/key=0/key2=20'
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list).write.format("avro").save(first_data_path))
    second_data_path = spark_tmp_path + '/AVRO_DATA/key=1/key2=21'
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list).write.format("avro").save(second_data_path))
    third_data_path = spark_tmp_path + '/AVRO_DATA/key=2/key2=22'
    with_cpu_session(
        lambda spark: gen_df(spark, gen_list).write.format("avro").save(third_data_path))

    data_path = spark_tmp_path + '/AVRO_DATA'

    all_confs = copy_and_update(_enable_all_types_conf, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format("avro").load(data_path),
        conf=all_confs)


@pytest.mark.parametrize('v1_enabled_list', ["", "avro"])
def test_avro_input_meta(spark_tmp_path, v1_enabled_list):
    first_data_path = spark_tmp_path + '/AVRO_DATA/key=0'
    with_cpu_session(
        lambda spark: unary_op_df(spark, long_gen).write.format("avro").save(first_data_path))
    second_data_path = spark_tmp_path + '/AVRO_DATA/key=1'
    with_cpu_session(
        lambda spark: unary_op_df(spark, long_gen).write.format("avro").save(second_data_path))
    data_path = spark_tmp_path + '/AVRO_DATA'

    all_confs = copy_and_update(_enable_all_types_conf, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format("avro").load(data_path)
            .filter(f.col('a') > 0)
            .selectExpr('a',
                        'input_file_name()',
                        'input_file_block_start()',
                        'input_file_block_length()'),
        conf=all_confs)
