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

rapids_reader_types = ['PERFILE', 'COALESCING']

# 50 files for the coalescing reading case
coalescingPartitionNum = 50

def gen_avro_files(gen_list, out_path):
    with_cpu_session(
        lambda spark: gen_df(spark,
            gen_list).repartition(coalescingPartitionNum).write.format("avro").save(out_path)
    )


@pytest.mark.parametrize('v1_enabled_list', ["avro", ""], ids=["v1", "v2"])
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_basic_read(spark_tmp_path, v1_enabled_list, reader_type):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(support_gens)]
    data_path = spark_tmp_path + '/AVRO_DATA'
    gen_avro_files(gen_list, data_path)

    all_confs = copy_and_update(_enable_all_types_conf, {
        'spark.rapids.sql.format.avro.reader.type': reader_type,
        'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format("avro").load(data_path),
        conf=all_confs)


@pytest.mark.parametrize('v1_enabled_list', ["", "avro"], ids=["v1", "v2"])
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_avro_simple_partitioned_read(spark_tmp_path, v1_enabled_list, reader_type):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(support_gens)]
    data_path = spark_tmp_path + '/AVRO_DATA'
    # generate partitioned files
    for v in [0, 1, 2]:
        out_path = data_path + '/key={}/key2=2{}'.format(v, v)
        gen_avro_files(gen_list, out_path)

    all_confs = copy_and_update(_enable_all_types_conf, {
        'spark.rapids.sql.format.avro.reader.type': reader_type,
        'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format("avro").load(data_path),
        conf=all_confs)


@pytest.mark.parametrize('v1_enabled_list', ["", "avro"], ids=["v1", "v2"])
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_avro_input_meta(spark_tmp_path, v1_enabled_list, reader_type):
    data_path = spark_tmp_path + '/AVRO_DATA'
    for v in [0, 1]:
        out_path = data_path + '/key={}'.format(v)
        with_cpu_session(
            lambda spark: unary_op_df(spark, long_gen).write.format("avro").save(out_path))

    all_confs = copy_and_update(_enable_all_types_conf, {
        'spark.rapids.sql.format.avro.reader.type': reader_type,
        'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format("avro").load(data_path)
            .filter(f.col('a') > 0)
            .selectExpr('a',
                        'input_file_name()',
                        'input_file_block_start()',
                        'input_file_block_length()'),
        conf=all_confs)
