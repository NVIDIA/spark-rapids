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
from src.main.python.marks import approximate_float

from src.main.python.spark_session import with_cpu_session

json_supported_gens = [
    # Spark does not escape '\r' or '\n' even though it uses it to mark end of record
    # This would require multiLine reads to work correctly, so we avoid these chars
    StringGen('(\\w| |\t|\ud720){0,10}', nullable=False),
    StringGen('[aAbB ]{0,10}'),
    byte_gen, short_gen, int_gen, long_gen, boolean_gen,
    # FloatGen(no_nans=True), # Test will fail
    DoubleGen(no_nans=True)
]

_enable_all_types_conf = {
    'spark.rapids.sql.format.json.enabled': 'true',
    'spark.rapids.sql.format.json.read.enabled': 'true'}

@approximate_float
@pytest.mark.parametrize('data_gen', json_supported_gens, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
def test_round_trip(spark_tmp_path, data_gen, v1_enabled_list):
    gen = StructGen([('a', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/JSON_DATA'
    schema = gen.data_type
    updated_conf = copy_and_update(_enable_all_types_conf, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.json(data_path))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.schema(schema).json(data_path),
            conf=updated_conf)
