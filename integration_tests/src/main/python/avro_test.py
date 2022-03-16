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
from spark_session import with_cpu_session
import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from marks import *
from pyspark.sql.types import *

support_gens = numeric_gens + [string_gen, boolean_gen]

_enable_all_types_conf = {
    'spark.rapids.sql.format.avro.enabled': 'true',
    'spark.rapids.sql.format.avro.read.enabled': 'true'}

@pytest.mark.parametrize('gen', support_gens)
@pytest.mark.parametrize('v1_enabled_list', ["avro", ""])
def test_basic_read(spark_tmp_path, gen, v1_enabled_list):

    all_confs = copy_and_update(_enable_all_types_conf, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list})

    data_path = spark_tmp_path + '/AVRO_DATA'

    with_cpu_session(
        lambda spark: unary_op_df(spark, gen).write.format("avro").save(data_path)
    )

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.read.format("avro").load(data_path),
        conf=all_confs)