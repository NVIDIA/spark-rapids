# Copyright (c) 2021-2023, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect
from data_gen import *
from pyspark.sql.types import *
from marks import *

def mk_json_str_gen(pattern):
    return StringGen(pattern).with_special_case('').with_special_pattern('.{0,10}')

@pytest.mark.parametrize('json_str_pattern', [r'\{"store": \{"fruit": \[\{"weight":\d,"type":"[a-z]{1,9}"\}\], ' \
                   r'"bicycle":\{"price":[1-9]\d\.\d\d,"color":"[a-z]{0,4}"\}\},' \
                   r'"email":"[a-z]{1,5}\@[a-z]{3,10}\.com","owner":"[a-z]{3,8}"\}',
                   r'\{"a": "[a-z]{1,3}"\}'], ids=idfn)
def test_get_json_object(json_str_pattern):
    gen = mk_json_str_gen(json_str_pattern)
    scalar_json = '{"store": {"fruit": [{"name": "test"}]}}'
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen, length=10).selectExpr(
            'get_json_object(a,"$.a")',
            'get_json_object(a, "$.owner")',
            'get_json_object(a, "$.store.fruit[0]")',
            'get_json_object(\'%s\', "$.store.fruit[0]")' % scalar_json,
            ),
        conf={'spark.sql.parser.escapedStringLiterals': 'true'})


@allow_non_gpu('ProjectExec')
@pytest.mark.parametrize('json_str_pattern', [r'\{"store": \{"fruit": \[\{"weight":\d,"type":"[a-z]{1,9}"\}\], ' \
                   r'"bicycle":\{"price":[1-9]\d\.\d\d,"color":"[a-z]{0,4}"\}\},' \
                   r'"email":"[a-z]{1,5}\@[a-z]{3,10}\.com","owner":"[a-z]{3,8}"\}',
                   r'\{"a": "[a-z]{1,3}"\}'], ids=idfn)
def test_unsupported_fallback_get_json_object(json_str_pattern):
    gen = mk_json_str_gen(json_str_pattern)
    scalar_json = '{"store": {"fruit": "test"}}'
    pattern = StringGen(pattern='\$\.[a-z]{1,9}')
    def assert_gpu_did_fallback(sql_text):
        assert_gpu_fallback_collect(lambda spark:
            gen_df(spark, [('a', gen), ('b', pattern)], length=10).selectExpr(sql_text),
        'GetJsonObject',
        conf={'spark.sql.parser.escapedStringLiterals': 'true'})

    assert_gpu_did_fallback('get_json_object(a, b)')
    assert_gpu_did_fallback('get_json_object(\'%s\', b)' % scalar_json)

