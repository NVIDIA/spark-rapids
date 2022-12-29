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

import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from marks import allow_non_gpu
from pyspark.sql.types import *

def mk_json_str_gen(pattern):
    return StringGen(pattern).with_special_case('').with_special_pattern('.{0,10}')

# @allow_non_gpu('GenerateExec')
# @pytest.mark.parametrize('json_str_pattern', [r'\{"store": \{"fruit": \[\{"weight":\d,"type":"[a-z]{1,9}"\}\], ' \
#                    r'"bicycle":\{"price":\d\d\.\d\d,"color":"[a-z]{0,4}"\}\},' \
#                    r'"email":"[a-z]{1,5}\@[a-z]{3,10}\.com","owner":"[a-z]{3,8}"\}',
#                    r'\{"a": "[a-z]{1,3}"\}'], ids=idfn)
# @pytest.mark.parametrize('json_str_pattern', [r'\{"store":\{"fruit": \[\{"weight":\d,"type":"[a-z]{1,9}"\}\],' \
#                    r'"bicycle":\{"price":\d\d\.\d\d,"color":"[a-z]{0,4}"\}\},' \
#                    r'"email":"[a-z]{1,5}\@[a-z]{3,10}\.com","owner":"[a-z]{3,8}"\}',
#                    r'\{"a": "[a-z]{1,3}"\}',
#                    r'\{"f1":"value1", "f2":"value\d"\}'], ids=idfn)
# @pytest.mark.parametrize('json_str_pattern', [r'\{"store":\{"fruit": \[\{"weight":\d,"type":"[a-z]{1,9}"\}\],' \
#                    r'"bicycle":\{"price":\d\d\.\d\d,"color":"[a-z]{0,4}"\}\},' \
#                    r'"email":"[a-z]{1,5}\@[a-z]{3,10}\.com","owner":"[a-z]{3,8}"\}'], ids=idfn)
@pytest.mark.parametrize('json_str_pattern', [r'\{"store":\{"fruit": \[\{"weight":\d,"type":"[a-z]{1,9}"\}\],' \
                   r'"bicycle":\{"price":\d\d\.\d\d,"color":"[a-z]{0,4}"\}\},' \
                   r'"email":"[a-z]{1,5}\@[a-z]{3,10}\.com","owner":"[a-z]{3,8}"\}' \
                   r'"address":"[a-z]{1,20}","phone number":"[0-9]{10}"\}' \
                   r'"flower":\{"price":\d\d\.\d\d,"color":"[a-z]{0,4}"\}\},' \
                   r'"car":\{"price":\d\d\.\d\d,"color":"[a-z]{0,4}"\}\},'], ids=idfn)
# @pytest.mark.parametrize('json_str_pattern', [r'\{"a": "[a-z]{1,3}"\}'], ids=idfn)
def test_json_tuple(json_str_pattern):
    gen = mk_json_str_gen(json_str_pattern)
    # assert_gpu_and_cpu_are_equal_collect(
    #     lambda spark: unary_op_df(spark, gen, length=10).selectExpr(
    #         'json_tuple(a, "a", "owner")'),
    #         # 'json_tuple(a, "$.owner")',
    #         # 'json_tuple(a, "$.store.fruit[0]")'),
    #     conf={'spark.sql.parser.escapedStringLiterals': 'true'})

    for i in range(10):
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen, length=10).selectExpr(
                # 'get_json_object(a, "$.store")'), #fail
                # 'get_json_object(a, "$.bicycle")'),
                # 'json_tuple(a, "a", "bicycle", "email", "owner", "f1", "f2", "xyz", "abc", "store\.fruit", "bicycle\.color")'),
                # 'json_tuple(a, "a", "store.fruit")'), #fail
                # 'json_tuple(a, "a", "store")'), #fail
                # 'json_tuple(a, "a")'), #1
                # 'json_tuple(a, "a", "bicycle", "email", "owner", "f1")'), #5
                # 'json_tuple(a, "a", "bicycle", "email", "owner", "f1", "f2", "xyz", "abc", "f3", "car")'), #10
                # 'json_tuple(a, "a", "bicycle", "email", "owner", "f1", "f2", "xyz", "abc", "f3", "car", "phone number", "phone", "number", "f4", "f5", "name", "fruit", "location", "color", "aa")'), #20
                'json_tuple(a, "a", "bicycle", "email", "owner", "f1", "f2", "xyz", "abc", "f3", "car", "phone number", "phone", "number", "f4", "f5", "name", "fruit", "location", "color", "aa", "ab", "address", "country", "city", "f6", "f7", "flower", "price", "plant", "car type", "weight", "type", "f8", "f9", "f10", "car color", "car name", "street", "car price", "nickname")'), #40
            conf={'spark.sql.parser.escapedStringLiterals': 'true'})
