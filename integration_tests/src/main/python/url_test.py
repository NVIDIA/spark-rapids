# Copyright (c) 2023, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_error
from data_gen import *
from marks import *
from pyspark.sql.types import *
import pyspark.sql.functions as f
from spark_session import is_before_spark_340

# regex to generate limit length urls with HOST, PATH, QUERY, REF, PROTOCOL, FILE, AUTHORITY, USERINFO
url_pattern = r'((http|https|ftp)://)(([a-zA-Z][a-zA-Z0-9]{0,2}\.){0,3}([a-zA-Z][a-zA-Z0-9]{0,2})\.([a-zA-Z][a-zA-Z0-9]{0,2}))' \
            r'(:[0-9]{1,3}){0,1}(/[a-zA-Z0-9]{1,3}){0,3}(\?[a-zA-Z0-9]{1,3}=[a-zA-Z0-9]{1,3}){0,1}(#([a-zA-Z0-9]{1,3})){0,1}'

url_pattern_with_key = r'((http|https|ftp|file)://)(([a-z]{1,3}\.){0,3}([a-z]{1,3})\.([a-z]{1,3}))' \
            r'(:[0-9]{1,3}){0,1}(/[a-z]{1,3}){0,3}(\?key=[a-z]{1,3}){0,1}(#([a-z]{1,3})){0,1}'

url_gen = StringGen(url_pattern)
    
def test_parse_url_protocol():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, url_gen).selectExpr(
                "a",
                "parse_url(a, 'PROTOCOL')"
                ))
    
def test_parse_url_with_no_query_key():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, url_gen, length=100).selectExpr(
                "a",
                "parse_url(a, 'HOST', '')",
                "parse_url(a, 'PATH', '')",
                "parse_url(a, 'REF', '')",
                "parse_url(a, 'PROTOCOL', '')",
                "parse_url(a, 'FILE', '')",
                "parse_url(a, 'AUTHORITY', '')",
                "parse_url(a, 'USERINFO', '')"
                ))
    
def test_parse_url_too_many_args():
    error_message = 'parse_url function requires two or three arguments'  \
        if is_before_spark_340() else  \
        '[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `parse_url` requires [2, 3] parameters'
    assert_gpu_and_cpu_error(
            lambda spark : unary_op_df(spark, StringGen()).selectExpr(
                "a","parse_url(a, 'USERINFO', 'key', 'value')").collect(),
            conf={},
            error_message=error_message)
