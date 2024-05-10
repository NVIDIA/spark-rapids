# Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_error, assert_gpu_fallback_collect
from data_gen import *
from marks import *
from pyspark.sql.types import *
import pyspark.sql.functions as f
from spark_session import is_before_spark_340

# regex to generate limit length urls with HOST, PATH, QUERY, REF, PROTOCOL, FILE, AUTHORITY, USERINFO
url_pattern = r'((http|https|ftp)://)(([a-zA-Z][a-zA-Z0-9]{0,2}\.){0,3}([a-zA-Z][a-zA-Z0-9]{0,2})\.([a-zA-Z][a-zA-Z0-9]{0,2}))' \
            r'(:[0-9]{1,3}){0,1}(/[a-zA-Z0-9]{1,3}){0,3}(\?[a-zA-Z0-9]{1,3}=[a-zA-Z0-9]{1,3}){0,1}(#([a-zA-Z0-9]{1,3})){0,1}'

url_pattern_with_key = r'((http|https|ftp|file)://)(([a-z]{1,3}\.){0,3}([a-z]{1,3})\.([a-z]{1,3}))' \
            r'(:[0-9]{1,3}){0,1}(/[a-z]{1,3}){0,3}(\?[a-c]{1,3}=[a-z]{1,3}(&[a-c]{1,3}=[a-z]{1,3}){0,3}){0,1}(#([a-z]{1,3})){0,1}'

edge_cases = [
    "userinfo@spark.apache.org/path?query=1#Ref",
    "http://foo.com/blah_blah",
    "http://foo.com/blah_blah/",
    "http://foo.com/blah_blah_(wikipedia)",
    "http://foo.com/blah_blah_(wikipedia)_(again)",
    "http://www.example.com/wpstyle/?p=364",
    "https://www.example.com/foo/?bar=baz&inga=42&quux",
    "http://✪df.ws/123",
    "http://userid:password@example.com:8080",
    "http://userid:password@example.com:8080/",
    "http://userid:password@example.com",
    "http://userid:password@example.com/",
    "http://142.42.1.1/",
    "http://142.42.1.1:8080/",
    "http://➡.ws/䨹",
    "http://⌘.ws",
    "http://⌘.ws/",
    "http://foo.com/blah_(wikipedia)#cite-1",
    "http://foo.com/blah_(wikipedia)_blah#cite-1",
    "http://foo.com/unicode_(✪)_in_parens",
    "http://foo.com/(something)?after=parens",
    "http://☺.damowmow.com/",
    "http://code.google.com/events/#&product=browser",
    "http://j.mp",
    "ftp://foo.bar/baz",
    r"http://foo.bar/?q=Test%20URL-encoded%20stuff",
    "http://مثال.إختبار",
    "http://例子.测试",
    "http://उदाहरण.परीक्षा",
    "http://-.~_!$&'()*+,;=:%40:80%2f::::::@example.com",
    "http://1337.net",
    "http://a.b-c.de",
    "http://223.255.255.254",
    "https://foo_bar.example.com/",
    "http:# ",
    "http://.",
    "http://..",
    "http://../",
    "http://?",
    "http://??",
    "http://??/",
    "http://#",
    "http://##",
    "http://##/",
    "http://foo.bar?q=Spaces should be encoded",
    "# ",
    "//a",
    "///a",
    "/# ",
    "http:///a",
    "foo.com",
    "rdar://1234",
    "h://test",
    "http:// shouldfail.com",
    ":// should fail",
    "http://foo.bar/foo(bar)baz quux",
    "ftps://foo.bar/",
    "http://-error-.invalid/",
    "http://a.b--c.de/",
    "http://-a.b.co",
    "http://a.b-.co",
    "http://0.0.0.0",
    "http://10.1.1.0",
    "http://10.1.1.255",
    "http://224.1.1.1",
    "http://1.1.1.1.1",
    "http://123.123.123",
    "http://3628126748",
    "http://.www.foo.bar/",
    "http://www.foo.bar./", 
    "http://.www.foo.bar./",
    "http://10.1.1.1",
    "http://10.1.1.254",
    "http://userinfo@spark.apache.org/path?query=1#Ref",
    r"https://use%20r:pas%20s@example.com/dir%20/pa%20th.HTML?query=x%20y&q2=2#Ref%20two",
    r"https://use%20r:pas%20s@example.com/dir%20/pa%20th.HTML?query=x%9Fy&q2=2#Ref%20two",
    "http://user:pass@host",
    "http://user:pass@host/",
    "http://user:pass@host/?#",
    "http://user:pass@host/file;param?query;p2",
    "inva lid://user:pass@host/file;param?query;p2",
    "http://[1:2:3:4:5:6:7:8]",
    "http://[1::]",
    "http://[1:2:3:4:5:6:7::]",
    "http://[1::8]",
    "http://[1:2:3:4:5:6::8]",
    "http://[1:2:3:4:5:6::8]",
    "http://[1::7:8]",
    "http://[1:2:3:4:5::7:8]",
    "http://[1:2:3:4:5::8]",
    "http://[1::6:7:8]",
    "http://[1:2:3:4::6:7:8]",
    "http://[1:2:3:4::8]",
    "http://[1::5:6:7:8]",
    "http://[1:2:3::5:6:7:8]",
    "http://[1:2:3::8]",
    "http://[1::4:5:6:7:8]",
    "http://[1:2::4:5:6:7:8]",
    "http://[1:2::8]",
    "http://[1::3:4:5:6:7:8]",
    "http://[1::3:4:5:6:7:8]",
    "http://[1::8]",
    "http://[::2:3:4:5:6:7:8]",
    "http://[::2:3:4:5:6:7:8]",
    "http://[::8]",
    "http://[::]",
    "http://[fe80::7:8%eth0]",
    "http://[fe80::7:8%1]",
    "http://[::255.255.255.255]",
    "http://[::ffff:255.255.255.255]",
    "http://[::ffff:0:255.255.255.255]",
    "http://[2001:db8:3:4::192.0.2.33]",
    "http://[64:ff9b::192.0.2.33]"
]

edge_cases_gen = SetValuesGen(StringType(), edge_cases)

url_gen = StringGen(url_pattern)

supported_parts = ['PROTOCOL', 'HOST', 'QUERY', 'PATH']
unsupported_parts = ['REF', 'FILE', 'AUTHORITY', 'USERINFO']
    
@pytest.mark.parametrize('data_gen', [url_gen, edge_cases_gen], ids=idfn)
@pytest.mark.parametrize('part', supported_parts, ids=idfn)
def test_parse_url_supported(data_gen, part):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr("a", "parse_url(a, '" + part + "')"))

@allow_non_gpu('ProjectExec', 'ParseUrl')
@pytest.mark.parametrize('part', unsupported_parts, ids=idfn)
def test_parse_url_unsupported_fallback(part):
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, url_gen).selectExpr("a", "parse_url(a, '" + part + "')"),
        'ParseUrl')

def test_parse_url_query_with_key():
    url_gen = StringGen(url_pattern_with_key)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, url_gen)
            .selectExpr("a", "parse_url(a, 'QUERY', 'abc')", "parse_url(a, 'QUERY', 'a')")
        )

def test_parse_url_query_with_key_column():
    url_gen = StringGen(url_pattern_with_key)
    key_gen = StringGen('[a-d]{1,3}')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, url_gen, key_gen)
            .selectExpr("a", "parse_url(a, 'QUERY', b)")
        )    

@pytest.mark.parametrize('key', ['a?c', '*'], ids=idfn)
@allow_non_gpu('ProjectExec', 'ParseUrl')
def test_parse_url_query_with_key_regex_fallback(key):
    url_gen = StringGen(url_pattern_with_key)
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, url_gen)
            .selectExpr("a", "parse_url(a, 'QUERY', '" + key + "')"),
            'ParseUrl')

@pytest.mark.parametrize('part', supported_parts, ids=idfn)
def test_parse_url_with_key(part):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, url_gen).selectExpr("parse_url(a, '" + part + "', 'key')"))

@allow_non_gpu('ProjectExec', 'ParseUrl')
@pytest.mark.parametrize('part', unsupported_parts, ids=idfn)
def test_parse_url_with_key_fallback(part):
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, url_gen).selectExpr("parse_url(a, '" + part + "', 'key')"),
        'ParseUrl')
