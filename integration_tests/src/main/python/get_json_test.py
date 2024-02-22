# Copyright (c) 2021-2024, NVIDIA CORPORATION.
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
from spark_session import is_databricks113_or_later, is_databricks_runtime

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
        conf={'spark.sql.parser.escapedStringLiterals': 'true',
            'spark.rapids.sql.expression.GetJsonObject': 'true'})

def test_get_json_object_quoted_index():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [[r'{"a":"A"}'],
            [r'{"b":"B"}']]

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
        f.get_json_object('jsonStr',r'''$['a']''').alias('sub_a'),
        f.get_json_object('jsonStr',r'''$['b']''').alias('sub_b')),
        conf={'spark.rapids.sql.expression.GetJsonObject': 'true'})

@pytest.mark.skipif(is_databricks_runtime() and not is_databricks113_or_later(), reason="get_json_object on \
                    DB 10.4 shows incorrect behaviour with single quotes")
def test_get_json_object_single_quotes():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [[r'''{'a':'A'}'''],
            [r'''{'b':'"B'}'''],
            [r'''{"c":"'C"}''']]

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
        f.get_json_object('jsonStr',r'''$['a']''').alias('sub_a'),
        f.get_json_object('jsonStr',r'''$['b']''').alias('sub_b'),
        f.get_json_object('jsonStr',r'''$['c']''').alias('sub_c')),
        conf={'spark.rapids.sql.expression.GetJsonObject': 'true'})
    
def test_get_json_object_inavlid_queries():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [[r'''{"a":"A"}'''],
            [r'''{"b":"B"}'''],
            [r'''{"c'":"C"}''']]

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
        f.get_json_object('jsonStr',r'''${a}''').alias('sub_a'),
        f.get_json_object('jsonStr',r'''.''').alias('sub_b'),
        f.get_json_object('jsonStr',r'''][''').alias('sub_c'),
        f.get_json_object('jsonStr',r'''$['c\'']''').alias('sub_d')
        ),
        conf={'spark.rapids.sql.expression.GetJsonObject': 'true'})

    
def test_get_json_object_queries_with_quotes():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [[r'''{"AB":1, "A.B":2, "'A":{"B'":3}, "A":{"B":4} }''']]

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
        f.get_json_object('jsonStr',"$.AB").alias('sub_a'),
        f.get_json_object('jsonStr',"$['A.B']").alias('sub_b'),
        f.get_json_object('jsonStr',"$.'A.B'").alias('sub_c'),
        f.get_json_object('jsonStr',"$.A.B").alias('sub_d'),
        f.get_json_object('jsonStr',"$.'A").alias('sub_e')
        ),
        conf={'spark.rapids.sql.expression.GetJsonObject': 'true'})

@pytest.mark.parametrize('query',["$.store.bicycle",
    "$['store'].bicycle",
    "$.store['bicycle']",
    "$['store']['bicycle']",
    "$['key with spaces']",
    "$.store.book",
    "$.store.book[0]",
    "$.store.book[*]",
    pytest.param("$",marks=[
        pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10218'),
        pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10196'),
        pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10194')]),
    "$.store.book[0].category",
    "$.store.book[*].category",
    "$.store.book[*].isbn",
    pytest.param("$.store.book[*].reader",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10216')),
    "$.store.basket[0][1]",
    "$.store.basket[*]",
    "$.store.basket[*][0]",
    "$.store.basket[0][*]",
    "$.store.basket[*][*]",
    "$.store.basket[0][2].b",
    pytest.param("$.store.basket[0][*].b",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10217')),
    "$.zip code",
    "$.fb:testid",
    pytest.param("$.a",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10196')),
    "$.non_exist_key",
    pytest.param("$..no_recursive", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10212')),
    "$.store.book[0].non_exist_key",
    "$.store.basket[*].non_exist_key"])
def test_get_json_object_spark_unit_tests(query):
    schema = StructType([StructField("jsonStr", StringType())])
    data = [
            ['''{"store":{"fruit":[{"weight":8,"type":"apple"},{"weight":9,"type":"pear"}],"basket":[[1,2,{"b":"y","a":"x"}],[3,4],[5,6]],"book":[{"author":"Nigel Rees","title":"Sayings of the Century","category":"reference","price":8.95},{"author":"Herman Melville","title":"Moby Dick","category":"fiction","price":8.99,"isbn":"0-553-21311-3"},{"author":"J. R. R. Tolkien","title":"The Lord of the Rings","category":"fiction","reader":[{"age":25,"name":"bob"},{"age":26,"name":"jack"}],"price":22.99,"isbn":"0-395-19395-8"}],"bicycle":{"price":19.95,"color":"red"}},"email":"amy@only_for_json_udf_test.net","owner":"amy","zip code":"94025","fb:testid":"1234"}'''],
            ['''{ "key with spaces": "it works" }'''],
            ['''{"a":"b\nc"}'''],
            ['''{"a":"b\"c"}'''],
            ["\u0000\u0000\u0000A\u0001AAA"],
            ['{"big": "' + ('x' * 3000) + '"}']]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.get_json_object('jsonStr', query)),
        conf={'spark.rapids.sql.expression.GetJsonObject': 'true'})

@pytest.mark.xfail(reason="https://github.com/NVIDIA/spark-rapids/issues/10218")
def test_get_json_object_normalize_non_string_output():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [[' { "a": "A" } '],
            ['''{'a':'A"'}'''],
            [r'''{'a':"B\'"}'''],
            ['''['a','b','"C"']'''],
            ['[100.0,200.000,351.980]'],
            ['[12345678900000000000.0]'],
            ['[12345678900000000000]'],
            ['[1' + '0'* 400 + ']'],
            ['[1E308]'],
            ['[1.0E309,-1E309,1E5000]'],
            ['[true,false]'],
            ['[100,null,10]'],
            ['{"a":"A","b":null}']]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.col('jsonStr'),
            f.get_json_object('jsonStr', '$')),
        conf={'spark.rapids.sql.expression.GetJsonObject': 'true'})

@pytest.mark.xfail(reason="https://issues.apache.org/jira/browse/SPARK-46761")
def test_get_json_object_quoted_question():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [[r'{"?":"QUESTION"}']]

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.get_json_object('jsonStr',r'''$['?']''').alias('question')),
        conf={'spark.rapids.sql.expression.GetJsonObject': 'true'})

@pytest.mark.xfail(reason="https://github.com/NVIDIA/spark-rapids/issues/10196")
def test_get_json_object_escaped_string_data():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [[r'{"a":"A\"B"}'],
            [r'''{"a":"A\'B"}'''],
            [r'{"a":"A\/B"}'],
            [r'{"a":"A\\B"}'],
            [r'{"a":"A\bB"}'],
            [r'{"a":"A\fB"}'],
            [r'{"a":"A\nB"}'],
            [r'{"a":"A\tB"}']]

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).selectExpr('get_json_object(jsonStr,"$.a")'),
        conf={'spark.rapids.sql.expression.GetJsonObject': 'true'})

@pytest.mark.xfail(reason="https://github.com/NVIDIA/spark-rapids/issues/10196")
def test_get_json_object_escaped_key():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [
            [r'{"a\"":"Aq"}'],
            [r'''{"\'a":"sqA1"}'''],
            [r'''{"'a":"sqA2"}'''],
            [r'{"a\/":"Afs"}'],
            [r'{"a\\":"Abs"}'],
            [r'{"a\b":"Ab1"}'],
            ['{"a\b":"Ab2"}'],
            [r'{"a\f":"Af1"}'],
            ['{"a\f":"Af2"}'],
            [r'{"a\n":"An1"}'],
            ['{"a\n":"An2"}'],
            [r'{"a\t":"At1"}'],
            ['{"a\t":"At2"}']]

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.col('jsonStr'),
            f.get_json_object('jsonStr', r'$.a\"').alias('qaq1'),
            f.get_json_object('jsonStr', '$.a"').alias('qaq2'),
            f.get_json_object('jsonStr', r'''$.\'a''').alias('qsqa1'),
            f.get_json_object('jsonStr', r'$.a\/').alias('qafs1'),
            f.get_json_object('jsonStr', '$.a/').alias('qafs2'), 
            f.get_json_object('jsonStr', r'''$['a\/']''').alias('qafs3'), 
            f.get_json_object('jsonStr', r'$.a\\').alias('qabs1'),
            f.get_json_object('jsonStr', r'$.a\b').alias('qab1'),
            f.get_json_object('jsonStr','$.a\b').alias('qab2'),
            f.get_json_object('jsonStr', r'$.a\f').alias('qaf1'),
            f.get_json_object('jsonStr','$.a\f').alias('qaf2'),
            f.get_json_object('jsonStr', r'$.a\n').alias('qan1'),
            f.get_json_object('jsonStr','$.a\n').alias('qan2'),
            f.get_json_object('jsonStr', r'$.a\t').alias('qat1'),
            f.get_json_object('jsonStr','$.a\t').alias('qat2')
            ),
        conf={'spark.rapids.sql.expression.GetJsonObject': 'true'})

@pytest.mark.xfail(reason="https://github.com/NVIDIA/spark-rapids/issues/10212")
def test_get_json_object_invalid_path():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['{"a":"A"}'],
            [r'{"a\"":"A"}'],
            [r'''{"'a":"A"}'''],
            ['{"b":"B"}'],
            ['["A","B"]'],
            ['{"c":["A","B"]}']]

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.col('jsonStr'),
            f.get_json_object('jsonStr', '''$ ['a']''').alias('with_space'),
            f.get_json_object('jsonStr', r'''$['\'a']''').alias('qsqa2'),
            f.get_json_object('jsonStr', '''$.'a''').alias('qsqa2'),
            f.get_json_object('jsonStr', r'''$.['a\"']''').alias('qaq3'),
            f.get_json_object('jsonStr', '''$['a]''').alias('qsqa2'), # jsonpath.com thinks it is fine and ignores uncompleted ' and ], but not Spark
            f.get_json_object('jsonStr', 'a').alias('just_a'),
            f.get_json_object('jsonStr', '[-1]').alias('neg_one_index'),
            f.get_json_object('jsonStr', '$.c[-1]').alias('c_neg_one_index'),
            ),
        conf={'spark.rapids.sql.expression.GetJsonObject': 'true'})

@pytest.mark.xfail(reason="https://github.com/NVIDIA/spark-rapids/issues/10213")
def test_get_json_object_top_level_array_notation():
    # This is a special version of invalid path. It is something that the GPU supports
    # but the CPU thinks is invalid
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['["A","B"]'],
            ['{"a":"A","b":"B"}']]

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.col('jsonStr'),
            f.get_json_object('jsonStr', '[0]').alias('zero_index'),
            f.get_json_object('jsonStr', '$[1]').alias('one_index'),
            f.get_json_object('jsonStr', '''['a']''').alias('sub_a'),
            f.get_json_object('jsonStr', '''$['b']''').alias('sub_b'),
            ),
        conf={'spark.rapids.sql.expression.GetJsonObject': 'true'})

@pytest.mark.xfail(reason="https://github.com/NVIDIA/spark-rapids/issues/10214")
def test_get_json_object_unquoted_array_notation():
    # This is a special version of invalid path. It is something that the GPU supports
    # but the CPU thinks is invalid
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['{"a":"A","b":"B"}'],
            ['{"1":"ONE","a1":"A_ONE"}']]

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.col('jsonStr'),
            f.get_json_object('jsonStr', '$[a]').alias('a_index'),
            f.get_json_object('jsonStr', '$[1]').alias('one_index'),
            f.get_json_object('jsonStr', '''$['1']''').alias('quoted_one_index'),
            f.get_json_object('jsonStr', '$[a1]').alias('a_one_index')),
        conf={'spark.rapids.sql.expression.GetJsonObject': 'true'})


@pytest.mark.xfail(reason="https://github.com/NVIDIA/spark-rapids/issues/10215")
def test_get_json_object_white_space_removal():
    # This is a special version of invalid path. It is something that the GPU supports
    # but the CPU thinks is invalid
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['{" a":" A"," b":" B"}'],
            ['{"a":"A","b":"B"}'],
            ['{"a ":"A ","b ":"B "}'],
            ['{" a ":" A "," b ":" B "}']]

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.col('jsonStr'),
            f.get_json_object('jsonStr', '$.a').alias('dot_a'),
            f.get_json_object('jsonStr', '$. a').alias('dot_space_a'),
            f.get_json_object('jsonStr', '$.a ').alias('dot_a_space'),
            f.get_json_object('jsonStr', '$. a ').alias('dot_space_a_space'),
            f.get_json_object('jsonStr', "$['b']").alias('dot_b'),
            f.get_json_object('jsonStr', "$[' b']").alias('dot_space_b'),
            f.get_json_object('jsonStr', "$['b ']").alias('dot_b_space'),
            f.get_json_object('jsonStr', "$[' b ']").alias('dot_space_b_space'),
            ),
        conf={'spark.rapids.sql.expression.GetJsonObject': 'true'})


@allow_non_gpu('ProjectExec')
@pytest.mark.parametrize('json_str_pattern', [r'\{"store": \{"fruit": \[\{"weight":\d,"type":"[a-z]{1,9}"\}\], ' \
                   r'"bicycle":\{"price":[1-9]\d\.\d\d,"color":"[a-z]{0,4}"\}\},' \
                   r'"email":"[a-z]{1,5}\@[a-z]{3,10}\.com","owner":"[a-z]{3,8}"\}',
                   r'\{"a": "[a-z]{1,3}"\}'], ids=idfn)
def test_unsupported_fallback_get_json_object(json_str_pattern):
    gen = mk_json_str_gen(json_str_pattern)
    scalar_json = '{"store": {"fruit": "test"}}'
    pattern = StringGen(pattern=r'\$\.[a-z]{1,9}')
    def assert_gpu_did_fallback(sql_text):
        assert_gpu_fallback_collect(lambda spark:
            gen_df(spark, [('a', gen), ('b', pattern)], length=10).selectExpr(sql_text),
        'GetJsonObject',
        conf={'spark.sql.parser.escapedStringLiterals': 'true',
            'spark.rapids.sql.expression.GetJsonObject': 'true'})

    assert_gpu_did_fallback('get_json_object(a, b)')
    assert_gpu_did_fallback('get_json_object(\'%s\', b)' % scalar_json)

