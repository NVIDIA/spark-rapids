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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect, with_gpu_session
from data_gen import *
from pyspark.sql.types import *
from marks import *
from spark_init_internal import spark_version
from spark_session import is_before_spark_400, is_databricks113_or_later, is_databricks_runtime

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
    data = [[r'''{'a':'A'}''']]

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
        f.get_json_object('jsonStr',r'''$['a']''').alias('sub_a'),
        f.get_json_object('jsonStr',r'''$['b']''').alias('sub_b'),
        f.get_json_object('jsonStr',r'''$['c']''').alias('sub_c')),
        conf={'spark.rapids.sql.expression.GetJsonObject': 'true'})

@pytest.mark.parametrize('query',["$.store.bicycle",
    "$['store'].bicycle",
    "$.store['bicycle']",
    "$['store']['bicycle']",
    "$['key with spaces']",
    "$.store.book",
    "$.store.book[0]",
    "$",
    "$.store.book[0].category",
    "$.store.basket[0][1]",
    "$.store.basket[0][2].b",
    "$.zip code",
    "$.fb:testid",
    "$.a",
    "$.non_exist_key",
    "$..no_recursive",
    "$.store.book[0].non_exist_key",
    "$.store.basket[0][*].b", 
    "$.store.book[*].reader",
    "$.store.book[*]",
    "$.store.book[*].category",
    "$.store.book[*].isbn",
    "$.store.basket[*]",
    "$.store.basket[*][0]",
    "$.store.basket[0][*]",
    "$.store.basket[*][*]",
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


@pytest.mark.skipif(condition=not is_before_spark_400(),
                    reason="https://github.com/NVIDIA/spark-rapids/issues/11130")
def test_get_json_object_quoted_question():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [[r'{"?":"QUESTION"}']]

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.get_json_object('jsonStr',r'''$['?']''').alias('question')),
            conf={'spark.rapids.sql.expression.GetJsonObject': 'true'})

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


def test_get_json_object_white_space_removal():
    # This is a special version of invalid path. It is something that the GPU supports
    # but the CPU thinks is invalid
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['{" a":" A"," b":" B"}'],
            ['{"a":"A","b":"B"}'],
            ['{"a ":"A ","b ":"B "}'],
            ['{" a ":" A "," b ":" B "}'],
            ['{" a ": {" a ":" A "}," b ": " B "}'],
            ['{" a":"b","a.a":"c","b":{"a":"ab"}}'],
            ['{" a":"b"," a. a":"c","b":{"a":"ab"}}'],
            ['{" a":"b","a .a ":"c","b":{"a":"ab"}}'],
            ['{" a":"b"," a . a ":"c","b":{"a":"ab"}}']
            ]

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.col('jsonStr'),
            f.get_json_object('jsonStr', '$.a').alias('dot_a'),
            f.get_json_object('jsonStr', '$. a').alias('dot_space_a'),
            f.get_json_object('jsonStr', '$.\ta').alias('dot_tab_a'),
            f.get_json_object('jsonStr', '$.    a').alias('dot_spaces_a3'),
            f.get_json_object('jsonStr', '$.a ').alias('dot_a_space'),
            f.get_json_object('jsonStr', '$. a ').alias('dot_space_a_space'),
            f.get_json_object('jsonStr', "$['b']").alias('dot_b'),
            f.get_json_object('jsonStr', "$[' b']").alias('dot_space_b'),
            f.get_json_object('jsonStr', "$['b ']").alias('dot_b_space'),
            f.get_json_object('jsonStr', "$[' b ']").alias('dot_space_b_space'),
            f.get_json_object('jsonStr', "$. a. a").alias('dot_space_a_dot_space_a'),
            f.get_json_object('jsonStr', "$.a .a ").alias('dot_a_space_dot_a_space'),
            f.get_json_object('jsonStr', "$. a . a ").alias('dot_space_a_space_dot_space_a_space'),
            f.get_json_object('jsonStr', "$[' a. a']").alias('space_a_dot_space_a'),
            f.get_json_object('jsonStr', "$['a .a ']").alias('a_space_dot_a_space'),
            f.get_json_object('jsonStr', "$[' a . a ']").alias('space_a_space_dot_space_a_space'),
            ),
            conf={'spark.rapids.sql.expression.GetJsonObject': 'true'})


def test_get_json_object_jni_java_tests():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['\'abc\''],
            ['[ [11, 12], [21, [221, [2221, [22221, 22222]]]], [31, 32] ]'],
            ['123'],
            ['{ \'k\' : \'v\'  }'],
            ['[  [[[ {\'k\': \'v1\'} ], {\'k\': \'v2\'}]], [[{\'k\': \'v3\'}], {\'k\': \'v4\'}], {\'k\': \'v5\'}  ]'],
            ['[1, [21, 22], 3]'],
            ['[ {\'k\': [0, 1, 2]}, {\'k\': [10, 11, 12]}, {\'k\': [20, 21, 22]}  ]'],
            ['[ [0], [10, 11, 12], [2] ]'],
            ['[[0, 1, 2], [10, [111, 112, 113], 12], [20, 21, 22]]'],
            ['[[0, 1, 2], [10, [], 12], [20, 21, 22]]'],
            ['{\'k\' : [0,1,2]}'],
            ['{\'k\' : null}']
            ]

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.col('jsonStr'),
            f.get_json_object('jsonStr', '$').alias('dollor'),
            f.get_json_object('jsonStr', '$[*][*]').alias('s_w_s_w'),
            f.get_json_object('jsonStr', '$.k').alias('dot_k'),
            f.get_json_object('jsonStr', '$[*]').alias('s_w'),
            f.get_json_object('jsonStr', '$[*].k[*]').alias('s_w_k_s_w'),
            f.get_json_object('jsonStr', '$[1][*]').alias('s_1_s_w'),
            f.get_json_object('jsonStr', "$[1][1][*]").alias('s_1_s_1_s_w'),
            f.get_json_object('jsonStr', "$.k[1]").alias('dot_k_s_1'),
            f.get_json_object('jsonStr', "$.*").alias('w'),
            ),
            conf={'spark.rapids.sql.expression.GetJsonObject': 'true'})


def test_get_json_object_deep_nested_json():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['{"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":{"i":{"j":{"k":{"l":{"m":{"n":{"o":{"p":{"q":{"r":{"s":{"t":{"u":{"v":{"w":{"x":{"y":{"z":"A"}}'
            ]]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.get_json_object('jsonStr', '$.a.b.c.d.e.f.g.h.i').alias('i'),
            f.get_json_object('jsonStr', '$.a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p').alias('p')
            ),
            conf={'spark.rapids.sql.expression.GetJsonObject': 'true'})

@allow_non_gpu('ProjectExec')
def test_get_json_object_deep_nested_json_fallback():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['{"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":{"i":{"j":{"k":{"l":{"m":{"n":{"o":{"p":{"q":{"r":{"s":{"t":{"u":{"v":{"w":{"x":{"y":{"z":"A"}}'
            ]]
    assert_gpu_fallback_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.get_json_object('jsonStr', '$.a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z').alias('z')),
        'GetJsonObject',
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

@pytest.mark.parametrize('json_str_pattern', [r'\{"store": \{"fruit": \[\{"weight":\d,"type":"[a-z]{1,9}"\}\], ' \
                   r'"bicycle":\{"price":[1-9]\d\.\d\d,"color":"[a-z]{0,4}"\}\},' \
                   r'"email":"[a-z]{1,5}\@[a-z]{3,10}\.com","owner":"[a-z]{3,8}"\}',
                   r'\{"a": "[a-z]{1,3}"\}'], ids=idfn)
def test_get_json_object_legacy(json_str_pattern):
    gen = mk_json_str_gen(json_str_pattern)
    scalar_json = '{"store": {"fruit": [{"name": "test"}]}}'
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen, length=10).selectExpr(
            'get_json_object(a,"$.a")',
            'get_json_object(a, "$.owner")',
            'get_json_object(a, "$.store.fruit[0]")',
            'get_json_object(\'%s\', "$.store.fruit[0]")' % scalar_json,
            ),
        conf={'spark.rapids.sql.expression.GetJsonObject': 'true',
              'spark.sql.parser.escapedStringLiterals': 'true',
              'spark.rapids.sql.getJsonObject.legacy.enabled': 'true'})

# In the legacy mode, the output of get_json_object is not normalized.
# Verify that the output is not normalized for floating point to check the legacy mode is working.
def test_get_json_object_number_normalization_legacy():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['[100.0,200.000,351.980]'],
            ['[12345678900000000000.0]'],
            ['[12345678900000000000]'],
            ['[1' + '0'* 400 + ']'],
            ['[1E308]'],
            ['[1.0E309,-1E309,1E5000]']]
    gpu_result = with_gpu_session(lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.col('jsonStr'),
            f.get_json_object('jsonStr', '$')).collect(),
        conf={'spark.rapids.sql.expression.GetJsonObject': 'true',
              'spark.rapids.sql.getJsonObject.legacy.enabled': 'true'})
    assert([[row[1]] for row in gpu_result] == data)

@pytest.mark.parametrize('data_gen', [StringGen(r'''-?[1-9]\d{0,5}\.\d{1,20}''', nullable=False),
                                      StringGen(r'''-?[1-9]\d{0,20}\.\d{1,5}''', nullable=False),
                                      StringGen(r'''-?[1-9]\d{0,5}E-?\d{1,20}''', nullable=False),
                                      StringGen(r'''-?[1-9]\d{0,20}E-?\d{1,5}''', nullable=False)], ids=idfn)
def test_get_json_object_floating_normalization(data_gen):
    schema = StructType([StructField("jsonStr", StringType())])
    normalization = lambda spark: unary_op_df(spark, data_gen).selectExpr(
                        'a',
                        'get_json_object(a,"$")'
                        ).collect()
    gpu_res = [[row[1]] for row in with_gpu_session(
        normalization,
        conf={'spark.rapids.sql.expression.GetJsonObject': 'true'})]
    cpu_res = [[row[1]] for row in with_cpu_session(normalization)]
    def json_string_to_float(x):
        if x == '"-Infinity"':
            return float('-inf')
        elif x == '"Infinity"':
            return float('inf')
        else:
            return float(x)
    for i in range(len(gpu_res)):
        # verify relatively diff < 1e-9 (default value for is_close)
        assert math.isclose(json_string_to_float(gpu_res[i][0]), json_string_to_float(cpu_res[i][0]))
