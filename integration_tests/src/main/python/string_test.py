# Copyright (c) 2020, NVIDIA CORPORATION.
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
from marks import incompat
from pyspark.sql.types import *
import pyspark.sql.functions as f

def mk_str_gen(pattern):
    return StringGen(pattern).with_special_case('').with_special_pattern('.{0,10}')

@pytest.mark.parametrize('data_gen,delim', [(mk_str_gen('([ABC]{0,3}_?){0,7}'), '_'),
    (mk_str_gen('([MNP_]{0,3}\\.?){0,5}'), '.'),
    (mk_str_gen('([123]{0,3}\\^?){0,5}'), '^')], ids=idfn)
def test_substring_index(data_gen,delim):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(
                f.substring_index(f.col('a'), delim, 1),
                f.substring_index(f.col('a'), delim, 3),
                f.substring_index(f.col('a'), delim, 0),
                f.substring_index(f.col('a'), delim, -1),
                f.substring_index(f.col('a'), delim, -4)))

# ONLY LITERAL SEARCH PARAMS ARE SUPPORTED
def test_position():
    gen = mk_str_gen('.{0,3}Z_Z.{0,3}A.{0,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'POSITION(NULL IN a)',
                'POSITION("Z_" IN a)',
                'POSITION("" IN a)',
                'POSITION("_" IN a)',
                'POSITION("A" IN a)'))

def test_locate():
    gen = mk_str_gen('.{0,3}Z_Z.{0,3}A.{0,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'locate("Z", a, -1)',
                'locate("Z", a, 4)',
                'locate("A", a, 500)',
                'locate("_", a, NULL)'))

def test_contains():
    gen = mk_str_gen('.{0,3}Z?_Z?.{0,3}A?.{0,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).select(
                f.col('a').contains('Z'),
                f.col('a').contains('Z_'),
                f.col('a').contains(''),
                f.col('a').contains(None)
                ))

def test_trim():
    gen = mk_str_gen('[Ab \ud720]{0,3}A.{0,3}Z[ Ab]{0,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'TRIM(a)',
                'TRIM("Ab" FROM a)',
                'TRIM("A\ud720" FROM a)',
                'TRIM(BOTH NULL FROM a)',
                'TRIM("" FROM a)'))

def test_ltrim():
    gen = mk_str_gen('[Ab \ud720]{0,3}A.{0,3}Z[ Ab]{0,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'LTRIM(a)',
                'LTRIM("Ab", a)',
                'TRIM(LEADING "A\ud720" FROM a)',
                'TRIM(LEADING NULL FROM a)',
                'TRIM(LEADING "" FROM a)'))

def test_rtrim():
    gen = mk_str_gen('[Ab \ud720]{0,3}A.{0,3}Z[ Ab]{0,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'RTRIM(a)',
                'RTRIM("Ab", a)',
                'TRIM(TRAILING "A\ud720" FROM a)',
                'TRIM(TRAILING NULL FROM a)',
                'TRIM(TRAILING "" FROM a)'))

# Once https://github.com/NVIDIA/spark-rapids/issues/112 is fixed this should be
# deleted and the corresponding lines in the other tests should be uncommented
@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/112')
def test_start_n_ends_with_xfail():
    gen = mk_str_gen('[Ab\ud720]{3}A.{0,3}Z[Ab\ud720]{3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).select(
                f.col('a').startswith(''),
                f.col('a').endswith('')))

def test_startswith():
    gen = mk_str_gen('[Ab\ud720]{3}A.{0,3}Z[Ab\ud720]{3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).select(
                f.col('a').startswith('A'),
                #https://github.com/NVIDIA/spark-rapids/issues/112
                #f.col('a').startswith(''),
                f.col('a').startswith(None),
                f.col('a').startswith('A\ud720')))


def test_endswith():
    gen = mk_str_gen('[Ab\ud720]{3}A.{0,3}Z[Ab\ud720]{3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).select(
                f.col('a').endswith('A'),
                #https://github.com/NVIDIA/spark-rapids/issues/112
                #f.col('a').endswith(''),
                f.col('a').endswith(None),
                f.col('a').endswith('A\ud720')))

# We currently only support strings, but this should be extended to other types
# later on
def test_concat():
    gen = mk_str_gen('.{0,5}')
    (s1, s2) = gen_scalars(gen, 2, force_no_nulls=True)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: binary_op_df(spark, gen).select(
                f.concat(f.col('a'), f.col('b')),
                f.concat(f.col('a'), f.col('b'), f.col('a')),
                f.concat(s1, f.col('b')),
                f.concat(f.col('a'), s2),
                f.concat(f.lit(None).cast('string'), f.col('b')),
                f.concat(f.col('a'), f.lit(None).cast('string')),
                f.concat(f.lit(''), f.col('b')),
                f.concat(f.col('a'), f.lit(''))))

def test_substring():
    gen = mk_str_gen('.{0,30}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'SUBSTRING(a, 1, 5)',
                'SUBSTRING(a, 1)',
                'SUBSTRING(a, -3)',
                'SUBSTRING(a, 3, -2)',
                'SUBSTRING(a, 100)',
                'SUBSTRING(a, NULL)',
                'SUBSTRING(a, 1, NULL)',
                'SUBSTRING(a, 0, 0)'))

def test_replace():
    gen = mk_str_gen('.{0,5}TEST[\ud720 A]{0,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'REPLACE(a, "TEST", "PROD")',
                'REPLACE(a, "T\ud720", "PROD")',
                'REPLACE(a, "", "PROD")',
                'REPLACE(a, "T", NULL)',
                'REPLACE(a, NULL, "PROD")',
                'REPLACE(a, "T", "")'))

def test_re_replace():
    gen = mk_str_gen('.{0,5}TEST[\ud720 A]{0,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'REGEXP_REPLACE(a, "TEST", "PROD")',
                'REGEXP_REPLACE(a, "TEST", "")',
                'REGEXP_REPLACE(a, "TEST", "%^[]\ud720")',
                'REGEXP_REPLACE(a, "TEST", NULL)'))

def test_length():
    gen = mk_str_gen('.{0,5}TEST[\ud720 A]{0,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'LENGTH(a)',
                'CHAR_LENGTH(a)',
                'CHARACTER_LENGTH(a)'))

# Once the xfail is fixed this can replace test_initcap_space
@incompat
@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/120')
def test_initcap():
    # Because we don't use the same unicode version we need to limit
    # the charicter set to something more reasonable
    # upper and lower should cover the corner cases, this is mostly to
    # see if there are issues with spaces
    gen = mk_str_gen('([aAbB]{0,5}[ \r\n\t]{1,2}){1,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).select(
                f.initcap(f.col('a'))))

@incompat
def test_initcap_space():
    # we see a lot more space delim
    gen = StringGen('([aAbB]{0,5}[ ]{1,2}){1,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).select(
                f.initcap(f.col('a'))))

@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/119')
def test_like_null_xfail():
    gen = mk_str_gen('.{0,3}a[|b*.$\r\n]{0,2}c.{0,3}')\
            .with_special_pattern('.{0,3}oo.{0,3}', weight=100.0)\
            .with_special_case('_')\
            .with_special_case('\r')\
            .with_special_case('\n')\
            .with_special_case('%SystemDrive%\\Users\\John')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).select(
                f.col('a').like('_'))) 

def test_like():
    gen = mk_str_gen('(\u20ac|\\w){0,3}a[|b*.$\r\n]{0,2}c\\w{0,3}')\
            .with_special_pattern('\\w{0,3}oo\\w{0,3}', weight=100.0)\
            .with_special_case('_')\
            .with_special_case('\r')\
            .with_special_case('\n')\
            .with_special_case('a{3}bar')\
            .with_special_case('12345678')\
            .with_special_case('12345678901234')\
            .with_special_case('%SystemDrive%\\Users\\John')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).select(
                f.col('a').like('%o%'), # turned into contains
                f.col('a').like('%a%'), # turned into contains
                f.col('a').like(''), #turned into equals
                f.col('a').like('12345678'), #turned into equals
                f.col('a').like('\\%SystemDrive\\%\\\\Users%'),
                f.col('a').like('_'),
                f.col('a').like('_oo_'),
                f.col('a').like('_oo%'),
                f.col('a').like('%oo_'),
                f.col('a').like('_\u201c%'),
                f.col('a').like('_a[d]%'),
                f.col('a').like('_a(d)%'),
                f.col('a').like('_$'),
                f.col('a').like('_$%'),
                f.col('a').like('_._'),
                f.col('a').like('_?|}{_%'),
                f.col('a').like('%a{3}%'))) 

def test_like_simple_escape():
    gen = mk_str_gen('(\u20ac|\\w){0,3}a[|b*.$\r\n]{0,2}c\\w{0,3}')\
            .with_special_pattern('\\w{0,3}oo\\w{0,3}', weight=100.0)\
            .with_special_case('_')\
            .with_special_case('\r')\
            .with_special_case('\n')\
            .with_special_case('a{3}bar')\
            .with_special_case('12345678')\
            .with_special_case('12345678901234')\
            .with_special_case('%SystemDrive%\\Users\\John')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'a like "_a^d%" escape "c"',
                'a like "a_a" escape "c"',
                'a like "a%a" escape "c"',
                'a like "c_" escape "c"',
                'a like x "6162632325616263" escape "#"',
                'a like x "61626325616263" escape "#"'))
 
def test_like_complex_escape():
    gen = mk_str_gen('(\u20ac|\\w){0,3}a[|b*.$\r\n]{0,2}c\\w{0,3}')\
            .with_special_pattern('\\w{0,3}oo\\w{0,3}', weight=100.0)\
            .with_special_case('_')\
            .with_special_case('\r')\
            .with_special_case('\n')\
            .with_special_case('a{3}bar')\
            .with_special_case('12345678')\
            .with_special_case('12345678901234')\
            .with_special_case('%SystemDrive%\\Users\\John')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'a like x "256f6f5f"',
                'a like x "6162632325616263" escape "#"',
                'a like x "61626325616263" escape "#"',
                'a like ""',
                'a like "_oo_"',
                'a like "_oo%"',
                'a like "%oo_"',
                'a like "_\u20AC_"',
                'a like "\\%SystemDrive\\%\\\\\\\\Users%"',
                'a like "_oo"'),
            conf={'spark.sql.parser.escapedStringLiterals': 'true'})
 
