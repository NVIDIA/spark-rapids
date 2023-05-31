import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect, \
    assert_cpu_and_gpu_are_equal_collect_with_capture, assert_gpu_and_cpu_error, \
    assert_gpu_sql_fallback_collect
from data_gen import *
from marks import *

_regexp_conf = { 'spark.rapids.sql.regexp.enabled': True }

def mk_str_gen(pattern):
    return StringGen(pattern).with_special_case('').with_special_pattern('.{0,10}')

@allow_non_gpu('ProjectExec', 'RLike')
def test_cache_regex_pre2():
    gen = mk_str_gen('(\u20ac|\\w){0,3}a[|b*.$\r\n]{0,2}c\\w{0,3}')
    assert_gpu_fallback_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'a rlike "a*+"'),
                'RLike',
        conf=_regexp_conf)

def test_cache_regex_pre():
    gen = mk_str_gen('[abcd]{0,3}[0-9]{0,3}-[0-9]{0,3}[abcd]{1,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'regexp_extract_all(a, "([a-d]+).*([0-9])", 0)',
                'regexp_extract_all(a, "(a)(b)", 0)',
                'regexp_extract_all(a, "([a-z0-9]([abcd]))", 0)',
                'regexp_extract_all(a, "(\\\\d+)-(\\\\d+)", 0)',
            ),
        conf=_regexp_conf)

def test_cache_regex_cache():
    gen = mk_str_gen('[abcd]{0,3}[0-9]{0,3}-[0-9]{0,3}[abcd]{1,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'regexp_extract_all(a, "([a-d]+).*([0-9])", 1)',
                'regexp_extract_all(a, "(a)(b)", 2)',
                'regexp_extract_all(a, "([a-z0-9]((([abcd](\\\\d?)))))", 3)',
                'regexp_extract_all(a, "(\\\\d+)-(\\\\d+)", 2)',
            ),
        conf=_regexp_conf)

