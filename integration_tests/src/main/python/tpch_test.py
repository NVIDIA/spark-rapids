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
from marks import approximate_float, incompat, ignore_order, allow_non_gpu
from spark_session import with_spark_session, is_before_spark_310

_base_conf = {'spark.rapids.sql.variableFloatAgg.enabled': 'true',
        'spark.rapids.sql.hasNans': 'false'}

_adaptive_conf = _base_conf.copy()
_adaptive_conf.update({'spark.sql.adaptive.enabled': 'true'})

@approximate_float
@pytest.mark.parametrize('conf', [_base_conf, _adaptive_conf])
def test_tpch_q1(tpch, conf):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q1"), conf=conf)

@approximate_float
@incompat
@allow_non_gpu('TakeOrderedAndProjectExec', 'SortOrder', 'AttributeReference')
@pytest.mark.parametrize('conf', [_base_conf, _adaptive_conf])
def test_tpch_q2(tpch, conf):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q2"), conf=conf)

@approximate_float
@allow_non_gpu('TakeOrderedAndProjectExec', 'SortOrder', 'AttributeReference')
@pytest.mark.parametrize('conf', [_base_conf, _adaptive_conf])
def test_tpch_q3(tpch, conf):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q3"), conf=conf)

@pytest.mark.parametrize('conf', [_base_conf, _adaptive_conf])
def test_tpch_q4(tpch, conf):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q4"), conf=conf)

@approximate_float
@pytest.mark.parametrize('conf', [_base_conf, _adaptive_conf])
def test_tpch_q5(tpch, conf):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q5"), conf=conf)

@approximate_float
@pytest.mark.parametrize('conf', [_base_conf, _adaptive_conf])
def test_tpch_q6(tpch, conf):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q6"), conf=conf)

@approximate_float
@pytest.mark.parametrize('conf', [_base_conf, _adaptive_conf])
def test_tpch_q7(tpch, conf):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q7"), conf=conf)

@approximate_float
@pytest.mark.parametrize('conf', [_base_conf, _adaptive_conf])
def test_tpch_q8(tpch, conf):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q8"), conf=conf)

@approximate_float
@pytest.mark.parametrize('conf', [_base_conf, _adaptive_conf])
def test_tpch_q9(tpch, conf):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q9"), conf=conf)

@incompat
@approximate_float
@allow_non_gpu('TakeOrderedAndProjectExec', 'SortOrder', 'AttributeReference')
@pytest.mark.parametrize('conf', [_base_conf, _adaptive_conf])
def test_tpch_q10(tpch, conf):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q10"), conf=conf)

@approximate_float
@allow_non_gpu('FilterExec', 'And', 'IsNotNull', 'GreaterThan', 'AttributeReference', 'ScalarSubquery')
@pytest.mark.parametrize('conf', [_base_conf, _adaptive_conf])
def test_tpch_q11(tpch, conf):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q11"), conf=conf)

@pytest.mark.parametrize('conf', [_base_conf, _adaptive_conf])
def test_tpch_q12(tpch, conf):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q12"), conf=conf)

@pytest.mark.parametrize('conf', [_base_conf, _adaptive_conf])
def test_tpch_q13(tpch, conf):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q13"), conf=conf)

@approximate_float
@pytest.mark.parametrize('conf', [_base_conf, _adaptive_conf])
def test_tpch_q14(tpch, conf):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q14"), conf=conf)

#fp sum does not work on Q15
@allow_non_gpu(any=True)
@pytest.mark.parametrize('conf', [_base_conf, _adaptive_conf])
def test_tpch_q15(tpch, conf):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q15"))

@pytest.mark.xfail(
    condition=not(is_before_spark_310()),
    reason='https://github.com/NVIDIA/spark-rapids/issues/586')
@allow_non_gpu('BroadcastNestedLoopJoinExec', 'Or', 'IsNull', 'EqualTo', 'AttributeReference', 'BroadcastExchangeExec')
@pytest.mark.parametrize('conf', [_base_conf, _adaptive_conf])
def test_tpch_q16(tpch, conf):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q16"), conf=conf)

@pytest.mark.xfail(
    condition=not(is_before_spark_310()),
    reason='https://github.com/NVIDIA/spark-rapids/issues/586')
@approximate_float
@pytest.mark.parametrize('conf', [_base_conf, _adaptive_conf])
def test_tpch_q17(tpch, conf):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q17"), conf=conf)

@pytest.mark.xfail(
    condition=not(is_before_spark_310()),
    reason='https://github.com/NVIDIA/spark-rapids/issues/586')
@incompat
@approximate_float
@allow_non_gpu('TakeOrderedAndProjectExec', 'SortOrder', 'AttributeReference')
@pytest.mark.parametrize('conf', [_base_conf, _adaptive_conf])
def test_tpch_q18(tpch, conf):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q18"), conf=conf)

@approximate_float
@pytest.mark.parametrize('conf', [_base_conf, _adaptive_conf])
def test_tpch_q19(tpch, conf):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q19"), conf=conf)

@pytest.mark.xfail(
    condition=not(is_before_spark_310()),
    reason='https://github.com/NVIDIA/spark-rapids/issues/586')
@pytest.mark.parametrize('conf', [_base_conf, _adaptive_conf])
def test_tpch_q20(tpch, conf):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q20"), conf=conf)

@allow_non_gpu('TakeOrderedAndProjectExec', 'SortOrder', 'AttributeReference',
        'SortMergeJoinExec', 'BroadcastHashJoinExec', 'BroadcastExchangeExec',
        'Alias', 'Not', 'EqualTo')
@pytest.mark.parametrize('conf', [_base_conf, _adaptive_conf])
def test_tpch_q21(tpch, conf):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q21"), conf=conf)

@approximate_float
#Once ScalarSubqery if fixed the rest should just work
@allow_non_gpu('FilterExec', 'And', 'AttributeReference', 'IsNotNull', 'In', 'Substring', 'Literal', 'GreaterThan', 'ScalarSubquery')
@pytest.mark.parametrize('conf', [_base_conf, _adaptive_conf])
def test_tpch_q22(tpch, conf):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q22"), conf=conf)
