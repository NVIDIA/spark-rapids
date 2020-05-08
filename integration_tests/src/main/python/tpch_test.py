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
from spark_session import spark, get_jvm, get_jvm_session
from pyspark.sql.dataframe import DataFrame

class TpchRunner:
  def __init__(self, tpch_format, tpch_path):
    self.tpch_format = tpch_format
    self.tpch_path = tpch_path
    self.setup(spark)

  def setup(self, spark):
    jvm_session = get_jvm_session(spark)
    jvm = get_jvm(spark)
    formats = {
      "csv": jvm.ai.rapids.sparkexamples.tpch.TpchLikeSpark.setupAllCSV,
      "parquet": jvm.ai.rapids.sparkexamples.tpch.TpchLikeSpark.setupAllParquet,
      "orc": jvm.ai.rapids.sparkexamples.tpch.TpchLikeSpark.setupAllOrc
    }
    formats.get(self.tpch_format)(jvm_session, self.tpch_path)

  def do_test_query(self, query):
    jvm_session = get_jvm_session(spark)
    jvm = get_jvm(spark)
    tests = {
      "q1": jvm.ai.rapids.sparkexamples.tpch.Q1Like,
      "q2": jvm.ai.rapids.sparkexamples.tpch.Q2Like,
      "q3": jvm.ai.rapids.sparkexamples.tpch.Q3Like,
      "q4": jvm.ai.rapids.sparkexamples.tpch.Q4Like,
      "q5": jvm.ai.rapids.sparkexamples.tpch.Q5Like,
      "q6": jvm.ai.rapids.sparkexamples.tpch.Q6Like,
      "q7": jvm.ai.rapids.sparkexamples.tpch.Q7Like,
      "q8": jvm.ai.rapids.sparkexamples.tpch.Q8Like,
      "q9": jvm.ai.rapids.sparkexamples.tpch.Q9Like,
      "q10": jvm.ai.rapids.sparkexamples.tpch.Q10Like,
      "q11": jvm.ai.rapids.sparkexamples.tpch.Q11Like,
      "q12": jvm.ai.rapids.sparkexamples.tpch.Q12Like,
      "q13": jvm.ai.rapids.sparkexamples.tpch.Q13Like,
      "q14": jvm.ai.rapids.sparkexamples.tpch.Q14Like,
      "q15": jvm.ai.rapids.sparkexamples.tpch.Q15Like,
      "q16": jvm.ai.rapids.sparkexamples.tpch.Q16Like,
      "q17": jvm.ai.rapids.sparkexamples.tpch.Q17Like,
      "q18": jvm.ai.rapids.sparkexamples.tpch.Q18Like,
      "q19": jvm.ai.rapids.sparkexamples.tpch.Q19Like,
      "q20": jvm.ai.rapids.sparkexamples.tpch.Q20Like,
      "q21": jvm.ai.rapids.sparkexamples.tpch.Q21Like,
      "q22": jvm.ai.rapids.sparkexamples.tpch.Q22Like
    }
    df = tests.get(query).apply(jvm_session)
    return DataFrame(df, spark.getActiveSession())
   
@pytest.fixture(scope="session")
def tpch(request):
  tpch_format = request.config.getoption("tpch_format")
  tpch_path = request.config.getoption("tpch_path")
  if tpch_path is None:
    pytest.skip("TPCH not configured to run")
  else:
    yield TpchRunner(tpch_format, tpch_path)

_base_conf = {'spark.rapids.sql.variableFloatAgg.enabled': 'true',
        'spark.rapids.sql.hasNans': 'false'}

@approximate_float
def test_tpch_q1(tpch):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q1"), conf=_base_conf)

@approximate_float
@incompat
@allow_non_gpu('TakeOrderedAndProjectExec', 'SortOrder', 'AttributeReference')
def test_tpch_q2(tpch):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q2"), conf=_base_conf)

@approximate_float
@allow_non_gpu('TakeOrderedAndProjectExec', 'SortOrder', 'AttributeReference')
def test_tpch_q3(tpch):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q3"), conf=_base_conf)

def test_tpch_q4(tpch):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q4"), conf=_base_conf)

@approximate_float
def test_tpch_q5(tpch):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q5"), conf=_base_conf)

@approximate_float
def test_tpch_q6(tpch):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q6"), conf=_base_conf)

@approximate_float
def test_tpch_q7(tpch):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q7"), conf=_base_conf)

@approximate_float
def test_tpch_q8(tpch):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q8"), conf=_base_conf)

@approximate_float
def test_tpch_q9(tpch):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q9"), conf=_base_conf)

@incompat
@approximate_float
@allow_non_gpu('TakeOrderedAndProjectExec', 'SortOrder', 'AttributeReference')
def test_tpch_q10(tpch):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q10"), conf=_base_conf)

@approximate_float
@allow_non_gpu('FilterExec', 'And', 'IsNotNull', 'GreaterThan', 'AttributeReference', 'ScalarSubquery')
def test_tpch_q11(tpch):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q11"), conf=_base_conf)

def test_tpch_q12(tpch):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q12"), conf=_base_conf)

def test_tpch_q13(tpch):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q13"), conf=_base_conf)

@approximate_float
def test_tpch_q14(tpch):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q14"), conf=_base_conf)

#fp sum does not work on Q15
@allow_non_gpu(any=True)
def test_tpch_q15(tpch):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q15"))

@allow_non_gpu('BroadcastNestedLoopJoinExec', 'Or', 'IsNull', 'EqualTo', 'AttributeReference', 'BroadcastExchangeExec')
def test_tpch_q16(tpch):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q16"), conf=_base_conf)

@approximate_float
def test_tpch_q17(tpch):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q17"), conf=_base_conf)

@incompat
@approximate_float
@allow_non_gpu('TakeOrderedAndProjectExec', 'SortOrder', 'AttributeReference')
def test_tpch_q18(tpch):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q18"), conf=_base_conf)

def test_tpch_q19(tpch):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q19"), conf=_base_conf)

def test_tpch_q20(tpch):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q20"), conf=_base_conf)

@allow_non_gpu('TakeOrderedAndProjectExec', 'SortOrder', 'AttributeReference', 'SortMergeJoinExec', 'Alias', 'Not', 'EqualTo')
def test_tpch_q21(tpch):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q21"), conf=_base_conf)

@approximate_float
#Once ScalarSubqery if fixed the rest should just work
@allow_non_gpu('FilterExec', 'And', 'AttributeReference', 'IsNotNull', 'In', 'Substring', 'Literal', 'GreaterThan', 'ScalarSubquery')
def test_tpch_q22(tpch):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpch.do_test_query("q22"), conf=_base_conf)
