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
from marks import incompat, ignore_order, allow_non_gpu, approximate_float
from spark_session import spark, get_jvm, get_jvm_session
from pyspark.sql.dataframe import DataFrame

class TpcxbbRunner:
  def __init__(self, tpcxbb_format, tpcxbb_path):
    self.tpcxbb_format = tpcxbb_format
    self.tpcxbb_path = tpcxbb_path
    self.setup(spark)

  def setup(self, spark):
    jvm_session = get_jvm_session(spark)
    jvm = get_jvm(spark)
    formats = {
      "csv": jvm.ai.rapids.sparkexamples.tpcxbb.TpcxbbLikeSpark.setupAllCSV,
      "parquet": jvm.ai.rapids.sparkexamples.tpcxbb.TpcxbbLikeSpark.setupAllParquet,
      "orc": jvm.ai.rapids.sparkexamples.tpcxbb.TpcxbbLikeSpark.setupAllOrc
    }
    formats.get(self.tpcxbb_format)(jvm_session,self.tpcxbb_path)

  def do_test_query(self, query):
    jvm_session = get_jvm_session(spark)
    jvm = get_jvm(spark)
    tests = {
      "q5": jvm.ai.rapids.sparkexamples.tpcxbb.Q5Like,
      "q16": jvm.ai.rapids.sparkexamples.tpcxbb.Q16Like,
      "q21": jvm.ai.rapids.sparkexamples.tpcxbb.Q21Like,
      "q22": jvm.ai.rapids.sparkexamples.tpcxbb.Q22Like
    }
    df = tests.get(query).apply(jvm_session)
    return DataFrame(df, spark.getActiveSession())
   
@pytest.fixture(scope="session")
def tpcxbb(request):
  tpcxbb_format = request.config.getoption("tpcxbb_format")
  tpcxbb_path = request.config.getoption("tpcxbb_path")
  if tpcxbb_path is None:
    pytest.skip("TPCxBB not configured to run")
  else:
    yield TpcxbbRunner(tpcxbb_format, tpcxbb_path)

@ignore_order
def test_tpcxbb_q5(tpcxbb):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpcxbb.do_test_query("q5"))

@incompat
@approximate_float
@ignore_order
@allow_non_gpu(any=True)
def test_tpcxbb_q16(tpcxbb):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpcxbb.do_test_query("q16"))

@ignore_order
@allow_non_gpu(any=True)
def test_tpcxbb_q21(tpcxbb):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpcxbb.do_test_query("q21"))

@ignore_order
@allow_non_gpu(any=True)
def test_tpcxbb_q22(tpcxbb):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpcxbb.do_test_query("q22"))
