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

from asserts import assert_gpu_and_cpu_are_equal_iterator
from marks import incompat, ignore_order, allow_any_non_gpu
from spark_session import spark, get_jvm, get_jvm_session
from pyspark.sql.dataframe import DataFrame

class MortgageRunner:
  def __init__(self, mortgage_format, mortgage_path):
    self.mortgage_format = mortgage_format
    self.mortgage_path = mortgage_path

  def do_test_query(self, spark):
    jvm_session = get_jvm_session(spark)
    jvm = get_jvm(spark)
    acq = self.mortgage_path + '/acq'
    perf = self.mortgage_path + '/perf'
    run = jvm.ai.rapids.sparkexamples.mortgage.Run
    if self.mortgage_format == 'csv':
        df = run.csv(jvm_session, perf, acq)
    elif self.mortgage_format == 'parquet':
        df = run.parquet(jvm_session, perf, acq)
    elif self.mortgage_format == 'orc':
        df = run.orc(jvm_session, perf, acq)
    else:
        raise AssertionError('Not Supported Format {}'.format(self.mortgage_format))

    return DataFrame(df, spark.getActiveSession())
   
@pytest.fixture(scope="session")
def mortgage(request):
  mortgage_format = request.config.getoption("mortgage_format")
  mortgage_path = request.config.getoption("mortgage_path")
  if mortgage_path is None:
    pytest.skip("Mortgage not configured to run")
  else:
    yield MortgageRunner(mortgage_format, mortgage_path)

@incompat
@ignore_order
@allow_any_non_gpu
def test_mortgage(mortgage):
  assert_gpu_and_cpu_are_equal_iterator(
          lambda spark : mortgage.do_test_query(spark))
