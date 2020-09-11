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
import random
from spark_init_internal import get_spark_i_know_what_i_am_doing
from pyspark.sql.dataframe import DataFrame

_approximate_float_args = None

def get_float_check():
    if not _approximate_float_args is None:
        return lambda lhs,rhs: lhs == pytest.approx(rhs, **_approximate_float_args)
    else:
        return lambda lhs,rhs: lhs == rhs

_incompat = False

def is_incompat():
    return _incompat

_sort_on_spark = False
_sort_locally = False

def should_sort_on_spark():
    return _sort_on_spark

def should_sort_locally():
    return _sort_locally

_allow_any_non_gpu = False
_non_gpu_allowed = []

def is_allowing_any_non_gpu():
    return _allow_any_non_gpu

def get_non_gpu_allowed():
    return _non_gpu_allowed

_runtime_env = "apache"

def runtime_env():
    return _runtime_env.lower()

def is_apache_runtime():
    return runtime_env() == "apache"

def is_databricks_runtime():
    return runtime_env() == "databricks"

_limit = -1

def get_limit():
    return _limit

def _get_limit_from_mark(mark):
    if mark.args:
        return mark.args[0]
    else:
        return mark.kwargs.get('num_rows', 100000)

def pytest_runtest_setup(item):
    global _sort_on_spark
    global _sort_locally
    order = item.get_closest_marker('ignore_order')
    if order:
        if order.kwargs.get('local', False):
            _sort_on_spark = False
            _sort_locally = True
        else:
            _sort_on_spark = True
            _sort_locally = False
    else:
        _sort_on_spark = False
        _sort_locally = False

    global _incompat
    if item.get_closest_marker('incompat'):
        _incompat = True
    else:
        _incompat = False

    global _approximate_float_args
    app_f = item.get_closest_marker('approximate_float')
    if app_f:
        _approximate_float_args = app_f.kwargs
    else:
        _approximate_float_args = None

    global _allow_any_non_gpu
    global _non_gpu_allowed
    non_gpu = item.get_closest_marker('allow_non_gpu')
    if non_gpu:
        if non_gpu.kwargs and non_gpu.kwargs['any']:
            _allow_any_non_gpu = True
            _non_gpu_allowed = []
        elif non_gpu.args:
            _allow_any_non_gpu = False
            _non_gpu_allowed = non_gpu.args
        else:
            pytest.warn('allow_non_gpu marker without anything allowed')
            _allow_any_non_gpu = False
            _non_gpu_allowed = []
    else:
        _allow_any_non_gpu = False
        _non_gpu_allowed = []

    global _limit
    limit_mrk = item.get_closest_marker('limit')
    if limit_mrk:
        _limit = _get_limit_from_mark(limit_mrk)
    else:
        _limit = -1

def pytest_configure(config):
    global _runtime_env
    _runtime_env = config.getoption('runtime_env')

def pytest_collection_modifyitems(config, items):
    for item in items:
        extras = []
        order = item.get_closest_marker('ignore_order')
        if order:
            if order.kwargs:
                extras.append('IGNORE_ORDER(' + str(order.kwargs) + ')')
            else:
                extras.append('IGNORE_ORDER')
        if item.get_closest_marker('incompat'):
            extras.append('INCOMPAT')
        app_f = item.get_closest_marker('approximate_float')
        if app_f:
            if app_f.kwargs:
                extras.append('APPROXIMATE_FLOAT(' + str(app_f.kwargs) + ')')
            else:
                extras.append('APPROXIMATE_FLOAT')
        non_gpu = item.get_closest_marker('allow_non_gpu')
        if non_gpu:
            if non_gpu.kwargs and non_gpu.kwargs['any']:
                extras.append('ALLOW_NON_GPU(ANY)')
            elif non_gpu.args:
                extras.append('ALLOW_NON_GPU(' + ','.join(non_gpu.args) + ')')

        limit_mrk = item.get_closest_marker('limit')
        if limit_mrk:
            extras.append('LIMIT({})'.format(_get_limit_from_mark(limit_mrk)))

        if extras:
            # This is not ideal because we are reaching into an internal value
            item._nodeid = item.nodeid + '[' + ', '.join(extras) + ']'

@pytest.fixture(scope="session")
def std_input_path(request):
    path = request.config.getoption("std_input_path")
    if path is None:
        pytest.skip("std_input_path is not configured")
    else:
        yield path

@pytest.fixture
def spark_tmp_path(request):
    debug = request.config.getoption('debug_tmp_path')
    ret = request.config.getoption('tmp_path')
    if ret is None:
        ret = '/tmp/pyspark_tests/'
    ret = ret + '/' + str(random.randint(0, 1000000)) + '/'
    # Make sure it is there and accessible
    sc = get_spark_i_know_what_i_am_doing().sparkContext
    config = sc._jsc.hadoopConfiguration()
    path = sc._jvm.org.apache.hadoop.fs.Path(ret)
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(config)
    fs.mkdirs(path)
    yield ret
    if not debug:
        fs.delete(path)

def _get_jvm_session(spark):
    return spark._jsparkSession

def _get_jvm(spark):
    return spark.sparkContext._jvm

def spark_jvm():
    return _get_jvm(get_spark_i_know_what_i_am_doing())

class TpchRunner:
  def __init__(self, tpch_format, tpch_path):
    self.tpch_format = tpch_format
    self.tpch_path = tpch_path
    self.setup(get_spark_i_know_what_i_am_doing())

  def setup(self, spark):
    jvm_session = _get_jvm_session(spark)
    jvm = _get_jvm(spark)
    formats = {
      "csv": jvm.com.nvidia.spark.rapids.tests.tpch.TpchLikeSpark.setupAllCSV,
      "parquet": jvm.com.nvidia.spark.rapids.tests.tpch.TpchLikeSpark.setupAllParquet,
      "orc": jvm.com.nvidia.spark.rapids.tests.tpch.TpchLikeSpark.setupAllOrc
    }
    if not self.tpch_format in formats:
        raise RuntimeError("{} is not a supported tpch input type".format(self.tpch_format))
    formats.get(self.tpch_format)(jvm_session, self.tpch_path)

  def do_test_query(self, query):
    spark = get_spark_i_know_what_i_am_doing()
    jvm_session = _get_jvm_session(spark)
    jvm = _get_jvm(spark)
    tests = {
      "q1": jvm.com.nvidia.spark.rapids.tests.tpch.Q1Like,
      "q2": jvm.com.nvidia.spark.rapids.tests.tpch.Q2Like,
      "q3": jvm.com.nvidia.spark.rapids.tests.tpch.Q3Like,
      "q4": jvm.com.nvidia.spark.rapids.tests.tpch.Q4Like,
      "q5": jvm.com.nvidia.spark.rapids.tests.tpch.Q5Like,
      "q6": jvm.com.nvidia.spark.rapids.tests.tpch.Q6Like,
      "q7": jvm.com.nvidia.spark.rapids.tests.tpch.Q7Like,
      "q8": jvm.com.nvidia.spark.rapids.tests.tpch.Q8Like,
      "q9": jvm.com.nvidia.spark.rapids.tests.tpch.Q9Like,
      "q10": jvm.com.nvidia.spark.rapids.tests.tpch.Q10Like,
      "q11": jvm.com.nvidia.spark.rapids.tests.tpch.Q11Like,
      "q12": jvm.com.nvidia.spark.rapids.tests.tpch.Q12Like,
      "q13": jvm.com.nvidia.spark.rapids.tests.tpch.Q13Like,
      "q14": jvm.com.nvidia.spark.rapids.tests.tpch.Q14Like,
      "q15": jvm.com.nvidia.spark.rapids.tests.tpch.Q15Like,
      "q16": jvm.com.nvidia.spark.rapids.tests.tpch.Q16Like,
      "q17": jvm.com.nvidia.spark.rapids.tests.tpch.Q17Like,
      "q18": jvm.com.nvidia.spark.rapids.tests.tpch.Q18Like,
      "q19": jvm.com.nvidia.spark.rapids.tests.tpch.Q19Like,
      "q20": jvm.com.nvidia.spark.rapids.tests.tpch.Q20Like,
      "q21": jvm.com.nvidia.spark.rapids.tests.tpch.Q21Like,
      "q22": jvm.com.nvidia.spark.rapids.tests.tpch.Q22Like
    }
    df = tests.get(query).apply(jvm_session)
    return DataFrame(df, spark.getActiveSession())
   
@pytest.fixture(scope="session")
def tpch(request):
    tpch_format = request.config.getoption("tpch_format")
    tpch_path = request.config.getoption("tpch_path")
    if tpch_path is None:
        std_path = request.config.getoption("std_input_path")
        if std_path is None:
            pytest.skip("TPCH not configured to run")
        else:
            tpch_path = std_path + '/tpch/'
            tpch_format = 'parquet'
    yield TpchRunner(tpch_format, tpch_path)

class TpcxbbRunner:
  def __init__(self, tpcxbb_format, tpcxbb_path):
    self.tpcxbb_format = tpcxbb_format
    self.tpcxbb_path = tpcxbb_path
    self.setup(get_spark_i_know_what_i_am_doing())

  def setup(self, spark):
    jvm_session = _get_jvm_session(spark)
    jvm = _get_jvm(spark)
    formats = {
      "csv": jvm.com.nvidia.spark.rapids.tests.tpcxbb.TpcxbbLikeSpark.setupAllCSV,
      "parquet": jvm.com.nvidia.spark.rapids.tests.tpcxbb.TpcxbbLikeSpark.setupAllParquet,
      "orc": jvm.com.nvidia.spark.rapids.tests.tpcxbb.TpcxbbLikeSpark.setupAllOrc
    }
    if not self.tpcxbb_format in formats:
        raise RuntimeError("{} is not a supported tpcxbb input type".format(self.tpcxbb_format))
    formats.get(self.tpcxbb_format)(jvm_session,self.tpcxbb_path)

  def do_test_query(self, query):
    spark = get_spark_i_know_what_i_am_doing()
    jvm_session = _get_jvm_session(spark)
    jvm = _get_jvm(spark)
    tests = {
      "q5": jvm.com.nvidia.spark.rapids.tests.tpcxbb.Q5Like,
      "q16": jvm.com.nvidia.spark.rapids.tests.tpcxbb.Q16Like,
      "q21": jvm.com.nvidia.spark.rapids.tests.tpcxbb.Q21Like,
      "q22": jvm.com.nvidia.spark.rapids.tests.tpcxbb.Q22Like
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

class MortgageRunner:
  def __init__(self, mortgage_format, mortgage_acq_path, mortgage_perf_path):
    self.mortgage_format = mortgage_format
    self.mortgage_acq_path = mortgage_acq_path
    self.mortgage_perf_path = mortgage_perf_path

  def do_test_query(self, spark):
    jvm_session = _get_jvm_session(spark)
    jvm = _get_jvm(spark)
    acq = self.mortgage_acq_path
    perf = self.mortgage_perf_path
    run = jvm.com.nvidia.spark.rapids.tests.mortgage.Run
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
        std_path = request.config.getoption("std_input_path")
        if std_path is None:
            pytest.skip("Mortgage not configured to run")
        else:
            yield MortgageRunner('parquet', std_path + '/parquet_acq', std_path + '/parquet_perf')
    else:
        yield MortgageRunner(mortgage_format, mortgage_path + '/acq', mortgage_path + '/perf')

class TpcdsRunner:
  def __init__(self, tpcds_format, tpcds_path):
    self.tpcds_format = tpcds_format
    self.tpcds_path = tpcds_path
    self.setup(get_spark_i_know_what_i_am_doing())

  def setup(self, spark):
    jvm_session = _get_jvm_session(spark)
    jvm = _get_jvm(spark)
    formats = {
      "csv": jvm.com.nvidia.spark.rapids.tests.tpcds.TpcdsLikeSpark.setupAllCSV,
      "parquet": jvm.com.nvidia.spark.rapids.tests.tpcds.TpcdsLikeSpark.setupAllParquet,
      "orc": jvm.com.nvidia.spark.rapids.tests.tpcds.TpcdsLikeSpark.setupAllOrc
    }
    if not self.tpcds_format in formats:
        raise RuntimeError("{} is not a supported tpcds input type".format(self.tpcds_format))
    formats.get(self.tpcds_format)(jvm_session, self.tpcds_path, True)

  def do_test_query(self, query):
    spark = get_spark_i_know_what_i_am_doing()
    jvm_session = _get_jvm_session(spark)
    jvm = _get_jvm(spark)
    df = jvm.com.nvidia.spark.rapids.tests.tpcds.TpcdsLikeSpark.run(jvm_session, query)
    return DataFrame(df, spark.getActiveSession())
   
@pytest.fixture(scope="session")
def tpcds(request):
  tpcds_format = request.config.getoption("tpcds_format")
  tpcds_path = request.config.getoption("tpcds_path")
  if tpcds_path is None:
    pytest.skip("TPC-DS not configured to run")
  else:
    yield TpcdsRunner(tpcds_format, tpcds_path)

