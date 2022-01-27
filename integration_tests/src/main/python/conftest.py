# Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

def get_validate_execs_in_gpu_plan():
    return _validate_execs_in_gpu_plan

_runtime_env = "apache"

def runtime_env():
    return _runtime_env.lower()

def is_apache_runtime():
    return runtime_env() == "apache"

def is_databricks_runtime():
    return runtime_env() == "databricks"

def is_emr_runtime():
    return runtime_env() == "emr"

def is_dataproc_runtime():
    return runtime_env() == "dataproc"

_is_nightly_run = False
_is_precommit_run = False

def is_nightly_run():
    return _is_nightly_run

def is_at_least_precommit_run():
    return _is_nightly_run or _is_precommit_run

def skip_unless_nightly_tests(description):
    if (_is_nightly_run):
        raise AssertionError(description + ' during nightly test run')
    else:
        pytest.skip(description)

def skip_unless_precommit_tests(description):
    if (_is_nightly_run):
        raise AssertionError(description + ' during nightly test run')
    elif (_is_precommit_run):
        raise AssertionError(description + ' during pre-commit test run')
    else:
        pytest.skip(description)

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
    _non_gpu_allowed_databricks = []
    _allow_any_non_gpu_databricks = False
    non_gpu_databricks = item.get_closest_marker('allow_non_gpu_databricks')
    non_gpu = item.get_closest_marker('allow_non_gpu')
    if non_gpu_databricks:
        if is_databricks_runtime():
            if non_gpu_databricks.kwargs and non_gpu_databricks.kwargs['any']:
                _allow_any_non_gpu_databricks = True
            elif non_gpu_databricks.args:
                _non_gpu_allowed_databricks = non_gpu_databricks.args
            else:
                pytest.warn('allow_non_gpu_databricks marker without anything allowed')
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

    _allow_any_non_gpu = _allow_any_non_gpu | _allow_any_non_gpu_databricks
    if _non_gpu_allowed and _non_gpu_allowed_databricks:
        _non_gpu_allowed = _non_gpu_allowed + _non_gpu_allowed_databricks
    elif _non_gpu_allowed_databricks:
        _non_gpu_allowed = _non_gpu_allowed_databricks

    global _validate_execs_in_gpu_plan
    validate_execs = item.get_closest_marker('validate_execs_in_gpu_plan')
    if validate_execs and validate_execs.args:
        _validate_execs_in_gpu_plan = validate_execs.args
    else:
        _validate_execs_in_gpu_plan = []

    global _limit
    limit_mrk = item.get_closest_marker('limit')
    if limit_mrk:
        _limit = _get_limit_from_mark(limit_mrk)
    else:
        _limit = -1

def pytest_configure(config):
    global _runtime_env
    _runtime_env = config.getoption('runtime_env')
    global _is_nightly_run
    global _is_precommit_run
    test_type = config.getoption('test_type').lower()
    if "nightly" == test_type:
        _is_nightly_run = True
    elif "pre-commit" == test_type:
        _is_precommit_run = True
    elif "developer" != test_type:
        raise Exception("not supported test type {}".format(test_type))

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
        skip_unless_precommit_tests("std_input_path is not configured")
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

class TmpTableFactory:
  def __init__(self, base_id):
      self.base_id = base_id
      self.running_id = 0

  def get(self):
      ret = '{}_{}'.format(self.base_id, self.running_id)
      self.running_id = self.running_id + 1
      return ret

@pytest.fixture
def spark_tmp_table_factory(request):
    base_id = 'tmp_table_{}'.format(random.randint(0, 1000000))
    yield TmpTableFactory(base_id)
    sp = get_spark_i_know_what_i_am_doing()
    tables = sp.sql("SHOW TABLES".format(base_id)).collect()
    for row in tables:
        t_name = row['tableName']
        if (t_name.startswith(base_id)):
            sp.sql("DROP TABLE IF EXISTS {}".format(t_name))

def _get_jvm_session(spark):
    return spark._jsparkSession

def _get_jvm(spark):
    return spark.sparkContext._jvm

def spark_jvm():
    return _get_jvm(get_spark_i_know_what_i_am_doing())

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
            skip_unless_precommit_tests("Mortgage tests are not configured to run")
        else:
            yield MortgageRunner('parquet', std_path + '/parquet_acq', std_path + '/parquet_perf')
    else:
        yield MortgageRunner(mortgage_format, mortgage_path + '/acq', mortgage_path + '/perf')

@pytest.fixture(scope="session")
def enable_cudf_udf(request):
    enable_udf_cudf = request.config.getoption("cudf_udf")
    if not enable_udf_cudf:
        # cudf_udf tests are not required for any test runs
        pytest.skip("cudf_udf not configured to run")

@pytest.fixture(scope="session")
def enable_rapids_udf_example_native(request):
    native_enabled = request.config.getoption("rapids_udf_example_native")
    if not native_enabled:
        # udf_example_native tests are not required for any test runs
        pytest.skip("rapids_udf_example_native is not configured to run")
