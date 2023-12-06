# Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

import os
import pytest
import random
import warnings

# TODO redo _spark stuff using fixtures
#
# Don't import pyspark / _spark directly in conftest globally
# import as a plugin to do a lazy per-pytest-session initialization
#
pytest_plugins = [
    'spark_init_internal'
]

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

def get_test_tz():
    return os.environ.get('TZ', 'UTC')

def is_utc():
    return get_test_tz() == "UTC"

def is_not_utc():
    return not is_utc()

_is_nightly_run = False
_is_precommit_run = False

def is_nightly_run():
    return _is_nightly_run

def is_precommit_run():
    return _is_precommit_run

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

_is_parquet_testing_tests_forced = False

def is_parquet_testing_tests_forced():
    return _is_parquet_testing_tests_forced

_limit = -1

_inject_oom = None

def should_inject_oom():
    return _inject_oom != None

# For datagen: we expect a seed to be provided by the environment, or default to 0.
# Note that tests can override their seed when calling into datagen by setting seed= in their tests.
_test_datagen_random_seed = int(os.getenv("SPARK_RAPIDS_TEST_DATAGEN_SEED", 0))
print(f"Starting with datagen test seed: {_test_datagen_random_seed}. " 
      "Set env variable SPARK_RAPIDS_TEST_DATAGEN_SEED to override.")

def get_datagen_seed():
    return _test_datagen_random_seed

def get_limit():
    return _limit

def _get_limit_from_mark(mark):
    if mark.args:
        return mark.args[0]
    else:
        return mark.kwargs.get('num_rows', 100000)

_std_input_path = None
def get_std_input_path():
    return _std_input_path

def pytest_runtest_setup(item):
    global _sort_on_spark
    global _sort_locally
    global _inject_oom
    global _test_datagen_random_seed
    _inject_oom = item.get_closest_marker('inject_oom')
    datagen_overrides = item.get_closest_marker('datagen_overrides')
    if datagen_overrides:
        try:
            seed = datagen_overrides.kwargs["seed"]
        except KeyError:
            raise Exception("datagen_overrides requires an override seed value")

        override_seed = datagen_overrides.kwargs.get('condition', True)
        if override_seed:
            _test_datagen_random_seed = seed

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
                warnings.warn('allow_non_gpu_databricks marker without anything allowed')
    if non_gpu:
        if non_gpu.kwargs and non_gpu.kwargs['any']:
            _allow_any_non_gpu = True
            _non_gpu_allowed = []
        elif non_gpu.args:
            _allow_any_non_gpu = False
            _non_gpu_allowed = non_gpu.args
        else:
            warnings.warn('allow_non_gpu marker without anything allowed')
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

    if item.get_closest_marker('iceberg'):
        if not item.config.getoption('iceberg'):
            pytest.skip('Iceberg tests not configured to run')
        elif is_databricks_runtime():
            pytest.skip('Iceberg tests skipped on Databricks')

    if item.get_closest_marker('delta_lake'):
        if not item.config.getoption('delta_lake'):
            pytest.skip('delta lake tests not configured to run')

    if item.get_closest_marker('large_data_test'):
        if not item.config.getoption('large_data_test'):
            pytest.skip('tests for large data not configured to run')

    if item.get_closest_marker('pyarrow_test'):
        if not item.config.getoption('pyarrow_test'):
            pytest.skip('tests for pyarrow not configured to run')

def pytest_configure(config):
    global _runtime_env
    _runtime_env = config.getoption('runtime_env')
    global _std_input_path
    _std_input_path = config.getoption("std_input_path")
    global _is_nightly_run
    global _is_precommit_run
    test_type = config.getoption('test_type').lower()
    if "nightly" == test_type:
        _is_nightly_run = True
    elif "pre-commit" == test_type:
        _is_precommit_run = True
    elif "developer" != test_type:
        raise Exception("not supported test type {}".format(test_type))
    global _is_parquet_testing_tests_forced
    _is_parquet_testing_tests_forced = config.getoption("force_parquet_testing_tests")

# For OOM injection: we expect a seed to be provided by the environment, or default to 1.
# This is done such that any worker started by the xdist plugin for pytest will
# have the same seed. Since each worker creates a list of tests independently and then
# pytest expects this starting list to match for all workers, it is important that the same seed
# is set for all, either from the environment or as a constant.
oom_random_injection_seed = int(os.getenv("SPARK_RAPIDS_TEST_INJECT_OOM_SEED", 1))
print(f"Starting with OOM injection seed: {oom_random_injection_seed}. " 
      "Set env variable SPARK_RAPIDS_TEST_INJECT_OOM_SEED to override.")

def pytest_collection_modifyitems(config, items):
    r = random.Random(oom_random_injection_seed)
    for item in items:
        extras = []
        order = item.get_closest_marker('ignore_order')
        # decide if OOMs should be injected, and when
        injection_mode = config.getoption('test_oom_injection_mode').lower()
        inject_choice = False
        datagen_overrides = item.get_closest_marker('datagen_overrides')
        if datagen_overrides:
            test_datagen_random_seed_choice = datagen_overrides.kwargs.get('seed', _test_datagen_random_seed)
            if test_datagen_random_seed_choice != _test_datagen_random_seed:
                extras.append('DATAGEN_SEED_OVERRIDE=%s' % str(test_datagen_random_seed_choice))
            else:
                extras.append('DATAGEN_SEED=%s' % str(test_datagen_random_seed_choice))
        else:
            extras.append('DATAGEN_SEED=%s' % str(_test_datagen_random_seed))

        if injection_mode == 'random':
            inject_choice = r.randrange(0, 2) == 1
        elif injection_mode == 'always':
            inject_choice = True
        if inject_choice:
            extras.append('INJECT_OOM')
            item.add_marker('inject_oom', append=True)
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

def get_worker_id(request):
    try:
        import xdist
        return xdist.plugin.get_xdist_worker_id(request)
    except ImportError:
        return 'main'

@pytest.fixture
def spark_tmp_path(request):
    from spark_init_internal import get_spark_i_know_what_i_am_doing
    debug = request.config.getoption('debug_tmp_path')
    ret = request.config.getoption('tmp_path')
    if ret is None:
        ret = '/tmp/pyspark_tests/'
    worker_id = get_worker_id(request)
    pid = os.getpid()
    hostname = os.uname()[1]
    ret = f'{ret}/{hostname}-{worker_id}-{pid}-{random.randrange(0, 1<<31)}/'
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
    from spark_init_internal import get_spark_i_know_what_i_am_doing
    worker_id = get_worker_id(request)
    table_id = random.getrandbits(31)
    base_id = f'tmp_table_{worker_id}_{table_id}'
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
    from spark_init_internal import get_spark_i_know_what_i_am_doing
    return _get_jvm(get_spark_i_know_what_i_am_doing())

class MortgageRunner:
  def __init__(self, mortgage_format, mortgage_acq_path, mortgage_perf_path):
    self.mortgage_format = mortgage_format
    self.mortgage_acq_path = mortgage_acq_path
    self.mortgage_perf_path = mortgage_perf_path

  def do_test_query(self, spark):
    from pyspark.sql.dataframe import DataFrame
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
def enable_fuzz_test(request):
    enable_fuzz_test = request.config.getoption("fuzz_test")
    if not enable_fuzz_test:
        # fuzz tests are not required for any test runs
        pytest.skip("fuzz_test not configured to run")
