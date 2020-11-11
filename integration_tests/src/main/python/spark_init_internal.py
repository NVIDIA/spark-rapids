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

try:
    from pyspark.sql import SparkSession
except ImportError as error:
    import findspark
    findspark.init()
    from pyspark.sql import SparkSession

_DRIVER_ENV = 'PYSP_TEST_spark_driver_extraJavaOptions'

def _spark__init():
    #Force the RapidsPlugin to be enabled, so it blows up if the classpath is not set properly
    # DO NOT SET ANY OTHER CONFIGS HERE!!!
    # due to bugs in pyspark/pytest it looks like any configs set here
    # can be reset in the middle of a test if specific operations are done (some types of cast etc)
    import os
    _sb = SparkSession.builder
    _sb.config('spark.master', 'local') \
            .config('spark.plugins', 'com.nvidia.spark.SQLPlugin') \
            .config("spark.sql.adaptive.enabled", "false") \
            .config('spark.sql.queryExecutionListeners', 'com.nvidia.spark.rapids.ExecutionPlanCaptureCallback')

    for key, value in os.environ.items():
        if key.startswith('PYSP_TEST_') and key != _DRIVER_ENV:
            _sb.config(key[10:].replace('_', '.'), value)

    driver_opts = os.environ.get(_DRIVER_ENV, "")

    if ('PYTEST_XDIST_WORKER' in os.environ):
        wid = os.environ['PYTEST_XDIST_WORKER']
        d = "./derby_{}".format(wid)
        if not os.path.exists(d):
            os.makedirs(d)
        _sb.config('spark.driver.extraJavaOptions', driver_opts + ' -Dderby.system.home={}'.format(d))
    else:
        _sb.config('spark.driver.extraJavaOptions', driver_opts)
 
    # enableHiveSupport() is needed for parquet bucket tests
    _s = _sb.enableHiveSupport() \
            .appName('rapids spark plugin integration tests (python)').getOrCreate()
    #TODO catch the ClassNotFound error that happens if the classpath is not set up properly and
    # make it a better error message
    _s.sparkContext.setLogLevel("WARN")
    return _s

_spark = _spark__init()

def get_spark_i_know_what_i_am_doing():
    """
    Get the current SparkSession.
    This should almost never be called directly instead you should call
    with_spark_session, with_cpu_session, or with_gpu_session for spark_session.
    This is to guarantee that the session and it's config is setup in a repeatable way.
    """
    return _spark

def spark_version():
    return _spark.version
