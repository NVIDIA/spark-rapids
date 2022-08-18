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

import os
import re


has_findspark = False
try:
    import pyspark
except ImportError as error:
    import findspark
    findspark.init()
    has_findspark = True
    import pyspark

_CONF_ENV_PREFIX = 'PYSP_TEST_'
_EXECUTOR_ENV_PREFIX = 'spark_executorEnv_'

def env_for_conf(spark_conf_name):
    # escape underscores
    escaped_conf = spark_conf_name.replace('_', r'__')
    return _CONF_ENV_PREFIX + escaped_conf.replace('.', '_')

def conf_for_env(env_name):
    conf_key = env_name[len(_CONF_ENV_PREFIX):]
    if conf_key.startswith(_EXECUTOR_ENV_PREFIX):
        res = _EXECUTOR_ENV_PREFIX.replace('_', '.') + conf_key[len(_EXECUTOR_ENV_PREFIX):]
    else:
        # replace standalone underscores
        res1 = re.sub(r'(?<!_)_(?!_)', '.', conf_key)
        # unescape: remove duplicate underscores
        res = res1.replace('__', '_')
    return res

_DRIVER_ENV = env_for_conf('spark.driver.extraJavaOptions')
_SPARK_JARS = env_for_conf("spark.jars")
_SPARK_JARS_PACKAGES = env_for_conf("spark.jars.packages")
spark_jars_env = {
    _SPARK_JARS,
    _SPARK_JARS_PACKAGES
}

def _spark__init():
    if has_findspark:
        spark_jars = os.getenv(_SPARK_JARS)
        spark_jars_packages = os.getenv(_SPARK_JARS_PACKAGES)
        if spark_jars is not None:
            findspark.add_jars(spark_jars)

        if spark_jars_packages is not None:
            findspark.add_packages(spark_jars_packages)

    #Force the RapidsPlugin to be enabled, so it blows up if the classpath is not set properly
    # DO NOT SET ANY OTHER CONFIGS HERE!!!
    # due to bugs in pyspark/pytest it looks like any configs set here
    # can be reset in the middle of a test if specific operations are done (some types of cast etc)
    _sb = pyspark.sql.SparkSession.builder
    _sb.config('spark.plugins', 'com.nvidia.spark.SQLPlugin') \
            .config("spark.sql.adaptive.enabled", "false") \
            .config('spark.sql.queryExecutionListeners', 'com.nvidia.spark.rapids.ExecutionPlanCaptureCallback')

    for key, value in os.environ.items():
        if key.startswith(_CONF_ENV_PREFIX) and key != _DRIVER_ENV and key not in spark_jars_env:
            _sb.config(conf_for_env(key), value)

    driver_opts = os.environ.get(_DRIVER_ENV, "")

    if ('PYTEST_XDIST_WORKER' in os.environ):
        wid = os.environ['PYTEST_XDIST_WORKER']
        _handle_derby_dir(_sb, driver_opts, wid)
        _handle_event_log_dir(_sb, wid)
    else:
        _sb.config('spark.driver.extraJavaOptions', driver_opts)
        _handle_event_log_dir(_sb, 'gw0')

    # enableHiveSupport() is needed for parquet bucket tests
    _s = _sb.enableHiveSupport() \
            .appName('rapids spark plugin integration tests (python)').getOrCreate()
    #TODO catch the ClassNotFound error that happens if the classpath is not set up properly and
    # make it a better error message
    _s.sparkContext.setLogLevel("WARN")
    return _s


def _handle_derby_dir(sb, driver_opts, wid):
    d = "./derby_{}".format(wid)
    if not os.path.exists(d):
        os.makedirs(d)
    sb.config('spark.driver.extraJavaOptions', driver_opts + ' -Dderby.system.home={}'.format(d))


def _handle_event_log_dir(sb, wid):
    if os.environ.get('SPARK_EVENTLOG_ENABLED', str(True)).lower() in [
        str(False).lower(), 'off', '0'
    ]:
        print('Automatic configuration for spark event log disabled')
        return

    spark_conf = pyspark.SparkConf()
    master_url = os.environ.get(env_for_conf('spark.master'),
                                spark_conf.get("spark.master", 'local'))
    event_log_config = os.environ.get(env_for_conf('spark.eventLog.enabled'),
                                      spark_conf.get('spark.eventLog.enabled', str(False).lower()))
    event_log_codec = os.environ.get(env_for_conf('spark.eventLog.compression.codec'), 'zstd')

    if not master_url.startswith('local') or event_log_config != str(False).lower():
        print("SPARK_EVENTLOG_ENABLED is ignored for non-local Spark master and when "
              "it's pre-configured by the user")
        return
    d = "./eventlog_{}".format(wid)
    if not os.path.exists(d):
        os.makedirs(d)

    print('Spark event logs will appear under {}. Set the environmnet variable '
          'SPARK_EVENTLOG_ENABLED=false if you want to disable it'.format(d))

    sb\
        .config('spark.eventLog.dir', "file://{}".format(os.path.abspath(d))) \
        .config('spark.eventLog.compress', True) \
        .config('spark.eventLog.enabled', True) \
        .config('spark.eventLog.compression.codec', event_log_codec)


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
