# Copyright (c) 2020-2022, NVIDIA CORPORATION.
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
from conftest import is_allowing_any_non_gpu, get_non_gpu_allowed, get_validate_execs_in_gpu_plan, is_databricks_runtime, is_at_least_precommit_run
from pyspark.sql import DataFrame
from spark_init_internal import get_spark_i_know_what_i_am_doing, spark_version

def _from_scala_map(scala_map):
    ret = {}
    # The value we get is a scala map, not a java map, so we need to jump through some hoops
    keys = scala_map.keys().iterator()
    while keys.hasNext():
        key = keys.next()
        ret[key] = scala_map.get(key).get()
    return ret

_spark = get_spark_i_know_what_i_am_doing()
# Have to reach into a private member to get access to the API we need
_orig_conf = _from_scala_map(_spark.conf._jconf.getAll())
_orig_conf_keys = _orig_conf.keys()

# Default settings that should apply to CPU and GPU sessions.
# These settings can be overridden by specific tests if necessary.
# Many of these are redundant with default settings for the configs but are set here explicitly
# to ensure any cluster settings do not interfere with tests that assume the defaults.
_default_conf = {
    'spark.ansi.enabled': 'false',
    'spark.rapids.sql.castDecimalToFloat.enabled': 'false',
    'spark.rapids.sql.castDecimalToString.enabled': 'false',
    'spark.rapids.sql.castFloatToDecimal.enabled': 'false',
    'spark.rapids.sql.castFloatToIntegralTypes.enabled': 'false',
    'spark.rapids.sql.castFloatToString.enabled': 'false',
    'spark.rapids.sql.castStringToFloat.enabled': 'false',
    'spark.rapids.sql.castStringToTimestamp.enabled': 'false',
    'spark.rapids.sql.fast.sample': 'false',
    'spark.rapids.sql.hasExtendedYearValues': 'true',
    'spark.rapids.sql.hashOptimizeSort.enabled': 'false',
    'spark.rapids.sql.improvedFloatOps.enabled': 'false',
    'spark.rapids.sql.improvedTimeOps.enabled': 'false',
    'spark.rapids.sql.incompatibleDateFormats.enabled': 'false',
    'spark.rapids.sql.incompatibleOps.enabled': 'false',
    'spark.rapids.sql.mode': 'executeongpu',
    'spark.rapids.sql.variableFloatAgg.enabled': 'false',
    'spark.sql.legacy.allowNegativeScaleOfDecimal': 'true',
}

def is_tz_utc(spark=_spark):
    """
    true if the tz is UTC else false
    """
    # Now we have to do some kind of ugly internal java stuff
    jvm = spark.sparkContext._jvm
    utc = jvm.java.time.ZoneId.of('UTC').normalized()
    sys_tz = jvm.java.time.ZoneId.systemDefault().normalized()
    return utc == sys_tz

def _set_all_confs(conf):
    newconf = _default_conf.copy()
    newconf.update(conf)
    for key, value in newconf.items():
        if _spark.conf.get(key, None) != value:
            _spark.conf.set(key, value)

def reset_spark_session_conf():
    """Reset all of the configs for a given spark session."""
    _set_all_confs(_orig_conf)
    #We should clear the cache
    _spark.catalog.clearCache()
    # Have to reach into a private member to get access to the API we need
    current_keys = _from_scala_map(_spark.conf._jconf.getAll()).keys()
    for key in current_keys:
        if key not in _orig_conf_keys:
            _spark.conf.unset(key)

def _check_for_proper_return_values(something):
    """We don't want to return an DataFrame or Dataset from a with_spark_session. You will not get what you expect"""
    if (isinstance(something, DataFrame)):
        raise RuntimeError("You should never return a DataFrame from a with_*_session, you will not get the results that you expect")

def with_spark_session(func, conf={}):
    """Run func that takes a spark session as input with the given configs set."""
    reset_spark_session_conf()
    _add_job_description(conf)
    _set_all_confs(conf)
    ret = func(_spark)
    _check_for_proper_return_values(ret)
    return ret


def _add_job_description(conf):
    is_gpu_job = conf.get('spark.rapids.sql.enabled', False)
    job_type = 'GPU' if str(is_gpu_job).lower() == str(True).lower() else 'CPU'
    job_desc = '{}[{}]'.format(os.environ.get('PYTEST_CURRENT_TEST'), job_type)
    _spark.sparkContext.setJobDescription(job_desc)


def with_cpu_session(func, conf={}):
    """Run func that takes a spark session as input with the given configs set on the CPU."""
    copy = dict(conf)
    copy['spark.rapids.sql.enabled'] = 'false'
    return with_spark_session(func, conf=copy)

def with_gpu_session(func, conf={}):
    """
    Run func that takes a spark session as input with the given configs set on the GPU.
    Note that this forces you into test mode unless.  It is not a requirement, but is
    simplest for right now.
    """
    copy = dict(conf)
    copy['spark.rapids.sql.enabled'] = 'true'
    if is_allowing_any_non_gpu():
        copy['spark.rapids.sql.test.enabled'] = 'false'
    else:
        copy['spark.rapids.sql.test.enabled'] = 'true'
        copy['spark.rapids.sql.test.allowedNonGpu'] = ','.join(get_non_gpu_allowed())

    copy['spark.rapids.sql.test.validateExecsInGpuPlan'] = ','.join(get_validate_execs_in_gpu_plan())
    return with_spark_session(func, conf=copy)

def is_before_spark_312():
    return spark_version() < "3.1.2"

def is_before_spark_313():
    return spark_version() < "3.1.3"

def is_before_spark_314():
    return spark_version() < "3.1.4"

def is_before_spark_320():
    return spark_version() < "3.2.0"

def is_before_spark_322():
    return spark_version() < "3.2.2"

def is_before_spark_330():
    return spark_version() < "3.3.0"

def is_before_spark_331():
    return spark_version() < "3.3.1"

def is_before_spark_340():
    return spark_version() < "3.4.0"

def is_spark_330_or_later():
    return spark_version() >= "3.3.0"

def is_spark_340_or_later():
    return spark_version() >= "3.4.0"

def is_spark_330():
    return spark_version() == "3.3.0"

def is_spark_33X():
    return "3.3.0" <= spark_version() < "3.4.0"

def is_spark_321cdh():
    return "3.2.1.3.2.717" in spark_version()

def is_spark_330cdh():
    return "3.3.0.3.3.718" in spark_version()

def is_spark_cdh():
    return is_spark_321cdh() or is_spark_330cdh()

def is_databricks_version_or_later(major, minor):
    spark = get_spark_i_know_what_i_am_doing()
    version = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "0.0")
    parts = version.split(".")
    if (len(parts) < 2):
        raise RuntimeError("Unable to determine Databricks version from version string: " + version)
    return int(parts[0]) >= major and int(parts[1]) >= minor

def is_databricks91_or_later():
    return is_databricks_version_or_later(9, 1)

def is_databricks104_or_later():
    return is_databricks_version_or_later(10, 4)

def is_databricks113_or_later():
    return is_databricks_version_or_later(11, 3)

def get_java_major_version():
    ver = _spark.sparkContext._jvm.System.getProperty("java.version")
    # Allow these formats:
    # 1.8.0_72-ea
    # 9-ea
    # 9
    # 11.0.1
    if ver.startswith('1.'):
        ver = ver[2:]
    dot_pos = ver.find('.')
    dash_pos = ver.find('-')
    if dot_pos != -1:
        ver = ver[0:dot_pos]
    elif dash_pos != -1:
        ver = ver[0:dash_pos]
    return int(ver)

def get_jvm_charset():
    sc = _spark.sparkContext
    return str(sc._jvm.java.nio.charset.Charset.defaultCharset())

def is_jvm_charset_utf8():
    return get_jvm_charset() == 'UTF-8'

def is_hive_available():
    # precommit and nightly runs are supposed to have Hive,
    # so tests should fail if Hive is missing in those environments.
    if is_at_least_precommit_run():
        return True
    return _spark.conf.get("spark.sql.catalogImplementation") == "hive"
