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

from conftest import is_allowing_any_non_gpu, get_non_gpu_allowed, get_validate_execs_in_gpu_plan
from pyspark.sql import SparkSession, DataFrame
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
    for key, value in conf.items():
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
    _set_all_confs(conf)
    ret = func(_spark)
    _check_for_proper_return_values(ret)
    return ret

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
    # TODO: remove when decimal types can be enabled by default
    copy['spark.rapids.sql.decimalType.enabled'] = 'true'
    return with_spark_session(func, conf=copy)

def is_before_spark_311():
    return spark_version() < "3.1.1"
