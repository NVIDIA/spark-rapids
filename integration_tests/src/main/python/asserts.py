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

from conftest import is_incompat, should_sort_on_spark, should_sort_locally, get_float_check, get_limit, spark_jvm
from datetime import date, datetime
from decimal import Decimal
import math
from pyspark.sql import Row
from py4j.protocol import Py4JJavaError

import pytest
from spark_session import with_cpu_session, with_gpu_session
import time
import types as pytypes

def _assert_equal(cpu, gpu, float_check, path):
    t = type(cpu)
    if (t is Row):
        assert len(cpu) == len(gpu), "CPU and GPU row have different lengths at {}".format(path)
        if hasattr(cpu, "__fields__") and hasattr(gpu, "__fields__"):
            for field in cpu.__fields__:
                _assert_equal(cpu[field], gpu[field], float_check, path + [field])
        else:
            for index in range(len(cpu)):
                _assert_equal(cpu[index], gpu[index], float_check, path + [index])
    elif (t is list):
        assert len(cpu) == len(gpu), "CPU and GPU list have different lengths at {}".format(path)
        for index in range(len(cpu)):
            _assert_equal(cpu[index], gpu[index], float_check, path + [index])
    elif (t is pytypes.GeneratorType):
        index = 0
        # generator has no zip :( so we have to do this the hard way
        done = False
        while not done:
            sub_cpu = None
            sub_gpu = None
            try:
                sub_cpu = next(cpu)
            except StopIteration:
                done = True

            try:
                sub_gpu = next(gpu)
            except StopIteration:
                done = True

            if done:
                assert sub_cpu == sub_gpu and sub_cpu == None, "CPU and GPU generators have different lengths at {}".format(path)
            else:
                _assert_equal(sub_cpu, sub_gpu, float_check, path + [index])

            index = index + 1
    elif (t is dict):
        # TODO eventually we need to split this up so we can do the right thing for float/double
        # values stored under the map some where, especially for NaNs
        assert cpu == gpu, "GPU and CPU map values are different at {}".format(path)
    elif (t is int):
        assert cpu == gpu, "GPU and CPU int values are different at {}".format(path)
    elif (t is float):
        if (math.isnan(cpu)):
            assert math.isnan(gpu), "GPU and CPU float values are different at {}".format(path)
        else:
            assert float_check(cpu, gpu), "GPU and CPU float values are different {}".format(path)
    elif isinstance(cpu, str):
        assert cpu == gpu, "GPU and CPU string values are different at {}".format(path)
    elif isinstance(cpu, datetime):
        assert cpu == gpu, "GPU and CPU timestamp values are different at {}".format(path)
    elif isinstance(cpu, date):
        assert cpu == gpu, "GPU and CPU date values are different at {}".format(path)
    elif isinstance(cpu, bool):
        assert cpu == gpu, "GPU and CPU boolean values are different at {}".format(path)
    elif isinstance(cpu, Decimal):
        assert cpu == gpu, "GPU and CPU decimal values are different at {}".format(path)
    elif (cpu == None):
        assert cpu == gpu, "GPU and CPU are not both null at {}".format(path)
    else:
        assert False, "Found unexpected type {} at {}".format(t, path)

def assert_equal(cpu, gpu):
    """Verify that the result from the CPU and the GPU are equal"""
    _assert_equal(cpu, gpu, float_check=get_float_check(), path=[])

def _has_incompat_conf(conf):
    return ('spark.rapids.sql.incompatibleOps.enabled' in conf and
            conf['spark.rapids.sql.incompatibleOps.enabled'].lower() == 'true')

class _RowCmp(object):
    """Allows for sorting Rows in a consistent way"""
    def __init__(self, wrapped):
        #TODO will need others for maps, etc
        if isinstance(wrapped, Row):
            self.wrapped = [_RowCmp(c) for c in wrapped]
        else:
            self.wrapped = wrapped

        if isinstance(wrapped, float):
            self.is_nan = math.isnan(wrapped)
        else:
            self.is_nan = False

    def cmp(self, other):
        try:
            #None comes before anything else
            #NaN comes next
            if (self.wrapped is None and other.wrapped is None):
                return 0
            elif (self.wrapped is None):
                return -1
            elif (other.wrapped is None):
                return 1
            elif self.is_nan and other.is_nan:
                return 0
            elif self.is_nan:
                return -1
            elif other.is_nan:
                return 1
            elif self.wrapped == other.wrapped:
                return 0
            elif self.wrapped < other.wrapped:
                return -1
            else:
                return 1
        except TypeError as te:
            print("ERROR TRYING TO COMPARE {} to {} {}".format(self.wrapped, other.wrapped, te))
            raise te


    def __lt__(self, other):
        return self.cmp(other) < 0

    def __gt__(self, other):
        return self.cmp(other) > 0
           
    def __eq__(self, other):
        return self.cmp(other) == 0

    def __le__(self, other):
        return self.cmp(other) <= 0
 
    def __ge__(self, other):
        return self.cmp(other) >= 0

    def __ne__(self, other):
        return self.cmp(other) != 0

def _prep_func_for_compare(func, should_collect):
    sort_locally = should_sort_locally()
    if should_sort_on_spark():
        def with_sorted(spark):
            df = func(spark)
            return df.sort(df.columns)

        sorted_func = with_sorted
    else:
        sorted_func = func

    limit_val = get_limit()
    if limit_val > 0:
        def with_limit(spark):
            df = sorted_func(spark)
            return df.limit(limit_val)
        limit_func = with_limit
    else:
        limit_func = sorted_func

    if should_collect:
        bring_back = lambda spark: limit_func(spark).collect()
        collect_type = 'COLLECT'
    else:
        bring_back = lambda spark: limit_func(spark).toLocalIterator()
        collect_type = 'ITERATOR'
        if sort_locally:
            raise RuntimeError('Local Sort is only supported on a collect')
    return (bring_back, collect_type)

def _prep_incompat_conf(conf):
    if is_incompat():
        conf = dict(conf) # Make a copy before we change anything
        conf['spark.rapids.sql.incompatibleOps.enabled'] = 'true'
    elif _has_incompat_conf(conf):
        raise AssertionError("incompat must be enabled by the incompat fixture")
    return conf

def _assert_gpu_and_cpu_writes_are_equal(
        write_func,
        read_func,
        base_path,
        should_collect,
        conf={}):
    conf = _prep_incompat_conf(conf)

    print('### CPU RUN ###')
    cpu_start = time.time()
    cpu_path = base_path + '/CPU'
    with_cpu_session(lambda spark : write_func(spark, cpu_path), conf=conf)
    cpu_end = time.time()
    print('### GPU RUN ###')
    gpu_start = time.time()
    gpu_path = base_path + '/GPU'
    with_gpu_session(lambda spark : write_func(spark, gpu_path), conf=conf)
    gpu_end = time.time()
    print('### WRITE: GPU TOOK {} CPU TOOK {} ###'.format( 
        gpu_end - gpu_start, cpu_end - cpu_start))

    (cpu_bring_back, cpu_collect_type) = _prep_func_for_compare(
            lambda spark: read_func(spark, cpu_path), should_collect)
    (gpu_bring_back, gpu_collect_type) = _prep_func_for_compare(
            lambda spark: read_func(spark, gpu_path), should_collect)

    from_cpu = with_cpu_session(cpu_bring_back, conf=conf)
    from_gpu = with_cpu_session(gpu_bring_back, conf=conf)
    if should_sort_locally():
        from_cpu.sort(key=_RowCmp)
        from_gpu.sort(key=_RowCmp)

    assert_equal(from_cpu, from_gpu)

def assert_gpu_and_cpu_writes_are_equal_collect(write_func, read_func, base_path, conf={}):
    """
    Assert when running write_func on both the CPU and the GPU and reading using read_func
    ont he CPU that the results are equal.
    In this case the data is collected back to the driver and compared here, so be
    careful about the amount of data returned.
    """
    _assert_gpu_and_cpu_writes_are_equal(write_func, read_func, base_path, True, conf=conf)

def assert_gpu_and_cpu_writes_are_equal_iterator(write_func, read_func, base_path, conf={}):
    """
    Assert when running write_func on both the CPU and the GPU and reading using read_func
    ont he CPU that the results are equal.
    In this case the data is pulled back to the driver in chunks and compared here
    so any amount of data can work, just be careful about how long it might take.
    """
    _assert_gpu_and_cpu_writes_are_equal(write_func, read_func, base_path, False, conf=conf)

def assert_gpu_fallback_write(write_func,
        read_func,
        base_path,
        cpu_fallback_class_name,
        conf={}):
    conf = _prep_incompat_conf(conf)

    print('### CPU RUN ###')
    cpu_start = time.time()
    cpu_path = base_path + '/CPU'
    with_cpu_session(lambda spark : write_func(spark, cpu_path), conf=conf)
    cpu_end = time.time()
    print('### GPU RUN ###')
    jvm = spark_jvm()
    jvm.com.nvidia.spark.rapids.ExecutionPlanCaptureCallback.startCapture()
    gpu_start = time.time()
    gpu_path = base_path + '/GPU'
    with_gpu_session(lambda spark : write_func(spark, gpu_path), conf=conf)
    gpu_end = time.time()
    jvm.com.nvidia.spark.rapids.ExecutionPlanCaptureCallback.assertCapturedAndGpuFellBack(cpu_fallback_class_name, 2000)
    print('### WRITE: GPU TOOK {} CPU TOOK {} ###'.format(
        gpu_end - gpu_start, cpu_end - cpu_start))

    (cpu_bring_back, cpu_collect_type) = _prep_func_for_compare(
            lambda spark: read_func(spark, cpu_path), True)
    (gpu_bring_back, gpu_collect_type) = _prep_func_for_compare(
            lambda spark: read_func(spark, gpu_path), True)

    from_cpu = with_cpu_session(cpu_bring_back, conf=conf)
    from_gpu = with_cpu_session(gpu_bring_back, conf=conf)
    if should_sort_locally():
        from_cpu.sort(key=_RowCmp)
        from_gpu.sort(key=_RowCmp)

    assert_equal(from_cpu, from_gpu)

def assert_gpu_fallback_collect(func,
        cpu_fallback_class_name,
        conf={}):
    (bring_back, collect_type) = _prep_func_for_compare(func, True)
    conf = _prep_incompat_conf(conf)

    print('### CPU RUN ###')
    cpu_start = time.time()
    from_cpu = with_cpu_session(bring_back, conf=conf)
    cpu_end = time.time()
    print('### GPU RUN ###')
    jvm = spark_jvm()
    jvm.com.nvidia.spark.rapids.ExecutionPlanCaptureCallback.startCapture()
    gpu_start = time.time()
    from_gpu = with_gpu_session(bring_back,
            conf=conf)
    gpu_end = time.time()
    jvm.com.nvidia.spark.rapids.ExecutionPlanCaptureCallback.assertCapturedAndGpuFellBack(cpu_fallback_class_name, 2000)
    print('### {}: GPU TOOK {} CPU TOOK {} ###'.format(collect_type, 
        gpu_end - gpu_start, cpu_end - cpu_start))
    if should_sort_locally():
        from_cpu.sort(key=_RowCmp)
        from_gpu.sort(key=_RowCmp)

    assert_equal(from_cpu, from_gpu)

def _assert_gpu_and_cpu_are_equal(func,
        should_collect,
        conf={}):
    (bring_back, collect_type) = _prep_func_for_compare(func, should_collect)
    conf = _prep_incompat_conf(conf)

    print('### CPU RUN ###')
    cpu_start = time.time()
    from_cpu = with_cpu_session(bring_back, conf=conf)
    cpu_end = time.time()
    print('### GPU RUN ###')
    gpu_start = time.time()
    from_gpu = with_gpu_session(bring_back,
            conf=conf)
    gpu_end = time.time()
    print('### {}: GPU TOOK {} CPU TOOK {} ###'.format(collect_type, 
        gpu_end - gpu_start, cpu_end - cpu_start))
    if should_sort_locally():
        from_cpu.sort(key=_RowCmp)
        from_gpu.sort(key=_RowCmp)

    assert_equal(from_cpu, from_gpu)

def assert_gpu_and_cpu_are_equal_collect(func, conf={}):
    """
    Assert when running func on both the CPU and the GPU that the results are equal.
    In this case the data is collected back to the driver and compared here, so be
    careful about the amount of data returned.
    """
    _assert_gpu_and_cpu_are_equal(func, True, conf=conf)

def assert_gpu_and_cpu_are_equal_iterator(func, conf={}):
    """
    Assert when running func on both the CPU and the GPU that the results are equal.
    In this case the data is pulled back to the driver in chunks and compared here
    so any amount of data can work, just be careful about how long it might take.
    """
    _assert_gpu_and_cpu_are_equal(func, False, conf=conf)


def assert_gpu_and_cpu_are_equal_sql(df_fun, table_name, sql, conf=None):
    """
    Assert that the specified SQL query produces equal results on CPU and GPU.
    :param df_fun: a function that will create the dataframe
    :param table_name: Name of table to be created with the dataframe
    :param sql: SQL query to be run on the specified table
    :param conf: Any user-specified confs. Empty by default.
    :return: Assertion failure, if results from CPU and GPU do not match.
    """
    if conf is None:
        conf = {}
    def do_it_all(spark):
        df = df_fun(spark)
        df.createOrReplaceTempView(table_name)
        return spark.sql(sql)
    assert_gpu_and_cpu_are_equal_collect(do_it_all, conf)

def assert_py4j_exception(func, error_message):
    """
    Assert that a specific Java exception is thrown
    :param func: a function to be verified
    :param error_message: a string such as the one produce by java.lang.Exception.toString
    :return: Assertion failure if no exception matching error_message has occurred.
    """
    with pytest.raises(Py4JJavaError) as py4jError:
        func()
    assert error_message in str(py4jError.value.java_exception)

def assert_gpu_and_cpu_error(df_fun, conf, error_message):
    """
    Assert that GPU and CPU execution results in a specific Java exception thrown
    :param df_fun: a function to be verified
    :param conf: Spark config
    :param error_message: a string such as the one produce by java.lang.Exception.toString
    :return: Assertion failure if either GPU or CPU versions has not generated error messages
             expected
    """
    assert_py4j_exception(lambda: with_cpu_session(df_fun, conf), error_message)
    assert_py4j_exception(lambda: with_gpu_session(df_fun, conf), error_message)
