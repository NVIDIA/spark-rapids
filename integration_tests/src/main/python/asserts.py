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

import math
from pyspark.sql import Row
from spark_session import with_cpu_session, with_gpu_session
import time
import types as pytypes

def assert_equal(cpu, gpu, path=[]):
    t = type(cpu)
    if (t is Row):
        assert len(cpu) == len(gpu), "CPU and GPU row have different lengths at {}".format(path)
        if hasattr(cpu, "__fields__") and hasattr(gpu, "__fields__"):
            for field in cpu.__fields__:
                assert_equal(cpu[field], gpu[field], path + [field])
        else:
            for index in range(len(cpu)):
                assert_equal(cpu[index], gpu[index], path + [index])
    elif (t is list):
        assert len(cpu) == len(gpu), "CPU and GPU list have different lengths at {}".format(path)
        for index in range(len(cpu)):
            assert_equal(cpu[index], gpu[index], path + [index])
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
                assert_equal(sub_cpu, sub_gpu, path + [index])

            index = index + 1
    elif (t is int):
        assert cpu == gpu, "GPU and CPU int values are different at {}".format(path)
    elif (t is float):
        if (math.isnan(cpu)):
            assert math.isnan(gpu), "GPU and CPU float values are different at {}".format(path)
        else:
            assert cpu == gpu, "GPU and CPU float values are different at {}".format(path)
    elif (cpu == None):
        assert cpu == gpu, "GPU and CPU are not both null at {}".format(path)
    else:
        assert False, "Found unexpected type {} at {}".format(t, path)


def assert_gpu_and_cpu_are_equal_collect(func,
        conf={},
        sort_result=False,
        non_gpu_allowed=None):
    #TODO sort first if needed
    with_collect = lambda spark: func(spark).collect()

    print('### CPU RUN ###')
    cpu_start = time.time()
    from_cpu = with_cpu_session(with_collect,
            conf=conf)
    cpu_end = time.time()
    print('### GPU RUN ###')
    gpu_start = time.time()
    from_gpu = with_gpu_session(with_collect,
            conf=conf,
            non_gpu_allowed=non_gpu_allowed)
    gpu_end = time.time()
    print('### COLLECT: GPU TOOK {} CPU TOOK {} ###'.format(gpu_end - gpu_start, cpu_end - cpu_start))
    assert_equal(from_cpu, from_gpu)

def assert_gpu_and_cpu_are_equal_iterator(func,
        conf={},
        sort_result=False,
        non_gpu_allowed=None):
    #TODO sort first if needed
    with_iterator = lambda spark: func(spark).toLocalIterator()

    print('### CPU RUN ###')
    cpu_start = time.time()
    from_cpu = with_cpu_session(with_iterator,
            conf=conf)
    cpu_end = time.time()
    print('### GPU RUN ###')
    gpu_start = time.time()
    from_gpu = with_gpu_session(with_iterator,
            conf=conf,
            non_gpu_allowed=non_gpu_allowed)
    gpu_end = time.time()
    print('### ITERATOR: GPU TOOK {} CPU TOOK {} ###'.format(gpu_end - gpu_start, cpu_end - cpu_start))

    assert_equal(from_cpu, from_gpu)
