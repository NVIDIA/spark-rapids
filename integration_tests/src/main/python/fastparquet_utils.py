# Copyright (c) 2023, NVIDIA CORPORATION.
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

from pyspark.sql import Row

def get_fastparquet_result_converter():
    # get a converter which converts fastparquet result to Spark cpu type result
    # the convert is used to convet flatted Row to nested Row
    # e.g.:
    # Row(a.b1 = 1, a.b2 = 2) => Row(a = Row(b1 = 1, b2 = 2))
    return _convert_fastparquet_result

def _convert_fastparquet_result(fastparquet_cpu, gpu):
    """
    The result of fastparquet is different from the result of GPU, e.g.:
        --- CPU OUTPUT
        +++ GPU OUTPUT
        @@ -1 +1 @@
        -Row(a.first=-341142443, a.second=3.333994866005594e-37)
        +Row(a=Row(first=-341142443, second=3.333994866005594e-37))
    This method is used to canonicalize the fastparquet result
    :param fastparquet_cpu: the fastparquet result, it's a Spark Row list,
    :param gpu: the gpu result, it's a Spark Row list
    :return: (converted cpu, gpu) tuple
    """
    assert(len(fastparquet_cpu) == len(gpu))
    new_cpu = convert_fastparquet_result(fastparquet_cpu, gpu)
    return (new_cpu, gpu)

def convert_fastparquet_result(cpu, gpu):
    return [_convert_fastparquet_row(cpu_row, gpu_row) for (cpu_row, gpu_row) in zip(cpu, gpu)]

def _convert_fastparquet_row(cpu_row, gpu_row):
    cpu_dict = cpu_row.asDict()
    _merge_cpu_dict(cpu_dict, gpu_row.asDict())
    return Row(**cpu_dict)

def _merge_cpu_dict(cpu_dict, gpu_dict, cpu_prefix=""):
    # recursively merge items to sub Row, from bottom to top.
    # e.g.:
    # step0: dict(a.b.c1 = 1, a.b.c2 = 2, a.b2 = 3)
    # step1: dict(a.b = Row(c1 = 1, c2 = 2), a.b2 = 3)
    # step2: dict(a = Row(b = Row(c1 = 1, c2 = 2), b2 = 3))
    for gpu_k, gpu_v in gpu_dict.items():
        if isinstance(gpu_v, Row):
            cpu_prefix = cpu_prefix + gpu_k + "."
            _merge_cpu_dict(cpu_dict, gpu_v.asDict(), cpu_prefix)
            # reduce the dict for the `cpu_prefix`
            _reduce_cpu_dict(cpu_dict, cpu_prefix)

def _reduce_cpu_dict(cpu_dict, cpu_prefix):
    """
    Merge the items start with `cpu_prefix` in `cpu_dict` to a Spark Row.
    e.g.:
      cpu_dict = {"l1.l2.l3.key1": 1, "l1.l2.l3.key2": 2}, cpu_prefix = "l1.l2.l3."
      ==>>
      cpu_dict = {"l1.l2.l3": Row("key1": 1, "key2": 2)}
    """
    stripped_dict = {}
    for_removes = set()
    for cpu_k, cpu_v in cpu_dict.items():
        if cpu_k.startswith(cpu_prefix):
            stripped_key = cpu_k[len(cpu_prefix):]
            stripped_dict[stripped_key] = cpu_v
            for_removes.add(cpu_k)
    # remove the items start with prefix
    for for_remove_key in for_removes:
        cpu_dict.pop(for_remove_key)
    row = Row(**stripped_dict)
    # trim the end dot, e.g.:  "l1.l2.l3." => "l1.l2.l3"
    prefix_key = cpu_prefix[0 : len(cpu_prefix)-1]
    # set the merge row
    cpu_dict[prefix_key] = row


def _test_convert_fastparquet_result_no_null():
    """
    verify the method `_convert_fastparquet_result`
    """
    cpu_dict = {'v1_1' : 11, 'v1_2.v2_1' : 21, 'v1_2.v2_2.v3_1' : 31, 'v1_2.v2_2.v3_2' : 32}
    cpu_row = Row(**cpu_dict)
    gpu_row = Row(
                v1_1 = 11,
                v1_2 = Row(
                    v2_1 = 21,
                    v2_2 = Row(
                        v3_1 = 31,
                        v3_2 = 32)))

    cpu = [cpu_row]
    gpu = [gpu_row]
    print()
    print("before:")
    print(cpu, gpu)
    (cpu, gpu) = _convert_fastparquet_result(cpu, gpu)
    print()
    print("after:")
    print(cpu, gpu)
    assert cpu == gpu

def _test_convert_fastparquet_result_with_null():
    """
    verify the method `_convert_fastparquet_result`
    """
    cpu_dict = {'v1_1' : None, 'v1_2.v2_1' : None, 'v1_2.v2_2.v3_1' : None, 'v1_2.v2_2.v3_2' : 32}
    cpu_row = Row(**cpu_dict)
    gpu_row = Row(
                v1_1 = None,
                v1_2 = Row(
                    v2_1 = None,
                    v2_2 = Row(
                        v3_1 = None,
                        v3_2 = 32)))

    cpu = [cpu_row]
    gpu = [gpu_row]
    print()
    print("before:")
    print(cpu, gpu)
    (cpu, gpu) = _convert_fastparquet_result(cpu, gpu)
    print()
    print("after:")
    print(cpu, gpu)
    assert cpu == gpu

if __name__ == "__main__":
    _test_convert_fastparquet_result_no_null()
    _test_convert_fastparquet_result_with_null()
