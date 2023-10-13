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

def get_fastparquet_result_canonicalizer():
    # Get a converter like fastparquet that flattens the GPU results.
    # e.g.:
    # Row(a = Row(b1 = 1, b2 = 2)) => Row(a.b1 = 1, a.b2 = 2)
    return _convert_fastparquet_result

def _convert_fastparquet_result(fastparquet_cpu, gpu):
    """
    The result of fastparquet is different from the result of GPU, e.g.:
        --- CPU OUTPUT
        +++ GPU OUTPUT
        @@ -1 +1 @@
        -Row(a.first=-341142443, a.second=3.333994866005594e-37)    // fastparquet
        +Row(a=Row(first=-341142443, second=3.333994866005594e-37)) // gpu
    This method is used to canonicalize gpu result like fastparquet does
    :param fastparquet_cpu: the fastparquet result, it's a Spark Row list with nested Rows
    :param gpu: the gpu result, it's a Spark Row list
    :return: (fastparquet, converted_gpu) tuple
    """
    new_gpu = [_convert_gpu_row(gpu_row) for gpu_row in gpu]
    return (fastparquet_cpu, new_gpu)

def _convert_gpu_row(gpu_row):
    converted_dict = _get_converted_dict(gpu_row)
    return Row(**converted_dict)

def _get_converted_dict(gpu_row):
    # recursively flatten Row, from bottom to top.
    # e.g.:
    # step0:
    #   Row(
    #       a = Row(
    #           b = Row(
    #               c1 = 1,
    #               c2 = 2),
    #           b2 = 3))
    # step1:
    #   dict(
    #       a = Row(
    #           b.c1 = 1
    #           b.c2 = 2,
    #           b2 = 3))
    # step2:
    #   dict(
    #       a.b.c1 = 1
    #       a.b.c2 = 2,
    #       a.b2 = 3)
    gpu_dict = gpu_row.asDict()
    updated_dict = {}
    for gpu_k, gpu_v in gpu_dict.items():
        if isinstance(gpu_v, Row):
            d = _get_converted_dict(gpu_v)
            for k, v in d.items():
                # flatten and save to dict
                updated_dict[gpu_k + '.' + k] = v
        else:
            updated_dict[gpu_k] = gpu_v
    return updated_dict

def _test_convert_gpu_row():
    """
    verify the method `_convert_fastparquet_result`
    """
    gpu_row = Row(
            v1_1 = 11,
            v1_2 = Row(
                v2_1 = 21,
                v2_2 = Row(
                    v3_1 = 31,
                    v3_2 = 32)))
    expected_dict = {'v1_1' : 11, 'v1_2.v2_1' : 21, 'v1_2.v2_2.v3_1' : 31, 'v1_2.v2_2.v3_2' : 32}
    expected_row = Row(**expected_dict)
    print()
    print("before:")
    print("gpu row: " + str(gpu_row))
    converted_gpu_row = _convert_gpu_row(gpu_row)
    print()
    print("after:")
    print("converted gpu row: " + str(converted_gpu_row))
    print("expected: " + str(expected_row))
    assert converted_gpu_row == expected_row

def _test_convert_gpu_row_with_null():
    """
    verify the method `_convert_fastparquet_result`
    """
    gpu_row = Row(
            v1_1 = None,
            v1_2 = Row(
                v2_1 = None,
                v2_2 = Row(
                    v3_1 = None,
                    v3_2 = 32)))
    expected_dict = {'v1_1' : None, 'v1_2.v2_1' : None, 'v1_2.v2_2.v3_1' : None, 'v1_2.v2_2.v3_2' : 32}
    expected_row = Row(**expected_dict)
    print()
    print("before:")
    print("gpu row: " + str(gpu_row))
    converted_gpu_row = _convert_gpu_row(gpu_row)
    print()
    print("after:")
    print("converted gpu row: " + str(converted_gpu_row))
    print("expected: " + str(expected_row))
    assert converted_gpu_row == expected_row

if __name__ == "__main__":
    _test_convert_gpu_row()
    _test_convert_gpu_row_with_null()
