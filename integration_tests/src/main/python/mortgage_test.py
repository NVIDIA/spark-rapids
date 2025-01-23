# Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_iterator
from marks import approximate_float, incompat, ignore_order, allow_non_gpu, limit
from spark_session import is_databricks143_or_later

@incompat
@approximate_float
@limit
@ignore_order
@allow_non_gpu(any=True)
@pytest.mark.xfail(is_databricks143_or_later(), reason='https://github.com/NVIDIA/spark-rapids/issues/8910')
def test_mortgage(mortgage):
  assert_gpu_and_cpu_are_equal_iterator(
          lambda spark : mortgage.do_test_query(spark))
