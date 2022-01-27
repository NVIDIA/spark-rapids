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

import pytest

allow_non_gpu_databricks = pytest.mark.allow_non_gpu_databricks
allow_non_gpu = pytest.mark.allow_non_gpu
validate_execs_in_gpu_plan = pytest.mark.validate_execs_in_gpu_plan
approximate_float = pytest.mark.approximate_float
ignore_order = pytest.mark.ignore_order
incompat = pytest.mark.incompat
limit = pytest.mark.limit
qarun = pytest.mark.qarun
cudf_udf = pytest.mark.cudf_udf
rapids_udf_example_native = pytest.mark.rapids_udf_example_native
shuffle_test = pytest.mark.shuffle_test
nightly_gpu_mem_consuming_case = pytest.mark.nightly_gpu_mem_consuming_case
nightly_host_mem_consuming_case = pytest.mark.nightly_host_mem_consuming_case
