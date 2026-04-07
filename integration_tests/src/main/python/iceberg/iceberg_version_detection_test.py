# Copyright (c) 2026, NVIDIA CORPORATION.
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
import pytest

from marks import allow_non_gpu, iceberg
from spark_session import is_spark_35x, with_gpu_session
from spark_init_internal import spark_version

pytestmark = pytest.mark.skipif(not is_spark_35x(),
                                reason="Iceberg support only for Spark 3.5.x")

@allow_non_gpu(any=True)
@iceberg
def test_iceberg_version_detection():
    expected = os.environ.get("EXPECTED_ICEBERG_VERSION")
    if expected is None:
        pytest.skip("EXPECTED_ICEBERG_VERSION env var not set")

    def check(spark):
        jvm = spark.sparkContext._jvm
        actual = jvm.com.nvidia.spark.rapids.iceberg.IcebergProvider.detectedVersion()
        assert actual == expected, \
            "Iceberg version detection mismatch: expected '{}' on Spark {}, got '{}'".format(
                expected, spark_version(), actual)

    with_gpu_session(check)
