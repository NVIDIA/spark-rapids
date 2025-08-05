# Copyright (c) 2025, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from spark_session import is_before_spark_400


# The following tests are designed to verify the Spark change
# at [SPARK-45905](https://github.com/apache/spark/commit/7120e6b88f2)


@pytest.mark.skipif(is_before_spark_400(), reason="https://issues.apache.org/jira/browse/SPARK-45905")
def test_decimal_precision_over_max():
    # According to the decimal type computation in Spark (by widerDecimalType), the result
    # decimal type will be: DecimalType(38, 6) = widerDecimalType(38, 8, 38, 6), not (38, 8)
    # anymore.
    # And the temp. precision passed into "boundedPreferIntegralDigits" is "40"(= 32 + 8),
    # which is larger than MAX_PRECISION (=38). This is just the case we want to test.
    #
    #   def widerDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    #     val scale = max(s1, s2)
    #     val range = max(p1 - s1, p2 - s2)
    #     boundedPreferIntegralDigits(scale + range, scale)
    #   }
    #
    #   def boundedPreferIntegralDigits(precision: Int, scale: Int): DecimalType = {
    #     if (precision <= MAX_PRECISION) {
    #       DecimalType(precision, scale)
    #     } else {
    #       val diff = precision - MAX_PRECISION
    #       DecimalType(MAX_PRECISION, math.max(0, scale - diff))
    #     }
    #   }
    dec_gen = DecimalGen(38, 8, full_precision=True)
    dec2_gen = DecimalGen(38, 6, full_precision=True)
    expected_dec_type = DecimalType(38, 6)

    def test_fn(spark):
        # We can not cover all the operators, but some mentioned in that Spark PR comments.
        df = two_col_df(spark, dec_gen, dec2_gen, length=100).selectExpr(
            "array(a, b, null, 100)",
            "coalesce(a, b, 100)",
            "coalesce(b, a, null, 100)",
            "a > b",
            "a <= b")
        assert df.schema[0].dataType.elementType == expected_dec_type
        assert df.schema[1].dataType == expected_dec_type
        assert df.schema[2].dataType == expected_dec_type
        # the last two are boolean columns
        return df

    assert_gpu_and_cpu_are_equal_collect(test_fn)
