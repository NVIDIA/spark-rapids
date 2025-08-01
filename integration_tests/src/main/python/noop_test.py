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

from asserts import *
from data_gen import *
from marks import incompat, ignore_order
from spark_session import *


def test_noop_format_write():
    """Test for noop format write support - should work on GPU after our changes"""
    def test_noop_write(spark):
        data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
        columns = ["Name", "Age"]
        df = spark.createDataFrame(data, columns)
        # This should work on GPU after our implementation
        df.write.format("noop").mode("overwrite").save()
        return True
    
    # Test that it runs on GPU without falling back
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: test_noop_write(spark), 
        conf={"spark.rapids.sql.enabled": "true"}
    )


def test_noop_format_different_modes():
    """Test noop format with different write modes"""
    def test_modes(spark):
        data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
        columns = ["Name", "Age"]
        df = spark.createDataFrame(data, columns)
        
        # Test different write modes - noop should handle all of them
        df.write.format("noop").mode("append").save()
        df.write.format("noop").mode("overwrite").save()
        df.write.format("noop").mode("ignore").save()
        return True
    
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: test_modes(spark),
        conf={"spark.rapids.sql.enabled": "true"}
    )


def test_noop_format_complex_data():
    """Test noop format with complex data types"""
    data_gen = StructGen([
        ('id', int_gen),
        ('name', string_gen),
        ('scores', ArrayGen(int_gen)),
        ('metadata', MapGen(StringGen(pattern='key_[0-9]', nullable=False), int_gen))
    ], nullable=False)
    
    def write_noop(spark):
        df = gen_df(spark, data_gen)
        df.write.format("noop").mode("overwrite").save()
        return True
    
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: write_noop(spark),
        conf={"spark.rapids.sql.enabled": "true"}
    )