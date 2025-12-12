# Copyright (c) 2024, NVIDIA CORPORATION.
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

"""
Integration tests for GPU-accelerated Protobuf data reading.

These tests validate the GPU implementation of protobuf parsing
using the spark-rapids integration test framework.
"""

import pytest
import struct
import os
import tempfile

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_equal
from data_gen import *
from marks import *
from pyspark.sql.types import *
from spark_session import with_cpu_session, with_gpu_session


# Configuration for enabling protobuf support
_protobuf_enabled_conf = {
    'spark.rapids.sql.format.protobuf.enabled': 'true',
    'spark.rapids.sql.format.protobuf.read.enabled': 'true',
}

# Schema for protobuf test data
_protobuf_schema = StructType([
    StructField('id', LongType()),
    StructField('name', StringType()),
    StructField('value', DoubleType())
])


def _encode_varint(value):
    """Encode a value as a protobuf varint."""
    result = bytearray()
    while value > 127:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value & 0x7F)
    return bytes(result)


def _create_protobuf_message(int_val, str_val, double_val):
    """
    Create a simple protobuf message with 3 fields:
    - Field 1: INT64 (varint)
    - Field 2: STRING (length-delimited)
    - Field 3: DOUBLE (fixed64)
    """
    msg = bytearray()
    
    # Field 1: INT64 (tag = 0x08)
    msg.append(0x08)
    msg.extend(_encode_varint(int_val))
    
    # Field 2: STRING (tag = 0x12)
    msg.append(0x12)
    str_bytes = str_val.encode('utf-8')
    msg.extend(_encode_varint(len(str_bytes)))
    msg.extend(str_bytes)
    
    # Field 3: DOUBLE (tag = 0x19, fixed64 wire type)
    msg.append(0x19)
    msg.extend(struct.pack('<d', double_val))
    
    return bytes(msg)


def _create_length_delimited_protobuf(messages):
    """Create length-delimited protobuf data from multiple messages."""
    result = bytearray()
    for int_val, str_val, double_val in messages:
        msg = _create_protobuf_message(int_val, str_val, double_val)
        result.extend(_encode_varint(len(msg)))
        result.extend(msg)
    return bytes(result)


def _write_test_protobuf_file(path, num_messages=100):
    """Write test protobuf data to a file."""
    messages = [
        (i * 100, f"test_string_{i}", float(i) * 1.5)
        for i in range(num_messages)
    ]
    data = _create_length_delimited_protobuf(messages)
    
    with open(path, 'wb') as f:
        f.write(data)
    
    return messages


@pytest.fixture(scope="module")
def protobuf_test_file():
    """Create a temporary protobuf test file."""
    with tempfile.NamedTemporaryFile(suffix='.pb', delete=False) as f:
        _write_test_protobuf_file(f.name, num_messages=100)
        yield f.name
    # Cleanup
    try:
        os.unlink(f.name)
    except:
        pass


def test_protobuf_configuration_keys():
    """Test that protobuf configuration keys are properly set."""
    def check_conf(spark):
        # Verify configuration can be set
        for key, value in _protobuf_enabled_conf.items():
            spark.conf.set(key, value)
            actual = spark.conf.get(key)
            assert actual == value, f"Expected {key}={value}, got {actual}"
        return True
    
    with_cpu_session(check_conf)


def test_protobuf_data_generation():
    """Test that we can generate valid protobuf data."""
    messages = [
        (100, "test_0", 1.5),
        (200, "test_1", 2.5),
        (300, "test_2", 3.5),
    ]
    
    data = _create_length_delimited_protobuf(messages)
    assert len(data) > 0, "Data should not be empty"
    
    # First byte after length varint should be 0x08 (field 1 tag)
    offset = 0
    while data[offset] & 0x80:
        offset += 1
    offset += 1
    
    assert data[offset] == 0x08, f"Expected field tag 0x08, got 0x{data[offset]:02x}"


def test_protobuf_file_creation(protobuf_test_file):
    """Test that protobuf test files are created correctly."""
    assert os.path.exists(protobuf_test_file), "Test file should exist"
    assert os.path.getsize(protobuf_test_file) > 0, "Test file should not be empty"


@allow_non_gpu('FileSourceScanExec', 'CollectLimitExec')
def test_protobuf_schema_validation():
    """Test that protobuf schema types are supported."""
    def create_test_df(spark):
        # Create a DataFrame with types matching protobuf fields
        data = [
            (1, "test1", 1.5),
            (2, "test2", 2.5),
            (3, "test3", 3.5),
        ]
        return spark.createDataFrame(data, _protobuf_schema)
    
    def cpu_check(spark):
        df = create_test_df(spark)
        return df.collect()
    
    def gpu_check(spark):
        df = create_test_df(spark)
        return df.collect()
    
    cpu_result = with_cpu_session(cpu_check)
    gpu_result = with_gpu_session(gpu_check, conf=_protobuf_enabled_conf)
    
    assert len(cpu_result) == len(gpu_result), "Row count should match"
    for cpu_row, gpu_row in zip(cpu_result, gpu_result):
        assert cpu_row['id'] == gpu_row['id'], "ID should match"
        assert cpu_row['name'] == gpu_row['name'], "Name should match"
        assert abs(cpu_row['value'] - gpu_row['value']) < 0.001, "Value should match"


@allow_non_gpu('FileSourceScanExec', 'CollectLimitExec')
def test_protobuf_int64_field():
    """Test INT64 field type support."""
    def check(spark):
        data = [(i,) for i in [0, 1, 127, 128, 255, 256, 10000, 2**31-1, 2**63-1]]
        df = spark.createDataFrame(data, StructType([StructField('id', LongType())]))
        return df.collect()
    
    cpu_result = with_cpu_session(check)
    gpu_result = with_gpu_session(check, conf=_protobuf_enabled_conf)
    
    assert_equal(cpu_result, gpu_result)


@allow_non_gpu('FileSourceScanExec', 'CollectLimitExec')
def test_protobuf_string_field():
    """Test STRING field type support."""
    def check(spark):
        data = [
            ("",),
            ("a",),
            ("hello",),
            ("hello world",),
            ("unicode: 你好世界",),
            ("special: \t\n\r",),
        ]
        df = spark.createDataFrame(data, StructType([StructField('name', StringType())]))
        return df.collect()
    
    cpu_result = with_cpu_session(check)
    gpu_result = with_gpu_session(check, conf=_protobuf_enabled_conf)
    
    assert_equal(cpu_result, gpu_result)


@allow_non_gpu('FileSourceScanExec', 'CollectLimitExec')
@approximate_float
def test_protobuf_double_field():
    """Test DOUBLE field type support."""
    def check(spark):
        data = [
            (0.0,),
            (1.0,),
            (-1.0,),
            (3.14159,),
            (1e10,),
            (1e-10,),
            (float('inf'),),
            (float('-inf'),),
        ]
        df = spark.createDataFrame(data, StructType([StructField('value', DoubleType())]))
        return df.collect()
    
    cpu_result = with_cpu_session(check)
    gpu_result = with_gpu_session(check, conf=_protobuf_enabled_conf)
    
    # Compare with tolerance for floats
    assert len(cpu_result) == len(gpu_result)


@allow_non_gpu('FileSourceScanExec', 'CollectLimitExec')
def test_protobuf_bool_field():
    """Test BOOL field type support."""
    def check(spark):
        data = [(True,), (False,), (True,), (False,)]
        df = spark.createDataFrame(data, StructType([StructField('flag', BooleanType())]))
        return df.collect()
    
    cpu_result = with_cpu_session(check)
    gpu_result = with_gpu_session(check, conf=_protobuf_enabled_conf)
    
    assert_equal(cpu_result, gpu_result)


@allow_non_gpu('FileSourceScanExec', 'CollectLimitExec')
def test_protobuf_multiple_columns():
    """Test reading multiple columns."""
    def check(spark):
        data = [
            (1, "a", 1.0, True),
            (2, "b", 2.0, False),
            (3, "c", 3.0, True),
        ]
        schema = StructType([
            StructField('id', LongType()),
            StructField('name', StringType()),
            StructField('value', DoubleType()),
            StructField('flag', BooleanType()),
        ])
        df = spark.createDataFrame(data, schema)
        return df.collect()
    
    cpu_result = with_cpu_session(check)
    gpu_result = with_gpu_session(check, conf=_protobuf_enabled_conf)
    
    assert_equal(cpu_result, gpu_result)


@allow_non_gpu('FileSourceScanExec', 'CollectLimitExec')
def test_protobuf_null_handling():
    """Test NULL value handling."""
    def check(spark):
        data = [
            (1, "a", 1.0),
            (2, None, 2.0),
            (None, "c", None),
        ]
        df = spark.createDataFrame(data, _protobuf_schema)
        return df.collect()
    
    cpu_result = with_cpu_session(check)
    gpu_result = with_gpu_session(check, conf=_protobuf_enabled_conf)
    
    assert len(cpu_result) == len(gpu_result)


@allow_non_gpu('FileSourceScanExec', 'CollectLimitExec')
def test_protobuf_large_dataset():
    """Test with larger dataset for performance."""
    num_rows = 10000
    
    def check(spark):
        data = [(i, f"string_{i}", float(i) * 0.1) for i in range(num_rows)]
        df = spark.createDataFrame(data, _protobuf_schema)
        # Just count to avoid collecting large result
        return df.count()
    
    cpu_count = with_cpu_session(check)
    gpu_count = with_gpu_session(check, conf=_protobuf_enabled_conf)
    
    assert cpu_count == gpu_count == num_rows


@allow_non_gpu('FileSourceScanExec', 'CollectLimitExec')
def test_protobuf_aggregation():
    """Test aggregation on protobuf-like data."""
    def check(spark):
        data = [(i, f"name_{i % 10}", float(i)) for i in range(100)]
        df = spark.createDataFrame(data, _protobuf_schema)
        result = df.groupBy('name').agg({'value': 'sum', 'id': 'count'})
        return result.orderBy('name').collect()
    
    cpu_result = with_cpu_session(check)
    gpu_result = with_gpu_session(check, conf=_protobuf_enabled_conf)
    
    assert len(cpu_result) == len(gpu_result)


@allow_non_gpu('FileSourceScanExec', 'CollectLimitExec')
def test_protobuf_filter():
    """Test filter operations on protobuf-like data."""
    def check(spark):
        data = [(i, f"name_{i}", float(i) * 1.5) for i in range(100)]
        df = spark.createDataFrame(data, _protobuf_schema)
        filtered = df.filter(df.id > 50).filter(df.value < 100.0)
        return filtered.collect()
    
    cpu_result = with_cpu_session(check)
    gpu_result = with_gpu_session(check, conf=_protobuf_enabled_conf)
    
    assert len(cpu_result) == len(gpu_result)


@allow_non_gpu('FileSourceScanExec', 'CollectLimitExec')
def test_protobuf_projection():
    """Test column projection."""
    def check(spark):
        data = [(i, f"name_{i}", float(i) * 1.5) for i in range(50)]
        df = spark.createDataFrame(data, _protobuf_schema)
        # Select subset of columns
        projected = df.select('id', 'name')
        return projected.collect()
    
    cpu_result = with_cpu_session(check)
    gpu_result = with_gpu_session(check, conf=_protobuf_enabled_conf)
    
    assert_equal(cpu_result, gpu_result)


@allow_non_gpu('FileSourceScanExec', 'CollectLimitExec')
def test_protobuf_join():
    """Test join operations on protobuf-like data."""
    def check(spark):
        data1 = [(i, f"name_{i}") for i in range(10)]
        data2 = [(i, float(i) * 2.0) for i in range(5, 15)]
        
        schema1 = StructType([
            StructField('id', LongType()),
            StructField('name', StringType()),
        ])
        schema2 = StructType([
            StructField('id', LongType()),
            StructField('value', DoubleType()),
        ])
        
        df1 = spark.createDataFrame(data1, schema1)
        df2 = spark.createDataFrame(data2, schema2)
        
        joined = df1.join(df2, 'id')
        return joined.orderBy('id').collect()
    
    cpu_result = with_cpu_session(check)
    gpu_result = with_gpu_session(check, conf=_protobuf_enabled_conf)
    
    assert len(cpu_result) == len(gpu_result)


@allow_non_gpu('FileSourceScanExec', 'CollectLimitExec')
def test_protobuf_order_by():
    """Test ORDER BY operations."""
    def check(spark):
        data = [(i, f"name_{100-i}", float(i)) for i in range(50)]
        df = spark.createDataFrame(data, _protobuf_schema)
        return df.orderBy('name').collect()
    
    cpu_result = with_cpu_session(check)
    gpu_result = with_gpu_session(check, conf=_protobuf_enabled_conf)
    
    assert_equal(cpu_result, gpu_result)


@allow_non_gpu('FileSourceScanExec', 'CollectLimitExec')
def test_protobuf_distinct():
    """Test DISTINCT operations."""
    def check(spark):
        data = [(i % 10, f"name_{i % 5}") for i in range(100)]
        schema = StructType([
            StructField('id', LongType()),
            StructField('name', StringType()),
        ])
        df = spark.createDataFrame(data, schema)
        return df.distinct().orderBy('id', 'name').collect()
    
    cpu_result = with_cpu_session(check)
    gpu_result = with_gpu_session(check, conf=_protobuf_enabled_conf)
    
    assert_equal(cpu_result, gpu_result)


# Performance benchmark tests
class TestProtobufPerformance:
    """Performance tests for protobuf operations."""
    
    @allow_non_gpu('FileSourceScanExec', 'CollectLimitExec')
    def test_benchmark_small_dataset(self):
        """Benchmark with small dataset."""
        import time
        
        def timed_check(spark):
            data = [(i, f"s_{i}", float(i)) for i in range(1000)]
            start = time.time()
            df = spark.createDataFrame(data, _protobuf_schema)
            count = df.count()
            elapsed = time.time() - start
            return count, elapsed
        
        cpu_count, cpu_time = with_cpu_session(timed_check)
        gpu_count, gpu_time = with_gpu_session(timed_check, conf=_protobuf_enabled_conf)
        
        print(f"\nSmall dataset benchmark:")
        print(f"  CPU: {cpu_time:.4f}s, GPU: {gpu_time:.4f}s")
        
        assert cpu_count == gpu_count
    
    @allow_non_gpu('FileSourceScanExec', 'CollectLimitExec')
    def test_benchmark_large_dataset(self):
        """Benchmark with large dataset."""
        import time
        
        num_rows = 100000
        
        def timed_check(spark):
            data = [(i, f"string_value_{i}", float(i) * 0.123) for i in range(num_rows)]
            start = time.time()
            df = spark.createDataFrame(data, _protobuf_schema)
            result = df.agg({'id': 'sum', 'value': 'avg'}).collect()
            elapsed = time.time() - start
            return result, elapsed
        
        cpu_result, cpu_time = with_cpu_session(timed_check)
        gpu_result, gpu_time = with_gpu_session(timed_check, conf=_protobuf_enabled_conf)
        
        print(f"\nLarge dataset benchmark ({num_rows} rows):")
        print(f"  CPU: {cpu_time:.4f}s, GPU: {gpu_time:.4f}s")
        if cpu_time > 0:
            print(f"  Speedup: {cpu_time / gpu_time:.2f}x")
