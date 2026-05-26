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

import inspect
import os

import pytest

from asserts import assert_gpu_fallback_collect
from marks import allow_non_gpu
from spark_session import is_before_spark_340, with_cpu_session
import pyspark.sql.functions as f

if os.environ.get('INCLUDE_SPARK_PROTOBUF_JAR', 'true').lower() == 'false':
    pytestmark = pytest.mark.skip(reason="INCLUDE_SPARK_PROTOBUF_JAR is disabled")
else:
    pytestmark = pytest.mark.skipif(
        is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")


def _try_import_from_protobuf():
    try:
        from pyspark.sql.protobuf.functions import from_protobuf
        return from_protobuf
    except Exception:
        return None


@pytest.fixture(scope="module")
def from_protobuf_fn():
    fn = _try_import_from_protobuf()
    if fn is None:
        pytest.skip("from_protobuf not available")
    return fn


def _encode_varint(value):
    out = bytearray()
    value &= 0xFFFFFFFFFFFFFFFF
    while True:
        bits = value & 0x7F
        value >>= 7
        if value:
            out.append(bits | 0x80)
        else:
            out.append(bits)
            return bytes(out)


def _encode_simple_message(i32_value, s_value):
    buf = bytearray()
    buf += _encode_varint((1 << 3) | 0)  # field 1, VARINT
    buf += _encode_varint(i32_value)
    s_bytes = s_value.encode("utf-8")
    buf += _encode_varint((2 << 3) | 2)  # field 2, LENGTH-DELIMITED
    buf += _encode_varint(len(s_bytes))
    buf += s_bytes
    return bytes(buf)


def _build_simple_descriptor_bytes(spark):
    D = spark.sparkContext._jvm.com.google.protobuf.DescriptorProtos
    i32_field = D.FieldDescriptorProto.newBuilder() \
        .setName("i32").setNumber(1) \
        .setLabel(D.FieldDescriptorProto.Label.LABEL_OPTIONAL) \
        .setType(D.FieldDescriptorProto.Type.TYPE_INT32).build()
    s_field = D.FieldDescriptorProto.newBuilder() \
        .setName("s").setNumber(2) \
        .setLabel(D.FieldDescriptorProto.Label.LABEL_OPTIONAL) \
        .setType(D.FieldDescriptorProto.Type.TYPE_STRING).build()
    msg = D.DescriptorProto.newBuilder() \
        .setName("Simple").addField(i32_field).addField(s_field).build()
    file_builder = D.FileDescriptorProto.newBuilder() \
        .setName("simple.proto").setPackage("test").addMessageType(msg) \
        .setSyntax("proto2")
    fds = D.FileDescriptorSet.newBuilder().addFile(file_builder.build()).build()
    return bytes(fds.toByteArray())


def _write_bytes_to_hadoop_path(spark, path_str, data_bytes):
    sc = spark.sparkContext
    config = sc._jsc.hadoopConfiguration()
    jpath = sc._jvm.org.apache.hadoop.fs.Path(path_str)
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(config)
    out = fs.create(jpath, True)
    try:
        out.write(bytearray(data_bytes))
    finally:
        out.close()


def _setup_simple_desc(spark_tmp_path):
    desc_path = spark_tmp_path + "/simple.desc"
    desc_bytes = with_cpu_session(_build_simple_descriptor_bytes)
    with_cpu_session(
        lambda spark: _write_bytes_to_hadoop_path(spark, desc_path, desc_bytes))
    return desc_path, desc_bytes


_smoke_rows = [(1, "a"), (-2, "bb"), (0, ""), (12345, "hello")]


def _make_smoke_df(spark):
    encoded = [(_encode_simple_message(i, s),) for (i, s) in _smoke_rows]
    return spark.createDataFrame(encoded, ["bin"])


@allow_non_gpu("ProjectExec", "ProtobufDataToCatalyst")
def test_from_protobuf_smoke_path_api(spark_tmp_path, from_protobuf_fn):
    desc_path, _ = _setup_simple_desc(spark_tmp_path)

    def run(spark):
        return _make_smoke_df(spark).select(
            from_protobuf_fn(f.col("bin"), "test.Simple", desc_path).alias("d"))

    assert_gpu_fallback_collect(run, "ProtobufDataToCatalyst")


@allow_non_gpu("ProjectExec", "ProtobufDataToCatalyst")
def test_from_protobuf_smoke_binary_descriptor_api(spark_tmp_path, from_protobuf_fn):
    if "binaryDescriptorSet" not in inspect.signature(from_protobuf_fn).parameters:
        pytest.skip("binaryDescriptorSet kwarg is Spark 3.5+ only")
    _, desc_bytes = _setup_simple_desc(spark_tmp_path)

    def run(spark):
        return _make_smoke_df(spark).select(
            from_protobuf_fn(f.col("bin"), "test.Simple",
                             binaryDescriptorSet=bytearray(desc_bytes)).alias("d"))

    assert_gpu_fallback_collect(run, "ProtobufDataToCatalyst")
