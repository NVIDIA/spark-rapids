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
import struct

import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import (
    BooleanGen, IntegerGen, LongGen, FloatGen, DoubleGen, StringGen, BinaryGen,
    ProtobufMessageGen, PbScalar, PbNested, PbRepeated, PbRepeatedMessage,
    encode_pb_message, gen_df, idfn
)
from marks import ignore_order
from spark_session import with_cpu_session, is_before_spark_340
import pyspark.sql.functions as f

pytestmark = [pytest.mark.premerge_ci_1]


# Random data generation configurations for simple scalars
_random_scalar_test_configs = [
    # (test_id, data_gen_config)
    ("all_types", [
        PbScalar("b", 1, BooleanGen()),
        PbScalar("i32", 2, IntegerGen()),
        PbScalar("i64", 3, LongGen()),
        PbScalar("f32", 4, FloatGen()),
        PbScalar("f64", 5, DoubleGen()),
        PbScalar("s", 6, StringGen()),
    ]),
    ("integers_edge_cases", [
        PbScalar("b", 1, BooleanGen()),
        PbScalar("i32", 2, IntegerGen(
            min_val=-2147483648, max_val=2147483647,
            special_cases=[-2147483648, -1, 0, 1, 2147483647])),
        PbScalar("i64", 3, LongGen(
            min_val=-9223372036854775808, max_val=9223372036854775807,
            special_cases=[-9223372036854775808, -1, 0, 1, 9223372036854775807])),
        PbScalar("f32", 4, FloatGen()),
        PbScalar("f64", 5, DoubleGen()),
        PbScalar("s", 6, StringGen()),
    ]),
    ("floats_edge_cases", [
        PbScalar("b", 1, BooleanGen()),
        PbScalar("i32", 2, IntegerGen()),
        PbScalar("i64", 3, LongGen()),
        PbScalar("f32", 4, FloatGen(no_nans=True, special_cases=[-0.0, 0.0, 1.0, -1.0])),
        PbScalar("f64", 5, DoubleGen(no_nans=True, special_cases=[-0.0, 0.0, 1.0, -1.0])),
        PbScalar("s", 6, StringGen()),
    ]),
    ("large_dataset", [
        PbScalar("b", 1, BooleanGen()),
        PbScalar("i32", 2, IntegerGen()),
        PbScalar("i64", 3, LongGen()),
        PbScalar("f32", 4, FloatGen()),
        PbScalar("f64", 5, DoubleGen()),
        PbScalar("s", 6, StringGen(pattern="[a-z]{0,50}")),
    ]),
]


def _try_import_from_protobuf():
    try:
        from pyspark.sql.protobuf.functions import from_protobuf
        return from_protobuf
    except Exception:
        return None


def _spark_protobuf_jvm_available(spark) -> bool:
    """
    `spark-protobuf` is an optional external module. PySpark may have the Python wrappers
    even when the JVM side isn't present on the classpath, which manifests as:
      TypeError: 'JavaPackage' object is not callable
    when calling into `sc._jvm.org.apache.spark.sql.protobuf.functions.from_protobuf`.
    """
    jvm = spark.sparkContext._jvm
    candidates = [
        # Scala object `functions` compiles to `functions$`
        "org.apache.spark.sql.protobuf.functions$",
        # Some environments may expose it differently
        "org.apache.spark.sql.protobuf.functions",
    ]
    for cls in candidates:
        try:
            jvm.java.lang.Class.forName(cls)
            return True
        except Exception:
            continue
    return False


# ---------------------------------------------------------------------------
# Shared fixture and helpers to reduce per-test boilerplate
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def from_protobuf_fn():
    """Skip the entire module if from_protobuf or the JVM module is unavailable."""
    fn = _try_import_from_protobuf()
    if fn is None:
        pytest.skip("from_protobuf not available")
    if not with_cpu_session(_spark_protobuf_jvm_available):
        pytest.skip("spark-protobuf JVM not available")
    return fn


def _setup_protobuf_desc(spark_tmp_path, desc_name, build_fn):
    """Build descriptor bytes via JVM, write to HDFS, return (desc_path, desc_bytes)."""
    desc_path = spark_tmp_path + "/" + desc_name
    desc_bytes = with_cpu_session(build_fn)
    with_cpu_session(
        lambda spark: _write_bytes_to_hadoop_path(spark, desc_path, desc_bytes))
    return desc_path, desc_bytes


def _call_from_protobuf(from_protobuf_fn, col, message_name,
                         desc_path, desc_bytes, options=None):
    """Call from_protobuf using the right API variant."""
    sig = inspect.signature(from_protobuf_fn)
    if "binaryDescriptorSet" in sig.parameters:
        kw = dict(binaryDescriptorSet=bytearray(desc_bytes))
        if options is not None:
            kw["options"] = options
        return from_protobuf_fn(col, message_name, **kw)
    if options is not None and "options" in sig.parameters:
        return from_protobuf_fn(col, message_name, desc_path, options)
    return from_protobuf_fn(col, message_name, desc_path)


def _build_simple_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for:
      package test;
      syntax = "proto2";
      message Simple {
        optional bool   b   = 1;
        optional int32  i32 = 2;
        optional int64  i64 = 3;
        optional float  f32 = 4;
        optional double f64 = 5;
        optional string s   = 6;
      }
    """
    D, fd = _new_proto2_file(spark, "simple.proto")

    msg = D.DescriptorProto.newBuilder().setName("Simple")
    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL

    def add_field(name, number, ftype):
        msg.addField(
            D.FieldDescriptorProto.newBuilder()
              .setName(name)
              .setNumber(number)
              .setLabel(label_opt)
              .setType(ftype)
              .build()
        )

    add_field("b", 1, D.FieldDescriptorProto.Type.TYPE_BOOL)
    add_field("i32", 2, D.FieldDescriptorProto.Type.TYPE_INT32)
    add_field("i64", 3, D.FieldDescriptorProto.Type.TYPE_INT64)
    add_field("f32", 4, D.FieldDescriptorProto.Type.TYPE_FLOAT)
    add_field("f64", 5, D.FieldDescriptorProto.Type.TYPE_DOUBLE)
    add_field("s", 6, D.FieldDescriptorProto.Type.TYPE_STRING)

    fd.addMessageType(msg.build())

    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    # py4j converts Java byte[] to a Python bytes-like object
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


def _new_proto2_file(spark, name):
    """Create a proto2 FileDescriptorProto builder with common defaults."""
    D = spark.sparkContext._jvm.com.google.protobuf.DescriptorProtos
    fd = D.FileDescriptorProto.newBuilder() \
        .setName(name) \
        .setPackage("test")
    try:
        fd = fd.setSyntax("proto2")
    except Exception:
        pass
    return D, fd


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_simple_parquet_binary_round_trip(spark_tmp_path, from_protobuf_fn):
    data_path = spark_tmp_path + "/PROTOBUF_SIMPLE_PARQUET/"
    message_name = "test.Simple"
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "simple.desc", _build_simple_descriptor_set_bytes)

    # Build a DF with scalar columns + binary protobuf column and write to parquet
    row_gen = ProtobufMessageGen([
        PbScalar("b", 1, BooleanGen(nullable=True)),
        PbScalar("i32", 2, IntegerGen(nullable=True, min_val=0, max_val=1 << 20)),
        PbScalar("i64", 3, LongGen(nullable=True, min_val=0, max_val=1 << 40, special_cases=[])),
        PbScalar("f32", 4, FloatGen(nullable=True, no_nans=True)),
        PbScalar("f64", 5, DoubleGen(nullable=True, no_nans=True)),
        PbScalar("s", 6, StringGen(nullable=True)),
    ], binary_col_name="bin")

    def write_parquet(spark):
        df = gen_df(spark, row_gen)
        df.write.mode("overwrite").parquet(data_path)

    with_cpu_session(write_parquet)

    # Sanity check correctness on CPU (decoded struct matches the original scalar columns)
    def cpu_correctness_check(spark):
        df = spark.read.parquet(data_path)
        expected = f.struct(
            f.col("b").alias("b"),
            f.col("i32").alias("i32"),
            f.col("i64").alias("i64"),
            f.col("f32").alias("f32"),
            f.col("f64").alias("f64"),
            f.col("s").alias("s"),
        ).alias("expected")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name,
            desc_path, desc_bytes).alias("decoded")
        rows = df.select(expected, decoded).collect()
        for r in rows:
            assert r["expected"] == r["decoded"]

    with_cpu_session(cpu_correctness_check)

    # Main assertion: CPU and GPU results match for from_protobuf on a binary column read from parquet
    def run_on_spark(spark):
        df = spark.read.parquet(data_path)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(decoded.alias("decoded"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _load_nested_proto_desc_resource():
    """Load the pre-compiled nested proto descriptor file.

    The .desc file is checked into the repository under
    integration_tests/src/test/resources/protobuf_test/nested_proto/generated/.
    Returns the raw bytes or None if the file does not exist.
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
    desc_file = os.path.join(base_dir, "src", "test", "resources",
                             "protobuf_test", "nested_proto", "generated",
                             "main_log.desc")
    if not os.path.exists(desc_file):
        return None
    with open(desc_file, 'rb') as fp:
        return fp.read()


def _build_main_log_record_fields():
    """Build PbField tree matching MainLogRecord schema from nested proto files."""
    u32 = lambda: IntegerGen(min_val=0, max_val=100000)
    u64 = lambda: LongGen(min_val=0, max_val=(1 << 50))

    type_a_query_schema = [
        PbScalar("keyword", 1, StringGen()),
        PbScalar("session_id", 2, StringGen()),
    ]
    type_a_pair_schema = [
        PbScalar("record_id", 1, StringGen()),
        PbScalar("item_id", 2, StringGen()),
    ]
    schema_type_a = [
        PbNested("query_schema", 1, type_a_query_schema),
        PbRepeatedMessage("pair_schema", 2, type_a_pair_schema, min_len=0, max_len=3),
    ]

    type_b_query_schema = [
        PbScalar("profile_tag_id", 1, StringGen()),
        PbScalar("entity_id", 2, StringGen()),
    ]
    type_b_style_elem = [
        PbScalar("template_id", 1, StringGen()),
        PbScalar("material_id", 2, StringGen()),
    ]
    type_b_style_schema = [
        PbRepeatedMessage("values", 1, type_b_style_elem, min_len=0, max_len=3),
    ]
    schema_type_b = [
        PbNested("query_schema", 1, type_b_query_schema),
        PbRepeatedMessage("style_schema", 2, type_b_style_schema, min_len=0, max_len=3),
    ]

    type_c_query_schema = [
        PbScalar("keyword", 1, StringGen()),
        PbScalar("category", 2, StringGen()),
    ]
    type_c_pair_schema = [
        PbScalar("item_id", 1, StringGen()),
        PbScalar("target_url", 2, StringGen()),
    ]
    type_c_style_schema = [
        PbRepeatedMessage("values", 1, [], min_len=0, max_len=3),
    ]
    schema_type_c = [
        PbNested("query_schema", 1, type_c_query_schema),
        PbRepeatedMessage("pair_schema", 2, type_c_pair_schema, min_len=0, max_len=3),
        PbRepeatedMessage("style_schema", 3, type_c_style_schema, min_len=0, max_len=3),
    ]
    predictor_schema = [
        PbNested("type_a_schema", 1, schema_type_a),
        PbNested("type_b_schema", 2, schema_type_b),
        PbNested("type_c_schema", 3, schema_type_c),
    ]

    device_req_field = [
        PbScalar("os_type", 1, IntegerGen()),
        PbScalar("device_id", 2, BinaryGen(min_length=0, max_length=16)),
    ]
    partner_info = [
        PbScalar("token", 1, StringGen()),
        PbScalar("partner_id", 2, u64()),
    ]
    coordinate = [
        PbScalar("x", 1, DoubleGen()),
        PbScalar("y", 2, DoubleGen()),
    ]
    location_point = [
        PbScalar("frequency", 1, u32()),
        PbNested("coord", 2, coordinate),
        PbScalar("timestamp", 3, u64()),
    ]
    change_log = [
        PbScalar("value_before", 1, u32()),
        PbScalar("parameters", 2, StringGen()),
    ]
    kv_pair = [
        PbScalar("key", 1, BinaryGen(min_length=0, max_length=16)),
        PbScalar("value", 2, BinaryGen(min_length=0, max_length=16)),
    ]
    style_config = [
        PbScalar("style_id", 1, u32()),
        PbRepeatedMessage("kv_pairs", 2, kv_pair, min_len=0, max_len=3),
    ]
    module_a_res = [
        PbScalar("route_tag", 1, StringGen()),
        PbScalar("status_tag", 2, IntegerGen()),
        PbScalar("region_id", 3, u32()),
        PbRepeated("experiment_ids", 4, StringGen(), packed=False, min_len=0, max_len=3),
        PbScalar("quality_score", 5, DoubleGen()),
        PbRepeatedMessage("location_points", 6, location_point, min_len=0, max_len=3),
        PbRepeated("interest_ids", 7, u64(), packed=False, min_len=0, max_len=3),
    ]
    module_a_src_res = [
        PbScalar("match_type", 1, u32()),
    ]
    module_a_detail = [
        PbScalar("type_code", 1, u32()),
        PbScalar("item_id", 2, u64()),
        PbScalar("strategy_type", 3, IntegerGen()),
        PbScalar("min_value", 4, LongGen()),
        PbScalar("target_url", 5, BinaryGen(min_length=0, max_length=24)),
        PbScalar("title", 6, StringGen()),
        PbScalar("is_valid", 7, BooleanGen()),
        PbScalar("score_ratio", 8, FloatGen()),
        PbRepeated("template_ids", 9, u32(), packed=False, min_len=0, max_len=3),
        PbRepeated("material_ids", 10, u64(), packed=False, min_len=0, max_len=3),
        PbRepeatedMessage("styles", 11, style_config, min_len=0, max_len=3),
        PbRepeatedMessage("change_logs", 12, change_log, min_len=0, max_len=3),
        PbNested("partner_info", 13, partner_info),
        PbNested("predictor_schema", 14, predictor_schema),
    ]

    block_element = [
        PbScalar("element_id", 1, u64()),
        PbRepeated("ref_ids", 2, u64(), packed=False, min_len=0, max_len=3),
    ]
    block_info = [
        PbScalar("block_id", 1, u64()),
        PbRepeatedMessage("elements", 2, block_element, min_len=0, max_len=3),
    ]
    module_b_detail = [
        PbRepeated("tags", 1, u32(), packed=False, min_len=0, max_len=3),
        PbScalar("item_id", 2, u64()),
        PbScalar("name", 3, StringGen()),
        PbRepeatedMessage("blocks", 4, block_info, min_len=0, max_len=3),
    ]

    request_info = [
        PbScalar("page_num", 1, u32()),
        PbScalar("channel_code", 2, StringGen()),
        PbRepeated("experiment_ids", 3, u32(), packed=False, min_len=0, max_len=3),
        PbScalar("is_filtered", 4, BooleanGen()),
    ]
    extended_req_info = [
        PbNested("device_req_field", 1, device_req_field),
    ]
    server_added_field = [
        PbScalar("region_code", 1, u32()),
        PbScalar("flow_type", 2, StringGen()),
        PbScalar("filter_result", 3, IntegerGen()),
        PbRepeated("hit_rule_list", 4, IntegerGen(), packed=False, min_len=0, max_len=3),
        PbScalar("request_time", 5, u64()),
        PbScalar("skip_flag", 6, BooleanGen()),
    ]
    basic_info = [
        PbNested("request_info", 1, request_info),
        PbNested("extended_req_info", 2, extended_req_info),
        PbNested("server_added_field", 3, server_added_field),
    ]

    channel_info = [
        PbScalar("channel_id", 1, IntegerGen()),
        PbNested("module_a_res", 2, module_a_res),
    ]
    src_channel_info = [
        PbScalar("channel_id", 1, IntegerGen()),
        PbNested("module_a_src_res", 2, module_a_src_res),
    ]
    item_detail_field = [
        PbScalar("rank", 1, u32()),
        PbScalar("record_id", 2, u64()),
        PbScalar("keyword", 3, StringGen()),
        PbNested("module_a_detail", 4, module_a_detail),
        PbNested("module_b_detail", 5, module_b_detail),
    ]
    data_source_field = [
        PbScalar("source_id", 1, u32()),
        PbRepeatedMessage("src_channel_list", 2, src_channel_info, min_len=0, max_len=3),
        PbScalar("billing_name", 3, StringGen()),
        PbRepeatedMessage("item_list", 4, item_detail_field, min_len=0, max_len=3),
        PbScalar("is_free", 5, BooleanGen()),
    ]
    log_content = [
        PbNested("basic_info", 1, basic_info),
        PbRepeatedMessage("channel_list", 2, channel_info, min_len=0, max_len=3),
        PbRepeatedMessage("source_list", 3, data_source_field, min_len=0, max_len=3),
    ]

    return [
        PbScalar(
            "source", 1,
            IntegerGen(min_val=0, max_val=1, nullable=False, special_cases=[4])),
        PbScalar("timestamp", 2, LongGen(min_val=0, max_val=(1 << 50), nullable=False)),
        PbScalar("user_id", 3, StringGen()),
        PbScalar("account_id", 4, LongGen()),
        PbScalar("client_ip", 5, IntegerGen(min_val=0, max_val=0x7FFFFFFF), encoding='fixed'),
        PbNested("log_content", 6, log_content),
    ]


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("options", [None, {"enums.as.ints": "true"}])
@ignore_order(local=True)
def test_from_protobuf_customer_heavy_nested_proto(spark_tmp_path, from_protobuf_fn, options):
    """Integration test with real nested proto: multi-level nesting, cross-file imports, enums."""
    desc_bytes = _load_nested_proto_desc_resource()
    if desc_bytes is None:
        pytest.skip("nested_proto descriptor not found; run gen_nested_proto_data.sh first")

    desc_path = spark_tmp_path + "/main_log.desc"
    with_cpu_session(lambda spark: _write_bytes_to_hadoop_path(spark, desc_path, desc_bytes))

    message_name = "com.test.proto.sample.MainLogRecord"
    data_gen = ProtobufMessageGen(_build_main_log_record_fields())

    def run_on_spark(spark):
        generated = gen_df(spark, data_gen).select("bin")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options=options)

        return generated.select(decoded.alias("decoded"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_nested_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for a message with both simple fields and nested message:
      package test;
      syntax = "proto2";
      message Nested {
        optional int32 x = 1;
      }
      message WithNested {
        optional int32  simple_int  = 1;
        optional string simple_str  = 2;
        optional Nested nested_msg  = 3;   // nested message
        optional int64  simple_long = 4;
      }
    """
    D, fd = _new_proto2_file(spark, "nested.proto")

    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL

    # Define Nested message
    nested_msg = D.DescriptorProto.newBuilder().setName("Nested")
    nested_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("x")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
            .build()
    )
    fd.addMessageType(nested_msg.build())

    # Define WithNested message
    with_nested_msg = D.DescriptorProto.newBuilder().setName("WithNested")
    # simple_int
    with_nested_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("simple_int")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
            .build()
    )
    # simple_str
    with_nested_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("simple_str")
            .setNumber(2)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_STRING)
            .build()
    )
    # nested_msg (nested message type)
    with_nested_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("nested_msg")
            .setNumber(3)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_MESSAGE)
            .setTypeName(".test.Nested")
            .build()
    )
    # simple_long
    with_nested_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("simple_long")
            .setNumber(4)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT64)
            .build()
    )
    fd.addMessageType(with_nested_msg.build())

    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_schema_projection_simple_fields_only(spark_tmp_path, from_protobuf_fn):
    """
    Test schema projection: when only simple fields are selected from a protobuf message
    that also contains unsupported types (nested message), GPU should be able to decode
    just the simple fields without falling back to CPU.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "nested.desc", _build_nested_descriptor_set_bytes)
    message_name = "test.WithNested"

    data_gen = ProtobufMessageGen([
        PbScalar("simple_int", 1, IntegerGen()),
        PbScalar("simple_str", 2, StringGen()),
        PbScalar("simple_long", 4, LongGen()),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        # Only select simple fields, not the nested_msg field
        return df.select(
            decoded.getField("simple_int").alias("simple_int"),
            decoded.getField("simple_str").alias("simple_str"),
            decoded.getField("simple_long").alias("simple_long")
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_enum_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for a message with enum field:
      package test;
      syntax = "proto2";
      message WithEnum {
        enum Color {
          RED = 0;
          GREEN = 1;
          BLUE = 2;
        }
        optional Color color = 1;
        optional int32 count = 2;
        optional string name = 3;
      }
    """
    D, fd = _new_proto2_file(spark, "enum.proto")

    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL

    # Define WithEnum message with nested enum
    msg = D.DescriptorProto.newBuilder().setName("WithEnum")

    # Add enum type definition
    enum_type = D.EnumDescriptorProto.newBuilder().setName("Color")
    enum_type.addValue(D.EnumValueDescriptorProto.newBuilder().setName("RED").setNumber(0).build())
    enum_type.addValue(D.EnumValueDescriptorProto.newBuilder().setName("GREEN").setNumber(1).build())
    enum_type.addValue(D.EnumValueDescriptorProto.newBuilder().setName("BLUE").setNumber(2).build())
    msg.addEnumType(enum_type.build())

    # Add color field (enum type)
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("color")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_ENUM)
            .setTypeName(".test.WithEnum.Color")
            .build()
    )
    # Add count field (int32)
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("count")
            .setNumber(2)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
            .build()
    )
    # Add name field (string)
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("name")
            .setNumber(3)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_STRING)
            .build()
    )

    fd.addMessageType(msg.build())
    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


def _build_nested_enum_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for a nested enum message:
      package test;
      syntax = "proto2";
      message WithNestedEnum {
        optional int32 id = 1;
        optional Detail detail = 2;
        optional string name = 3;
      }
      message Detail {
        enum Status {
          UNKNOWN = 0;
          OK = 1;
          BAD = 2;
        }
        optional Status status = 1;
        optional int32 count = 2;
      }
    """
    D, fd = _new_proto2_file(spark, "nested_enum.proto")
    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL

    detail_msg = D.DescriptorProto.newBuilder().setName("Detail")
    detail_enum = D.EnumDescriptorProto.newBuilder().setName("Status")
    detail_enum.addValue(
        D.EnumValueDescriptorProto.newBuilder().setName("UNKNOWN").setNumber(0).build())
    detail_enum.addValue(
        D.EnumValueDescriptorProto.newBuilder().setName("OK").setNumber(1).build())
    detail_enum.addValue(
        D.EnumValueDescriptorProto.newBuilder().setName("BAD").setNumber(2).build())
    detail_msg.addEnumType(detail_enum.build())
    detail_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("status")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_ENUM)
            .setTypeName(".test.Detail.Status")
            .build()
    )
    detail_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("count")
            .setNumber(2)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
            .build()
    )
    fd.addMessageType(detail_msg.build())

    top_msg = D.DescriptorProto.newBuilder().setName("WithNestedEnum")
    top_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("id")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
            .build()
    )
    top_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("detail")
            .setNumber(2)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_MESSAGE)
            .setTypeName(".test.Detail")
            .build()
    )
    top_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("name")
            .setNumber(3)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_STRING)
            .build()
    )
    fd.addMessageType(top_msg.build())

    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("enum_case", [
    "as_int",
    "unknown_as_int",
    "as_string",
    "unknown_as_string",
], ids=lambda x: x)
@ignore_order(local=True)
def test_from_protobuf_enum_cases(spark_tmp_path, from_protobuf_fn, enum_case):
    """Parametrized enum decoding tests for int/string modes and unknown values."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "enum.desc", _build_enum_descriptor_set_bytes)
    message_name = "test.WithEnum"

    _ef_full = [
        PbScalar("color", 1, IntegerGen()),
        PbScalar("count", 2, IntegerGen()),
        PbScalar("name", 3, StringGen()),
    ]
    _ef_partial = [PbScalar("color", 1, IntegerGen()), PbScalar("count", 2, IntegerGen())]

    if enum_case == "as_int":
        rows = [
            (encode_pb_message(_ef_full, [1, 42, "test"]),),
            (encode_pb_message(_ef_full, [0, 100, None]),),
            (encode_pb_message(_ef_full, [None, 200, "hello"]),),
            (None,),
        ]
        options = {"enums.as.ints": "true"}
        select_mode = "fields3"
    elif enum_case == "unknown_as_int":
        rows = [(encode_pb_message(_ef_partial, [999, 42]),)]
        options = {"enums.as.ints": "true", "mode": "PERMISSIVE"}
        select_mode = "fields2"
    elif enum_case == "as_string":
        rows = [
            (encode_pb_message(_ef_full, [1, 42, "test"]),),
            (encode_pb_message(_ef_full, [0, 100, None]),),
            (encode_pb_message(_ef_full, [2, 200, "hello"]),),
            (encode_pb_message(_ef_full, [None, 300, "world"]),),
            (None,),
        ]
        options = None
        select_mode = "fields3"
    else:
        rows = [
            (encode_pb_message(_ef_partial, [1, 10]),),
            (encode_pb_message(_ef_partial, [999, 20]),),
            (encode_pb_message(_ef_partial, [2, 30]),),
        ]
        options = {"mode": "PERMISSIVE"}
        select_mode = "decoded"

    def run_on_spark(spark):
        df = spark.createDataFrame(rows, schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options=options)
        if select_mode == "fields3":
            return df.select(
                decoded.getField("color").alias("color"),
                decoded.getField("count").alias("count"),
                decoded.getField("name").alias("name"))
        if select_mode == "fields2":
            return df.select(
                decoded.getField("color").alias("color"),
                decoded.getField("count").alias("count"))
        return df.select(decoded.alias("decoded"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_nested_enum_permissive_invalid_row_null(spark_tmp_path, from_protobuf_fn):
    """
    Nested enum decode parity test:
      - valid nested enum values decode to names in string mode
      - missing nested enum stays null
    This protects nested enum metadata propagation in GPU schema flattening.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "nested_enum.desc", _build_nested_enum_descriptor_set_bytes)
    message_name = "test.WithNestedEnum"

    fields = [
        PbScalar("id", 1, IntegerGen()),
        PbNested("detail", 2, [
            PbScalar("status", 1, IntegerGen()),
            PbScalar("count", 2, IntegerGen()),
        ]),
        PbScalar("name", 3, StringGen(nullable=True)),
    ]
    rows = [
        (0, encode_pb_message(fields, [1, (1, 10), "ok"])),       # status=OK
        (1, encode_pb_message(fields, [2, (2, 20), "bad"])),      # status=BAD
        (2, encode_pb_message(fields, [3, (None, 30), "none"])),  # missing enum
        (3, None),
    ]

    def run_on_spark(spark):
        df = spark.createDataFrame(rows, schema="idx int, bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        return df.select(
            f.col("idx"),
            decoded.isNull().alias("decoded_is_null"),
            decoded.getField("id").alias("id"),
            decoded.getField("detail").getField("status").alias("status"),
            decoded.getField("name").alias("name")).orderBy("idx")

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_required_field_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for a message with required fields (proto2):
      package test;
      syntax = "proto2";
      message WithRequired {
        required int64 id = 1;
        optional string name = 2;
        optional int32 count = 3;
      }
    """
    D, fd = _new_proto2_file(spark, "required.proto")

    label_required = D.FieldDescriptorProto.Label.LABEL_REQUIRED
    label_optional = D.FieldDescriptorProto.Label.LABEL_OPTIONAL

    msg = D.DescriptorProto.newBuilder().setName("WithRequired")
    
    # id field (required)
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("id")
            .setNumber(1)
            .setLabel(label_required)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT64)
            .build()
    )
    # name field (optional)
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("name")
            .setNumber(2)
            .setLabel(label_optional)
            .setType(D.FieldDescriptorProto.Type.TYPE_STRING)
            .build()
    )
    # count field (optional)
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("count")
            .setNumber(3)
            .setLabel(label_optional)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
            .build()
    )

    fd.addMessageType(msg.build())
    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_required_field_present(spark_tmp_path, from_protobuf_fn):
    """
    Test that required fields decode correctly when present.
    GPU should produce same results as CPU.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "required.desc", _build_required_field_descriptor_set_bytes)
    message_name = "test.WithRequired"

    # Create test data with required field present
    # Row 0: id=100, name="test", count=42
    # Row 1: id=200, name missing, count missing
    _rf = [
        PbScalar("id", 1, LongGen()),
        PbScalar("name", 2, StringGen()),
        PbScalar("count", 3, IntegerGen()),
    ]
    test_data_row0 = encode_pb_message(_rf, [100, "test", 42])
    test_data_row1 = encode_pb_message(_rf, [200, None, None])

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(test_data_row0,), (test_data_row1,)],
            schema="bin binary",
        )
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("name").alias("name"),
            decoded.getField("count").alias("count")
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_default_value_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for a message with default values (proto2):
      package test;
      syntax = "proto2";
      message WithDefaults {
        optional int32 count = 1 [default = 42];
        optional string name = 2 [default = "unknown"];
        optional bool flag = 3 [default = true];
      }
    Note: Setting explicit defaults requires using FieldOptions which may not be
    available via the simple DescriptorProtos API. For testing, we rely on proto2
    implicit behavior where hasDefaultValue() returns true for optional fields.
    """
    D, fd = _new_proto2_file(spark, "defaults.proto")

    label_optional = D.FieldDescriptorProto.Label.LABEL_OPTIONAL

    msg = D.DescriptorProto.newBuilder().setName("WithDefaults")
    
    # count field with default
    count_field = D.FieldDescriptorProto.newBuilder() \
        .setName("count") \
        .setNumber(1) \
        .setLabel(label_optional) \
        .setType(D.FieldDescriptorProto.Type.TYPE_INT32) \
        .setDefaultValue("42")
    msg.addField(count_field.build())
    
    # name field with default
    name_field = D.FieldDescriptorProto.newBuilder() \
        .setName("name") \
        .setNumber(2) \
        .setLabel(label_optional) \
        .setType(D.FieldDescriptorProto.Type.TYPE_STRING) \
        .setDefaultValue("unknown")
    msg.addField(name_field.build())
    
    # flag field with default
    flag_field = D.FieldDescriptorProto.newBuilder() \
        .setName("flag") \
        .setNumber(3) \
        .setLabel(label_optional) \
        .setType(D.FieldDescriptorProto.Type.TYPE_BOOL) \
        .setDefaultValue("true")
    msg.addField(flag_field.build())

    fd.addMessageType(msg.build())
    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("default_case", [
    "field_present",
    "missing_fields",
    "string_default_only",
], ids=lambda x: x)
@ignore_order(local=True)
def test_from_protobuf_default_values_cases(spark_tmp_path, from_protobuf_fn, default_case):
    """Parametrized tests for proto2 default-value behavior."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "defaults.desc", _build_default_value_descriptor_set_bytes)
    message_name = "test.WithDefaults"

    _df = [
        PbScalar("count", 1, IntegerGen()),
        PbScalar("name", 2, StringGen()),
        PbScalar("flag", 3, BooleanGen()),
    ]

    if default_case == "field_present":
        rows = [(encode_pb_message(_df, [100, "test", False]),)]
        select_mode = "all"
    elif default_case == "missing_fields":
        rows = [
            (encode_pb_message(_df, [None, None, None]),),
            (encode_pb_message(_df, [100, None, None]),),
        ]
        select_mode = "all"
    else:
        rows = [(encode_pb_message(_df, [42, None, True]),)]
        select_mode = "name_only"

    def run_on_spark(spark):
        df = spark.createDataFrame(rows, schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        if select_mode == "all":
            return df.select(
                decoded.getField("count").alias("count"),
                decoded.getField("name").alias("name"),
                decoded.getField("flag").alias("flag"))
        return df.select(decoded.getField("name").alias("name"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_all_scalars_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for message with all scalar types:
      message AllScalars {
        optional bool b = 1;
        optional int32 i32 = 2;
        optional int64 i64 = 3;
        optional float f32 = 4;
        optional double f64 = 5;
        optional string s = 6;
        optional sint32 si32 = 7;  // zigzag encoding
        optional sint64 si64 = 8;  // zigzag encoding
        optional fixed32 fx32 = 9;
        optional fixed64 fx64 = 10;
      }
    """
    D, fd = _new_proto2_file(spark, "all_scalars.proto")

    msg = D.DescriptorProto.newBuilder().setName("AllScalars")
    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL

    def add_field(name, number, ftype):
        msg.addField(
            D.FieldDescriptorProto.newBuilder()
              .setName(name)
              .setNumber(number)
              .setLabel(label_opt)
              .setType(ftype)
              .build()
        )

    T = D.FieldDescriptorProto.Type
    add_field("b", 1, T.TYPE_BOOL)
    add_field("i32", 2, T.TYPE_INT32)
    add_field("i64", 3, T.TYPE_INT64)
    add_field("f32", 4, T.TYPE_FLOAT)
    add_field("f64", 5, T.TYPE_DOUBLE)
    add_field("s", 6, T.TYPE_STRING)
    add_field("si32", 7, T.TYPE_SINT32)
    add_field("si64", 8, T.TYPE_SINT64)
    add_field("fx32", 9, T.TYPE_FIXED32)
    add_field("fx64", 10, T.TYPE_FIXED64)

    fd.addMessageType(msg.build())
    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


def _build_scalar_bytes_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for:
      message ScalarBytes {
        optional bytes payload = 1;
      }
    """
    D, fd = _new_proto2_file(spark, "scalar_bytes.proto")
    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL

    msg = D.DescriptorProto.newBuilder().setName("ScalarBytes")
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("payload")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_BYTES)
            .build()
    )
    fd.addMessageType(msg.build())

    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


def _scalar_test_id(config):
    """Generate stable test ID using only the first element (test name)."""
    return config[0] if isinstance(config, tuple) else str(config)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("test_config", _random_scalar_test_configs, ids=_scalar_test_id)
@ignore_order(local=True)
def test_from_protobuf_random_scalars(spark_tmp_path, from_protobuf_fn, test_config):
    """
    Parametrized test for from_protobuf with randomly generated scalar data.
    Covers: all types, integer edge cases, float edge cases, large datasets.
    """
    test_id, field_configs = test_config

    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "simple.desc", _build_simple_descriptor_set_bytes)
    message_name = "test.Simple"

    data_gen = ProtobufMessageGen(field_configs)

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)

        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)

        # Select all decoded fields
        return df.select(
            decoded.getField("b").alias("b"),
            decoded.getField("i32").alias("i32"),
            decoded.getField("i64").alias("i64"),
            decoded.getField("f32").alias("f32"),
            decoded.getField("f64").alias("f64"),
            decoded.getField("s").alias("s"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_all_scalar_types(spark_tmp_path, from_protobuf_fn):
    """Decode all scalar wire encodings together in one message."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "all_scalars.desc", _build_all_scalars_descriptor_set_bytes)
    message_name = "test.AllScalars"

    data_gen = ProtobufMessageGen([
        PbScalar("b", 1, BooleanGen()),
        PbScalar("i32", 2, IntegerGen()),
        PbScalar("i64", 3, LongGen()),
        PbScalar("f32", 4, FloatGen()),
        PbScalar("f64", 5, DoubleGen()),
        PbScalar("s", 6, StringGen()),
        PbScalar("si32", 7, IntegerGen(
            special_cases=[-1, 0, 1, -2147483648, 2147483647]), encoding='zigzag'),
        PbScalar("si64", 8, LongGen(
            special_cases=[-1, 0, 1, -9223372036854775808, 9223372036854775807]),
            encoding='zigzag'),
        PbScalar("fx32", 9, IntegerGen(
            special_cases=[0, 1, -1, 2147483647, -2147483648]), encoding='fixed'),
        PbScalar("fx64", 10, LongGen(
            special_cases=[0, 1, -1]), encoding='fixed'),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("b").alias("b"),
            decoded.getField("i32").alias("i32"),
            decoded.getField("i64").alias("i64"),
            decoded.getField("f32").alias("f32"),
            decoded.getField("f64").alias("f64"),
            decoded.getField("s").alias("s"),
            decoded.getField("si32").alias("si32"),
            decoded.getField("si64").alias("si64"),
            decoded.getField("fx32").alias("fx32"),
            decoded.getField("fx64").alias("fx64"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_scalar_bytes(spark_tmp_path, from_protobuf_fn):
    """Decode optional bytes scalar field, including empty bytes."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "scalar_bytes.desc", _build_scalar_bytes_descriptor_set_bytes)
    message_name = "test.ScalarBytes"

    data_gen = ProtobufMessageGen([
        PbScalar("payload", 1, BinaryGen(min_length=0, max_length=16)),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(decoded.getField("payload").alias("payload"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_duplicate_fields(spark_tmp_path, from_protobuf_fn):
    """Duplicate non-repeated field should follow protobuf last-one-wins semantics."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "simple.desc", _build_simple_descriptor_set_bytes)
    message_name = "test.Simple"

    # i32 (field 2) appears twice in row0 and three times in row1.
    test_data_row0 = bytes([0x10, 0x01, 0x10, 0x2A])
    test_data_row1 = bytes([0x10, 0x03, 0x10, 0x04, 0x10, 0x05])

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(test_data_row0,), (test_data_row1,), (None,)],
            schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(decoded.getField("i32").alias("i32"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_repeated_int_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for a message with repeated int32 field:
      message WithRepeatedInt {
        optional int32 id = 1;
        repeated int32 values = 2;
      }
    """
    D, fd = _new_proto2_file(spark, "repeated_int.proto")

    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL
    label_rep = D.FieldDescriptorProto.Label.LABEL_REPEATED

    msg = D.DescriptorProto.newBuilder().setName("WithRepeatedInt")
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("id")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
            .build()
    )
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("values")
            .setNumber(2)
            .setLabel(label_rep)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
            .build()
    )
    fd.addMessageType(msg.build())

    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_int32(spark_tmp_path, from_protobuf_fn):
    """
    Test decoding repeated int32 field (ArrayType of integers).
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "repeated_int.desc", _build_repeated_int_descriptor_set_bytes)
    message_name = "test.WithRepeatedInt"

    data_gen = ProtobufMessageGen([
        PbScalar("id", 1, IntegerGen()),
        PbRepeated("values", 2, IntegerGen(), min_len=0, max_len=10),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("values").alias("values"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_repeated_string_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for a message with repeated string field:
      message WithRepeatedString {
        optional string name = 1;
        repeated string tags = 2;
      }
    """
    D, fd = _new_proto2_file(spark, "repeated_string.proto")

    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL
    label_rep = D.FieldDescriptorProto.Label.LABEL_REPEATED

    msg = D.DescriptorProto.newBuilder().setName("WithRepeatedString")
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("name")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_STRING)
            .build()
    )
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("tags")
            .setNumber(2)
            .setLabel(label_rep)
            .setType(D.FieldDescriptorProto.Type.TYPE_STRING)
            .build()
    )
    fd.addMessageType(msg.build())

    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_string(spark_tmp_path, from_protobuf_fn):
    """
    Test decoding repeated string field (ArrayType of strings).
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "repeated_string.desc", _build_repeated_string_descriptor_set_bytes)
    message_name = "test.WithRepeatedString"

    data_gen = ProtobufMessageGen([
        PbScalar("name", 1, StringGen()),
        PbRepeated("tags", 2, StringGen(), min_len=0, max_len=5),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("name").alias("name"),
            decoded.getField("tags").alias("tags"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_nested_message(spark_tmp_path, from_protobuf_fn):
    """
    Test decoding nested message field (StructType).
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "nested.desc", _build_nested_descriptor_set_bytes)
    message_name = "test.WithNested"

    data_gen = ProtobufMessageGen([
        PbScalar("simple_int", 1, IntegerGen()),
        PbScalar("simple_str", 2, StringGen(nullable=True)),
        PbNested("nested_msg", 3, [PbScalar("x", 1, IntegerGen())]),
        PbScalar("simple_long", 4, LongGen()),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        # Select all fields including nested
        return df.select(
            decoded.getField("simple_int").alias("simple_int"),
            decoded.getField("simple_str").alias("simple_str"),
            decoded.getField("nested_msg").alias("nested_msg"),
            decoded.getField("simple_long").alias("simple_long"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_nested_message_field_access(spark_tmp_path, from_protobuf_fn):
    """
    Test accessing fields within nested message.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "nested.desc", _build_nested_descriptor_set_bytes)
    message_name = "test.WithNested"

    data_gen = ProtobufMessageGen([
        PbScalar("simple_int", 1, IntegerGen()),
        PbScalar("simple_str", 2, StringGen(nullable=True)),
        PbNested("nested_msg", 3, [PbScalar("x", 1, IntegerGen())]),
        PbScalar("simple_long", 4, LongGen()),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        # Access nested field directly
        return df.select(
            decoded.getField("simple_int").alias("simple_int"),
            decoded.getField("nested_msg").getField("x").alias("nested_x"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_deep_nested_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for deeply nested message:
      message Inner {
        optional int32 value = 1;
      }
      message Middle {
        optional string name = 1;
        optional Inner inner = 2;
      }
      message Outer {
        optional int32 id = 1;
        optional Middle middle = 2;
      }
    """
    D, fd = _new_proto2_file(spark, "deep_nested.proto")

    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL

    # Inner message
    inner_msg = D.DescriptorProto.newBuilder().setName("Inner")
    inner_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("value")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
            .build()
    )
    fd.addMessageType(inner_msg.build())

    # Middle message
    middle_msg = D.DescriptorProto.newBuilder().setName("Middle")
    middle_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("name")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_STRING)
            .build()
    )
    middle_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("inner")
            .setNumber(2)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_MESSAGE)
            .setTypeName(".test.Inner")
            .build()
    )
    fd.addMessageType(middle_msg.build())

    # Outer message
    outer_msg = D.DescriptorProto.newBuilder().setName("Outer")
    outer_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("id")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
            .build()
    )
    outer_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("middle")
            .setNumber(2)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_MESSAGE)
            .setTypeName(".test.Middle")
            .build()
    )
    fd.addMessageType(outer_msg.build())

    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_deep_nested(spark_tmp_path, from_protobuf_fn):
    """
    Test decoding deeply nested messages (3 levels).
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "deep_nested.desc", _build_deep_nested_descriptor_set_bytes)
    message_name = "test.Outer"

    data_gen = ProtobufMessageGen([
        PbScalar("id", 1, IntegerGen()),
        PbNested("middle", 2, [
            PbScalar("name", 1, StringGen()),
            PbNested("inner", 2, [PbScalar("value", 1, IntegerGen())]),
        ]),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("middle").alias("middle"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_repeated_message_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for repeated message field (array of structs):
      message Item {
        optional int32 id = 1;
        optional string name = 2;
      }
      message Container {
        optional string title = 1;
        repeated Item items = 2;
      }
    """
    D, fd = _new_proto2_file(spark, "repeated_message.proto")

    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL
    label_rep = D.FieldDescriptorProto.Label.LABEL_REPEATED

    # Item message
    item_msg = D.DescriptorProto.newBuilder().setName("Item")
    item_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("id")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
            .build()
    )
    item_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("name")
            .setNumber(2)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_STRING)
            .build()
    )
    fd.addMessageType(item_msg.build())

    # Container message
    container_msg = D.DescriptorProto.newBuilder().setName("Container")
    container_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("title")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_STRING)
            .build()
    )
    container_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("items")
            .setNumber(2)
            .setLabel(label_rep)
            .setType(D.FieldDescriptorProto.Type.TYPE_MESSAGE)
            .setTypeName(".test.Item")
            .build()
    )
    fd.addMessageType(container_msg.build())

    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_message(spark_tmp_path, from_protobuf_fn):
    """
    Test decoding repeated message field (ArrayType of StructType).
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "repeated_message.desc", _build_repeated_message_descriptor_set_bytes)
    message_name = "test.Container"

    data_gen = ProtobufMessageGen([
        PbScalar("title", 1, StringGen()),
        PbRepeatedMessage("items", 2, [
            PbScalar("id", 1, IntegerGen()),
            PbScalar("name", 2, StringGen()),
        ], min_len=0, max_len=5),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("title").alias("title"),
            decoded.getField("items").alias("items"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_nested_with_repeated_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for nested message with repeated fields:
      message NestedWithRepeated {
        optional string name = 1;
        repeated int32 values = 2;
        optional int32 count = 3;
      }
      message OuterWithNestedRepeated {
        optional int32 id = 1;
        optional NestedWithRepeated nested = 2;
      }
    """
    D, fd = _new_proto2_file(spark, "nested_with_repeated.proto")

    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL
    label_rep = D.FieldDescriptorProto.Label.LABEL_REPEATED

    # NestedWithRepeated message
    nested_msg = D.DescriptorProto.newBuilder().setName("NestedWithRepeated")
    nested_msg.addField(D.FieldDescriptorProto.newBuilder()
        .setName("name").setNumber(1).setLabel(label_opt)
        .setType(D.FieldDescriptorProto.Type.TYPE_STRING))
    nested_msg.addField(D.FieldDescriptorProto.newBuilder()
        .setName("values").setNumber(2).setLabel(label_rep)
        .setType(D.FieldDescriptorProto.Type.TYPE_INT32))
    nested_msg.addField(D.FieldDescriptorProto.newBuilder()
        .setName("count").setNumber(3).setLabel(label_opt)
        .setType(D.FieldDescriptorProto.Type.TYPE_INT32))
    fd.addMessageType(nested_msg)

    # OuterWithNestedRepeated message
    outer_msg = D.DescriptorProto.newBuilder().setName("OuterWithNestedRepeated")
    outer_msg.addField(D.FieldDescriptorProto.newBuilder()
        .setName("id").setNumber(1).setLabel(label_opt)
        .setType(D.FieldDescriptorProto.Type.TYPE_INT32))
    outer_msg.addField(D.FieldDescriptorProto.newBuilder()
        .setName("nested").setNumber(2).setLabel(label_opt)
        .setType(D.FieldDescriptorProto.Type.TYPE_MESSAGE)
        .setTypeName(".test.NestedWithRepeated"))
    fd.addMessageType(outer_msg)

    fds = D.FileDescriptorSet.newBuilder().addFile(fd).build()
    return bytes(fds.toByteArray())


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_nested_with_repeated(spark_tmp_path, from_protobuf_fn):
    """
    Test decoding nested message that contains repeated fields.
    Schema: OuterWithNestedRepeated { id, nested: NestedWithRepeated { name, values[], count } }
    This tests StructType containing StructType containing ArrayType.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "nested_with_repeated.desc",
        _build_nested_with_repeated_descriptor_set_bytes)
    message_name = "test.OuterWithNestedRepeated"

    data_gen = ProtobufMessageGen([
        PbScalar("id", 1, IntegerGen()),
        PbNested("nested", 2, [
            PbScalar("name", 1, StringGen()),
            PbRepeated("values", 2, IntegerGen(), min_len=0, max_len=5),
            PbScalar("count", 3, IntegerGen()),
        ]),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("nested").alias("nested"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_repeated_with_nested_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for repeated message with nested message:
      message Inner {
        optional int32 value = 1;
      }
      message ItemWithNested {
        optional int32 id = 1;
        optional Inner inner = 2;
        optional string name = 3;
      }
      message ContainerWithNestedItems {
        optional string title = 1;
        repeated ItemWithNested items = 2;
      }
    """
    D, fd = _new_proto2_file(spark, "repeated_with_nested.proto")

    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL
    label_rep = D.FieldDescriptorProto.Label.LABEL_REPEATED

    # Inner message
    inner_msg = D.DescriptorProto.newBuilder().setName("Inner")
    inner_msg.addField(D.FieldDescriptorProto.newBuilder()
        .setName("value").setNumber(1).setLabel(label_opt)
        .setType(D.FieldDescriptorProto.Type.TYPE_INT32))
    fd.addMessageType(inner_msg)

    # ItemWithNested message
    item_msg = D.DescriptorProto.newBuilder().setName("ItemWithNested")
    item_msg.addField(D.FieldDescriptorProto.newBuilder()
        .setName("id").setNumber(1).setLabel(label_opt)
        .setType(D.FieldDescriptorProto.Type.TYPE_INT32))
    item_msg.addField(D.FieldDescriptorProto.newBuilder()
        .setName("inner").setNumber(2).setLabel(label_opt)
        .setType(D.FieldDescriptorProto.Type.TYPE_MESSAGE)
        .setTypeName(".test.Inner"))
    item_msg.addField(D.FieldDescriptorProto.newBuilder()
        .setName("name").setNumber(3).setLabel(label_opt)
        .setType(D.FieldDescriptorProto.Type.TYPE_STRING))
    fd.addMessageType(item_msg)

    # ContainerWithNestedItems message
    container_msg = D.DescriptorProto.newBuilder().setName("ContainerWithNestedItems")
    container_msg.addField(D.FieldDescriptorProto.newBuilder()
        .setName("title").setNumber(1).setLabel(label_opt)
        .setType(D.FieldDescriptorProto.Type.TYPE_STRING))
    container_msg.addField(D.FieldDescriptorProto.newBuilder()
        .setName("items").setNumber(2).setLabel(label_rep)
        .setType(D.FieldDescriptorProto.Type.TYPE_MESSAGE)
        .setTypeName(".test.ItemWithNested"))
    fd.addMessageType(container_msg)

    fds = D.FileDescriptorSet.newBuilder().addFile(fd).build()
    return bytes(fds.toByteArray())


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_with_nested(spark_tmp_path, from_protobuf_fn):
    """
    Test decoding repeated message that contains nested message.
    Schema: ContainerWithNestedItems { title, items[]: ItemWithNested { id, inner: Inner { value }, name } }
    This tests ArrayType(StructType(StructType)) - nested struct inside repeated message.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "repeated_with_nested.desc",
        _build_repeated_with_nested_descriptor_set_bytes)
    message_name = "test.ContainerWithNestedItems"

    data_gen = ProtobufMessageGen([
        PbScalar("title", 1, StringGen()),
        PbRepeatedMessage("items", 2, [
            PbScalar("id", 1, IntegerGen()),
            PbNested("inner", 2, [PbScalar("value", 1, IntegerGen())]),
            PbScalar("name", 3, StringGen()),
        ], min_len=0, max_len=3),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("title").alias("title"),
            decoded.getField("items").alias("items"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_packed_repeated_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for message with packed repeated fields (proto2):
      message WithPackedRepeated {
        optional int32 id = 1;
        repeated int32 int_values = 2 [packed=true];
        repeated double double_values = 3 [packed=true];
        repeated bool bool_values = 4 [packed=true];
      }
    Uses proto2 with FieldOptions.packed=true (not proto3).
    """
    D, fd = _new_proto2_file(spark, "packed_repeated.proto")

    label_rep = D.FieldDescriptorProto.Label.LABEL_REPEATED
    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL

    # Build FieldOptions with packed=true for repeated numeric fields
    packed_opts = D.FieldOptions.newBuilder().setPacked(True).build()

    msg = D.DescriptorProto.newBuilder().setName("WithPackedRepeated")
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("id")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
            .build()
    )
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("int_values")
            .setNumber(2)
            .setLabel(label_rep)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
            .setOptions(packed_opts)
            .build()
    )
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("double_values")
            .setNumber(3)
            .setLabel(label_rep)
            .setType(D.FieldDescriptorProto.Type.TYPE_DOUBLE)
            .setOptions(packed_opts)
            .build()
    )
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("bool_values")
            .setNumber(4)
            .setLabel(label_rep)
            .setType(D.FieldDescriptorProto.Type.TYPE_BOOL)
            .setOptions(packed_opts)
            .build()
    )
    fd.addMessageType(msg.build())

    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_packed_repeated(spark_tmp_path, from_protobuf_fn):
    """
    Test packed repeated fields (int, double, bool) using DataGen.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "packed.desc", _build_packed_repeated_descriptor_set_bytes)
    message_name = "test.WithPackedRepeated"

    data_gen = ProtobufMessageGen([
        PbScalar("id", 1, IntegerGen()),
        PbRepeated("int_values", 2, IntegerGen(), packed=True, min_len=0, max_len=10),
        PbRepeated("double_values", 3, DoubleGen(), packed=True, min_len=0, max_len=5),
        PbRepeated("bool_values", 4, BooleanGen(), packed=True, min_len=0, max_len=5),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("int_values").alias("int_values"),
            decoded.getField("double_values").alias("double_values"),
            decoded.getField("bool_values").alias("bool_values"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_repeated_all_types_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for message with various repeated field types:
      message WithRepeatedAllTypes {
        optional int32 id = 1;
        repeated int64 long_values = 2;
        repeated float float_values = 3;
        repeated double double_values = 4;
        repeated bool bool_values = 5;
        repeated bytes bytes_values = 6;
      }
    """
    D, fd = _new_proto2_file(spark, "repeated_all.proto")

    label_rep = D.FieldDescriptorProto.Label.LABEL_REPEATED
    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL

    msg = D.DescriptorProto.newBuilder().setName("WithRepeatedAllTypes")
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("id")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
            .build()
    )
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("long_values")
            .setNumber(2)
            .setLabel(label_rep)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT64)
            .build()
    )
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("float_values")
            .setNumber(3)
            .setLabel(label_rep)
            .setType(D.FieldDescriptorProto.Type.TYPE_FLOAT)
            .build()
    )
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("double_values")
            .setNumber(4)
            .setLabel(label_rep)
            .setType(D.FieldDescriptorProto.Type.TYPE_DOUBLE)
            .build()
    )
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("bool_values")
            .setNumber(5)
            .setLabel(label_rep)
            .setType(D.FieldDescriptorProto.Type.TYPE_BOOL)
            .build()
    )
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("bytes_values")
            .setNumber(6)
            .setLabel(label_rep)
            .setType(D.FieldDescriptorProto.Type.TYPE_BYTES)
            .build()
    )
    fd.addMessageType(msg.build())

    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_all_types(spark_tmp_path, from_protobuf_fn):
    """Test repeated fields of various types (int64, float, double, bool, bytes) using DataGen."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "repeated_all.desc", _build_repeated_all_types_descriptor_set_bytes)
    message_name = "test.WithRepeatedAllTypes"

    data_gen = ProtobufMessageGen([
        PbScalar("id", 1, IntegerGen()),
        PbRepeated("long_values", 2, LongGen()),
        PbRepeated("float_values", 3, FloatGen()),
        PbRepeated("double_values", 4, DoubleGen()),
        PbRepeated("bool_values", 5, BooleanGen()),
        PbRepeated("bytes_values", 6, BinaryGen()),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("long_values").alias("long_values"),
            decoded.getField("float_values").alias("float_values"),
            decoded.getField("double_values").alias("double_values"),
            decoded.getField("bool_values").alias("bool_values"),
            decoded.getField("bytes_values").alias("bytes_values"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_large_repeated_array(spark_tmp_path, from_protobuf_fn):
    """
    Test decoding large repeated field (500-1000 elements).
    Stress test for array handling.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "repeated_int.desc", _build_repeated_int_descriptor_set_bytes)
    message_name = "test.WithRepeatedInt"

    data_gen = ProtobufMessageGen([
        PbScalar("id", 1, IntegerGen()),
        PbRepeated("values", 2, IntegerGen(), min_len=500, max_len=1000),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            f.size(decoded.getField("values")).alias("array_size"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_signed_int_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for message with signed integer types:
      message WithSignedInts {
        optional sint32 si32 = 1;  // zigzag encoding
        optional sint64 si64 = 2;  // zigzag encoding
        optional sfixed32 sf32 = 3;  // fixed 4-byte
        optional sfixed64 sf64 = 4;  // fixed 8-byte
      }
    """
    D, fd = _new_proto2_file(spark, "signed_int.proto")

    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL

    msg = D.DescriptorProto.newBuilder().setName("WithSignedInts")
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("si32")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_SINT32)
            .build()
    )
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("si64")
            .setNumber(2)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_SINT64)
            .build()
    )
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("sf32")
            .setNumber(3)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_SFIXED32)
            .build()
    )
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("sf64")
            .setNumber(4)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_SFIXED64)
            .build()
    )
    fd.addMessageType(msg.build())

    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_signed_integers(spark_tmp_path, from_protobuf_fn):
    """
    Test decoding signed integer types with zigzag encoding.
    Zigzag: -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, etc.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "signed.desc", _build_signed_int_descriptor_set_bytes)
    message_name = "test.WithSignedInts"

    data_gen = ProtobufMessageGen([
        PbScalar("si32", 1, IntegerGen(
            special_cases=[-1, 0, 1, -2147483648, 2147483647]), encoding='zigzag'),
        PbScalar("si64", 2, LongGen(
            special_cases=[-1, 0, 1, -9223372036854775808, 9223372036854775807]),
            encoding='zigzag'),
        PbScalar("sf32", 3, IntegerGen(
            special_cases=[0, 1, -1, 2147483647, -2147483648]), encoding='fixed'),
        PbScalar("sf64", 4, LongGen(
            special_cases=[0, 1, -1]), encoding='fixed'),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        
        return df.select(
            decoded.getField("si32").alias("si32"),
            decoded.getField("si64").alias("si64"),
            decoded.getField("sf32").alias("sf32"),
            decoded.getField("sf64").alias("sf64"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_fixed_int_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for message with fixed-width integer types:
      message WithFixedInts {
        optional fixed32 fx32 = 1;
        optional fixed64 fx64 = 2;
      }
    """
    D, fd = _new_proto2_file(spark, "fixed_int.proto")

    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL

    msg = D.DescriptorProto.newBuilder().setName("WithFixedInts")
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("fx32")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_FIXED32)
            .build()
    )
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("fx64")
            .setNumber(2)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_FIXED64)
            .build()
    )
    fd.addMessageType(msg.build())

    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_fixed_integers(spark_tmp_path, from_protobuf_fn):
    """
    Test decoding fixed-width unsigned integer types.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "fixed.desc", _build_fixed_int_descriptor_set_bytes)
    message_name = "test.WithFixedInts"

    data_gen = ProtobufMessageGen([
        PbScalar("fx32", 1, IntegerGen(
            special_cases=[0, 1, -1, 2147483647, -2147483648]), encoding='fixed'),
        PbScalar("fx64", 2, LongGen(
            special_cases=[0, 1, -1]), encoding='fixed'),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("fx32").alias("fx32"),
            decoded.getField("fx64").alias("fx64"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_schema_projection_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for nested schema projection testing:
      message Detail {
        optional int32  a = 1;
        optional int32  b = 2;
        optional string c = 3;
      }
      message SchemaProj {
        optional int32  id     = 1;
        optional string name   = 2;
        optional Detail detail = 3;
        repeated Detail items  = 4;
      }
    The Detail message has 3 fields so we can test pruning subsets.
    """
    D, fd = _new_proto2_file(spark, "schema_proj.proto")

    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL
    label_rep = D.FieldDescriptorProto.Label.LABEL_REPEATED

    # Detail message: { a: int32, b: int32, c: string }
    detail_msg = D.DescriptorProto.newBuilder().setName("Detail")
    detail_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("a").setNumber(1).setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32).build())
    detail_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("b").setNumber(2).setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32).build())
    detail_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("c").setNumber(3).setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_STRING).build())
    fd.addMessageType(detail_msg.build())

    # SchemaProj message: { id, name, detail: Detail, items: repeated Detail }
    main_msg = D.DescriptorProto.newBuilder().setName("SchemaProj")
    main_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("id").setNumber(1).setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32).build())
    main_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("name").setNumber(2).setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_STRING).build())
    main_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("detail").setNumber(3).setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_MESSAGE)
            .setTypeName(".test.Detail").build())
    main_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("items").setNumber(4).setLabel(label_rep)
            .setType(D.FieldDescriptorProto.Type.TYPE_MESSAGE)
            .setTypeName(".test.Detail").build())
    fd.addMessageType(main_msg.build())

    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


# Field descriptors for SchemaProj: {id, name, detail: {a, b, c}, items[]: {a, b, c}}
_detail_children = [
    PbScalar("a", 1, IntegerGen()),
    PbScalar("b", 2, IntegerGen()),
    PbScalar("c", 3, StringGen()),
]
_schema_proj_fields = [
    PbScalar("id", 1, IntegerGen()),
    PbScalar("name", 2, StringGen()),
    PbNested("detail", 3, _detail_children),
    PbRepeatedMessage("items", 4, _detail_children),
]

_schema_proj_test_data = [
    encode_pb_message(_schema_proj_fields,
                      [1, "alice", (10, 20, "d1"), [(100, 200, "i1"), (101, 201, "i2")]]),
    encode_pb_message(_schema_proj_fields,
                      [2, "bob", (30, 40, "d2"), [(300, 400, "i3")]]),
    encode_pb_message(_schema_proj_fields,
                      [3, "carol", (50, 60, "d3"), []]),
]


_schema_proj_cases = [
    ("nested_single_field", [("id", ("id",)), ("detail_a", ("detail", "a"))]),
    ("nested_two_fields", [("detail_a", ("detail", "a")), ("detail_c", ("detail", "c"))]),
    ("whole_struct_no_pruning", [("id", ("id",)), ("detail", ("detail",))]),
    ("whole_and_subfield", [("detail", ("detail",)), ("detail_a", ("detail", "a"))]),
    ("scalar_plus_nested", [("id", ("id",)), ("name", ("name",)), ("detail_a", ("detail", "a"))]),
    ("repeated_msg_single_subfield", [("id", ("id",)), ("items_a", ("items", "a"))]),
    ("repeated_msg_two_subfields", [("items_a", ("items", "a")), ("items_c", ("items", "c"))]),
    ("repeated_whole_no_pruning", [("id", ("id",)), ("items", ("items",))]),
    ("mix_struct_and_repeated", [("id", ("id",)), ("detail_a", ("detail", "a")), ("items_c", ("items", "c"))]),
]


def _get_field_by_path(expr, path):
    current = expr
    for name in path:
        current = current.getField(name)
    return current


def _build_deep_nested_5_level_descriptor_set_bytes(spark):
    """
    Build a FileDescriptorSet for 5-level deep nesting:
      message Level5 { optional int32 val5 = 1; }
      message Level4 { optional int32 val4 = 1; optional Level5 level5 = 2; }
      message Level3 { optional int32 val3 = 1; optional Level4 level4 = 2; }
      message Level2 { optional int32 val2 = 1; optional Level3 level3 = 2; }
      message Level1 { optional int32 val1 = 1; optional Level2 level2 = 2; }
    """
    D, fd = _new_proto2_file(spark, "deep_nested_5_level.proto")

    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL

    # Level5 message
    level5_msg = D.DescriptorProto.newBuilder().setName("Level5")
    level5_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("val5")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
            .build()
    )
    fd.addMessageType(level5_msg.build())

    # Level4 message
    level4_msg = D.DescriptorProto.newBuilder().setName("Level4")
    level4_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("val4")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
            .build()
    )
    level4_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("level5")
            .setNumber(2)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_MESSAGE)
            .setTypeName(".test.Level5")
            .build()
    )
    fd.addMessageType(level4_msg.build())

    # Level3 message
    level3_msg = D.DescriptorProto.newBuilder().setName("Level3")
    level3_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("val3")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
            .build()
    )
    level3_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("level4")
            .setNumber(2)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_MESSAGE)
            .setTypeName(".test.Level4")
            .build()
    )
    fd.addMessageType(level3_msg.build())

    # Level2 message
    level2_msg = D.DescriptorProto.newBuilder().setName("Level2")
    level2_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("val2")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
            .build()
    )
    level2_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("level3")
            .setNumber(2)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_MESSAGE)
            .setTypeName(".test.Level3")
            .build()
    )
    fd.addMessageType(level2_msg.build())

    # Level1 message
    level1_msg = D.DescriptorProto.newBuilder().setName("Level1")
    level1_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("val1")
            .setNumber(1)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
            .build()
    )
    level1_msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("level2")
            .setNumber(2)
            .setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_MESSAGE)
            .setTypeName(".test.Level2")
            .build()
    )
    fd.addMessageType(level1_msg.build())

    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_deep_nesting_5_levels(spark_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "deep_nested_5_level.desc",
        _build_deep_nested_5_level_descriptor_set_bytes)
    message_name = "test.Level1"
    data_gen = ProtobufMessageGen([
        PbScalar("val1", 1, IntegerGen()),
        PbNested("level2", 2, [
            PbScalar("val2", 1, IntegerGen()),
            PbNested("level3", 2, [
                PbScalar("val3", 1, IntegerGen()),
                PbNested("level4", 2, [
                    PbScalar("val4", 1, IntegerGen()),
                    PbNested("level5", 2, [
                        PbScalar("val5", 1, IntegerGen()),
                    ]),
                ]),
            ]),
        ]),
    ])
    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("val1").alias("val1"),
            decoded.getField("level2").alias("level2"),
        )
    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("case_id,select_specs", _schema_proj_cases, ids=lambda c: c[0] if isinstance(c, tuple) else str(c))
@ignore_order(local=True)
def test_from_protobuf_schema_projection_cases(
        spark_tmp_path, from_protobuf_fn, case_id, select_specs):
    """Parametrized nested-schema projection tests."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "schema_proj.desc", _build_schema_projection_descriptor_set_bytes)
    message_name = "test.SchemaProj"

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(d,) for d in _schema_proj_test_data], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        selected = [_get_field_by_path(decoded, path).alias(alias)
                    for alias, path in select_specs]
        return df.select(*selected)

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)

def _build_name_collision_descriptor_set_bytes(spark):
    D, fd = _new_proto2_file(spark, "name_collision.proto")
    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL

    # User message
    user_msg = D.DescriptorProto.newBuilder().setName("User")
    user_msg.addField(D.FieldDescriptorProto.newBuilder().setName("age").setNumber(1).setLabel(label_opt).setType(D.FieldDescriptorProto.Type.TYPE_INT32).build())
    user_msg.addField(D.FieldDescriptorProto.newBuilder().setName("id").setNumber(2).setLabel(label_opt).setType(D.FieldDescriptorProto.Type.TYPE_INT32).build())
    fd.addMessageType(user_msg.build())

    # Ad message
    ad_msg = D.DescriptorProto.newBuilder().setName("Ad")
    ad_msg.addField(D.FieldDescriptorProto.newBuilder().setName("id").setNumber(1).setLabel(label_opt).setType(D.FieldDescriptorProto.Type.TYPE_INT32).build())
    fd.addMessageType(ad_msg.build())

    # Event message
    event_msg = D.DescriptorProto.newBuilder().setName("Event")
    event_msg.addField(D.FieldDescriptorProto.newBuilder().setName("user_info").setNumber(1).setLabel(label_opt).setType(D.FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".test.User").build())
    event_msg.addField(D.FieldDescriptorProto.newBuilder().setName("ad_info").setNumber(2).setLabel(label_opt).setType(D.FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".test.Ad").build())
    fd.addMessageType(event_msg.build())

    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())

@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_bug1_name_collision(spark_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "name_collision.desc",
        _build_name_collision_descriptor_set_bytes)
    message_name = "test.Event"

    data_gen = ProtobufMessageGen([
        PbNested("user_info", 1, [
            PbScalar("age", 1, IntegerGen()),
            PbScalar("id", 2, IntegerGen()),
        ]),
        PbNested("ad_info", 2, [
            PbScalar("id", 1, IntegerGen()),
        ]),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        
        return df.select(
            decoded.getField("user_info").getField("age").alias("age"),
            decoded.getField("user_info").getField("id").alias("user_id"),
            decoded.getField("ad_info").getField("id").alias("ad_id")
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_filter_jump_descriptor_set_bytes(spark):
    D, fd = _new_proto2_file(spark, "filter_jump.proto")
    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL

    msg = D.DescriptorProto.newBuilder().setName("Event")
    msg.addField(D.FieldDescriptorProto.newBuilder().setName("status").setNumber(1).setLabel(label_opt).setType(D.FieldDescriptorProto.Type.TYPE_INT32).build())
    msg.addField(D.FieldDescriptorProto.newBuilder().setName("ad_info").setNumber(2).setLabel(label_opt).setType(D.FieldDescriptorProto.Type.TYPE_STRING).build())
    fd.addMessageType(msg.build())

    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())

@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_bug2_filter_jump(spark_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "filter_jump.desc",
        _build_filter_jump_descriptor_set_bytes)
    message_name = "test.Event"

    data_gen = ProtobufMessageGen([
        PbScalar("status", 1, IntegerGen(min_val=1, max_val=1)),
        PbScalar("ad_info", 2, StringGen()),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        pb_expr1 = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        pb_expr2 = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        
        return df.filter(pb_expr1.getField("status") == 1).select(pb_expr2.getField("ad_info").alias("ad_info"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_unrelated_struct_name_collision_descriptor_set_bytes(spark):
    D, fd = _new_proto2_file(spark, "unrelated_struct.proto")
    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL

    # Nested message
    nested_msg = D.DescriptorProto.newBuilder().setName("Nested")
    nested_msg.addField(D.FieldDescriptorProto.newBuilder().setName("dummy").setNumber(1).setLabel(label_opt).setType(D.FieldDescriptorProto.Type.TYPE_INT32).build())
    nested_msg.addField(D.FieldDescriptorProto.newBuilder().setName("winfoid").setNumber(2).setLabel(label_opt).setType(D.FieldDescriptorProto.Type.TYPE_INT32).build())
    fd.addMessageType(nested_msg.build())

    msg = D.DescriptorProto.newBuilder().setName("Event")
    msg.addField(D.FieldDescriptorProto.newBuilder().setName("ad_info").setNumber(1).setLabel(label_opt).setType(D.FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".test.Nested").build())
    fd.addMessageType(msg.build())

    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())

@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_bug3_unrelated_struct_name_collision(spark_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "unrelated_struct.desc",
        _build_unrelated_struct_name_collision_descriptor_set_bytes)
    message_name = "test.Event"

    data_gen = ProtobufMessageGen([
        PbNested("ad_info", 1, [
            PbScalar("dummy", 1, IntegerGen()),
            PbScalar("winfoid", 2, IntegerGen()),
        ]),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        # Write to parquet to prevent Catalyst from optimizing away the GetStructField,
        # and to ensure it runs on the GPU.
        df_with_other = df.withColumn("other_struct",
                                      f.struct(f.lit("hello").alias("dummy_str"), f.lit(42).alias("winfoid")))

        path = spark_tmp_path + "/bug3_data.parquet"
        df_with_other.write.mode("overwrite").parquet(path)
        read_df = spark.read.parquet(path)
        
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        
        # We only select decoded.ad_info.winfoid, so dummy is pruned.
        # winfoid gets ordinal 0 in the pruned schema.
        # But for other_struct, winfoid is ordinal 1.
        # GpuGetStructFieldMeta will see "winfoid", query the ThreadLocal, get 0, 
        # and extract ordinal 0 ("hello") for other_winfoid, causing a mismatch!
        return read_df.select(
            decoded.getField("ad_info").getField("winfoid").alias("pb_winfoid"),
            f.col("other_struct").getField("winfoid").alias("other_winfoid")
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_max_depth_descriptor_set_bytes(spark):
    D, fd = _new_proto2_file(spark, "max_depth.proto")
    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL

    # Generate 12 levels of nesting
    for i in range(12, 0, -1):
        msg = D.DescriptorProto.newBuilder().setName(f"Level{i}")
        msg.addField(D.FieldDescriptorProto.newBuilder().setName(f"val{i}").setNumber(1).setLabel(label_opt).setType(D.FieldDescriptorProto.Type.TYPE_INT32).build())
        if i < 12:
            msg.addField(D.FieldDescriptorProto.newBuilder().setName(f"level{i+1}").setNumber(2).setLabel(label_opt).setType(D.FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(f".test.Level{i+1}").build())
        fd.addMessageType(msg.build())

    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())

@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_bug4_max_depth(spark_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "max_depth.desc",
        _build_max_depth_descriptor_set_bytes)
    message_name = "test.Level1"

    # Build the deeply nested data gen spec
    def build_nested_gen(level):
        if level == 12:
            return [PbScalar(f"val{level}", 1, IntegerGen())]
        return [
            PbScalar(f"val{level}", 1, IntegerGen()),
            PbNested(f"level{level+1}", 2, build_nested_gen(level+1))
        ]

    data_gen = ProtobufMessageGen(build_nested_gen(1))

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        # Deep access
        field = decoded
        for i in range(2, 13):
            field = field.getField(f"level{i}")
        return df.select(field.getField("val12").alias("val12"))

    # Depth 12 exceeds GPU max nesting depth (10), so the query should
    # gracefully fall back to CPU. Verify that it still produces correct
    # results (CPU path) without crashing.
    from spark_session import with_cpu_session
    cpu_result = with_cpu_session(lambda spark: run_on_spark(spark).collect())
    assert len(cpu_result) > 0


# ===========================================================================
# Regression tests for known bugs found by code review
# ===========================================================================

def _encode_varint(value):
    """Encode a non-negative integer as a protobuf varint (for hand-crafting test bytes)."""
    out = bytearray()
    v = int(value)
    while True:
        b = v & 0x7F
        v >>= 7
        if v:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)


def _encode_tag(field_number, wire_type):
    return _encode_varint((field_number << 3) | wire_type)


# ---------------------------------------------------------------------------
# Bug 1: BOOL8 truncation — non-canonical bool varint values >= 256
#
# Protobuf spec: bool is a varint; any non-zero value means true.
# CPU decoder (protobuf-java): CodedInputStream.readBool() = readRawVarint64() != 0  →  true
# GPU decoder: extract_varint_kernel writes static_cast<uint8_t>(v).
#   For v = 256, static_cast<uint8_t>(256) == 0  →  false.  BUG.
# ---------------------------------------------------------------------------

@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_bool_noncanonical_varint_scalar(spark_tmp_path, from_protobuf_fn):
    """Scalar bool field encoded as varint 256 should decode as true, not false."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "simple_bool_bug.desc", _build_simple_descriptor_set_bytes)
    message_name = "test.Simple"

    # Hand-craft protobuf wire bytes:
    #   field 1 (bool b): tag = (1<<3)|0 = 0x08, value = varint(256) = 0x80 0x02
    #   field 2 (int32 i32): tag = (2<<3)|0 = 0x10, value = varint(99) = 0x63
    # Varint 256 is a perfectly valid encoding; CPU reads it as true.
    # GPU truncates uint8_t(256) = 0 → false.
    row_bool_256 = _encode_tag(1, 0) + _encode_varint(256) + \
                   _encode_tag(2, 0) + _encode_varint(99)

    # Control row: canonical bool true (varint 1) — should work on both
    row_bool_1 = _encode_tag(1, 0) + _encode_varint(1) + \
                 _encode_tag(2, 0) + _encode_varint(100)

    # Another problematic value: 512 → uint8_t(0) → false
    row_bool_512 = _encode_tag(1, 0) + _encode_varint(512) + \
                   _encode_tag(2, 0) + _encode_varint(101)

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(row_bool_256,), (row_bool_1,), (row_bool_512,)],
            schema="bin binary",
        )
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("b").alias("b"),
            decoded.getField("i32").alias("i32"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_repeated_bool_descriptor_set_bytes(spark):
    """
    message WithRepeatedBool {
        optional int32 id = 1;
        repeated bool flags = 2;
    }
    """
    D, fd = _new_proto2_file(spark, "repeated_bool.proto")
    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL
    label_rep = D.FieldDescriptorProto.Label.LABEL_REPEATED
    msg = D.DescriptorProto.newBuilder().setName("WithRepeatedBool")
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("id").setNumber(1).setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32).build())
    msg.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("flags").setNumber(2).setLabel(label_rep)
            .setType(D.FieldDescriptorProto.Type.TYPE_BOOL).build())
    fd.addMessageType(msg.build())
    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_bool_noncanonical_varint_repeated(spark_tmp_path, from_protobuf_fn):
    """Repeated bool with non-canonical varint values (256, 512) should all decode as true."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "repeated_bool_bug.desc", _build_repeated_bool_descriptor_set_bytes)
    message_name = "test.WithRepeatedBool"

    # Repeated bool field 2 (wire type 0 = varint), unpacked.
    # Three elements: varint(256), varint(1), varint(512)
    # CPU: [true, true, true]
    # GPU (buggy): [false, true, false]  — because uint8(256)=0, uint8(512)=0
    row = (_encode_tag(1, 0) + _encode_varint(42) +
           _encode_tag(2, 0) + _encode_varint(256) +
           _encode_tag(2, 0) + _encode_varint(1) +
           _encode_tag(2, 0) + _encode_varint(512))

    def run_on_spark(spark):
        df = spark.createDataFrame([(row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("flags").alias("flags"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


# ---------------------------------------------------------------------------
# Bug 2: Nested message child field default values are lost
#
# In ProtobufExprShims.scala, addChildFieldsFromStruct always sets
# defaultValue = None for nested children, even when hasDefaultValue = true.
# This means proto2 defaults for fields inside nested messages are never
# passed to the GPU decoder.
#
# CPU: missing child field → proto2 default value
# GPU: missing child field → null
# ---------------------------------------------------------------------------

def _build_nested_with_defaults_descriptor_set_bytes(spark):
    """
    message Inner {
        optional int32  count = 1 [default = 42];
        optional string label = 2 [default = "hello"];
        optional bool   flag  = 3 [default = true];
    }
    message OuterWithNestedDefaults {
        optional int32 id    = 1;
        optional Inner inner = 2;
    }
    """
    D, fd = _new_proto2_file(spark, "nested_defaults.proto")
    label_opt = D.FieldDescriptorProto.Label.LABEL_OPTIONAL

    inner = D.DescriptorProto.newBuilder().setName("Inner")
    inner.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("count").setNumber(1).setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32)
            .setDefaultValue("42").build())
    inner.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("label").setNumber(2).setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_STRING)
            .setDefaultValue("hello").build())
    inner.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("flag").setNumber(3).setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_BOOL)
            .setDefaultValue("true").build())
    fd.addMessageType(inner.build())

    outer = D.DescriptorProto.newBuilder().setName("OuterWithNestedDefaults")
    outer.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("id").setNumber(1).setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_INT32).build())
    outer.addField(
        D.FieldDescriptorProto.newBuilder()
            .setName("inner").setNumber(2).setLabel(label_opt)
            .setType(D.FieldDescriptorProto.Type.TYPE_MESSAGE)
            .setTypeName(".test.Inner").build())
    fd.addMessageType(outer.build())

    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_nested_child_default_values(spark_tmp_path, from_protobuf_fn):
    """Proto2 default values for fields inside nested messages must be honored.

    When `inner` is present but its child fields are absent, the CPU decoder
    returns the proto2 defaults (count=42, label="hello", flag=true).
    The GPU decoder currently returns null for all three because
    defaultValue is never populated for nested children.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "nested_defaults.desc",
        _build_nested_with_defaults_descriptor_set_bytes)
    message_name = "test.OuterWithNestedDefaults"

    # Row 1: outer.id = 10, inner is present but EMPTY (0-length nested message).
    #   Wire: field 1 varint(10), field 2 length-delimited with length 0.
    #   CPU should fill inner.count=42, inner.label="hello", inner.flag=true.
    row_empty_inner = (_encode_tag(1, 0) + _encode_varint(10) +
                       _encode_tag(2, 2) + _encode_varint(0))

    # Row 2: outer.id = 20, inner has only count=7 (label and flag should get defaults).
    inner_partial = _encode_tag(1, 0) + _encode_varint(7)
    row_partial_inner = (_encode_tag(1, 0) + _encode_varint(20) +
                         _encode_tag(2, 2) + _encode_varint(len(inner_partial)) +
                         inner_partial)

    # Row 3: outer.id = 30, inner is fully absent → inner itself is null.
    row_no_inner = _encode_tag(1, 0) + _encode_varint(30)

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(row_empty_inner,), (row_partial_inner,), (row_no_inner,)],
            schema="bin binary",
        )
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("inner").getField("count").alias("inner_count"),
            decoded.getField("inner").getField("label").alias("inner_label"),
            decoded.getField("inner").getField("flag").alias("inner_flag"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


# ===========================================================================
# Deep nested schema pruning tests
#
# These verify that the GPU path correctly prunes nested fields at depth > 2.
# Previously, collectStructFieldReferences only recognized 2-level
# GetStructField chains, so accessing decoded.level2.level3.val3 would
# decode ALL of level3's children instead of only val3.
# ===========================================================================

def _deep_5_level_data_gen():
    return ProtobufMessageGen([
        PbScalar("val1", 1, IntegerGen()),
        PbNested("level2", 2, [
            PbScalar("val2", 1, IntegerGen()),
            PbNested("level3", 2, [
                PbScalar("val3", 1, IntegerGen()),
                PbNested("level4", 2, [
                    PbScalar("val4", 1, IntegerGen()),
                    PbNested("level5", 2, [
                        PbScalar("val5", 1, IntegerGen()),
                    ]),
                ]),
            ]),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_deep_pruning_3_level_leaf(spark_tmp_path, from_protobuf_fn):
    """Access decoded.level2.level3.val3 -- triggers 3-level deep pruning."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "dp3.desc", _build_deep_nested_5_level_descriptor_set_bytes)
    message_name = "test.Level1"
    data_gen = _deep_5_level_data_gen()

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("val1").alias("val1"),
            decoded.getField("level2").getField("level3").getField("val3").alias("deep_val3"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_deep_pruning_5_level_leaf(spark_tmp_path, from_protobuf_fn):
    """Access decoded.level2.level3.level4.level5.val5 -- deepest leaf."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "dp5.desc", _build_deep_nested_5_level_descriptor_set_bytes)
    message_name = "test.Level1"
    data_gen = _deep_5_level_data_gen()

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            _get_field_by_path(decoded, ["level2", "level3", "level4", "level5", "val5"])
                .alias("val5"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_deep_pruning_mixed_depths(spark_tmp_path, from_protobuf_fn):
    """Access leaves at different depths in the same query.

    Select val1 (depth 1), val2 (depth 2), val3 (depth 3), and val5 (depth 5)
    to exercise pruning at every intermediate level simultaneously.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "dp_mix.desc", _build_deep_nested_5_level_descriptor_set_bytes)
    message_name = "test.Level1"
    data_gen = _deep_5_level_data_gen()

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("val1").alias("val1"),
            decoded.getField("level2").getField("val2").alias("val2"),
            _get_field_by_path(decoded, ["level2", "level3", "val3"]).alias("val3"),
            _get_field_by_path(decoded, ["level2", "level3", "level4", "level5", "val5"])
                .alias("val5"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_deep_pruning_sibling_at_depth_3(spark_tmp_path, from_protobuf_fn):
    """At depth 3, access val3 but NOT level4 -- level4 subtree should be pruned."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "dp_sib3.desc", _build_deep_nested_5_level_descriptor_set_bytes)
    message_name = "test.Level1"
    data_gen = _deep_5_level_data_gen()

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            _get_field_by_path(decoded, ["level2", "level3", "val3"]).alias("val3"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_deep_pruning_whole_struct_at_depth_3(spark_tmp_path, from_protobuf_fn):
    """Select the whole level3 struct -- no deep pruning inside level3."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "dp_whole3.desc", _build_deep_nested_5_level_descriptor_set_bytes)
    message_name = "test.Level1"
    data_gen = _deep_5_level_data_gen()

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("level2").getField("level3").alias("level3"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)
