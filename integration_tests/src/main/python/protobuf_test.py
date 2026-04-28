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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_error
from data_gen import (
    BooleanGen, IntegerGen, LongGen, FloatGen, DoubleGen, StringGen, BinaryGen,
    gen_df, idfn
)
from protobuf_data_gen import pb, encode_pb_message
from marks import ignore_order, protobuf_test
from spark_session import with_cpu_session, is_before_spark_340, is_protobuf_runtime_available
import pyspark.sql.functions as f

_protobuf_jars_available = os.environ.get('PROTOBUF_JARS_AVAILABLE', 'false').lower() == 'true'

pytestmark = [
    pytest.mark.premerge_ci_1,
    protobuf_test,
    pytest.mark.skipif(not _protobuf_jars_available, reason="Protobuf JARs not available"),
]


_xfail_gpu_protobuf = pytest.mark.xfail(
    reason="GPU from_protobuf plugin not yet merged (spark-rapids-jni#4107 + spark-rapids#14354)"
)


# Random data generation configurations for simple scalars
_random_scalar_test_configs = [
    # (test_id, data_gen_config)
    ("all_types", [
        pb.field("b", 1, BooleanGen()),
        pb.field("i32", 2, IntegerGen()),
        pb.field("i64", 3, LongGen()),
        pb.field("f32", 4, FloatGen()),
        pb.field("f64", 5, DoubleGen()),
        pb.field("s", 6, StringGen()),
    ]),
    ("integers_edge_cases", [
        pb.field("b", 1, BooleanGen()),
        pb.field("i32", 2, IntegerGen(
            min_val=-2147483648, max_val=2147483647,
            special_cases=[-2147483648, -1, 0, 1, 2147483647])),
        pb.field("i64", 3, LongGen(
            min_val=-9223372036854775808, max_val=9223372036854775807,
            special_cases=[-9223372036854775808, -1, 0, 1, 9223372036854775807])),
        pb.field("f32", 4, FloatGen()),
        pb.field("f64", 5, DoubleGen()),
        pb.field("s", 6, StringGen()),
    ]),
]


# ---------------------------------------------------------------------------
# Shared fixture and helpers to reduce per-test boilerplate
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def from_protobuf_fn():
    """Skip the module if spark-protobuf is not on the classpath at runtime."""
    if not is_protobuf_runtime_available():
        pytest.skip("spark-protobuf runtime not available on the classpath")
    from pyspark.sql.protobuf.functions import from_protobuf
    return from_protobuf


def _setup_protobuf_desc(spark_tmp_path, desc_name, build_fn):
    """Build descriptor bytes via JVM, write to HDFS, return (desc_path, desc_bytes)."""
    desc_path = spark_tmp_path + "/" + desc_name
    desc_bytes = with_cpu_session(build_fn)
    with_cpu_session(
        lambda spark: _write_bytes_to_hadoop_path(spark, desc_path, desc_bytes))
    return desc_path, desc_bytes


def _call_from_protobuf(from_protobuf_fn, col, message_name,
                         desc_path, desc_bytes, options=None):
    """Call from_protobuf using the right API variant.

    Spark 3.5+ PySpark exposes `binaryDescriptorSet` as a keyword parameter.
    Spark 3.4 only has the path-based API. We detect which variant is available
    by inspecting the function signature rather than checking the Spark version,
    so this works correctly even if backports change the parameter name boundary.
    """
    sig = inspect.signature(from_protobuf_fn)
    if "binaryDescriptorSet" in sig.parameters:
        kw = dict(binaryDescriptorSet=bytearray(desc_bytes))
        if options is not None:
            kw["options"] = options
        return from_protobuf_fn(col, message_name, **kw)
    if options is not None:
        return from_protobuf_fn(col, message_name, desc_path, options)
    return from_protobuf_fn(col, message_name, desc_path)


def _encode_varint(value):
    """Encode a non-negative integer as a protobuf varint (for hand-crafting test bytes)."""
    if value < 0:
        raise ValueError(f"_encode_varint only accepts non-negative integers, got: {value}")
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


def _build_simple_descriptor_set_bytes(spark):
    """Build a simple scalar proto2 descriptor."""
    return _build_proto2_descriptor(spark, "simple.proto", [
        _msg("Simple", [
            _field("b", 1, "BOOL"),
            _field("i32", 2, "INT32"),
            _field("i64", 3, "INT64"),
            _field("f32", 4, "FLOAT"),
            _field("f64", 5, "DOUBLE"),
            _field("s", 6, "STRING"),
        ]),
    ])


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
    """Create a proto2 FileDescriptorProto builder with common defaults.

    Tries the unshaded `com.google.protobuf` path first (available when a standalone
    protobuf-java.jar is on the driver classpath).  Falls back to the path shaded inside
    spark-protobuf itself (`org.sparkproject.spark_protobuf.protobuf`), which is always
    present when spark-protobuf_*.jar is loaded.
    """
    jvm = spark.sparkContext._jvm
    try:
        D = jvm.com.google.protobuf.DescriptorProtos
        D.FileDescriptorProto.newBuilder()  # probe: JavaPackage stubs raise here
    except Exception:
        D = jvm.org.sparkproject.spark_protobuf.protobuf.DescriptorProtos
    fd = D.FileDescriptorProto.newBuilder() \
        .setName(name) \
        .setPackage("test")
    try:
        fd = fd.setSyntax("proto2")
    except Exception:
        pass
    return D, fd


def _field(name, number, ftype, label="optional", default=None,
           type_name=None, packed=False):
    """Declarative field spec for `_build_proto2_descriptor`."""
    return {
        "name": name,
        "number": number,
        "type": ftype,
        "label": label,
        "default": default,
        "type_name": type_name,
        "packed": packed,
    }


def _msg(name, fields, enums=None):
    """Declarative message spec for `_build_proto2_descriptor`."""
    return {"name": name, "fields": fields, "enums": enums or []}


def _enum(name, values):
    """Declarative enum spec. Values are `(name, number)` tuples."""
    return {"name": name, "values": values}


def _build_proto2_descriptor(spark, filename, messages, file_enums=None):
    """Build FileDescriptorSet bytes from declarative message and enum specs."""
    D, fd = _new_proto2_file(spark, filename)
    type_map = {
        "BOOL": D.FieldDescriptorProto.Type.TYPE_BOOL,
        "INT32": D.FieldDescriptorProto.Type.TYPE_INT32,
        "INT64": D.FieldDescriptorProto.Type.TYPE_INT64,
        "UINT32": D.FieldDescriptorProto.Type.TYPE_UINT32,
        "UINT64": D.FieldDescriptorProto.Type.TYPE_UINT64,
        "SINT32": D.FieldDescriptorProto.Type.TYPE_SINT32,
        "SINT64": D.FieldDescriptorProto.Type.TYPE_SINT64,
        "FIXED32": D.FieldDescriptorProto.Type.TYPE_FIXED32,
        "FIXED64": D.FieldDescriptorProto.Type.TYPE_FIXED64,
        "SFIXED32": D.FieldDescriptorProto.Type.TYPE_SFIXED32,
        "SFIXED64": D.FieldDescriptorProto.Type.TYPE_SFIXED64,
        "FLOAT": D.FieldDescriptorProto.Type.TYPE_FLOAT,
        "DOUBLE": D.FieldDescriptorProto.Type.TYPE_DOUBLE,
        "STRING": D.FieldDescriptorProto.Type.TYPE_STRING,
        "BYTES": D.FieldDescriptorProto.Type.TYPE_BYTES,
        "MESSAGE": D.FieldDescriptorProto.Type.TYPE_MESSAGE,
        "ENUM": D.FieldDescriptorProto.Type.TYPE_ENUM,
    }
    label_map = {
        "optional": D.FieldDescriptorProto.Label.LABEL_OPTIONAL,
        "repeated": D.FieldDescriptorProto.Label.LABEL_REPEATED,
        "required": D.FieldDescriptorProto.Label.LABEL_REQUIRED,
    }


    def _default_literal(value):
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)


    def _build_enum(enum_spec):
        enum_builder = D.EnumDescriptorProto.newBuilder().setName(enum_spec["name"])
        for value_name, value_number in enum_spec["values"]:
            enum_builder.addValue(
                D.EnumValueDescriptorProto.newBuilder()
                    .setName(value_name)
                    .setNumber(value_number)
                    .build()
            )
        return enum_builder.build()

    packed_options = D.FieldOptions.newBuilder().setPacked(True).build()

    for enum_spec in file_enums or []:
        fd.addEnumType(_build_enum(enum_spec))

    for message_spec in messages:
        message_builder = D.DescriptorProto.newBuilder().setName(message_spec["name"])
        for enum_spec in message_spec["enums"]:
            message_builder.addEnumType(_build_enum(enum_spec))
        for field_spec in message_spec["fields"]:
            field_builder = (
                D.FieldDescriptorProto.newBuilder()
                    .setName(field_spec["name"])
                    .setNumber(field_spec["number"])
                    .setLabel(label_map[field_spec["label"]])
                    .setType(type_map[field_spec["type"]])
            )
            if field_spec["type_name"] is not None:
                field_builder.setTypeName(field_spec["type_name"])
            if field_spec["default"] is not None:
                field_builder.setDefaultValue(_default_literal(field_spec["default"]))
            if field_spec["packed"]:
                field_builder.setOptions(packed_options)
            message_builder.addField(field_builder.build())
        fd.addMessageType(message_builder.build())

    fds = D.FileDescriptorSet.newBuilder().addFile(fd.build()).build()
    return bytes(fds.toByteArray())


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_simple_parquet_binary_round_trip(spark_tmp_path, from_protobuf_fn):
    data_path = spark_tmp_path + "/PROTOBUF_SIMPLE_PARQUET/"
    message_name = "test.Simple"
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "simple.desc", _build_simple_descriptor_set_bytes)

    # Build a DF with scalar columns + binary protobuf column and write to parquet
    row_gen = pb.as_datagen([
        pb.field("b", 1, BooleanGen(nullable=True)),
        pb.field("i32", 2, IntegerGen(nullable=True, min_val=0, max_val=1 << 20)),
        pb.field("i64", 3, LongGen(nullable=True, min_val=0, max_val=1 << 40, special_cases=[])),
        pb.field("f32", 4, FloatGen(nullable=True, no_nans=True)),
        pb.field("f64", 5, DoubleGen(nullable=True, no_nans=True)),
        pb.field("s", 6, StringGen(nullable=True)),
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
        pb.field("keyword", 1, StringGen()),
        pb.field("session_id", 2, StringGen()),
    ]
    type_a_pair_schema = [
        pb.field("record_id", 1, StringGen()),
        pb.field("item_id", 2, StringGen()),
    ]
    schema_type_a = [
        pb.nested_field("query_schema", 1, type_a_query_schema),
        pb.repeated_message_field("pair_schema", 2, type_a_pair_schema, min_len=0, max_len=3),
    ]

    type_b_query_schema = [
        pb.field("profile_tag_id", 1, StringGen()),
        pb.field("entity_id", 2, StringGen()),
    ]
    type_b_style_elem = [
        pb.field("template_id", 1, StringGen()),
        pb.field("material_id", 2, StringGen()),
    ]
    type_b_style_schema = [
        pb.repeated_message_field("values", 1, type_b_style_elem, min_len=0, max_len=3),
    ]
    schema_type_b = [
        pb.nested_field("query_schema", 1, type_b_query_schema),
        pb.repeated_message_field("style_schema", 2, type_b_style_schema, min_len=0, max_len=3),
    ]

    type_c_query_schema = [
        pb.field("keyword", 1, StringGen()),
        pb.field("category", 2, StringGen()),
    ]
    type_c_pair_schema = [
        pb.field("item_id", 1, StringGen()),
        pb.field("target_url", 2, StringGen()),
    ]
    type_c_style_schema = [
        pb.repeated_message_field("values", 1, [], min_len=0, max_len=3),
    ]
    schema_type_c = [
        pb.nested_field("query_schema", 1, type_c_query_schema),
        pb.repeated_message_field("pair_schema", 2, type_c_pair_schema, min_len=0, max_len=3),
        pb.repeated_message_field("style_schema", 3, type_c_style_schema, min_len=0, max_len=3),
    ]
    predictor_schema = [
        pb.nested_field("type_a_schema", 1, schema_type_a),
        pb.nested_field("type_b_schema", 2, schema_type_b),
        pb.nested_field("type_c_schema", 3, schema_type_c),
    ]

    device_req_field = [
        pb.field("os_type", 1, IntegerGen()),
        pb.field("device_id", 2, BinaryGen(min_length=0, max_length=16)),
    ]
    partner_info = [
        pb.field("token", 1, StringGen()),
        pb.field("partner_id", 2, u64()),
    ]
    coordinate = [
        pb.field("x", 1, DoubleGen()),
        pb.field("y", 2, DoubleGen()),
    ]
    location_point = [
        pb.field("frequency", 1, u32()),
        pb.nested_field("coord", 2, coordinate),
        pb.field("timestamp", 3, u64()),
    ]
    change_log = [
        pb.field("value_before", 1, u32()),
        pb.field("parameters", 2, StringGen()),
    ]
    kv_pair = [
        pb.field("key", 1, BinaryGen(min_length=0, max_length=16)),
        pb.field("value", 2, BinaryGen(min_length=0, max_length=16)),
    ]
    style_config = [
        pb.field("style_id", 1, u32()),
        pb.repeated_message_field("kv_pairs", 2, kv_pair, min_len=0, max_len=3),
    ]
    module_a_res = [
        pb.field("route_tag", 1, StringGen()),
        pb.field("status_tag", 2, IntegerGen()),
        pb.field("region_id", 3, u32()),
        pb.repeated_field("experiment_ids", 4, StringGen(), packed=False, min_len=0, max_len=3),
        pb.field("quality_score", 5, DoubleGen()),
        pb.repeated_message_field("location_points", 6, location_point, min_len=0, max_len=3),
        pb.repeated_field("interest_ids", 7, u64(), packed=False, min_len=0, max_len=3),
    ]
    module_a_src_res = [
        pb.field("match_type", 1, u32()),
    ]
    module_a_detail = [
        pb.field("type_code", 1, u32()),
        pb.field("item_id", 2, u64()),
        pb.field("strategy_type", 3, IntegerGen()),
        pb.field("min_value", 4, LongGen()),
        pb.field("target_url", 5, BinaryGen(min_length=0, max_length=24)),
        pb.field("title", 6, StringGen()),
        pb.field("is_valid", 7, BooleanGen()),
        pb.field("score_ratio", 8, FloatGen()),
        pb.repeated_field("template_ids", 9, u32(), packed=False, min_len=0, max_len=3),
        pb.repeated_field("material_ids", 10, u64(), packed=False, min_len=0, max_len=3),
        pb.repeated_message_field("styles", 11, style_config, min_len=0, max_len=3),
        pb.repeated_message_field("change_logs", 12, change_log, min_len=0, max_len=3),
        pb.nested_field("partner_info", 13, partner_info),
        pb.nested_field("predictor_schema", 14, predictor_schema),
    ]

    block_element = [
        pb.field("element_id", 1, u64()),
        pb.repeated_field("ref_ids", 2, u64(), packed=False, min_len=0, max_len=3),
    ]
    block_info = [
        pb.field("block_id", 1, u64()),
        pb.repeated_message_field("elements", 2, block_element, min_len=0, max_len=3),
    ]
    module_b_detail = [
        pb.repeated_field("tags", 1, u32(), packed=False, min_len=0, max_len=3),
        pb.field("item_id", 2, u64()),
        pb.field("name", 3, StringGen()),
        pb.repeated_message_field("blocks", 4, block_info, min_len=0, max_len=3),
    ]

    request_info = [
        pb.field("page_num", 1, u32()),
        pb.field("channel_code", 2, StringGen()),
        pb.repeated_field("experiment_ids", 3, u32(), packed=False, min_len=0, max_len=3),
        pb.field("is_filtered", 4, BooleanGen()),
    ]
    extended_req_info = [
        pb.nested_field("device_req_field", 1, device_req_field),
    ]
    server_added_field = [
        pb.field("region_code", 1, u32()),
        pb.field("flow_type", 2, StringGen()),
        pb.field("filter_result", 3, IntegerGen()),
        pb.repeated_field("hit_rule_list", 4, IntegerGen(), packed=False, min_len=0, max_len=3),
        pb.field("request_time", 5, u64()),
        pb.field("skip_flag", 6, BooleanGen()),
    ]
    basic_info = [
        pb.nested_field("request_info", 1, request_info),
        pb.nested_field("extended_req_info", 2, extended_req_info),
        pb.nested_field("server_added_field", 3, server_added_field),
    ]

    channel_info = [
        pb.field("channel_id", 1, IntegerGen()),
        pb.nested_field("module_a_res", 2, module_a_res),
    ]
    src_channel_info = [
        pb.field("channel_id", 1, IntegerGen()),
        pb.nested_field("module_a_src_res", 2, module_a_src_res),
    ]
    item_detail_field = [
        pb.field("rank", 1, u32()),
        pb.field("record_id", 2, u64()),
        pb.field("keyword", 3, StringGen()),
        pb.nested_field("module_a_detail", 4, module_a_detail),
        pb.nested_field("module_b_detail", 5, module_b_detail),
    ]
    data_source_field = [
        pb.field("source_id", 1, u32()),
        pb.repeated_message_field("src_channel_list", 2, src_channel_info, min_len=0, max_len=3),
        pb.field("billing_name", 3, StringGen()),
        pb.repeated_message_field("item_list", 4, item_detail_field, min_len=0, max_len=3),
        pb.field("is_free", 5, BooleanGen()),
    ]
    log_content = [
        pb.nested_field("basic_info", 1, basic_info),
        pb.repeated_message_field("channel_list", 2, channel_info, min_len=0, max_len=3),
        pb.repeated_message_field("source_list", 3, data_source_field, min_len=0, max_len=3),
    ]

    return [
        pb.field(
            "source", 1,
            IntegerGen(min_val=0, max_val=1, nullable=False, special_cases=[4])),
        pb.field("timestamp", 2, LongGen(min_val=0, max_val=(1 << 50), nullable=False)),
        pb.field("user_id", 3, StringGen()),
        pb.field("account_id", 4, LongGen()),
        pb.field("client_ip", 5, IntegerGen(min_val=0, max_val=0x7FFFFFFF), encoding='fixed'),
        pb.nested_field("log_content", 6, log_content),
    ]


@_xfail_gpu_protobuf
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
    data_gen = pb.as_datagen(_build_main_log_record_fields())


    def run_on_spark(spark):
        generated = gen_df(spark, data_gen).select("bin")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options=options)

        return generated.select(decoded.alias("decoded"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_nested_descriptor_set_bytes(spark):
    """Build a descriptor for a message with a nested child struct."""
    return _build_proto2_descriptor(spark, "nested.proto", [
        _msg("Nested", [_field("x", 1, "INT32")]),
        _msg("WithNested", [
            _field("simple_int", 1, "INT32"),
            _field("simple_str", 2, "STRING"),
            _field("nested_msg", 3, "MESSAGE", type_name=".test.Nested"),
            _field("simple_long", 4, "INT64"),
        ]),
    ])


@_xfail_gpu_protobuf
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

    data_gen = pb.as_datagen([
        pb.field("simple_int", 1, IntegerGen()),
        pb.field("simple_str", 2, StringGen()),
        pb.field("simple_long", 4, LongGen()),
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
    """Build a descriptor with a message-local enum field."""
    return _build_proto2_descriptor(spark, "enum.proto", [
        _msg("WithEnum", [
            _field("color", 1, "ENUM", type_name=".test.WithEnum.Color"),
            _field("count", 2, "INT32"),
            _field("name", 3, "STRING"),
        ], enums=[
            _enum("Color", [("RED", 0), ("GREEN", 1), ("BLUE", 2)]),
        ]),
    ])


def _build_nested_enum_descriptor_set_bytes(spark):
    """Build a descriptor with a nested message that owns an enum field."""
    return _build_proto2_descriptor(spark, "nested_enum.proto", [
        _msg("Detail", [
            _field("status", 1, "ENUM", type_name=".test.Detail.Status"),
            _field("count", 2, "INT32"),
        ], enums=[
            _enum("Status", [("UNKNOWN", 0), ("OK", 1), ("BAD", 2)]),
        ]),
        _msg("WithNestedEnum", [
            _field("id", 1, "INT32"),
            _field("detail", 2, "MESSAGE", type_name=".test.Detail"),
            _field("name", 3, "STRING"),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("enum_case", [
    "as_int",
    "unknown_as_int",
    "as_string",
    "unknown_as_string",
], ids=lambda x: x)
@_xfail_gpu_protobuf
@ignore_order(local=True)
def test_from_protobuf_enum_cases(spark_tmp_path, from_protobuf_fn, enum_case):
    """Parametrized enum decoding tests for int/string modes and unknown values."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "enum.desc", _build_enum_descriptor_set_bytes)
    message_name = "test.WithEnum"
    color_enum = pb.enum_type("Color", [("RED", 0), ("GREEN", 1), ("BLUE", 2)])
    full_schema = pb.schema("WithEnumManual", [
        pb.enum_field("color", 1, color_enum),
        pb.field("count", 2, IntegerGen()),
        pb.field("name", 3, StringGen()),
    ])
    partial_schema = pb.schema("WithEnumManualPartial", [
        pb.enum_field("color", 1, color_enum),
        pb.field("count", 2, IntegerGen()),
    ])

    if enum_case == "as_int":
        rows = [
            (encode_pb_message(full_schema, {"color": 1, "count": 42, "name": "test"}),),
            (encode_pb_message(full_schema, {"color": 0, "count": 100}),),
            (encode_pb_message(full_schema, {"count": 200, "name": "hello"}),),
            (None,),
        ]
        options = {"enums.as.ints": "true"}
        select_mode = "fields3"
    elif enum_case == "unknown_as_int":
        rows = [(encode_pb_message(partial_schema, {"color": 999, "count": 42}),)]
        options = {"enums.as.ints": "true", "mode": "PERMISSIVE"}
        select_mode = "fields2"
    elif enum_case == "as_string":
        rows = [
            (encode_pb_message(full_schema, {"color": 1, "count": 42, "name": "test"}),),
            (encode_pb_message(full_schema, {"color": 0, "count": 100}),),
            (encode_pb_message(full_schema, {"color": 2, "count": 200, "name": "hello"}),),
            (encode_pb_message(full_schema, {"count": 300, "name": "world"}),),
            (None,),
        ]
        options = None
        select_mode = "fields3"
    else:
        rows = [
            (encode_pb_message(partial_schema, {"color": 1, "count": 10}),),
            (encode_pb_message(partial_schema, {"color": 999, "count": 20}),),
            (encode_pb_message(partial_schema, {"color": 2, "count": 30}),),
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


@_xfail_gpu_protobuf
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
    status_enum = pb.enum_type("Status", [("UNKNOWN", 0), ("OK", 1), ("BAD", 2)])
    schema = pb.schema("WithNestedEnumManual", [
        pb.field("id", 1, IntegerGen()),
        pb.nested_field("detail", 2, [
            pb.enum_field("status", 1, status_enum),
            pb.field("count", 2, IntegerGen()),
        ]),
        pb.field("name", 3, StringGen(nullable=True)),
    ])
    rows = [
        (0, encode_pb_message(schema, {"id": 1, "detail": {"status": 1, "count": 10}, "name": "ok"})),
        (1, encode_pb_message(schema, {"id": 2, "detail": {"status": 2, "count": 20}, "name": "bad"})),
        (2, encode_pb_message(schema, {"id": 3, "detail": {"count": 30}, "name": "none"})),
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


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_nested_enum_invalid_permissive_nulls_sibling_fields(
        spark_tmp_path, from_protobuf_fn):
    """Invalid nested enums must null the full row, including sibling fields."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "nested_enum.desc", _build_nested_enum_descriptor_set_bytes)
    message_name = "test.WithNestedEnum"

    detail_valid = (_encode_tag(1, 0) + _encode_varint(1) +
                    _encode_tag(2, 0) + _encode_varint(10))
    row_valid = (_encode_tag(1, 0) + _encode_varint(1) +
                 _encode_tag(2, 2) + _encode_varint(len(detail_valid)) + detail_valid +
                 _encode_tag(3, 2) + _encode_varint(2) + b"ok")
    detail_invalid = (_encode_tag(1, 0) + _encode_varint(999) +
                      _encode_tag(2, 0) + _encode_varint(20))
    row_invalid = (_encode_tag(1, 0) + _encode_varint(2) +
                   _encode_tag(2, 2) + _encode_varint(len(detail_invalid)) + detail_invalid +
                   _encode_tag(3, 2) + _encode_varint(3) + b"bad")


    def run_on_spark(spark):
        df = spark.createDataFrame([(0, row_valid), (1, row_invalid)], schema="idx int, bin binary")
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


def _build_default_with_enum_descriptor_set_bytes(spark):
    """Build a descriptor combining a proto2 default-value field and an enum field.

    Used to verify that PERMISSIVE-mode row nulling due to an invalid enum does not
    partially apply proto2 defaults: the entire struct must be null, not a struct
    whose default-valued fields are populated and whose enum field is null.
    """
    return _build_proto2_descriptor(spark, "default_with_enum.proto", [
        _msg("WithDefaultAndEnum", [
            _field("count", 1, "INT32", default=42),
            _field("status", 2, "ENUM", type_name=".test.WithDefaultAndEnum.Status"),
        ], enums=[
            _enum("Status", [("UNKNOWN", 0), ("ACTIVE", 1), ("INACTIVE", 2)]),
        ]),
    ])


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_default_permissive_invalid_enum_nulls_default_field(
        spark_tmp_path, from_protobuf_fn):
    """Proto2 default values must not appear when PERMISSIVE nulls a row due to invalid enum.

    Row 0: count absent, enum valid (ACTIVE=1)  -> count decodes to default 42.
    Row 1: count present (100), enum invalid (99) -> entire struct is null in PERMISSIVE,
           count is null (not 42) and status is null.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "default_with_enum.desc",
        _build_default_with_enum_descriptor_set_bytes)
    message_name = "test.WithDefaultAndEnum"

    # count absent, status=ACTIVE(1): count should default to 42
    row_valid_enum = _encode_tag(2, 0) + _encode_varint(1)
    # count=100, status=99 (unknown): entire struct null in PERMISSIVE
    row_invalid_enum = (
        _encode_tag(1, 0) + _encode_varint(100) +
        _encode_tag(2, 0) + _encode_varint(99)
    )

    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(0, row_valid_enum), (1, row_invalid_enum)],
            schema="idx int, bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        return df.select(
            f.col("idx"),
            decoded.isNull().alias("decoded_is_null"),
            decoded.getField("count").alias("count"),
            decoded.getField("status").alias("status")).orderBy("idx")

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_nested_enum_default_struct_descriptor_set_bytes(spark):
    """Build a descriptor with file-level enum defaults inside a nested struct."""
    return _build_proto2_descriptor(
        spark,
        "nested_enum_defaults.proto",
        [
            _msg("CommonWithEnumDefaults", [
                _field("logid", 1, "STRING"),
                _field("language", 2, "ENUM", type_name=".test.Language", default="EN"),
                _field("code_type", 3, "ENUM", type_name=".test.CodeType", default="UTF8"),
            ]),
            _msg("OuterWithCommonEnumDefaults", [
                _field("id", 1, "INT32"),
                _field("common", 2, "MESSAGE", type_name=".test.CommonWithEnumDefaults"),
            ]),
        ],
        file_enums=[
            _enum("Language", [
                ("UNKNOWN_LANGUAGE", 0),
                ("EN", 1),
                ("ZH", 2),
            ]),
            _enum("CodeType", [
                ("UNKNOWN_CODE", 0),
                ("UTF8", 1),
                ("GBK", 2),
            ]),
        ],
    )


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_nested_enum_defaults_string_mode(spark_tmp_path, from_protobuf_fn):
    """Regression test: nested enum defaults in string mode must not crash GPU planning.

    Selecting the whole nested struct forces GPU planning to visit all of its children,
    including enum fields whose proto2 defaults are represented as EnumValueDescriptor
    objects by spark-protobuf.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "nested_enum_defaults.desc",
        _build_nested_enum_default_struct_descriptor_set_bytes)
    message_name = "test.OuterWithCommonEnumDefaults"

    common_with_logid = (_encode_tag(1, 2) + _encode_varint(5) + b"log-1")
    row_logid_only = (_encode_tag(1, 0) + _encode_varint(1) +
                      _encode_tag(2, 2) + _encode_varint(len(common_with_logid)) +
                      common_with_logid)
    row_empty_common = (_encode_tag(1, 0) + _encode_varint(2) +
                        _encode_tag(2, 2) + _encode_varint(0))
    row_no_common = _encode_tag(1, 0) + _encode_varint(3)


    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(row_logid_only,), (row_empty_common,), (row_no_common,)],
            schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("common").alias("common"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_repeated_enum_descriptor_set_bytes(spark):
    """Build a descriptor with a repeated enum field."""
    return _build_proto2_descriptor(spark, "repeated_enum.proto", [
        _msg("WithRepeatedEnum", [
            _field("id", 1, "INT32"),
            _field("priority", 2, "ENUM", label="repeated",
                   type_name=".test.WithRepeatedEnum.Priority"),
        ], enums=[
            _enum("Priority", [("UNKNOWN", 0), ("FOO", 1), ("BAR", 2)]),
        ]),
    ])


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_enum_invalid_permissive(spark_tmp_path, from_protobuf_fn):
    """Repeated enum with invalid values in PERMISSIVE mode.

    Row with any invalid enum value should become null (entire struct row),
    while rows with all-valid enum values decode normally.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "repeated_enum.desc", _build_repeated_enum_descriptor_set_bytes)
    message_name = "test.WithRepeatedEnum"

    row1 = (_encode_tag(1, 0) + _encode_varint(1) +
            _encode_tag(2, 0) + _encode_varint(0) +
            _encode_tag(2, 0) + _encode_varint(1) +
            _encode_tag(2, 0) + _encode_varint(2))
    row2 = (_encode_tag(1, 0) + _encode_varint(2) +
            _encode_tag(2, 0) + _encode_varint(1) +
            _encode_tag(2, 0) + _encode_varint(99))
    row3 = (_encode_tag(1, 0) + _encode_varint(3) +
            _encode_tag(2, 0) + _encode_varint(0))


    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(row1,), (row2,), (row3,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        return df.select(decoded.alias("decoded"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_enum_string_invalid_permissive_nulls_sibling_fields(
        spark_tmp_path, from_protobuf_fn):
    """Repeated enum string mode must null sibling fields when any enum value is invalid."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "repeated_enum.desc", _build_repeated_enum_descriptor_set_bytes)
    message_name = "test.WithRepeatedEnum"

    row_valid = (_encode_tag(1, 0) + _encode_varint(1) +
                 _encode_tag(2, 0) + _encode_varint(0) +
                 _encode_tag(2, 0) + _encode_varint(2))
    row_invalid = (_encode_tag(1, 0) + _encode_varint(2) +
                   _encode_tag(2, 0) + _encode_varint(1) +
                   _encode_tag(2, 0) + _encode_varint(99))


    def run_on_spark(spark):
        df = spark.createDataFrame([(0, row_valid), (1, row_invalid)], schema="idx int, bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        return df.select(
            f.col("idx"),
            decoded.isNull().alias("decoded_is_null"),
            decoded.getField("id").alias("id"),
            decoded.getField("priority").alias("priority")).orderBy("idx")

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_repeated_message_enum_descriptor_set_bytes(spark):
    """Build a descriptor for repeated messages whose child struct contains an enum."""
    return _build_proto2_descriptor(spark, "repeated_message_enum.proto", [
        _msg("ItemWithPriority", [
            _field("priority", 1, "ENUM", type_name=".test.ItemWithPriority.Priority"),
            _field("count", 2, "INT32"),
        ], enums=[
            _enum("Priority", [("UNKNOWN", 0), ("FOO", 1), ("BAR", 2)]),
        ]),
        _msg("ContainerWithPriorityItems", [
            _field("id", 1, "INT32"),
            _field("items", 2, "MESSAGE", label="repeated", type_name=".test.ItemWithPriority"),
            _field("title", 3, "STRING"),
        ]),
    ])


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_message_child_enum_string(
        spark_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "repeated_message_enum.desc",
        _build_repeated_message_enum_descriptor_set_bytes)
    message_name = "test.ContainerWithPriorityItems"

    item_foo = (_encode_tag(1, 0) + _encode_varint(1) +
                _encode_tag(2, 0) + _encode_varint(10))
    item_bar = (_encode_tag(1, 0) + _encode_varint(2) +
                _encode_tag(2, 0) + _encode_varint(20))
    row_with_items = (_encode_tag(1, 0) + _encode_varint(1) +
                      _encode_tag(2, 2) + _encode_varint(len(item_foo)) + item_foo +
                      _encode_tag(2, 2) + _encode_varint(len(item_bar)) + item_bar +
                      _encode_tag(3, 2) + _encode_varint(5) + b"hello")
    row_no_items = (_encode_tag(1, 0) + _encode_varint(2) +
                    _encode_tag(3, 2) + _encode_varint(5) + b"empty")


    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(row_with_items,), (row_no_items,), (None,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("title").alias("title"),
            decoded.getField("items").getField("priority").alias("priorities"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_message_child_enum_string_invalid_permissive(
        spark_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "repeated_message_enum.desc",
        _build_repeated_message_enum_descriptor_set_bytes)
    message_name = "test.ContainerWithPriorityItems"

    item_valid = (_encode_tag(1, 0) + _encode_varint(1) +
                  _encode_tag(2, 0) + _encode_varint(10))
    item_invalid = (_encode_tag(1, 0) + _encode_varint(99) +
                    _encode_tag(2, 0) + _encode_varint(20))
    row_valid = (_encode_tag(1, 0) + _encode_varint(1) +
                 _encode_tag(2, 2) + _encode_varint(len(item_valid)) + item_valid +
                 _encode_tag(3, 2) + _encode_varint(2) + b"ok")
    row_invalid = (_encode_tag(1, 0) + _encode_varint(2) +
                   _encode_tag(2, 2) + _encode_varint(len(item_invalid)) + item_invalid +
                   _encode_tag(3, 2) + _encode_varint(3) + b"bad")


    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(0, row_valid), (1, row_invalid)], schema="idx int, bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        return df.select(
            f.col("idx"),
            decoded.isNull().alias("decoded_is_null"),
            decoded.getField("id").alias("id"),
            decoded.getField("title").alias("title"),
            decoded.getField("items").getField("priority").getItem(0).alias("priority0")
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_nested_repeated_enum_descriptor_set_bytes(spark):
    """Build a descriptor for a nested struct containing a repeated enum field."""
    return _build_proto2_descriptor(spark, "nested_repeated_enum.proto", [
        _msg("InnerWithRepeatedPriority", [
            _field("priority", 1, "ENUM", label="repeated",
                   type_name=".test.InnerWithRepeatedPriority.Priority"),
            _field("count", 2, "INT32"),
        ], enums=[
            _enum("Priority", [("UNKNOWN", 0), ("FOO", 1), ("BAR", 2)]),
        ]),
        _msg("OuterWithNestedRepeatedEnum", [
            _field("id", 1, "INT32"),
            _field("inner", 2, "MESSAGE", type_name=".test.InnerWithRepeatedPriority"),
            _field("name", 3, "STRING"),
        ]),
    ])


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_nested_repeated_enum_string(
        spark_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "nested_repeated_enum.desc",
        _build_nested_repeated_enum_descriptor_set_bytes)
    message_name = "test.OuterWithNestedRepeatedEnum"

    inner = (_encode_tag(1, 0) + _encode_varint(0) +
             _encode_tag(1, 0) + _encode_varint(2) +
             _encode_tag(1, 0) + _encode_varint(1) +
             _encode_tag(2, 0) + _encode_varint(7))
    row_with_inner = (_encode_tag(1, 0) + _encode_varint(1) +
                      _encode_tag(2, 2) + _encode_varint(len(inner)) + inner +
                      _encode_tag(3, 2) + _encode_varint(5) + b"hello")
    row_no_inner = (_encode_tag(1, 0) + _encode_varint(2) +
                    _encode_tag(3, 2) + _encode_varint(4) + b"none")


    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(row_with_inner,), (row_no_inner,), (None,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("id").alias("id"),
            decoded.getField("name").alias("name"),
            decoded.getField("inner").getField("priority").alias("priorities"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_nested_repeated_enum_string_invalid_permissive(
        spark_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "nested_repeated_enum.desc",
        _build_nested_repeated_enum_descriptor_set_bytes)
    message_name = "test.OuterWithNestedRepeatedEnum"

    inner_valid = (_encode_tag(1, 0) + _encode_varint(1) +
                   _encode_tag(1, 0) + _encode_varint(2) +
                   _encode_tag(2, 0) + _encode_varint(7))
    inner_invalid = (_encode_tag(1, 0) + _encode_varint(1) +
                     _encode_tag(1, 0) + _encode_varint(99) +
                     _encode_tag(2, 0) + _encode_varint(9))
    row_valid = (_encode_tag(1, 0) + _encode_varint(1) +
                 _encode_tag(2, 2) + _encode_varint(len(inner_valid)) + inner_valid +
                 _encode_tag(3, 2) + _encode_varint(2) + b"ok")
    row_invalid = (_encode_tag(1, 0) + _encode_varint(2) +
                   _encode_tag(2, 2) + _encode_varint(len(inner_invalid)) + inner_invalid +
                   _encode_tag(3, 2) + _encode_varint(3) + b"bad")


    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(0, row_valid), (1, row_invalid)], schema="idx int, bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        return df.select(
            f.col("idx"),
            decoded.isNull().alias("decoded_is_null"),
            decoded.getField("id").alias("id"),
            decoded.getField("name").alias("name"),
            decoded.getField("inner").getField("priority").getItem(0).alias("priority0"),
            decoded.getField("inner").getField("priority").getItem(1).alias("priority1")
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_required_field_descriptor_set_bytes(spark):
    """Build a descriptor for top-level proto2 required-field validation."""
    return _build_proto2_descriptor(spark, "required.proto", [
        _msg("WithRequired", [
            _field("id", 1, "INT64", label="required"),
            _field("name", 2, "STRING"),
            _field("count", 3, "INT32"),
        ]),
    ])


def _build_nested_required_field_descriptor_set_bytes(spark):
    """Build a descriptor with a nested proto2 required field."""
    return _build_proto2_descriptor(spark, "nested_required.proto", [
        _msg("InnerRequired", [
            _field("child_id", 1, "INT32", label="required"),
            _field("note", 2, "STRING"),
        ]),
        _msg("WithNestedRequired", [
            _field("id", 1, "INT64"),
            _field("inner", 2, "MESSAGE", type_name=".test.InnerRequired"),
            _field("name", 3, "STRING"),
        ]),
    ])


@_xfail_gpu_protobuf
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
    required_schema = pb.schema("WithRequiredManual", [
        pb.field("id", 1, LongGen()),
        pb.field("name", 2, StringGen()),
        pb.field("count", 3, IntegerGen()),
    ])
    test_data_row0 = encode_pb_message(required_schema, {"id": 100, "name": "test", "count": 42})
    test_data_row1 = encode_pb_message(required_schema, {"id": 200})


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


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_required_field_missing_failfast(spark_tmp_path, from_protobuf_fn):
    """Required-field violations should fail consistently on CPU and GPU in FAILFAST mode."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "required.desc", _build_required_field_descriptor_set_bytes)
    message_name = "test.WithRequired"

    missing_required_row = _encode_tag(2, 2) + _encode_varint(4) + b"oops"


    def run_on_spark(spark):
        df = spark.createDataFrame([(missing_required_row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(decoded.alias("decoded")).collect()

    assert_gpu_and_cpu_error(run_on_spark, conf={}, error_message="Malformed")


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_required_field_missing_permissive(spark_tmp_path, from_protobuf_fn):
    """Required-field violations should null the whole row in PERMISSIVE mode."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "required.desc", _build_required_field_descriptor_set_bytes)
    message_name = "test.WithRequired"

    missing_required_row = _encode_tag(2, 2) + _encode_varint(5) + b"hello"


    def run_on_spark(spark):
        df = spark.createDataFrame([(missing_required_row,)], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        return df.select(
            decoded.isNull().alias("decoded_is_null"),
            decoded.getField("id").alias("id"),
            decoded.getField("name").alias("name"),
            decoded.getField("count").alias("count")
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_nested_required_field_missing_permissive(
        spark_tmp_path, from_protobuf_fn):
    """Observe CPU/GPU parity when a nested proto2 required field is missing in PERMISSIVE mode."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "nested_required.desc", _build_nested_required_field_descriptor_set_bytes)
    message_name = "test.WithNestedRequired"

    inner_valid = (_encode_tag(1, 0) + _encode_varint(7) +
                   _encode_tag(2, 2) + _encode_varint(2) + b"ok")
    inner_missing_required = _encode_tag(2, 2) + _encode_varint(4) + b"oops"

    row_valid = (_encode_tag(1, 0) + _encode_varint(100) +
                 _encode_tag(2, 2) + _encode_varint(len(inner_valid)) + inner_valid +
                 _encode_tag(3, 2) + _encode_varint(5) + b"valid")
    row_missing_required = (_encode_tag(1, 0) + _encode_varint(200) +
                            _encode_tag(2, 2) + _encode_varint(len(inner_missing_required)) +
                            inner_missing_required +
                            _encode_tag(3, 2) + _encode_varint(7) + b"missing")
    row_missing_inner = (_encode_tag(1, 0) + _encode_varint(300) +
                         _encode_tag(3, 2) + _encode_varint(8) + b"no_inner")


    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(0, row_valid), (1, row_missing_required), (2, row_missing_inner)],
            schema="idx int, bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        inner = decoded.getField("inner")
        return df.select(
            f.col("idx"),
            decoded.isNull().alias("decoded_is_null"),
            decoded.getField("id").alias("id"),
            decoded.getField("name").alias("name"),
            inner.isNull().alias("inner_is_null"),
            inner.getField("child_id").alias("child_id"),
            inner.getField("note").alias("note")
        ).orderBy("idx")

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_default_value_descriptor_set_bytes(spark):
    """Build a descriptor for proto2 scalar default-value behavior."""
    return _build_proto2_descriptor(spark, "defaults.proto", [
        _msg("WithDefaults", [
            _field("count", 1, "INT32", default=42),
            _field("name", 2, "STRING", default="unknown"),
            _field("flag", 3, "BOOL", default=True),
        ]),
    ])


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("default_case", [
    "field_present",
    "missing_fields",
    "string_default_only",
], ids=lambda x: x)
@_xfail_gpu_protobuf
@ignore_order(local=True)
def test_from_protobuf_default_values_cases(spark_tmp_path, from_protobuf_fn, default_case):
    """Parametrized tests for proto2 default-value behavior."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "defaults.desc", _build_default_value_descriptor_set_bytes)
    message_name = "test.WithDefaults"

    defaults_schema = pb.schema("WithDefaultsManual", [
        pb.field("count", 1, IntegerGen(), default=42),
        pb.field("name", 2, StringGen(), default="unknown"),
        pb.field("flag", 3, BooleanGen(), default=True),
    ])

    if default_case == "field_present":
        rows = [(encode_pb_message(defaults_schema, {"count": 100, "name": "test", "flag": False}),)]
        select_mode = "all"
    elif default_case == "missing_fields":
        rows = [
            (encode_pb_message(defaults_schema, {}),),
            (encode_pb_message(defaults_schema, {"count": 100}),),
        ]
        select_mode = "all"
    else:
        rows = [(encode_pb_message(defaults_schema, {"count": 42, "flag": True}),)]
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
    """Build a descriptor covering all scalar wire encodings used in tests."""
    return _build_proto2_descriptor(spark, "all_scalars.proto", [
        _msg("AllScalars", [
            _field("b", 1, "BOOL"),
            _field("i32", 2, "INT32"),
            _field("i64", 3, "INT64"),
            _field("f32", 4, "FLOAT"),
            _field("f64", 5, "DOUBLE"),
            _field("s", 6, "STRING"),
            _field("si32", 7, "SINT32"),
            _field("si64", 8, "SINT64"),
            _field("fx32", 9, "FIXED32"),
            _field("fx64", 10, "FIXED64"),
        ]),
    ])


def _build_scalar_bytes_descriptor_set_bytes(spark):
    """Build a descriptor for a single optional bytes field."""
    return _build_proto2_descriptor(spark, "scalar_bytes.proto", [
        _msg("ScalarBytes", [_field("payload", 1, "BYTES")]),
    ])


def _scalar_test_id(config):
    """Generate stable test ID using only the first element (test name)."""
    return config[0] if isinstance(config, tuple) else str(config)


@_xfail_gpu_protobuf
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

    data_gen = pb.as_datagen(field_configs)


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


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_all_scalar_types(spark_tmp_path, from_protobuf_fn):
    """Decode all scalar wire encodings together in one message."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "all_scalars.desc", _build_all_scalars_descriptor_set_bytes)
    message_name = "test.AllScalars"

    data_gen = pb.as_datagen([
        pb.field("b", 1, BooleanGen()),
        pb.field("i32", 2, IntegerGen()),
        pb.field("i64", 3, LongGen()),
        pb.field("f32", 4, FloatGen()),
        pb.field("f64", 5, DoubleGen()),
        pb.field("s", 6, StringGen()),
        pb.field("si32", 7, IntegerGen(
            special_cases=[-1, 0, 1, -2147483648, 2147483647]), encoding='zigzag'),
        pb.field("si64", 8, LongGen(
            special_cases=[-1, 0, 1, -9223372036854775808, 9223372036854775807]),
            encoding='zigzag'),
        pb.field("fx32", 9, IntegerGen(
            special_cases=[0, 1, -1, 2147483647, -2147483648]), encoding='fixed'),
        pb.field("fx64", 10, LongGen(
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


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_scalar_bytes(spark_tmp_path, from_protobuf_fn):
    """Decode optional bytes scalar field, including empty bytes."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "scalar_bytes.desc", _build_scalar_bytes_descriptor_set_bytes)
    message_name = "test.ScalarBytes"

    data_gen = pb.as_datagen([
        pb.field("payload", 1, BinaryGen(min_length=0, max_length=16)),
    ])


    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(decoded.getField("payload").alias("payload"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@_xfail_gpu_protobuf
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
    """Build a descriptor for an optional id plus repeated int32 values."""
    return _build_proto2_descriptor(spark, "repeated_int.proto", [
        _msg("WithRepeatedInt", [
            _field("id", 1, "INT32"),
            _field("values", 2, "INT32", label="repeated"),
        ]),
    ])


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_int32(spark_tmp_path, from_protobuf_fn):
    """
    Test decoding repeated int32 field (ArrayType of integers).
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "repeated_int.desc", _build_repeated_int_descriptor_set_bytes)
    message_name = "test.WithRepeatedInt"

    data_gen = pb.as_datagen([
        pb.field("id", 1, IntegerGen()),
        pb.repeated_field("values", 2, IntegerGen(), min_len=0, max_len=10),
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
    """Build a descriptor for an optional string plus repeated strings."""
    return _build_proto2_descriptor(spark, "repeated_string.proto", [
        _msg("WithRepeatedString", [
            _field("name", 1, "STRING"),
            _field("tags", 2, "STRING", label="repeated"),
        ]),
    ])


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_string(spark_tmp_path, from_protobuf_fn):
    """
    Test decoding repeated string field (ArrayType of strings).
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "repeated_string.desc", _build_repeated_string_descriptor_set_bytes)
    message_name = "test.WithRepeatedString"

    data_gen = pb.as_datagen([
        pb.field("name", 1, StringGen()),
        pb.repeated_field("tags", 2, StringGen(), min_len=0, max_len=5),
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


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_nested_message(spark_tmp_path, from_protobuf_fn):
    """
    Test decoding nested message field (StructType).
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "nested.desc", _build_nested_descriptor_set_bytes)
    message_name = "test.WithNested"

    data_gen = pb.as_datagen([
        pb.field("simple_int", 1, IntegerGen()),
        pb.field("simple_str", 2, StringGen(nullable=True)),
        pb.nested_field("nested_msg", 3, [pb.field("x", 1, IntegerGen())]),
        pb.field("simple_long", 4, LongGen()),
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


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_nested_message_field_access_with_batch_merge(
        spark_tmp_path, from_protobuf_fn):
    """
    Test accessing fields within nested message, with protobuf post-project batch merge
    enabled. This protects correctness when schema-projected protobuf output is coalesced.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "nested.desc", _build_nested_descriptor_set_bytes)
    message_name = "test.WithNested"

    data_gen = pb.as_datagen([
        pb.field("simple_int", 1, IntegerGen()),
        pb.field("simple_str", 2, StringGen(nullable=True)),
        pb.nested_field("nested_msg", 3, [pb.field("x", 1, IntegerGen())]),
        pb.field("simple_long", 4, LongGen()),
    ])


    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("simple_int").alias("simple_int"),
            decoded.getField("nested_msg").getField("x").alias("nested_x"),
        )

    assert_gpu_and_cpu_are_equal_collect(
        run_on_spark,
        conf={"spark.rapids.sql.protobuf.batchMergeAfterProject.enabled": "true"})


def _build_deep_nested_descriptor_set_bytes(spark):
    """Build a descriptor for three levels of nested messages."""
    return _build_proto2_descriptor(spark, "deep_nested.proto", [
        _msg("Inner", [_field("value", 1, "INT32")]),
        _msg("Middle", [
            _field("name", 1, "STRING"),
            _field("inner", 2, "MESSAGE", type_name=".test.Inner"),
        ]),
        _msg("Outer", [
            _field("id", 1, "INT32"),
            _field("middle", 2, "MESSAGE", type_name=".test.Middle"),
        ]),
    ])


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_deep_nested(spark_tmp_path, from_protobuf_fn):
    """
    Test decoding deeply nested messages (3 levels).
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "deep_nested.desc", _build_deep_nested_descriptor_set_bytes)
    message_name = "test.Outer"

    data_gen = pb.as_datagen([
        pb.field("id", 1, IntegerGen()),
        pb.nested_field("middle", 2, [
            pb.field("name", 1, StringGen()),
            pb.nested_field("inner", 2, [pb.field("value", 1, IntegerGen())]),
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
    """Build a descriptor for an array-of-struct repeated message field."""
    return _build_proto2_descriptor(spark, "repeated_message.proto", [
        _msg("Item", [
            _field("id", 1, "INT32"),
            _field("name", 2, "STRING"),
        ]),
        _msg("Container", [
            _field("title", 1, "STRING"),
            _field("items", 2, "MESSAGE", label="repeated", type_name=".test.Item"),
        ]),
    ])


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_message(spark_tmp_path, from_protobuf_fn):
    """
    Test decoding repeated message field (ArrayType of StructType).
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "repeated_message.desc", _build_repeated_message_descriptor_set_bytes)
    message_name = "test.Container"

    data_gen = pb.as_datagen([
        pb.field("title", 1, StringGen()),
        pb.repeated_message_field("items", 2, [
            pb.field("id", 1, IntegerGen()),
            pb.field("name", 2, StringGen()),
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
    """Build a descriptor for a nested message that contains a repeated field."""
    return _build_proto2_descriptor(spark, "nested_with_repeated.proto", [
        _msg("NestedWithRepeated", [
            _field("name", 1, "STRING"),
            _field("values", 2, "INT32", label="repeated"),
            _field("count", 3, "INT32"),
        ]),
        _msg("OuterWithNestedRepeated", [
            _field("id", 1, "INT32"),
            _field("nested", 2, "MESSAGE", type_name=".test.NestedWithRepeated"),
        ]),
    ])


@_xfail_gpu_protobuf
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

    data_gen = pb.as_datagen([
        pb.field("id", 1, IntegerGen()),
        pb.nested_field("nested", 2, [
            pb.field("name", 1, StringGen()),
            pb.repeated_field("values", 2, IntegerGen(), min_len=0, max_len=5),
            pb.field("count", 3, IntegerGen()),
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
    """Build a descriptor for repeated messages that each contain a nested struct."""
    return _build_proto2_descriptor(spark, "repeated_with_nested.proto", [
        _msg("Inner", [_field("value", 1, "INT32")]),
        _msg("ItemWithNested", [
            _field("id", 1, "INT32"),
            _field("inner", 2, "MESSAGE", type_name=".test.Inner"),
            _field("name", 3, "STRING"),
        ]),
        _msg("ContainerWithNestedItems", [
            _field("title", 1, "STRING"),
            _field("items", 2, "MESSAGE", label="repeated", type_name=".test.ItemWithNested"),
        ]),
    ])


@_xfail_gpu_protobuf
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

    data_gen = pb.as_datagen([
        pb.field("title", 1, StringGen()),
        pb.repeated_message_field("items", 2, [
            pb.field("id", 1, IntegerGen()),
            pb.nested_field("inner", 2, [pb.field("value", 1, IntegerGen())]),
            pb.field("name", 3, StringGen()),
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
    """Build a descriptor for packed repeated numeric and bool fields."""
    return _build_proto2_descriptor(spark, "packed_repeated.proto", [
        _msg("WithPackedRepeated", [
            _field("id", 1, "INT32"),
            _field("int_values", 2, "INT32", label="repeated", packed=True),
            _field("double_values", 3, "DOUBLE", label="repeated", packed=True),
            _field("bool_values", 4, "BOOL", label="repeated", packed=True),
        ]),
    ])


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_packed_repeated(spark_tmp_path, from_protobuf_fn):
    """
    Test packed repeated fields (int, double, bool) using DataGen.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "packed.desc", _build_packed_repeated_descriptor_set_bytes)
    message_name = "test.WithPackedRepeated"

    data_gen = pb.as_datagen([
        pb.field("id", 1, IntegerGen()),
        pb.repeated_field("int_values", 2, IntegerGen(), packed=True, min_len=0, max_len=10),
        pb.repeated_field("double_values", 3, DoubleGen(), packed=True, min_len=0, max_len=5),
        pb.repeated_field("bool_values", 4, BooleanGen(), packed=True, min_len=0, max_len=5),
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
    """Build a descriptor with repeated fields of several scalar types."""
    return _build_proto2_descriptor(spark, "repeated_all.proto", [
        _msg("WithRepeatedAllTypes", [
            _field("id", 1, "INT32"),
            _field("long_values", 2, "INT64", label="repeated"),
            _field("float_values", 3, "FLOAT", label="repeated"),
            _field("double_values", 4, "DOUBLE", label="repeated"),
            _field("bool_values", 5, "BOOL", label="repeated"),
            _field("bytes_values", 6, "BYTES", label="repeated"),
        ]),
    ])


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_repeated_all_types(spark_tmp_path, from_protobuf_fn):
    """Test repeated fields of various types (int64, float, double, bool, bytes) using DataGen."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "repeated_all.desc", _build_repeated_all_types_descriptor_set_bytes)
    message_name = "test.WithRepeatedAllTypes"

    data_gen = pb.as_datagen([
        pb.field("id", 1, IntegerGen()),
        pb.repeated_field("long_values", 2, LongGen()),
        pb.repeated_field("float_values", 3, FloatGen()),
        pb.repeated_field("double_values", 4, DoubleGen()),
        pb.repeated_field("bool_values", 5, BooleanGen()),
        pb.repeated_field("bytes_values", 6, BinaryGen()),
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


@_xfail_gpu_protobuf
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

    data_gen = pb.as_datagen([
        pb.field("id", 1, IntegerGen()),
        pb.repeated_field("values", 2, IntegerGen(), min_len=500, max_len=1000),
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
    """Build a descriptor for zigzag and signed fixed-width integer fields."""
    return _build_proto2_descriptor(spark, "signed_int.proto", [
        _msg("WithSignedInts", [
            _field("si32", 1, "SINT32"),
            _field("si64", 2, "SINT64"),
            _field("sf32", 3, "SFIXED32"),
            _field("sf64", 4, "SFIXED64"),
        ]),
    ])


@_xfail_gpu_protobuf
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

    data_gen = pb.as_datagen([
        pb.field("si32", 1, IntegerGen(
            special_cases=[-1, 0, 1, -2147483648, 2147483647]), encoding='zigzag'),
        pb.field("si64", 2, LongGen(
            special_cases=[-1, 0, 1, -9223372036854775808, 9223372036854775807]),
            encoding='zigzag'),
        pb.field("sf32", 3, IntegerGen(
            special_cases=[0, 1, -1, 2147483647, -2147483648]), encoding='fixed'),
        pb.field("sf64", 4, LongGen(
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
    """Build a descriptor for fixed-width integer fields."""
    return _build_proto2_descriptor(spark, "fixed_int.proto", [
        _msg("WithFixedInts", [
            _field("fx32", 1, "FIXED32"),
            _field("fx64", 2, "FIXED64"),
        ]),
    ])


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_fixed_integers(spark_tmp_path, from_protobuf_fn):
    """
    Test decoding fixed-width unsigned integer types.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "fixed.desc", _build_fixed_int_descriptor_set_bytes)
    message_name = "test.WithFixedInts"

    data_gen = pb.as_datagen([
        pb.field("fx32", 1, IntegerGen(
            special_cases=[0, 1, -1, 2147483647, -2147483648]), encoding='fixed'),
        pb.field("fx64", 2, LongGen(
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


def _build_unsigned_int_descriptor_set_bytes(spark):
    """Build a descriptor for unsigned varint integer fields (uint32, uint64)."""
    return _build_proto2_descriptor(spark, "unsigned_int.proto", [
        _msg("WithUnsignedInts", [
            _field("u32", 1, "UINT32"),
            _field("u64", 2, "UINT64"),
        ]),
    ])


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_unsigned_integers(spark_tmp_path, from_protobuf_fn):
    """Test decoding uint32 and uint64 fields.

    uint32 maps to Spark IntegerType and uint64 to LongType. Values are kept
    non-negative so they fit within both the unsigned wire encoding and the
    signed Spark column type.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "unsigned_int.desc", _build_unsigned_int_descriptor_set_bytes)
    message_name = "test.WithUnsignedInts"

    data_gen = pb.as_datagen([
        pb.uint32("u32", 1, gen=IntegerGen(min_val=0,
                                           special_cases=[0, 1, 2147483647])),
        pb.uint64("u64", 2, gen=LongGen(min_val=0,
                                        special_cases=[0, 1, 9223372036854775807])),
    ])

    def run_on_spark(spark):
        df = gen_df(spark, data_gen)
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("u32").alias("u32"),
            decoded.getField("u64").alias("u64"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_schema_projection_descriptor_set_bytes(spark):
    """Build a descriptor with nested and repeated struct fields for pruning tests."""
    return _build_proto2_descriptor(spark, "schema_proj.proto", [
        _msg("Detail", [
            _field("a", 1, "INT32"),
            _field("b", 2, "INT32"),
            _field("c", 3, "STRING"),
        ]),
        _msg("SchemaProj", [
            _field("id", 1, "INT32"),
            _field("name", 2, "STRING"),
            _field("detail", 3, "MESSAGE", type_name=".test.Detail"),
            _field("items", 4, "MESSAGE", label="repeated", type_name=".test.Detail"),
        ]),
    ])


# Field descriptors for SchemaProj: {id, name, detail: {a, b, c}, items[]: {a, b, c}}
_detail_children = [
    pb.field("a", 1, IntegerGen()),
    pb.field("b", 2, IntegerGen()),
    pb.field("c", 3, StringGen()),
]
_schema_proj_schema = pb.schema("SchemaProjManual", [
    pb.field("id", 1, IntegerGen()),
    pb.field("name", 2, StringGen()),
    pb.nested_field("detail", 3, _detail_children),
    pb.repeated_message_field("items", 4, _detail_children),
])

_schema_proj_test_data = [
    encode_pb_message(_schema_proj_schema, {
        "id": 1,
        "name": "alice",
        "detail": {"a": 10, "b": 20, "c": "d1"},
        "items": [
            {"a": 100, "b": 200, "c": "i1"},
            {"a": 101, "b": 201, "c": "i2"},
        ],
    }),
    encode_pb_message(_schema_proj_schema, {
        "id": 2,
        "name": "bob",
        "detail": {"a": 30, "b": 40, "c": "d2"},
        "items": [
            {"a": 300, "b": 400, "c": "i3"},
        ],
    }),
    encode_pb_message(_schema_proj_schema, {
        "id": 3,
        "name": "carol",
        "detail": {"a": 50, "b": 60, "c": "d3"},
        "items": [],
    }),
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


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@pytest.mark.parametrize("boundary", ["alias", "withcolumn"], ids=idfn)
@ignore_order(local=True)
def test_from_protobuf_projection_across_plan_boundary(
        spark_tmp_path, from_protobuf_fn, boundary):
    """Schema projection across alias (select→select) and withColumn plan boundaries."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "schema_proj_boundary.desc",
        _build_schema_projection_descriptor_set_bytes)
    message_name = "test.SchemaProj"


    def run_on_spark(spark):
        df = spark.createDataFrame([(row,) for row in _schema_proj_test_data], schema="bin binary")
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        if boundary == "alias":
            aliased = df.select(decoded.alias("decoded"))
            return aliased.select(
                f.col("decoded").getField("detail").getField("a").alias("detail_a"),
                f.col("decoded").getField("id").alias("id"))
        else:
            with_decoded = df.withColumn("decoded", decoded)
            return with_decoded.select(
                f.col("decoded").getField("items").getField("a").alias("items_a"),
                f.col("decoded").getField("id").alias("id"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_dual_message_projection_descriptor_set_bytes(spark):
    """Build descriptors for two logical views over the same binary payload column."""
    return _build_proto2_descriptor(spark, "dual_projection.proto", [
        _msg("NestedPayload", [_field("count", 1, "INT32")]),
        _msg("BytesView", [
            _field("status", 1, "INT32"),
            _field("payload", 2, "BYTES"),
        ]),
        _msg("NestedView", [
            _field("status", 1, "INT32"),
            _field("payload", 2, "MESSAGE", type_name=".test.NestedPayload"),
        ]),
    ])


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_different_messages_same_binary_column_do_not_interfere(
        spark_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "dual_projection.desc",
        _build_dual_message_projection_descriptor_set_bytes)

    payload_keep = _encode_tag(1, 0) + _encode_varint(7)
    payload_drop = _encode_tag(1, 0) + _encode_varint(9)
    row_keep = (_encode_tag(1, 0) + _encode_varint(1) +
                _encode_tag(2, 2) + _encode_varint(len(payload_keep)) + payload_keep)
    row_drop = (_encode_tag(1, 0) + _encode_varint(0) +
                _encode_tag(2, 2) + _encode_varint(len(payload_drop)) + payload_drop)


    def run_on_spark(spark):
        df = spark.createDataFrame([(row_keep,), (row_drop,)], schema="bin binary")
        bytes_view = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.BytesView", desc_path, desc_bytes)
        nested_view = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), "test.NestedView", desc_path, desc_bytes)
        return df.filter(bytes_view.getField("status") == 1).select(
            nested_view.getField("payload").getField("count").alias("payload_count"))

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


def _build_deep_nested_5_level_descriptor_set_bytes(spark):
    """Build a descriptor for a five-level nested message chain."""
    return _build_proto2_descriptor(spark, "deep_nested_5_level.proto", [
        _msg("Level5", [_field("val5", 1, "INT32")]),
        _msg("Level4", [
            _field("val4", 1, "INT32"),
            _field("level5", 2, "MESSAGE", type_name=".test.Level5"),
        ]),
        _msg("Level3", [
            _field("val3", 1, "INT32"),
            _field("level4", 2, "MESSAGE", type_name=".test.Level4"),
        ]),
        _msg("Level2", [
            _field("val2", 1, "INT32"),
            _field("level3", 2, "MESSAGE", type_name=".test.Level3"),
        ]),
        _msg("Level1", [
            _field("val1", 1, "INT32"),
            _field("level2", 2, "MESSAGE", type_name=".test.Level2"),
        ]),
    ])


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_deep_nesting_5_levels(spark_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "deep_nested_5_level.desc",
        _build_deep_nested_5_level_descriptor_set_bytes)
    message_name = "test.Level1"
    data_gen = pb.as_datagen([
        pb.field("val1", 1, IntegerGen()),
        pb.nested_field("level2", 2, [
            pb.field("val2", 1, IntegerGen()),
            pb.nested_field("level3", 2, [
                pb.field("val3", 1, IntegerGen()),
                pb.nested_field("level4", 2, [
                    pb.field("val4", 1, IntegerGen()),
                    pb.nested_field("level5", 2, [
                        pb.field("val5", 1, IntegerGen()),
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


@_xfail_gpu_protobuf
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
    """Build a regression schema with same-named fields in unrelated nested messages."""
    return _build_proto2_descriptor(spark, "name_collision.proto", [
        _msg("User", [
            _field("age", 1, "INT32"),
            _field("id", 2, "INT32"),
        ]),
        _msg("Ad", [_field("id", 1, "INT32")]),
        _msg("Event", [
            _field("user_info", 1, "MESSAGE", type_name=".test.User"),
            _field("ad_info", 2, "MESSAGE", type_name=".test.Ad"),
        ]),
    ])


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_bug1_name_collision(spark_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "name_collision.desc",
        _build_name_collision_descriptor_set_bytes)
    message_name = "test.Event"

    data_gen = pb.as_datagen([
        pb.nested_field("user_info", 1, [
            pb.field("age", 1, IntegerGen()),
            pb.field("id", 2, IntegerGen()),
        ]),
        pb.nested_field("ad_info", 2, [
            pb.field("id", 1, IntegerGen()),
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
    """Build a minimal descriptor used by the filter-jump regression test."""
    return _build_proto2_descriptor(spark, "filter_jump.proto", [
        _msg("Event", [
            _field("status", 1, "INT32"),
            _field("ad_info", 2, "STRING"),
        ]),
    ])


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_bug2_filter_jump(spark_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "filter_jump.desc",
        _build_filter_jump_descriptor_set_bytes)
    message_name = "test.Event"

    data_gen = pb.as_datagen([
        pb.field("status", 1, IntegerGen(min_val=1, max_val=1)),
        pb.field("ad_info", 2, StringGen()),
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
    """Build a regression schema where an unrelated nested struct shares a field name."""
    return _build_proto2_descriptor(spark, "unrelated_struct.proto", [
        _msg("Nested", [
            _field("dummy", 1, "INT32"),
            _field("winfoid", 2, "INT32"),
        ]),
        _msg("Event", [
            _field("ad_info", 1, "MESSAGE", type_name=".test.Nested"),
        ]),
    ])


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_bug3_unrelated_struct_name_collision(spark_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "unrelated_struct.desc",
        _build_unrelated_struct_name_collision_descriptor_set_bytes)
    message_name = "test.Event"

    data_gen = pb.as_datagen([
        pb.nested_field("ad_info", 1, [
            pb.field("dummy", 1, IntegerGen()),
            pb.field("winfoid", 2, IntegerGen()),
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
    """Build a descriptor for a 12-level nested message chain."""
    messages = []
    for i in range(12, 0, -1):
        fields = [_field(f"val{i}", 1, "INT32")]
        if i < 12:
            fields.append(
                _field(f"level{i + 1}", 2, "MESSAGE", type_name=f".test.Level{i + 1}")
            )
        messages.append(_msg(f"Level{i}", fields))
    return _build_proto2_descriptor(spark, "max_depth.proto", messages)


@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
def test_from_protobuf_bug4_max_depth(spark_tmp_path, from_protobuf_fn):
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "max_depth.desc",
        _build_max_depth_descriptor_set_bytes)
    message_name = "test.Level1"

    # Build the deeply nested data gen spec
    def build_nested_gen(level):
        if level == 12:
            return [pb.field(f"val{level}", 1, IntegerGen(nullable=False))]
        return [
            pb.field(f"val{level}", 1, IntegerGen(nullable=False)),
            pb.nested_field(f"level{level+1}", 2, build_nested_gen(level+1))
        ]

    data_gen = pb.as_datagen(build_nested_gen(1))


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
    cpu_result = with_cpu_session(lambda spark: run_on_spark(spark).collect())
    assert len(cpu_result) > 0
    assert cpu_result[0]["val12"] is not None


# ---------------------------------------------------------------------------
# Bug 1: BOOL8 truncation — non-canonical bool varint values >= 256
#
# Protobuf spec: bool is a varint; any non-zero value means true.
# CPU decoder (protobuf-java): CodedInputStream.readBool() = readRawVarint64() != 0  →  true
# GPU decoder: extract_varint_kernel writes static_cast<uint8_t>(v).
#   For v = 256, static_cast<uint8_t>(256) == 0  →  false.  BUG.
# ---------------------------------------------------------------------------


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_bool_noncanonical_varint_scalar(spark_tmp_path, from_protobuf_fn):
    """Regression test: scalar bool encoded as non-canonical varint (e.g. 256) must decode as true.

    Protobuf allows any non-zero varint for bool true. The GPU decoder previously
    truncated to uint8_t, causing values >= 256 to wrap to 0 (false).
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "simple_bool_bug.desc", _build_simple_descriptor_set_bytes)
    message_name = "test.Simple"

    # varint(256) = 0x80 0x02, varint(512) = 0x80 0x04 — valid non-canonical bool true
    row_bool_256 = _encode_tag(1, 0) + _encode_varint(256) + \
                   _encode_tag(2, 0) + _encode_varint(99)

    # Control row: canonical bool true (varint 1) — should work on both
    row_bool_1 = _encode_tag(1, 0) + _encode_varint(1) + \
                 _encode_tag(2, 0) + _encode_varint(100)

    # Another non-canonical value: varint(512)
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
    """Build a descriptor for an optional id plus repeated bool flags."""
    return _build_proto2_descriptor(spark, "repeated_bool.proto", [
        _msg("WithRepeatedBool", [
            _field("id", 1, "INT32"),
            _field("flags", 2, "BOOL", label="repeated"),
        ]),
    ])


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_bool_noncanonical_varint_repeated(spark_tmp_path, from_protobuf_fn):
    """Regression test: repeated bool with non-canonical varint values must all decode as true.

    Same uint8_t truncation issue as the scalar case, exercised with repeated fields.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "repeated_bool_bug.desc", _build_repeated_bool_descriptor_set_bytes)
    message_name = "test.WithRepeatedBool"

    # Repeated bool field 2 (wire type 0 = varint), unpacked.
    # Three elements: varint(256), varint(1), varint(512) — all should decode as true.
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
# Regression guard: nested message child field default values
# ---------------------------------------------------------------------------


def _build_nested_with_defaults_descriptor_set_bytes(spark):
    """Build a descriptor with proto2 defaults inside a nested child struct."""
    return _build_proto2_descriptor(spark, "nested_defaults.proto", [
        _msg("Inner", [
            _field("count", 1, "INT32", default=42),
            _field("label", 2, "STRING", default="hello"),
            _field("flag", 3, "BOOL", default=True),
        ]),
        _msg("OuterWithNestedDefaults", [
            _field("id", 1, "INT32"),
            _field("inner", 2, "MESSAGE", type_name=".test.Inner"),
        ]),
    ])


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_nested_child_default_values(spark_tmp_path, from_protobuf_fn):
    """Regression test: proto2 default values for fields inside nested messages must be honored.

    When `inner` is present but its child fields are absent, the decoder must
    return the proto2 defaults (count=42, label="hello", flag=true), not null.
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
    return pb.as_datagen([
        pb.field("val1", 1, IntegerGen()),
        pb.nested_field("level2", 2, [
            pb.field("val2", 1, IntegerGen()),
            pb.nested_field("level3", 2, [
                pb.field("val3", 1, IntegerGen()),
                pb.nested_field("level4", 2, [
                    pb.field("val4", 1, IntegerGen()),
                    pb.nested_field("level5", 2, [
                        pb.field("val5", 1, IntegerGen()),
                    ]),
                ]),
            ]),
        ]),
    ])


@_xfail_gpu_protobuf
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


@_xfail_gpu_protobuf
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


@_xfail_gpu_protobuf
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


@_xfail_gpu_protobuf
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


# ===========================================================================
# FAILFAST mode tests
# ===========================================================================


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
def test_from_protobuf_failfast_malformed_data(spark_tmp_path, from_protobuf_fn):
    """FAILFAST mode should throw on malformed protobuf data (both CPU and GPU)."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "failfast.desc", _build_simple_descriptor_set_bytes)
    message_name = "test.Simple"

    # Craft a valid row and a malformed row (truncated varint with continuation bit)
    valid_row = _encode_tag(1, 0) + _encode_varint(1) + \
                _encode_tag(2, 0) + _encode_varint(42)
    malformed_row = bytes([0x08, 0x80])  # field 1, varint, but only continuation byte -- no end


    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(valid_row,), (malformed_row,)],
            schema="bin binary",
        )
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "FAILFAST"})
        # Must call .collect() so the exception surfaces inside with_*_session
        return df.select(decoded.getField("b").alias("b")).collect()

    assert_gpu_and_cpu_error(run_on_spark, {}, "Malformed")


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_permissive_malformed_returns_null(spark_tmp_path, from_protobuf_fn):
    """PERMISSIVE mode should return null for malformed rows, not throw.

    Note: Spark's from_protobuf defaults to FAILFAST (unlike JSON/CSV which
    default to PERMISSIVE), so mode must be set explicitly.
    """
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "permissive.desc", _build_simple_descriptor_set_bytes)
    message_name = "test.Simple"

    valid_row = _encode_tag(2, 0) + _encode_varint(99)
    malformed_row = bytes([0x08, 0x80])  # truncated varint


    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(valid_row,), (malformed_row,)],
            schema="bin binary",
        )
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes,
            options={"mode": "PERMISSIVE"})
        return df.select(
            decoded.getField("i32").alias("i32"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)


@_xfail_gpu_protobuf
@pytest.mark.skipif(is_before_spark_340(), reason="from_protobuf is Spark 3.4.0+")
@ignore_order(local=True)
def test_from_protobuf_all_null_input(spark_tmp_path, from_protobuf_fn):
    """All rows in the input binary column are null (not empty bytes, actual nulls).
    GPU should produce all-null struct rows matching CPU behavior."""
    desc_path, desc_bytes = _setup_protobuf_desc(
        spark_tmp_path, "allnull.desc", _build_simple_descriptor_set_bytes)
    message_name = "test.Simple"


    def run_on_spark(spark):
        df = spark.createDataFrame(
            [(None,), (None,), (None,)],
            schema="bin binary",
        )
        decoded = _call_from_protobuf(
            from_protobuf_fn, f.col("bin"), message_name, desc_path, desc_bytes)
        return df.select(
            decoded.getField("i32").alias("i32"),
            decoded.getField("s").alias("s"),
        )

    assert_gpu_and_cpu_are_equal_collect(run_on_spark)
