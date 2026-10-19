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

from decimal import Decimal

import pytest

from asserts import assert_cpu_and_gpu_are_equal_collect_with_capture, \
    assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect
from conftest import is_iceberg_remote_catalog, spark_jvm
from data_gen import LongGen, StructGen, UniqueLongGen, disable_parquet_field_id_write, gen_df
from iceberg import _build_tblprops, get_full_table_name, iceberg_unsupported_mark, \
    rapids_reader_types, supports_iceberg_v3, ICEBERG_V3_UNSUPPORTED_REASON
from marks import allow_non_gpu, iceberg, ignore_order
from spark_session import with_cpu_session

pytestmark = iceberg_unsupported_mark


def _setup_iceberg_v3_defaults_table(table_name):
    props = _build_tblprops({"format-version": "3"})
    props_sql = ", ".join(f"'{key}' = '{value}'" for key, value in props.items())

    def setup_table(spark):
        spark.sql(
            f"CREATE TABLE {table_name} "
            "(id BIGINT, s STRUCT<present: BIGINT>) USING ICEBERG "
            f"TBLPROPERTIES ({props_sql})")
        gen_df(
            spark,
            [("id", UniqueLongGen()),
             ("s", StructGen([("present", LongGen())]))],
            length=32).coalesce(1).writeTo(table_name).append()

        jvm = spark_jvm()
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name)
        literals = jvm.org.apache.iceberg.expressions.Literal
        types = jvm.org.apache.iceberg.types.Types
        update = table.updateSchema()

        def add_default(name, iceberg_type, value):
            update.addColumn(name, iceberg_type, literals.of(value).to(iceberg_type))

        # Cover every Iceberg 1.9 primitive type that Spark SQL can query. Iceberg TIME is omitted
        # because Iceberg's Spark adapter rejects it with "Spark does not support time fields".
        update.addRequiredColumn(
            "required_added", types.IntegerType.get(), None, literals.of(7))
        update.addColumn("optional_added", types.StringType.get(), literals.of("legacy"))
        update.addColumn("s", "nested_added", types.IntegerType.get(), literals.of(11))
        add_default("boolean_added", types.BooleanType.get(), True)
        add_default("long_added", types.LongType.get(), jvm.java.lang.Long.valueOf(5000000000))
        add_default("float_added", types.FloatType.get(), jvm.java.lang.Float.valueOf("1.25"))
        add_default("double_added", types.DoubleType.get(), 2.5)
        add_default("date_added", types.DateType.get(), "2024-01-02")
        add_default(
            "timestamp_added",
            types.TimestampType.withZone(),
            "2024-01-02T03:04:05Z")
        add_default(
            "binary_added", types.BinaryType.get(),
            jvm.java.nio.ByteBuffer.wrap(bytearray([1, 2, 3])))
        add_default(
            "decimal_added", types.DecimalType.of(9, 2),
            jvm.java.math.BigDecimal("12345.67"))

        # These types are representable by Iceberg/Spark but are not supported by the GPU
        # default-materialization path and must cause scan fallback when projected.
        add_default(
            "timestamp_ntz_added",
            types.TimestampType.withoutZone(),
            "2024-01-02T03:04:05")
        add_default(
            "uuid_added", types.UUIDType.get(),
            "123e4567-e89b-12d3-a456-426614174000")
        add_default(
            "fixed_added", types.FixedType.ofLength(3),
            jvm.java.nio.ByteBuffer.wrap(bytearray([4, 5, 6])))
        update.commit()
        spark.sql(f"REFRESH TABLE {table_name}")

    with_cpu_session(setup_table)


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@ignore_order(local=True)
def test_iceberg_v3_initial_defaults_all_types(spark_tmp_table_factory):
    table_name = get_full_table_name(spark_tmp_table_factory)
    _setup_iceberg_v3_defaults_table(table_name)
    v3_conf = {"spark.rapids.sql.format.iceberg.v3.enabled": "true"}
    query = (
        f"SELECT id, s.present, s.nested_added, required_added, optional_added, "
        "boolean_added, long_added, float_added, double_added, date_added, "
        "timestamp_added, binary_added, decimal_added "
        f"FROM {table_name} ORDER BY id")
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.sql(query),
        exist_classes="GpuBatchScanExec",
        conf=v3_conf)


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@ignore_order(local=True)
def test_iceberg_v3_project_only_defaulted_struct_child(spark_tmp_table_factory, reader_type):
    table_name = get_full_table_name(spark_tmp_table_factory)
    props_sql = ", ".join(
        f"'{key}' = '{value}'" for key, value in _build_tblprops({"format-version": "3"}).items())

    def setup_table(spark):
        spark.sql(
            f"CREATE TABLE {table_name} "
            "(id BIGINT, s STRUCT<present: BIGINT>) USING ICEBERG "
            f"TBLPROPERTIES ({props_sql})")
        # A null physical leaf must still distinguish a non-null struct from a null parent.
        spark.createDataFrame(
            [(0, None), (1, (None,)), (2, (7,))],
            "id BIGINT, s STRUCT<present: BIGINT>"
        ).coalesce(1).writeTo(table_name).append()

        jvm = spark_jvm()
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name)
        table.updateSchema().addColumn(
            "s", "nested_added", jvm.org.apache.iceberg.types.Types.IntegerType.get(),
            jvm.org.apache.iceberg.expressions.Literal.of(11)).commit()
        spark.sql(f"REFRESH TABLE {table_name}")

    with_cpu_session(setup_table)

    def check_defaults(cpu_rows, gpu_rows):
        for rows in (cpu_rows, gpu_rows):
            assert sorted((row.id, row.nested_added) for row in rows) == [
                (0, None), (1, 11), (2, 11)]
        return cpu_rows, gpu_rows

    def read_defaults(spark):
        # Iceberg's CPU nested pruning loses parent validity for this projection. Use an
        # unpruned CPU reference while keeping GPU pruning enabled to exercise the regression.
        gpu_enabled = spark.conf.get("spark.rapids.sql.enabled") == "true"
        spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", gpu_enabled)
        return spark.sql(f"SELECT id, s.nested_added FROM {table_name}")

    assert_gpu_and_cpu_are_equal_collect(
        read_defaults,
        conf={
            "spark.rapids.sql.format.iceberg.v3.enabled": "true",
            "spark.rapids.sql.format.parquet.reader.type": reader_type},
        result_canonicalize_func_before_compare=check_defaults)


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@allow_non_gpu("BatchScanExec", "ColumnarToRowExec")
@ignore_order(local=True)
def test_iceberg_v3_unsupported_initial_defaults_fallback(spark_tmp_table_factory):
    table_name = get_full_table_name(spark_tmp_table_factory)
    _setup_iceberg_v3_defaults_table(table_name)
    v3_conf = {"spark.rapids.sql.format.iceberg.v3.enabled": "true"}

    for unsupported_column in ["timestamp_ntz_added", "uuid_added", "fixed_added"]:
        assert_gpu_fallback_collect(
            lambda spark, column=unsupported_column: spark.sql(
                f"SELECT id, {column} FROM {table_name}"),
            "BatchScanExec",
            conf=v3_conf)


def _check_default_rows(expected):
    def check(cpu_rows, gpu_rows):
        for rows in (cpu_rows, gpu_rows):
            assert sorted([tuple(row) for row in rows], key=repr) == sorted(expected, key=repr)
        return cpu_rows, gpu_rows
    return check


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize('project_default', [False, True], ids=['implicit', 'projected'])
@ignore_order(local=True)
def test_iceberg_v3_defaults_with_predicates(
        spark_tmp_table_factory, reader_type, project_default):
    table_name = get_full_table_name(spark_tmp_table_factory)

    def setup(spark):
        spark.sql(f"CREATE TABLE {table_name} (id BIGINT) USING ICEBERG "
                  "TBLPROPERTIES ('format-version'='3')")
        spark.createDataFrame([(0,), (1,)], 'id BIGINT').coalesce(1).writeTo(table_name).append()
        jvm = spark_jvm()
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name)
        table.updateSchema().addColumn(
            'added', jvm.org.apache.iceberg.types.Types.IntegerType.get(),
            jvm.org.apache.iceberg.expressions.Literal.of(11)).commit()
        spark.sql(f"REFRESH TABLE {table_name}")
        spark.createDataFrame([(2, None), (3, 23)], 'id BIGINT, added INT') \
            .coalesce(1).writeTo(table_name).append()
        frame = spark.table(table_name)
        return frame.collect(), frame.schema

    reference_rows, reference_schema = with_cpu_session(setup)
    assert sorted(tuple(row) for row in reference_rows) == [
        (0, 11), (1, 11), (2, None), (3, 23)]
    predicates = [
        ('added = 11', [0, 1]),
        ('added > 10', [0, 1, 3]),
        ('added < 12', [0, 1]),
        ('added IN (11, 23)', [0, 1, 3]),
        ('added IS NULL', [2]),
        ('added IS NOT NULL', [0, 1, 3]),
        ('added != 23', [0, 1]),
        ('added = 11 AND id = 1', [1]),
        ('added = 11 OR id = 2', [0, 1, 2]),
        ('NOT (added = 23 OR id = 0)', [1]),
        ('added = 99', [])]
    columns = ['id', 'added'] if project_default else ['id']
    for predicate, expected_ids in predicates:
        def read(spark):
            # Iceberg's CPU row-group pruning also treats an absent defaulted field as null.
            # Materialize its unfiltered result before evaluating the CPU predicate.
            frame = (spark.table(table_name) if
                     spark.conf.get('spark.rapids.sql.enabled') == 'true' else
                     spark.createDataFrame(reference_rows, reference_schema))
            return frame.where(predicate).select(*columns)

        expected = [tuple(row) if project_default else (row.id,)
                    for row in reference_rows if row.id in expected_ids]
        assert_gpu_and_cpu_are_equal_collect(
            read,
            conf={'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
                  'spark.rapids.sql.format.parquet.reader.type': reader_type},
            result_canonicalize_func_before_compare=_check_default_rows(expected))


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize('shape', ['struct', 'list', 'map'])
@ignore_order(local=True)
def test_iceberg_v3_nested_defaults_mixed_files(spark_tmp_table_factory, reader_type, shape):
    table_name = get_full_table_name(spark_tmp_table_factory)
    if shape == 'struct':
        old_type, new_type = 'STRUCT<present: BIGINT>', 'STRUCT<present: BIGINT, added: INT>'
        old = [(0, None), (1, (None,)), (2, (7,))]
        current = [(4, None), (5, (None, None)), (6, (9, 99))]
        parent, expression = 'c', 'c.added'
        expected = [(0, None), (1, 11), (2, 11), (4, None), (5, None), (6, 99)]
    elif shape == 'list':
        old_type = 'ARRAY<STRUCT<present: BIGINT>>'
        new_type = 'ARRAY<STRUCT<present: BIGINT, added: INT>>'
        old = [(0, None), (1, []), (2, [None]), (3, [(None,), (7,), None])]
        current = [(4, None), (5, []), (6, [None, (None, None), (9, 99)])]
        parent, expression = 'c.element', 'c.added'
        expected = [(0, None), (1, []), (2, [None]), (3, [11, 11, None]),
                    (4, None), (5, []), (6, [None, None, 99])]
    else:
        old_type = 'MAP<STRING, STRUCT<present: BIGINT>>'
        new_type = 'MAP<STRING, STRUCT<present: BIGINT, added: INT>>'
        old = [(0, None), (1, {}), (2, {'a': None}),
               (3, {'a': (None,), 'b': (7,), 'c': None})]
        current = [(4, None), (5, {}), (6, {'a': None, 'b': (None, None), 'c': (9, 99)})]
        parent, expression = 'c.value', 'map_values(c).added'
        expected = [(0, None), (1, []), (2, [None]), (3, [11, 11, None]),
                    (4, None), (5, []), (6, [None, None, 99])]

    expected = [(-1, None)] + expected

    def setup(spark):
        spark.sql(f"CREATE TABLE {table_name} (id BIGINT) USING ICEBERG "
                  "TBLPROPERTIES ('format-version'='3', 'read.split.open-file-cost'='0')")
        # This oldest file lacks the entire parent container: descendant defaults must not
        # turn its null parent into a non-null struct, list or map.
        spark.createDataFrame([(-1,)], 'id BIGINT').coalesce(1).writeTo(table_name).append()
        spark.sql(f'ALTER TABLE {table_name} ADD COLUMN c {old_type}')
        for rows in (old[:2], old[2:]):
            spark.createDataFrame(rows, f'id BIGINT, c {old_type}') \
                .coalesce(1).writeTo(table_name).append()
        jvm = spark_jvm()
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name)
        table.updateSchema().addColumn(
            parent, 'added', jvm.org.apache.iceberg.types.Types.IntegerType.get(),
            jvm.org.apache.iceberg.expressions.Literal.of(11)).commit()
        spark.sql(f"REFRESH TABLE {table_name}")
        for rows in (current[:2], current[2:]):
            spark.createDataFrame(rows, f'id BIGINT, c {new_type}') \
                .coalesce(1).writeTo(table_name).append()

    with_cpu_session(setup)

    def read(spark):
        gpu = spark.conf.get('spark.rapids.sql.enabled') == 'true'
        spark.conf.set('spark.sql.optimizer.nestedSchemaPruning.enabled', gpu)
        return spark.sql(f'SELECT id, {expression} AS added FROM {table_name}')

    def check(cpu_rows, gpu_rows):
        if shape != 'map':
            return _check_default_rows(expected)(cpu_rows, gpu_rows)
        # The order of map_values is unspecified; array-element order remains checked above.
        def normalize(rows):
            return [(row[0], None if row[1] is None else sorted(row[1], key=repr)) for row in rows]
        normalized_expected = normalize(expected)
        normalized_cpu, normalized_gpu = normalize(cpu_rows), normalize(gpu_rows)
        _check_default_rows(normalized_expected)(normalized_cpu, normalized_gpu)
        return normalized_cpu, normalized_gpu

    assert_gpu_and_cpu_are_equal_collect(
        read,
        conf={'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
              'spark.rapids.sql.format.parquet.reader.type': reader_type},
        result_canonicalize_func_before_compare=check)

    def collect_reference(spark):
        frame = spark.table(table_name)
        return frame.collect(), frame.schema

    reference_rows, reference_schema = with_cpu_session(collect_reference)
    predicate = 'c.added = 11' if shape == 'struct' else f'array_contains({expression}, 11)'
    expected_ids = [1, 2] if shape == 'struct' else [3]
    filtered_expected = [row for row in expected if row[0] in expected_ids]

    def read_filtered(spark):
        gpu = spark.conf.get('spark.rapids.sql.enabled') == 'true'
        spark.conf.set('spark.sql.optimizer.nestedSchemaPruning.enabled', gpu)
        frame = (spark.table(table_name) if gpu else
                 spark.createDataFrame(reference_rows, reference_schema))
        return frame.where(predicate).selectExpr('id', f'{expression} AS added')

    def check_filtered(cpu_rows, gpu_rows):
        if shape == 'map':
            cpu_rows = [(row[0], sorted(row[1], key=repr)) for row in cpu_rows]
            gpu_rows = [(row[0], sorted(row[1], key=repr)) for row in gpu_rows]
            wanted = [(row[0], sorted(row[1], key=repr)) for row in filtered_expected]
        else:
            wanted = filtered_expected
        return _check_default_rows(wanted)(cpu_rows, gpu_rows)

    assert_gpu_and_cpu_are_equal_collect(
        read_filtered,
        conf={'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
              'spark.rapids.sql.format.parquet.reader.type': reader_type},
        result_canonicalize_func_before_compare=check_filtered)


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@ignore_order(local=True)
def test_iceberg_v3_defaults_combining_and_type_evolution(spark_tmp_table_factory, reader_type):
    table_name = get_full_table_name(spark_tmp_table_factory)
    defaults = (Decimal('1.25'), Decimal('1234567890.25'), Decimal('12345678901234567890.25'))
    physical = (Decimal('2.50'), Decimal('2345678901.25'), Decimal('-22345678901234567890.25'))
    expected = [(11, i, *defaults) for i in range(6)] + [
        (None, 6, None, None, None), (7, 7, *physical), (5000000000, 8, *defaults)]
    file_count = 0

    def setup(spark):
        nonlocal file_count
        spark.sql(f"CREATE TABLE {table_name} (id BIGINT) USING ICEBERG TBLPROPERTIES ("
                  "'format-version'='3', 'read.split.open-file-cost'='0', "
                  "'read.split.target-size'='134217728')")
        for i in range(6):
            spark.createDataFrame([(i,)], 'id BIGINT').coalesce(1).writeTo(table_name).append()
        jvm = spark_jvm()
        types = jvm.org.apache.iceberg.types.Types
        literals = jvm.org.apache.iceberg.expressions.Literal
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name)
        update = table.updateSchema().addColumn('added', types.IntegerType.get(), literals.of(11))
        for name, precision, value in zip(('d32', 'd64', 'd128'), (9, 18, 38), defaults):
            dtype = types.DecimalType.of(precision, 2)
            update.addColumn(name, dtype, literals.of(jvm.java.math.BigDecimal(str(value))))
        update.commit()
        spark.sql(f'REFRESH TABLE {table_name}')
        schema = 'id BIGINT, added INT, d32 DECIMAL(9,2), d64 DECIMAL(18,2), d128 DECIMAL(38,2)'
        for row in [(6, None, None, None, None), (7, 7, *physical)]:
            spark.createDataFrame([row], schema).coalesce(1).writeTo(table_name).append()
        table.refresh()
        table.updateSchema().updateColumn('added', types.LongType.get()) \
            .moveFirst('added').renameColumn('added', 'renamed').commit()
        spark.sql(f'REFRESH TABLE {table_name}')
        spark.createDataFrame([(5000000000, 8, *defaults)],
            'renamed BIGINT, id BIGINT, d32 DECIMAL(9,2), d64 DECIMAL(18,2), d128 DECIMAL(38,2)') \
            .coalesce(1).writeTo(table_name).append()
        table.refresh()
        file_count = spark.sql(f'SELECT count(*) FROM {table_name}.data_files').first()[0]
        assert file_count == 9
        groups = table.newScan().planTasks()
        try:
            iterator = groups.iterator()
            assert iterator.hasNext()
            assert iterator.next().files().size() == file_count
            assert not iterator.hasNext()
        finally:
            groups.close()

    with_cpu_session(setup)
    gpu_frame = []

    def read(spark):
        gpu = spark.conf.get('spark.rapids.sql.enabled') == 'true'
        # Iceberg's CPU Arrow reader copies decimal defaults onto its physical int/long
        # fields and fails to cast them. Use its row reader as the independent CPU oracle.
        spark.conf.set('spark.sql.iceberg.vectorization.enabled', gpu)
        frame = spark.sql(f'SELECT renamed, id, d32, d64, d128 FROM {table_name}')
        if gpu:
            gpu_frame.append(frame)
        return frame

    assert_gpu_and_cpu_are_equal_collect(
        read,
        conf={'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
              'spark.rapids.sql.format.parquet.reader.type': reader_type,
              'spark.rapids.sql.reader.multithreaded.combine.sizeBytes': '64m',
              'spark.sql.adaptive.enabled': 'false'},
        result_canonicalize_func_before_compare=_check_default_rows(expected))
    if reader_type == 'COALESCING':
        nodes = [gpu_frame[0]._jdf.queryExecution().executedPlan()]
        while nodes:
            node = nodes.pop()
            if node.getClass().getSimpleName() == 'GpuBatchScanExec':
                batches = node.metrics().get('numOutputBatches').get().value()
                assert 0 < batches < file_count, \
                    f'Expected actual combining, found {batches} batches'
                break
            children = node.children()
            nodes.extend(children.apply(i) for i in range(children.size()))
        else:
            raise AssertionError('GpuBatchScanExec was not executed')

    def collect_reference(spark):
        frame = read(spark)
        return frame.collect(), frame.schema

    reference_rows, reference_schema = with_cpu_session(collect_reference)

    def read_filtered(spark):
        gpu = spark.conf.get('spark.rapids.sql.enabled') == 'true'
        spark.conf.set('spark.sql.iceberg.vectorization.enabled', gpu)
        frame = (spark.table(table_name) if gpu else
                 spark.createDataFrame(reference_rows, reference_schema))
        return frame.where('renamed = 11 OR id = 7').select('renamed', 'id', 'd32', 'd64', 'd128')

    assert_gpu_and_cpu_are_equal_collect(
        read_filtered,
        conf={'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
              'spark.rapids.sql.format.parquet.reader.type': reader_type},
        result_canonicalize_func_before_compare=_check_default_rows(
            [row for row in expected if row[1] < 6 or row[1] == 7]))


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason='add_files requires a local catalog')
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@ignore_order(local=True)
def test_iceberg_v3_nested_defaults_name_mapping(spark_tmp_table_factory, reader_type):
    table_name = get_full_table_name(spark_tmp_table_factory)
    source_name = get_full_table_name(spark_tmp_table_factory)

    def setup(spark):
        spark.sql(f'CREATE TABLE {table_name} (unused INT, id BIGINT, s STRUCT<present: BIGINT>) '
                  "USING ICEBERG TBLPROPERTIES ('format-version'='3')")
        spark.sql(f'ALTER TABLE {table_name} DROP COLUMN unused')
        spark.sql(f'CREATE TABLE {source_name} '
                  '(id BIGINT, s STRUCT<present: BIGINT>) USING PARQUET')
        spark.createDataFrame([(0, None), (1, (None,)), (2, (7,))],
            'id BIGINT, s STRUCT<present: BIGINT>').coalesce(1).write.insertInto(source_name)
        spark.sql(f"CALL spark_catalog.system.add_files(table => '{table_name}', "
                  f"source_table => '{source_name}')")
        jvm = spark_jvm()
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name)
        assert table.schema().findField('id').fieldId() != 1
        assert table.properties().get('schema.name-mapping.default') is not None
        table.updateSchema().addColumn('s', 'added',
            jvm.org.apache.iceberg.types.Types.IntegerType.get(),
            jvm.org.apache.iceberg.expressions.Literal.of(11)) \
            .renameColumn('s.present', 'legacy').renameColumn('s', 'renamed').commit()
        spark.sql(f'REFRESH TABLE {table_name}')

    with_cpu_session(setup, conf=disable_parquet_field_id_write)

    def read(spark):
        spark.conf.set('spark.sql.optimizer.nestedSchemaPruning.enabled',
                       spark.conf.get('spark.rapids.sql.enabled') == 'true')
        return spark.sql(f'SELECT id, renamed.added FROM {table_name}')

    assert_gpu_and_cpu_are_equal_collect(
        read,
        conf={'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
              'spark.rapids.sql.format.parquet.reader.type': reader_type},
        result_canonicalize_func_before_compare=_check_default_rows([(0, None), (1, 11), (2, 11)]))


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@ignore_order(local=True)
def test_iceberg_v3_default_with_unprojected_timestamp_ntz(spark_tmp_table_factory, reader_type):
    table_name = get_full_table_name(spark_tmp_table_factory)

    def setup(spark):
        spark.sql(f'CREATE TABLE {table_name} (id BIGINT, s STRUCT<legacy: TIMESTAMP_NTZ>) '
                  "USING ICEBERG TBLPROPERTIES ('format-version'='3')")
        spark.sql(f"INSERT INTO {table_name} VALUES (0, NULL), (1, named_struct('legacy', "
                  "CAST(NULL AS TIMESTAMP_NTZ))), (2, named_struct('legacy', "
                  "CAST('2024-01-02 03:04:05' AS TIMESTAMP_NTZ)))")
        jvm = spark_jvm()
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name)
        table.updateSchema().addColumn('s', 'added',
            jvm.org.apache.iceberg.types.Types.IntegerType.get(),
            jvm.org.apache.iceberg.expressions.Literal.of(11)).commit()
        spark.sql(f'REFRESH TABLE {table_name}')

    with_cpu_session(setup)

    def read(spark):
        spark.conf.set('spark.sql.optimizer.nestedSchemaPruning.enabled',
                       spark.conf.get('spark.rapids.sql.enabled') == 'true')
        return spark.sql(f'SELECT id, s.added FROM {table_name}')

    assert_gpu_and_cpu_are_equal_collect(
        read,
        conf={'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
              'spark.rapids.sql.format.parquet.reader.type': reader_type},
        result_canonicalize_func_before_compare=_check_default_rows([(0, None), (1, 11), (2, 11)]))
