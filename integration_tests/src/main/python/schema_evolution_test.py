# Copyright (c) 2023, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect
from conftest import is_not_utc
from data_gen import *
from datetime import date, datetime, timezone
from marks import ignore_order, datagen_overrides
import pytest
from spark_session import is_databricks_runtime, is_databricks113_or_later

_formats = ("parquet", "orc")

_confs = {
    "spark.sql.legacy.parquet.datetimeRebaseModeInRead": "CORRECTED",
    "spark.sql.legacy.parquet.int96RebaseModeInRead": "CORRECTED",
    "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "CORRECTED",
    "spark.sql.legacy.parquet.int96RebaseModeInWrite": "CORRECTED",
}

# Using a custom date generator due to https://github.com/NVIDIA/spark-rapids/issues/9807
_custom_date_gen = DateGen(start=date(1590, 1, 1))

# List of additional column data generators to use when adding columns
_additional_gens = [
    boolean_gen,
    byte_gen,
    short_gen,
    int_gen,
    long_gen,
    float_gen,
    double_gen,
    string_gen,
    _custom_date_gen,
    TimestampGen(start=datetime(1677, 9, 22, tzinfo=timezone.utc), end=datetime(2262, 4, 11, tzinfo=timezone.utc)),
    # RAPIDS Accelerator does not support MapFromArrays yet
    # https://github.com/NVIDIA/spark-rapids/issues/8696
    # simple_string_to_string_map_gen),
    ArrayGen(_custom_date_gen),
    struct_gen_decimal128,
    StructGen([("c0", ArrayGen(long_gen)), ("c1", boolean_gen)]),
]

def get_additional_columns():
    """Returns a list of column_name, data_generator pairs to use when adding columns"""
    return [ (f"new_{i}", g) for i, g in enumerate(_additional_gens) ]

def get_ddl(col_gen_pairs):
    """Given a list of column_name, data_generator paris, returns the corresponding DDL string"""
    return ', '.join([f"{c} {g.data_type.simpleString()}" for c, g in col_gen_pairs])

@ignore_order(local=True)
@pytest.mark.parametrize("format", _formats)
@pytest.mark.xfail(condition = is_not_utc(), reason = 'xfail non-UTC time zone tests because of https://github.com/NVIDIA/spark-rapids/issues/9653')
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids/issues/9807')
def test_column_add_after_partition(spark_tmp_table_factory, format):
    # Databricks 10.4 appears to be missing https://issues.apache.org/jira/browse/SPARK-39417
    # so avoid generating nulls for numeric partitions
    before_gens = [("a", LongGen(min_val=-1, max_val=1,
                                 nullable=not is_databricks_runtime() or is_databricks113_or_later())),
                   ("b", SetValuesGen(StringType(), ["x", "y", "z"])),
                   ("c", long_gen)]
    new_cols_gens = get_additional_columns()
    new_ddl = get_ddl(new_cols_gens)
    after_gens = before_gens + new_cols_gens
    def testf(spark):
        table_name = spark_tmp_table_factory.get()
        df = gen_df(spark, before_gens)
        df.write\
            .format(format)\
            .partitionBy("a", "b")\
            .saveAsTable(table_name)
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({new_ddl})")
        df = gen_df(spark, after_gens)
        df.write\
            .format(format)\
            .mode("append")\
            .partitionBy("a", "b")\
            .saveAsTable(table_name)
        return spark.sql(f"SELECT * FROM {table_name}")
    assert_gpu_and_cpu_are_equal_collect(testf, conf=_confs)
