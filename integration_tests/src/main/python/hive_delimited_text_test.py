# Copyright (c) 2022, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect
from conftest import get_non_gpu_allowed
from data_gen import *
from marks import *
from pyspark.sql.types import *
from spark_session import with_cpu_session

hive_text_enabled_conf = {"spark.rapids.sql.format.hive.text.enabled": True,
                          "spark.rapids.sql.format.hive.text.read.enabled": True}

acq_schema = StructType([
    StructField('loan_id', LongType()),
    StructField('orig_channel', StringType()),
    StructField('seller_name', StringType()),
    StructField('orig_interest_rate', DoubleType()),
    StructField('orig_upb', IntegerType()),
    StructField('orig_loan_term', IntegerType()),
    StructField('orig_date', StringType()),
    StructField('first_pay_date', StringType()),
    StructField('orig_ltv', DoubleType()),
    StructField('orig_cltv', DoubleType()),
    StructField('num_borrowers', DoubleType()),
    StructField('dti', DoubleType()),
    StructField('borrower_credit_score', DoubleType()),
    StructField('first_home_buyer', StringType()),
    StructField('loan_purpose', StringType()),
    StructField('property_type', StringType()),
    StructField('num_units', IntegerType()),
    StructField('occupancy_status', StringType()),
    StructField('property_state', StringType()),
    StructField('zip', IntegerType()),
    StructField('mortgage_insurance_percent', DoubleType()),
    StructField('product_type', StringType()),
    StructField('coborrow_credit_score', DoubleType()),
    StructField('mortgage_insurance_type', DoubleType()),
    StructField('relocation_mortgage_indicator', StringType())])

perf_schema = StructType([
    StructField('loan_id', LongType()),
    StructField('monthly_reporting_period', StringType()),
    StructField('servicer', StringType()),
    StructField('interest_rate', DoubleType()),
    StructField('current_actual_upb', DoubleType()),
    StructField('loan_age', DoubleType()),
    StructField('remaining_months_to_legal_maturity', DoubleType()),
    StructField('adj_remaining_months_to_maturity', DoubleType()),
    StructField('maturity_date', StringType()),
    StructField('msa', DoubleType()),
    StructField('current_loan_delinquency_status', IntegerType()),
    StructField('mod_flag', StringType()),
    StructField('zero_balance_code', StringType()),
    StructField('zero_balance_effective_date', StringType()),
    StructField('last_paid_installment_date', StringType()),
    StructField('foreclosed_after', StringType()),
    StructField('disposition_date', StringType()),
    StructField('foreclosure_costs', DoubleType()),
    StructField('prop_preservation_and_repair_costs', DoubleType()),
    StructField('asset_recovery_costs', DoubleType()),
    StructField('misc_holding_expenses', DoubleType()),
    StructField('holding_taxes', DoubleType()),
    StructField('net_sale_proceeds', DoubleType()),
    StructField('credit_enhancement_proceeds', DoubleType()),
    StructField('repurchase_make_whole_proceeds', StringType()),
    StructField('other_foreclosure_proceeds', DoubleType()),
    StructField('non_interest_bearing_upb', DoubleType()),
    StructField('principal_forgiveness_upb', StringType()),
    StructField('repurchase_make_whole_proceeds_flag', StringType()),
    StructField('foreclosure_principal_write_off_amount', StringType()),
    StructField('servicing_activity_indicator', StringType())])

timestamp_schema = StructType([
    StructField('ts', TimestampType())])

date_schema = StructType([
    StructField('date', DateType())])

trucks_schema = StructType([
    StructField('make', StringType()),
    StructField('model', StringType()),
    StructField('year', IntegerType()),
    StructField('price', StringType()),
    StructField('comment', StringType())])


def make_schema(column_type):
    """
    Constructs a table schema with a single column of the specified type
    """
    return StructType([StructField('number', column_type)])


def read_hive_text_sql(data_path, schema, spark_tmp_table_factory, options=None):
    if options is None:
        options = {}
    opts = options
    if schema is not None:
        opts = copy_and_update(options, {'schema': schema})

    def read_impl(spark):
        tmp_name = spark_tmp_table_factory.get()
        return spark.catalog.createTable(tmp_name, source='hive', path=data_path, **opts)

    return read_impl


@approximate_float
@pytest.mark.parametrize('name,schema,options', [

    # Numeric Reads.
    ('hive-delim-text/simple-boolean-values', make_schema(BooleanType()),        {}),
    ('hive-delim-text/simple-int-values',     make_schema(ByteType()),           {}),
    ('hive-delim-text/simple-int-values',     make_schema(ShortType()),          {}),
    ('hive-delim-text/simple-int-values',     make_schema(IntegerType()),        {}),
    ('hive-delim-text/simple-int-values',     make_schema(LongType()),           {}),
    ('hive-delim-text/simple-int-values',     make_schema(FloatType()),          {}),
    ('hive-delim-text/simple-int-values',     make_schema(DoubleType()),         {}),
    ('hive-delim-text/simple-int-values',     make_schema(DecimalType(10, 2)),   {}),
    ('hive-delim-text/simple-int-values',     make_schema(DecimalType(10, 3)),   {}),
    ('hive-delim-text/simple-int-values',     make_schema(StringType()),         {}),

    # Floating Point.
    ('hive-delim-text/simple-float-values',   make_schema(FloatType()),          {}),
    ('hive-delim-text/simple-float-values',   make_schema(DoubleType()),         {}),
    ('hive-delim-text/simple-float-values',   make_schema(DecimalType(10, 3)),   {}),
    ('hive-delim-text/simple-float-values',   make_schema(DecimalType(38, 10)),   {}),
    ('hive-delim-text/simple-float-values',   make_schema(IntegerType()),         {}),
    ('hive-delim-text/extended-float-values',   make_schema(IntegerType()),         {}),
    ('hive-delim-text/extended-float-values',   make_schema(FloatType()),          {}),
    ('hive-delim-text/extended-float-values',   make_schema(DoubleType()),         {}), 
    pytest.param('hive-delim-text/extended-float-values',   make_schema(DecimalType(10, 3)),   {},
        marks=pytest.mark.xfail(reason="GPU supports more valid values than CPU. "
            "https://github.com/NVIDIA/spark-rapids/issues/7246")),
    pytest.param('hive-delim-text/extended-float-values',   make_schema(DecimalType(38, 10)),   {},
        marks=pytest.mark.xfail(reason="GPU supports more valid values than CPU. "
            "https://github.com/NVIDIA/spark-rapids/issues/7246")),

    # Custom datasets
    ('hive-delim-text/Acquisition_2007Q3', acq_schema, {}),
    ('hive-delim-text/Performance_2007Q3', perf_schema, {'serialization.null.format': ''}),
    ('hive-delim-text/Performance_2007Q3', perf_schema, {}),
    ('hive-delim-text/trucks-1', trucks_schema, {}),
    ('hive-delim-text/trucks-err', trucks_schema, {}),

    # Date/Time
    ('hive-delim-text/timestamp', timestamp_schema, {}),
    ('hive-delim-text/date', date_schema, {}),

    # Test that lines beginning with comments ('#') aren't skipped.
    ('hive-delim-text/comments', StructType([StructField("str", StringType()),
                                             StructField("num", IntegerType()),
                                             StructField("another_str", StringType())]), {}),

    # Test that carriage returns ('\r'/'^M') are treated similarly to newlines ('\n')
    ('hive-delim-text/carriage-return', StructType([StructField("str", StringType())]), {}),
    ('hive-delim-text/carriage-return-err', StructType([StructField("str", StringType())]), {}),
], ids=idfn)
def test_basic_hive_text_read(std_input_path, name, schema, spark_tmp_table_factory, options):
    assert_gpu_and_cpu_are_equal_collect(read_hive_text_sql(std_input_path + '/' + name,
                                                            schema, spark_tmp_table_factory, options),
                                         conf=hive_text_enabled_conf)


hive_text_supported_gens = [
    StringGen('(\\w| |\t|\ud720){0,10}', nullable=False),
    StringGen('[aAbB ]{0,10}'),
    StringGen('[nN][aA][nN]'),
    StringGen('[+-]?[iI][nN][fF]([iI][nN][iI][tT][yY])?'),
    byte_gen, short_gen, int_gen, long_gen, boolean_gen, date_gen,
    float_gen,
    FloatGen(no_nans=False),
    double_gen,
    DoubleGen(no_nans=False),
    TimestampGen(),
]


def create_hive_text_table(spark, column_gen, text_table_name, data_path, fields="my_field"):
    """
    Helper method to create a Hive Text table with contents from the specified
    column generator.
    :param spark: Spark context for the test
    :param column_gen: Data generator for the table's column
    :param text_table_name: (Temp) Name of the created Hive Text table
    :param data_path: Data location for the created Hive Text table
    :param fields: The fields composing the table to be created
    """
    gen_df(spark, column_gen).repartition(1).createOrReplaceTempView("input_view")
    spark.sql("DROP TABLE IF EXISTS " + text_table_name)
    spark.sql("CREATE TABLE " + text_table_name + " STORED AS TEXTFILE " +
              "LOCATION '" + data_path + "' " +
              "AS SELECT " + fields + " FROM input_view")


def read_hive_text_table(spark, text_table_name, fields="my_field"):
    """
    Helper method to read the contents of a Hive (Text) table.
    :param spark: Spark context for the test
    :param text_table_name: Name of the Hive (Text) table to be read
    :param fields: The fields to be read from the specified table
    """
    return spark.sql("SELECT " + fields + " FROM " + text_table_name)


@approximate_float
@pytest.mark.parametrize('data_gen', hive_text_supported_gens, ids=idfn)
def test_hive_text_round_trip(spark_tmp_path, data_gen, spark_tmp_table_factory):
    gen = StructGen([('my_field', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/hive_text_table'
    table_name = spark_tmp_table_factory.get()

    with_cpu_session(lambda spark: create_hive_text_table(spark, gen, table_name, data_path))

    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: read_hive_text_table(spark, table_name),
            conf=hive_text_enabled_conf)


def create_hive_text_table_partitioned(spark, column_gen, text_table_name, data_path):
    gen_df(spark, column_gen).repartition(1).createOrReplaceTempView("input_view")
    spark.sql("DROP TABLE IF EXISTS " + text_table_name)
    column_type = column_gen.children[0][1].data_type.simpleString()  # Because StructGen([('my_field', gen)]).
    spark.sql("CREATE TABLE " + text_table_name +
              "( my_field " + column_type + ") "
              "PARTITIONED BY (dt STRING) "
              "STORED AS TEXTFILE "
              "LOCATION '" + data_path + "' ")
    spark.sql("INSERT OVERWRITE " + text_table_name + " PARTITION( dt='1' ) "
              "SELECT my_field FROM input_view")
    spark.sql("INSERT OVERWRITE " + text_table_name + " PARTITION( dt='2' ) "
              "SELECT my_field FROM input_view")


def read_hive_text_table_partitions(spark, text_table_name):
    """
    Helper method to read the contents of a Hive (Text) table.
    :param spark: Spark context for the test
    :param text_table_name: Name of the Hive (Text) table to be read
    """
    return spark.sql("SELECT my_field FROM " + text_table_name + " WHERE dt='1' ")


@approximate_float
@allow_non_gpu("EqualTo,IsNotNull,Literal")  # Accounts for partition predicate: `WHERE dt='1'`
@pytest.mark.parametrize('data_gen', hive_text_supported_gens, ids=idfn)
def test_hive_text_round_trip_partitioned(spark_tmp_path, data_gen, spark_tmp_table_factory):
    gen = StructGen([('my_field', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/hive_text_table'
    table_name = spark_tmp_table_factory.get()

    with_cpu_session(lambda spark: create_hive_text_table_partitioned(spark, gen, table_name, data_path))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: read_hive_text_table_partitions(spark, table_name),
        conf=hive_text_enabled_conf)


hive_text_unsupported_gens = [
    ArrayGen(string_gen),
    StructGen([('int_field', int_gen), ('string_field', string_gen)]),
    MapGen(StringGen(nullable=False), string_gen),
    binary_gen,
    StructGen([('b', byte_gen), ('i', int_gen), ('arr_of_i', ArrayGen(int_gen))]),
    ArrayGen(StructGen([('b', byte_gen), ('i', int_gen), ('arr_of_i', ArrayGen(int_gen))]))
]


@allow_non_gpu("org.apache.spark.sql.hive.execution.HiveTableScanExec")
@pytest.mark.parametrize('unsupported_gen', hive_text_unsupported_gens, ids=idfn)
def test_hive_text_fallback_for_unsupported_types(spark_tmp_path, unsupported_gen, spark_tmp_table_factory):
    supported_gen = int_gen  # Generator for 1 supported data type. (IntegerGen chosen arbitrarily.)
    gen = StructGen([('my_supported_int_field', supported_gen),
                     ('my_unsupported_field', unsupported_gen), ], nullable=False)
    data_path = spark_tmp_path + '/hive_text_table'
    table_name = spark_tmp_table_factory.get()

    with_cpu_session(lambda spark: create_hive_text_table(spark,
                                                          gen,
                                                          table_name,
                                                          data_path,
                                                          "my_supported_int_field, my_unsupported_field"))

    assert_gpu_fallback_collect(
            lambda spark: read_hive_text_table(spark, table_name, "my_unsupported_field"),
            cpu_fallback_class_name=get_non_gpu_allowed()[0],
            conf=hive_text_enabled_conf)

    # GpuHiveTableScanExec cannot partially read only those columns that are of supported types.
    # Even if the output-projection uses only supported types, the read should fall back to CPU
    # if the table has even one column of an unsupported type.
    assert_gpu_fallback_collect(
        lambda spark: read_hive_text_table(spark, table_name, "my_supported_int_field"),
        cpu_fallback_class_name=get_non_gpu_allowed()[0],
        conf=hive_text_enabled_conf)


@pytest.mark.parametrize('data_gen', [StringGen()], ids=idfn)
def test_hive_text_default_enabled(spark_tmp_path, data_gen, spark_tmp_table_factory):
    gen = StructGen([('my_field', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/hive_text_table'
    table_name = spark_tmp_table_factory.get()

    with_cpu_session(lambda spark: create_hive_text_table(spark, gen, table_name, data_path))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: read_hive_text_table(spark, table_name),
        conf={})


@allow_non_gpu("org.apache.spark.sql.hive.execution.HiveTableScanExec")
@pytest.mark.parametrize('data_gen', [TimestampGen()], ids=idfn)
def test_custom_timestamp_formats_disabled(spark_tmp_path, data_gen, spark_tmp_table_factory):
    """
    This is to test that the plugin falls back to CPU execution, in case a Hive delimited
    text table is set up with a custom timestamp format, via the "timestamp.formats"
    property.
    Note that this property could be specified in either table properties,
    or SerDe properties.
    """
    gen = StructGen([('my_field', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/hive_text_table'
    table_name = spark_tmp_table_factory.get()

    from enum import Enum

    class PropertyLocation(Enum):
        TBLPROPERTIES = 1,
        SERDEPROPERTIES = 2

    def create_hive_table_with_custom_timestamp_format(spark, property_location):
        gen_df(spark, gen).repartition(1).createOrReplaceTempView("input_view")
        spark.sql("DROP TABLE IF EXISTS " + table_name)
        spark.sql("CREATE TABLE " + table_name + " (my_field TIMESTAMP) "
                  "STORED AS TEXTFILE " +
                  "LOCATION '" + data_path + "' ")
        spark.sql("ALTER TABLE " + table_name + " SET " +
                  ("TBLPROPERTIES" if property_location == PropertyLocation.TBLPROPERTIES else "SERDEPROPERTIES") +
                  "('timestamp.formats'='yyyy-MM-dd HH:mm:ss.SSS')")
        spark.sql("INSERT INTO TABLE " + table_name + " SELECT * FROM input_view")

    with_cpu_session(lambda spark:
                     create_hive_table_with_custom_timestamp_format(spark, PropertyLocation.TBLPROPERTIES))
    assert_gpu_fallback_collect(
        lambda spark: read_hive_text_table(spark, table_name),
        cpu_fallback_class_name=get_non_gpu_allowed()[0],
        conf=hive_text_enabled_conf)

    with_cpu_session(lambda spark:
                     create_hive_table_with_custom_timestamp_format(spark, PropertyLocation.SERDEPROPERTIES))
    assert_gpu_fallback_collect(
        lambda spark: read_hive_text_table(spark, table_name),
        cpu_fallback_class_name=get_non_gpu_allowed()[0],
        conf=hive_text_enabled_conf)

