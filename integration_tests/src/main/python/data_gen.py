# Copyright (c) 2020, NVIDIA CORPORATION.
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

import random
from pyspark.sql.types import *

def gen_null():
    return None

def make_equal_choice(choices):
    return lambda : random.choice(choices)()

def make_weighted_choice(weighted_choices):
    """
    Returns a data gen function that will choose between different gen functions
    based off of the weight given them.

    make_weights_choice([(0.01, gen_null), (0.99, gen_long)])

    Will return a data gen function that inserts null/None 1% of the time
    """
    total = float(sum(weight for weight,gen in weighted_choices))
    normalized_choices = [(weight/total, gen) for weight,gen in weighted_choices]
    def choose_one():
        pick = random.random()
        total = 0
        for (weight, gen) in normalized_choices:
            total += weight
            if total >= pick:
                return gen()
        raise RuntimeError('Random did not pick something we expected')
    return choose_one

def make_std_nullable(gen):
    return make_weighted_choice([(0.05, gen_null), (0.95, gen)])

BYTE_MIN = -128
BYTE_MAX = 127
# Bytes are a small enough range we don't have to worry about not getting
# enough coverage and inserting in corner cases
def gen_byte():
    return random.randint(BYTE_MIN, BYTE_MAX)

SHORT_MIN = -32768
SHORT_MAX = 32767
def gen_short_basic():
    return random.randint(SHORT_MIN, SHORT_MAX)

gen_short = make_weighted_choice([
    (0.01, lambda : SHORT_MIN),
    (0.01, lambda : SHORT_MAX),
    (0.01, lambda : 0),
    (0.01, lambda : 1),
    (0.01, lambda : -1),
    (0.95, gen_short_basic)])

INT_MIN = -2147483648
INT_MAX = 2147483647
def gen_int_basic():
    return random.randint(INT_MIN, INT_MAX)

gen_int = make_weighted_choice([
    (0.01, lambda : INT_MIN),
    (0.01, lambda : INT_MAX),
    (0.01, lambda : 0),
    (0.01, lambda : 1),
    (0.01, lambda : -1),
    (0.95, gen_int_basic)])

LONG_MIN = -922337203685477580
LONG_MAX = 9223372036854775807
def gen_long_basic():
    return random.randint(LONG_MIN, LONG_MAX)

gen_long = make_weighted_choice([
    (0.01, lambda : LONG_MIN),
    (0.01, lambda : LONG_MAX),
    (0.01, lambda : 0),
    (0.01, lambda : 1),
    (0.01, lambda : -1),
    (0.95, gen_long_basic)])

FLOAT_MIN = -3.4028235E38
FLOAT_MAX = 3.4028235E38
def gen_float_basic():
    return random.uniform(FLOAT_MIN, FLOAT_MAX)

gen_float = make_weighted_choice([
    (0.01, lambda : FLOAT_MIN),
    (0.01, lambda : FLOAT_MAX),
    (0.01, lambda : 0.0),
    (0.01, lambda : 1.0),
    (0.01, lambda : -1.0),
    (0.01, lambda : float('inf')),
    (0.01, lambda : float('-inf')),
    (0.01, lambda : float('nan')),
    (0.92, gen_float_basic)])

DOUBLE_MIN = -1.7976931348623157E308
DOUBLE_MAX = 1.7976931348623157E308
def gen_double_basic():
    return random.uniform(DOUBLE_MIN, DOUBLE_MAX)

gen_double = make_weighted_choice([
    (0.01, lambda : DOUBLE_MIN),
    (0.01, lambda : DOUBLE_MAX),
    (0.01, lambda : 0.0),
    (0.01, lambda : 1.0),
    (0.01, lambda : -1.0),
    (0.01, lambda : float('inf')),
    (0.01, lambda : float('-inf')),
    (0.01, lambda : float('nan')),
    (0.92, gen_double_basic)])

def _make_nullable_if_needed(gen, nullable):
    if (nullable):
        return make_std_nullable(gen)
    else:
        return gen

class StructFieldWithDataGen(StructField):
    def __init__(self, gen, name, data_type, nullable=True, metadata=None, add_nullgen_when_nullable = True):
        super().__init__(name, data_type, nullable, metadata)
        self.gen = _make_nullable_if_needed(gen, nullable and add_nullgen_when_nullable)

# TODO class ArrayTypeWithDataGen

# TODO class MapTypeWithDataGen

def _make_gen_from(schema):
    if isinstance(schema, StructFieldWithDataGen):
        # nullability already included in generator
        return schema.gen
    elif isinstance(schema, StructField):
        gen = _make_gen_from(schema.dataType)
        return _make_nullable_if_needed(gen, schema.nullable)
    elif isinstance(schema, StructType):
        sub_gens = [_make_gen_from(sub) for sub in schema.fields]
        def make_tuple():
            data = [gen() for gen in sub_gens]
            return tuple(data)
        # nullability handled by StructField, MapType or ArrayType
        return make_tuple
    elif isinstance(schema, ByteType):
        # nullability handled by StructField, MapType or ArrayType
        return gen_byte
    elif isinstance(schema, ShortType):
        # nullability handled by StructField, MapType or ArrayType
        global gen_short
        return gen_short
    elif isinstance(schema, IntegerType):
        # nullability handled by StructField, MapType or ArrayType
        global gen_int
        return gen_int
    elif isinstance(schema, LongType):
        # nullability handled by StructField, MapType or ArrayType
        global gen_long
        return gen_long
    elif isinstance(schema, FloatType):
        # nullability handled by StructField, MapType or ArrayType
        global gen_float
        return gen_float
    elif isinstance(schema, DoubleType):
        # nullability handled by StructField, MapType or ArrayType
        global gen_double
        return gen_double
    else:
        raise RuntimeError('Unsupported data type {}'.format(type(schema)))

def random_df_from_schema(spark, schema, length = 2048, seed=0):
    # We always want to produce the same result for the same schema and length
    random.seed(seed)
    gen = _make_gen_from(schema)
    data = []
    for index in range(0, length):
        data.append(gen())
    return spark.createDataFrame(data, schema)
