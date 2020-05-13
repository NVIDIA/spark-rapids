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

from datetime import date, datetime, timedelta, timezone
import math
from pyspark.sql.types import *
import pyspark.sql.functions as f
import pytest
import random
import sre_yield
import struct

class DataGen:
    """Base class for data generation"""

    def __repr__(self):
        return self.__class__.__name__[:-3]

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)

    def __init__(self, data_type, nullable=True):
        self.data_type = data_type
        self.nullable = nullable
        self._special_cases = []
        if nullable:
            self.with_special_case(None, weight=5.0)

    def with_special_case(self, special_case, weight=1.0):
        """
        Add in a special case with a given weight. A special case can either be
        a function that takes an instance of Random and returns the generated data
        or it can be a constant.  By default the weight is 1.0, and the default
        number generation's weight is 100.0.  The number of lines that are generate in
        the data set should be proportional to the its weight/sum weights
        """
        if callable(special_case):
            sc = special_case
        else:
            sc = lambda rand: special_case
        self._special_cases.append((weight, sc))
        return self

    def start(self, rand):
        """Start data generation using the given rand"""
        raise TypeError('Children should implement this method and call _start')

    def _start(self, rand, gen_func):
        """Start internally, but use the given gen_func as the base"""
        if not self._special_cases:
            self._gen_func = gen_func
        else:
            weighted_choices = [(100.0, lambda rand: gen_func())]
            weighted_choices.extend(self._special_cases)
            total = float(sum(weight for weight,gen in weighted_choices))
            normalized_choices = [(weight/total, gen) for weight,gen in weighted_choices]

            def choose_one():
                pick = rand.random()
                total = 0
                for (weight, gen) in normalized_choices:
                    total += weight
                    if total >= pick:
                        return gen(rand)
                raise RuntimeError('Random did not pick something we expected')
            self._gen_func = choose_one

    def gen(self):
        """generate the next line"""
        if not self._gen_func:
            raise RuntimeError('start must be called before generating any data')
        return self._gen_func()

    def contains_ts(self):
        """Checks if this contains a TimestampGen"""
        return False

class ConvertGen(DataGen):
    """Provides a way to modify the data before it is returned"""
    def __init__(self, child_gen, func, data_type=None, nullable=True):
        if data_type is None:
            data_type = child_gen.data_type
        super().__init__(data_type, nullable=nullable)
        self._child_gen = child_gen
        self._func = func

    def __repr__(self):
        return super().__repr__() + '(' + str(self._child_gen) + ')'

    def start(self, rand):
        self._child_gen.start(rand)
        def modify():
            return self._func(self._child_gen.gen())

        self._start(rand, modify)

_MAX_CHOICES = 1 << 64
class StringGen(DataGen):
    """Generate strings that match a pattern"""
    def __init__(self, pattern="(.|\n){1,30}", flags=0, charset=sre_yield.CHARSET, nullable=True):
        super().__init__(StringType(), nullable=nullable)
        self.base_strs = sre_yield.AllStrings(pattern, flags=flags, charset=charset, max_count=_MAX_CHOICES)

    def with_special_pattern(self, special_pattern, flags=0, charset=sre_yield.CHARSET, weight=1.0):
        """
        Like with_special_case but you can provide a regexp pattern
        instead of a hard coded string value.
        """
        strs = sre_yield.AllStrings(pattern, flags=flags, charset=charset, max_count=_MAX_CHOICES)
        try:
            length = int(len(strs))
        except OverflowError:
            length = _MAX_CHOICES
        return self.with_special_case(lambda rand : strs[rand.randint(0, length)], weight=weight)

    def start(self, rand):
        strs = self.base_strs
        try:
            length = int(len(strs))
        except OverflowError:
            length = _MAX_CHOICES
        self._start(rand, lambda : strs[rand.randint(0, length)])

_BYTE_MIN = -(1 << 7)
_BYTE_MAX = (1 << 7) - 1
class ByteGen(DataGen):
    """Generate Bytes"""
    def __init__(self, nullable=True):
        super().__init__(ByteType(), nullable=nullable)

    def start(self, rand):
        self._start(rand, lambda : rand.randint(_BYTE_MIN, _BYTE_MAX))

_SHORT_MIN = -(1 << 15)
_SHORT_MAX = (1 << 15) - 1
class ShortGen(DataGen):
    """Generate Shorts, which some built in corner cases."""
    def __init__(self, nullable=True):
        super().__init__(ShortType(), nullable=nullable)
        self.with_special_case(_SHORT_MIN)
        self.with_special_case(_SHORT_MAX)
        self.with_special_case(0)
        self.with_special_case(1)
        self.with_special_case(-1)

    def start(self, rand):
        self._start(rand, lambda : rand.randint(_SHORT_MIN, _SHORT_MAX))

_INT_MIN = -(1 << 31)
_INT_MAX = (1 << 31) - 1
class IntegerGen(DataGen):
    """Generate Ints, which some built in corner cases."""
    def __init__(self, nullable=True):
        super().__init__(IntegerType(), nullable=nullable)
        self.with_special_case(_INT_MIN)
        self.with_special_case(_INT_MAX)
        self.with_special_case(0)
        self.with_special_case(1)
        self.with_special_case(-1)

    def start(self, rand):
        self._start(rand, lambda : rand.randint(_INT_MIN, _INT_MAX))

_LONG_MIN = -(1 << 63)
_LONG_MAX = (1 << 63) - 1
class LongGen(DataGen):
    """Generate Longs, which some built in corner cases."""
    def __init__(self, nullable=True):
        super().__init__(LongType(), nullable=nullable)
        self.with_special_case(_LONG_MIN)
        self.with_special_case(_LONG_MAX)
        self.with_special_case(0)
        self.with_special_case(1)
        self.with_special_case(-1)

    def start(self, rand):
        self._start(rand, lambda : rand.randint(_LONG_MIN, _LONG_MAX))

_FLOAT_MIN = -3.4028235E38
_FLOAT_MAX = 3.4028235E38
_NEG_FLOAT_NAN = struct.unpack('f', struct.pack('I', 0xfff00001))[0]
class FloatGen(DataGen):
    """Generate floats, which some built in corner cases."""
    def __init__(self, nullable=True):
        super().__init__(FloatType(), nullable=nullable)
        self.with_special_case(_FLOAT_MIN)
        self.with_special_case(_FLOAT_MAX)
        self.with_special_case(0.0)
        self.with_special_case(-0.0)
        self.with_special_case(1.0)
        self.with_special_case(-1.0)
        self.with_special_case(float('inf'))
        self.with_special_case(float('-inf'))
        self.with_special_case(float('nan'))
        self.with_special_case(_NEG_FLOAT_NAN)

    def start(self, rand):
        def gen_float():
            i = rand.randint(_INT_MIN, _INT_MAX)
            p = struct.pack('i', i)
            return struct.unpack('f', p)[0]
        self._start(rand, gen_float)

_DOUBLE_MIN_EXP = -1022
_DOUBLE_MAX_EXP = 1023
_DOUBLE_MAX_FRACTION = int('1'*52, 2)
_DOUBLE_MIN = -1.7976931348623157E308
_DOUBLE_MAX = 1.7976931348623157E308
_NEG_DOUBLE_NAN = struct.unpack('d', struct.pack('L', 0xfff0000000000001))[0]
class DoubleGen(DataGen):
    """Generate doubles, which some built in corner cases."""
    def __init__(self, min_exp=_DOUBLE_MIN_EXP, max_exp=_DOUBLE_MAX_EXP, nullable=True):
        super().__init__(DoubleType(), nullable=nullable)
        self._min_exp = min_exp
        self._max_exp = max_exp
        self._use_full_range = (self._min_exp == _DOUBLE_MIN_EXP) and (self._max_exp == _DOUBLE_MAX_EXP)

        self.with_special_case(self._make_from(1, self._max_exp, _DOUBLE_MAX_FRACTION))
        self.with_special_case(self._make_from(0, self._max_exp, _DOUBLE_MAX_FRACTION))
        self.with_special_case(self._make_from(1, self._min_exp, _DOUBLE_MAX_FRACTION))
        self.with_special_case(self._make_from(0, self._min_exp, _DOUBLE_MAX_FRACTION))
        if self._min_exp <= 0 and self._max_exp >= 0:
            self.with_special_case(0.0)
            self.with_special_case(-0.0)
        if self._min_exp <= 3 and self._max_exp >= 3:
            self.with_special_case(1.0)
            self.with_special_case(-1.0)
        self.with_special_case(float('inf'))
        self.with_special_case(float('-inf'))
        self.with_special_case(float('nan'))
        self.with_special_case(_NEG_DOUBLE_NAN)

    @staticmethod
    def _make_from(sign, exp, fraction):
        sign = sign & 1 # 1 bit
        exp = (exp + 1023) & 0x7FF # add bias and 11 bits
        fraction = fraction & _DOUBLE_MAX_FRACTION
        i = (sign << 63) | (exp << 52) | fraction
        p = struct.pack('L', i)
        ret = struct.unpack('d', p)[0]
        return ret

    def start(self, rand):
        if self._use_full_range:
            def gen_double():
                i = rand.randint(_LONG_MIN, _LONG_MAX)
                p = struct.pack('l', i)
                return struct.unpack('d', p)[0]
            self._start(rand, gen_double)
        else:
            def gen_part_double():
                sign = rand.getrandbits(1)
                exp = rand.randint(self._min_exp, self._max_exp)
                fraction = rand.getrandbits(52)
                return self._make_from(sign, exp, fraction)
            self._start(rand, gen_part_double)

class BooleanGen(DataGen):
    """Generate Bools (True/False)"""
    def __init__(self, nullable=True):
        super().__init__(BooleanType(), nullable=nullable)

    def start(self, rand):
        self._start(rand, lambda : bool(rand.getrandbits(1)))

class StructGen(DataGen):
    """Generate a Struct"""
    def __init__(self, children, nullable=True):
        """
        Initialize the struct with children.  The children should be of the form:
        [('name', Gen),('name_2', Gen2)]
        Where name is the name of the strict field and Gens are Generators of
        the type for that entry.
        """
        tmp = [StructField(name, child.data_type, nullable=child.nullable) for name, child in children]
        super().__init__(StructType(tmp), nullable=nullable)
        self.children = children

    def start(self, rand):
        for name, child in self.children:
            child.start(rand)
        def make_tuple():
            data = [child.gen() for name, child in self.children]
            return tuple(data)
        self._start(rand, make_tuple)

    def contains_ts(self):
        return any(child[1].contains_ts() for child in self.children)

class DateGen(DataGen):
    """Generate Dates in a given range"""
    def __init__(self, start=None, end=None, nullable=True):
        super().__init__(DateType(), nullable=nullable)
        if start is None:
            # spark supports times starting at
            # "0001-01-01 00:00:00.000000"
            start = date(1, 1, 1)
        elif not isinstance(start, date):
            raise RuntimeError('Unsupported type passed in for start {}'.format(start))

        if end is None:
            # spark supports time through
            # "9999-12-31 23:59:59.999999"
            end = date(9999, 12, 31)
        elif isinstance(end, timedelta):
            end = start + end
        elif not isinstance(start, date):
            raise RuntimeError('Unsupported type passed in for end {}'.format(end))

        self._start_day = self._to_days_since_epoch(start)
        self._end_day = self._to_days_since_epoch(end)
        
        self.with_special_case(start)
        self.with_special_case(end)

        # we want a few around the leap year if possible
        step = int((end.year - start.year) / 5.0)
        if (step != 0):
            years = {self._guess_leap_year(y) for y in range(start.year, end.year, step)}
            for y in years:
                leap_day = date(y, 2, 29)
                if (leap_day > start and leap_day < end):
                    self.with_special_case(leap_day)
                next_day = date(y, 3, 1)
                if (next_day > start and next_day < end):
                    self.with_special_case(next_day)

    @staticmethod
    def _guess_leap_year(t):
        y = int(math.ceil(t/4.0)) * 4
        if ((y % 100) == 0) and ((y % 400) != 0):
            y = y + 4
        return y

    _epoch = date(1970, 1, 1)
    _days = timedelta(days=1)
    def _to_days_since_epoch(self, val):
        return int((val - self._epoch)/self._days)

    def _from_days_since_epoch(self, days):
        return self._epoch + timedelta(days=days)

    def start(self, rand):
        start = self._start_day
        end = self._end_day
        self._start(rand, lambda : self._from_days_since_epoch(rand.randint(start, end)))

class TimestampGen(DataGen):
    """Generate Timestamps in a given range. All timezones are UTC by default."""
    def __init__(self, start=None, end=None, nullable=True):
        super().__init__(TimestampType(), nullable=nullable)
        if start is None:
            # spark supports times starting at
            # "0001-01-01 00:00:00.000000"
            start = datetime(1, 1, 1, tzinfo=timezone.utc)
        elif not isinstance(start, datetime):
            raise RuntimeError('Unsupported type passed in for start {}'.format(start))

        if end is None:
            # spark supports time through
            # "9999-12-31 23:59:59.999999"
            end = datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)
        elif isinstance(end, timedelta):
            end = start + end
        elif not isinstance(start, date):
            raise RuntimeError('Unsupported type passed in for end {}'.format(end))

        self._start_time = self._to_ms_since_epoch(start)
        self._end_time = self._to_ms_since_epoch(end)
        if (self._epoch >= start and self._epoch <= end):
            self.with_special_case(self._epoch)

    _epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    _ms = timedelta(milliseconds=1)
    def _to_ms_since_epoch(self, val):
        return int((val - self._epoch)/self._ms)

    def _from_ms_since_epoch(self, ms):
        return self._epoch + timedelta(milliseconds=ms)

    def start(self, rand):
        start = self._start_time
        end = self._end_time
        self._start(rand, lambda : self._from_ms_since_epoch(rand.randint(start, end)))

    def contains_ts(self):
        return True

class ArrayGen(DataGen):
    """Generate Arrays of data."""
    def __init__(self, child_gen, min_length=0, max_length=100, nullable=True):
        super().__init__(ArrayType(child_gen.data_type, containsNull=child_gen.nullable), nullable=nullable)
        self._min_length = min_length
        self._max_length = max_length
        self._child_gen = child_gen

    def start(self, rand):
        self._child_gen.start(rand)
        def gen_array():
            length = rand.randint(self._min_length, self._max_length)
            return [self._child_gen.gen() for index in range(0, length)]
        self._start(rand, gen_array)


def gen_df(spark, data_gen, length=2048, seed=0):
    """Generate a spark dataframe from the given data generators."""
    if isinstance(data_gen, list):
        src = StructGen(data_gen, nullable=False)
    else:
        src = data_gen
        # we cannot create a data frame from a nullable struct
        assert not data_gen.nullable

    # Before we get too far we need to verify that we can run with timestamps
    if src.contains_ts():
        # Now we have to do some kind of ugly internal java stuff
        jvm = spark.sparkContext._jvm
        utc = jvm.java.time.ZoneId.of('UTC').normalized()
        sys_tz = jvm.java.time.ZoneId.systemDefault().normalized()
        if (utc != sys_tz):
            pytest.skip('The java system time zone is not set to UTC but is {}'.format(sys_tz))

    rand = random.Random(seed)
    src.start(rand)
    data = [src.gen() for index in range(0, length)]
    return spark.createDataFrame(data, src.data_type)

def _mark_as_lit(data):
    # Sadly you cannot create a literal from just an array in pyspark
    if isinstance(data, list):
        return f.array([_mark_as_lit(x) for x in data])
    return f.lit(data)

def gen_scalar(data_gen, seed=0):
    """Generate a single scalar value."""
    if isinstance(data_gen, list):
        src = StructGen(data_gen, nullable=False)
    else:
        src = data_gen

    # Before we get too far we need to verify that we can run with timestamps
    if src.contains_ts():
        # Now we have to do some kind of ugly internal java stuff
        jvm = spark.sparkContext._jvm
        utc = jvm.java.time.ZoneId.of('UTC').normalized()
        sys_tz = jvm.java.time.ZoneId.systemDefault().normalized()
        if (utc != sys_tz):
            pytest.skip('The java system time zone is not set to UTC but is {}'.format(sys_tz))

    rand = random.Random(seed)
    src.start(rand)
    return _mark_as_lit(src.gen())

def debug_df(df):
    """print out the contents of a dataframe for debugging."""
    print('COLLECTED\n{}'.format(df.collect()))
    return df

def idfn(val):
    """Provide an API to provide display names for data type generators."""
    return str(val)

def to_cast_string(spark_type):
    if isinstance(spark_type, ByteType):
        return 'BYTE'
    elif isinstance(spark_type, ShortType):
        return 'SHORT'
    elif isinstance(spark_type, IntegerType):
        return 'INT'
    elif isinstance(spark_type, LongType):
        return 'LONG'
    elif isinstance(spark_type, FloatType):
        return 'FLOAT'
    elif isinstance(spark_type, DoubleType):
        return 'DOUBLE'
    elif isinstance(spark_type, BooleanType):
        return 'BOOLEAN'
    elif isinstance(spark_type, DateType):
        return 'DATE'
    elif isinstance(spark_type, TimestampType):
        return 'TIMESTAMP'
    elif isinstance(spark_type, StringType):
        return 'STRING'
    else:
        raise RuntimeError('CAST TO TYPE {} NOT SUPPORTED YET'.format(spark_type))

