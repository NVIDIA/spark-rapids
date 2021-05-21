# Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

import copy
from datetime import date, datetime, timedelta, timezone
from decimal import *
import math
from pyspark.sql import Row
from pyspark.sql.types import *
import pyspark.sql.functions as f
import pytest
import random
from spark_session import is_tz_utc
import sre_yield
import struct
from conftest import skip_unless_precommit_tests

class DataGen:
    """Base class for data generation"""

    def __repr__(self):
        if not self.nullable:
            return self.__class__.__name__[:-3] + '(not_null)'
        return self.__class__.__name__[:-3]

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)

    def __init__(self, data_type, nullable=True, special_cases =[]):
        self.data_type = data_type
        self.list_of_special_cases = special_cases
        self._special_cases = []
        if isinstance(nullable, tuple):
            self.nullable = nullable[0]
            weight = nullable[1]
        else:
            self.nullable = nullable
            weight = 5.0
        if self.nullable:
            self.with_special_case(None, weight)

        # Special cases can be a value or a tuple of (value, weight). If the
        # special_case itself is a tuple as in the case of StructGen, it MUST be added with a
        # weight like : ((special_case_tuple_v1, special_case_tuple_v2), weight).
        for element in special_cases:
            if isinstance(element, tuple):
                self.with_special_case(element[0], element[1])
            else:
                self.with_special_case(element)

    def copy_special_case(self, special_case, weight=1.0):
        # it would be good to do a deepcopy, but sre_yield is not happy with that.
        c = copy.copy(self)
        c._special_cases = copy.deepcopy(self._special_cases)

        return c.with_special_case(special_case, weight=weight)

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

    def get_types(self):
        return 'DataType: {}, nullable: {}, special_cases: {}'.format(self.data_type,
          self.nullable, self.list_of_special_cases)

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

    def gen(self, force_no_nulls=False):
        """generate the next line"""
        if not self._gen_func:
            raise RuntimeError('start must be called before generating any data')
        v = self._gen_func()
        if force_no_nulls:
            while v is None:
                v = self._gen_func()
        return v

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

    def with_special_pattern(self, pattern, flags=0, charset=sre_yield.CHARSET, weight=1.0):
        """
        Like with_special_case but you can provide a regexp pattern
        instead of a hard coded string value.
        """
        strs = sre_yield.AllStrings(pattern, flags=flags, charset=charset, max_count=_MAX_CHOICES)
        try:
            length = int(len(strs))
        except OverflowError:
            length = _MAX_CHOICES
        return self.with_special_case(lambda rand : strs[rand.randrange(0, length)], weight=weight)

    def start(self, rand):
        strs = self.base_strs
        try:
            length = int(len(strs))
        except OverflowError:
            length = _MAX_CHOICES
        self._start(rand, lambda : strs[rand.randrange(0, length)])

_BYTE_MIN = -(1 << 7)
_BYTE_MAX = (1 << 7) - 1
class ByteGen(DataGen):
    """Generate Bytes"""
    def __init__(self, nullable=True, min_val =_BYTE_MIN, max_val = _BYTE_MAX, special_cases=[]):
        super().__init__(ByteType(), nullable=nullable, special_cases=special_cases)
        self._min_val = min_val
        self._max_val = max_val

    def start(self, rand):
        self._start(rand, lambda : rand.randint(self._min_val, self._max_val))

SHORT_MIN = -(1 << 15)
SHORT_MAX = (1 << 15) - 1
class ShortGen(DataGen):
    """Generate Shorts, which some built in corner cases."""
    def __init__(self, nullable=True, min_val = SHORT_MIN, max_val = SHORT_MAX,
                 special_cases = [SHORT_MIN, SHORT_MAX, 0, 1, -1]):
        super().__init__(ShortType(), nullable=nullable, special_cases=special_cases)
        self._min_val = min_val
        self._max_val = max_val

    def start(self, rand):
        self._start(rand, lambda : rand.randint(self._min_val, self._max_val))

INT_MIN = -(1 << 31)
INT_MAX = (1 << 31) - 1
class IntegerGen(DataGen):
    """Generate Ints, which some built in corner cases."""
    def __init__(self, nullable=True, min_val = INT_MIN, max_val = INT_MAX,
                 special_cases = [INT_MIN, INT_MAX, 0, 1, -1]):
        super().__init__(IntegerType(), nullable=nullable, special_cases=special_cases)
        self._min_val = min_val
        self._max_val = max_val

    def start(self, rand):
        self._start(rand, lambda : rand.randint(self._min_val, self._max_val))

class DecimalGen(DataGen):
    """Generate Decimals, with some built in corner cases."""
    def __init__(self, precision=None, scale=None, nullable=True, special_cases=[]):
        if precision is None:
            #Maximum number of decimal digits a Long can represent is 18
            precision = 18
            scale = 0
        DECIMAL_MIN = Decimal('-' + ('9' * precision) + 'e' + str(-scale))
        DECIMAL_MAX = Decimal(('9'* precision) + 'e' + str(-scale))
        super().__init__(DecimalType(precision, scale), nullable=nullable, special_cases=special_cases)
        self.scale = scale
        self.precision = precision
        pattern = "[0-9]{1,"+ str(precision) + "}e" + str(-scale)
        self.base_strs = sre_yield.AllStrings(pattern, flags=0, charset=sre_yield.CHARSET, max_count=_MAX_CHOICES)

    def __repr__(self):
        return super().__repr__() + '(' + str(self.precision) + ',' + str(self.scale) + ')'

    def start(self, rand):
        strs = self.base_strs
        try:
            length = int(len(strs))
        except OverflowError:
            length = _MAX_CHOICES
        self._start(rand, lambda : Decimal(strs[rand.randrange(0, length)]))

LONG_MIN = -(1 << 63)
LONG_MAX = (1 << 63) - 1
class LongGen(DataGen):
    """Generate Longs, which some built in corner cases."""
    def __init__(self, nullable=True, min_val =LONG_MIN, max_val = LONG_MAX,
                 special_cases = [LONG_MIN, LONG_MAX, 0, 1, -1]):
        super().__init__(LongType(), nullable=nullable, special_cases=special_cases)
        self._min_val = min_val
        self._max_val = max_val

    def start(self, rand):
        self._start(rand, lambda : rand.randint(self._min_val, self._max_val))

class LongRangeGen(DataGen):
    """Generate Longs in incrementing order."""
    def __init__(self, nullable=False, start_val=0, direction="inc"):
        super().__init__(LongType(), nullable=nullable)
        self._start_val = start_val
        self._current_val = start_val
        if (direction == "dec"):
            def dec_it():
                tmp = self._current_val
                self._current_val -= 1
                return tmp
            self._do_it = dec_it
        else:
            def inc_it():
                tmp = self._current_val
                self._current_val += 1
                return tmp
            self._do_it = inc_it

    def start(self, rand):
        self._current_val = self._start_val
        self._start(rand, self._do_it)

class RepeatSeqGen(DataGen):
    """Generate Repeated seq of `length` random items"""
    def __init__(self, child, length):
        super().__init__(child.data_type, nullable=False)
        self.nullable = child.nullable
        self._child = child
        self._vals = []
        self._length = length
        self._index = 0

    def __repr__(self):
        return super().__repr__() + '(' + str(self._child) + ')'

    def _loop_values(self):
        ret = self._vals[self._index]
        self._index = (self._index + 1) % self._length
        return ret

    def start(self, rand):
        self._index = 0
        self._child.start(rand)
        self._start(rand, self._loop_values)
        self._vals = [self._child.gen() for _ in range(0, self._length)]

class SetValuesGen(DataGen):
    """A set of values that are randomly selected"""
    def __init__(self, data_type, data):
        super().__init__(data_type, nullable=False)
        self.nullable = any(x is None for x in data)
        self._vals = data

    def __repr__(self):
        return super().__repr__() + '(' + str(self._child) + ')'

    def start(self, rand):
        data = self._vals
        length = len(data)
        self._start(rand, lambda : data[rand.randrange(0, length)])

FLOAT_MIN = -3.4028235E38
FLOAT_MAX = 3.4028235E38
NEG_FLOAT_NAN_MIN_VALUE = struct.unpack('f', struct.pack('I', 0xffffffff))[0]
NEG_FLOAT_NAN_MAX_VALUE = struct.unpack('f', struct.pack('I', 0xff800001))[0]
POS_FLOAT_NAN_MIN_VALUE = struct.unpack('f', struct.pack('I', 0x7f800001))[0]
POS_FLOAT_NAN_MAX_VALUE = struct.unpack('f', struct.pack('I', 0x7fffffff))[0]
class FloatGen(DataGen):
    """Generate floats, which some built in corner cases."""
    def __init__(self, nullable=True,
            no_nans=False, special_cases=None):
        self._no_nans = no_nans
        if special_cases is None:
            special_cases = [FLOAT_MIN, FLOAT_MAX, 0.0, -0.0, 1.0, -1.0]
            if not no_nans:
                special_cases.append(float('inf'))
                special_cases.append(float('-inf'))
                special_cases.append(float('nan'))
                special_cases.append(NEG_FLOAT_NAN_MAX_VALUE)
        super().__init__(FloatType(), nullable=nullable, special_cases=special_cases)

    def _fixup_nans(self, v):
        if self._no_nans and (math.isnan(v) or v == math.inf or v == -math.inf):
            v = None if self.nullable else 0.0
        return v

    def start(self, rand):
        def gen_float():
            i = rand.randint(INT_MIN, INT_MAX)
            p = struct.pack('i', i)
            return self._fixup_nans(struct.unpack('f', p)[0])
        self._start(rand, gen_float)

DOUBLE_MIN_EXP = -1022
DOUBLE_MAX_EXP = 1023
DOUBLE_MAX_FRACTION = int('1'*52, 2)
DOUBLE_MIN = -1.7976931348623157E308
DOUBLE_MAX = 1.7976931348623157E308
NEG_DOUBLE_NAN_MIN_VALUE = struct.unpack('d', struct.pack('L', 0xffffffffffffffff))[0]
NEG_DOUBLE_NAN_MAX_VALUE = struct.unpack('d', struct.pack('L', 0xfff0000000000001))[0]
POS_DOUBLE_NAN_MIN_VALUE = struct.unpack('d', struct.pack('L', 0x7ff0000000000001))[0]
POS_DOUBLE_NAN_MAX_VALUE = struct.unpack('d', struct.pack('L', 0x7fffffffffffffff))[0]
class DoubleGen(DataGen):
    """Generate doubles, which some built in corner cases."""
    def __init__(self, min_exp=DOUBLE_MIN_EXP, max_exp=DOUBLE_MAX_EXP, no_nans=False,
            nullable=True, special_cases = None):
        self._min_exp = min_exp
        self._max_exp = max_exp
        self._no_nans = no_nans
        self._use_full_range = (self._min_exp == DOUBLE_MIN_EXP) and (self._max_exp == DOUBLE_MAX_EXP)
        if special_cases is None:
            special_cases = [
                self.make_from(1, self._max_exp, DOUBLE_MAX_FRACTION),
                self.make_from(0, self._max_exp, DOUBLE_MAX_FRACTION),
                self.make_from(1, self._min_exp, DOUBLE_MAX_FRACTION),
                self.make_from(0, self._min_exp, DOUBLE_MAX_FRACTION)
            ]
            if self._min_exp <= 0 and self._max_exp >= 0:
                special_cases.append(0.0)
                special_cases.append(-0.0)
            if self._min_exp <= 3 and self._max_exp >= 3:
                special_cases.append(1.0)
                special_cases.append(-1.0)
            if not no_nans:
                special_cases.append(float('inf'))
                special_cases.append(float('-inf'))
                special_cases.append(float('nan'))
                special_cases.append(NEG_DOUBLE_NAN_MAX_VALUE)
        super().__init__(DoubleType(), nullable=nullable, special_cases=special_cases)

    @staticmethod
    def make_from(sign, exp, fraction):
        sign = sign & 1 # 1 bit
        exp = (exp + 1023) & 0x7FF # add bias and 11 bits
        fraction = fraction & DOUBLE_MAX_FRACTION
        i = (sign << 63) | (exp << 52) | fraction
        p = struct.pack('L', i)
        ret = struct.unpack('d', p)[0]
        return ret

    def _fixup_nans(self, v):
        if self._no_nans and (math.isnan(v) or v == math.inf or v == -math.inf):
            v = None if self.nullable else 0.0
        return v

    def start(self, rand):
        if self._use_full_range:
            def gen_double():
                i = rand.randint(LONG_MIN, LONG_MAX)
                p = struct.pack('l', i)
                return self._fixup_nans(struct.unpack('d', p)[0])
            self._start(rand, gen_double)
        else:
            def gen_part_double():
                sign = rand.getrandbits(1)
                exp = rand.randint(self._min_exp, self._max_exp)
                fraction = rand.getrandbits(52)
                return self._fixup_nans(self.make_from(sign, exp, fraction))
            self._start(rand, gen_part_double)

class BooleanGen(DataGen):
    """Generate Bools (True/False)"""
    def __init__(self, nullable=True):
        super().__init__(BooleanType(), nullable=nullable)

    def start(self, rand):
        self._start(rand, lambda : bool(rand.getrandbits(1)))

class StructGen(DataGen):
    """Generate a Struct"""
    def __init__(self, children, nullable=True, special_cases=[]):
        """
        Initialize the struct with children.  The children should be of the form:
        [('name', Gen),('name_2', Gen2)]
        Where name is the name of the strict field and Gens are Generators of
        the type for that entry.
        """
        tmp = [StructField(name, child.data_type, nullable=child.nullable) for name, child in children]
        super().__init__(StructType(tmp), nullable=nullable, special_cases=special_cases)
        self.children = children

    def __repr__(self):
        return super().__repr__() + '(' + ','.join([str(i) for i in self.children]) + ')'

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
        if (y == 10000):
            y = y - 4
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
    def __init__(self, child_gen, min_length=0, max_length=20, nullable=True, all_null=False):
        super().__init__(ArrayType(child_gen.data_type, containsNull=child_gen.nullable), nullable=nullable)
        self._min_length = min_length
        self._max_length = max_length
        self._child_gen = child_gen
        self.all_null = all_null

    def __repr__(self):
        return super().__repr__() + '(' + str(self._child_gen) + ')'

    def start(self, rand):
        self._child_gen.start(rand)
        def gen_array():
            if self.all_null:
                return None
            length = rand.randint(self._min_length, self._max_length)
            return [self._child_gen.gen() for _ in range(0, length)]
        self._start(rand, gen_array)

    def contains_ts(self):
        return self._child_gen.contains_ts()

class MapGen(DataGen):
    """Generate a Map"""
    def __init__(self, key_gen, value_gen, min_length=0, max_length=20, nullable=True, special_cases=[]):
        # keys cannot be nullable
        assert not key_gen.nullable
        self._min_length = min_length
        self._max_length = max_length
        self._key_gen = key_gen
        self._value_gen = value_gen
        super().__init__(MapType(key_gen.data_type, value_gen.data_type, valueContainsNull=value_gen.nullable), nullable=nullable, special_cases=special_cases)

    def __repr__(self):
        return super().__repr__() + '(' + str(self._key_gen) + ',' + str(self._value_gen) + ')'

    def start(self, rand):
        self._key_gen.start(rand)
        self._value_gen.start(rand)
        def make_dict():
            length = rand.randint(self._min_length, self._max_length)
            return {self._key_gen.gen(): self._value_gen.gen() for idx in range(0, length)}
        self._start(rand, make_dict)

    def contains_ts(self):
        return self._key_gen.contains_ts() or self._value_gen.contains_ts()


class NullGen(DataGen):
    """Generate NullType values"""
    def __init__(self):
        super().__init__(NullType(), nullable=True)

    def start(self, rand):
        def make_null():
            return None
        self._start(rand, make_null)

def skip_if_not_utc():
    if (not is_tz_utc()):
        skip_unless_precommit_tests('The java system time zone is not set to UTC')

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
        skip_if_not_utc()

    rand = random.Random(seed)
    src.start(rand)
    data = [src.gen() for index in range(0, length)]
    return spark.createDataFrame(data, src.data_type)

def _mark_as_lit(data, data_type = None):
    # Sadly you cannot create a literal from just an array in pyspark
    if isinstance(data, list):
        return f.array([_mark_as_lit(x) for x in data])
    if data_type is None:
        return f.lit(data)
    else:
        # lit does not take a data type so we might have to cast it
        return f.lit(data).cast(data_type)

def _gen_scalars_common(data_gen, count, seed=0):
    if isinstance(data_gen, list):
        src = StructGen(data_gen, nullable=False)
    else:
        src = data_gen

    # Before we get too far we need to verify that we can run with timestamps
    if src.contains_ts():
        skip_if_not_utc()

    rand = random.Random(seed)
    src.start(rand)
    return src

def gen_scalars(data_gen, count, seed=0, force_no_nulls=False):
    """Generate scalar values."""
    if force_no_nulls:
        assert(not isinstance(data_gen, NullGen))
    src = _gen_scalars_common(data_gen, count, seed=seed)
    data_type = src.data_type
    return (_mark_as_lit(src.gen(force_no_nulls=force_no_nulls), data_type) for i in range(0, count))

def gen_scalar(data_gen, seed=0, force_no_nulls=False):
    """Generate a single scalar value."""
    v = list(gen_scalars(data_gen, 1, seed=seed, force_no_nulls=force_no_nulls))
    return v[0]

def gen_scalar_values(data_gen, count, seed=0, force_no_nulls=False):
    """Generate scalar values."""
    src = _gen_scalars_common(data_gen, count, seed=seed)
    return (src.gen(force_no_nulls=force_no_nulls) for i in range(0, count))

def gen_scalar_value(data_gen, seed=0, force_no_nulls=False):
    """Generate a single scalar value."""
    v = list(gen_scalar_values(data_gen, 1, seed=seed, force_no_nulls=force_no_nulls))
    return v[0]

def debug_df(df, path = None, file_format = 'json', num_parts = 1):
    """Print out or save the contents and the schema of a dataframe for debugging."""

    if path is not None:
        # Save the dataframe and its schema
        # The schema can be re-created by using DataType.fromJson and used
        # for loading the dataframe
        file_name = f"{path}.{file_format}"
        schema_file_name = f"{path}.schema.json"

        df.coalesce(num_parts).write.format(file_format).save(file_name)
        print(f"SAVED df output for debugging at {file_name}")

        schema_json = df.schema.json()
        schema_file = open(schema_file_name , 'w')
        schema_file.write(schema_json)
        schema_file.close()
        print(f"SAVED df schema for debugging along in the output dir")
    else:
        print('COLLECTED\n{}'.format(df.collect()))

    df.explain()
    df.printSchema()
    return df

def print_params(data_gen):
    print('Test Datagen Params=' + str([(a, b.get_types()) for a, b in data_gen]))

def idfn(val):
    """Provide an API to provide display names for data type generators."""
    return str(val)

def three_col_df(spark, a_gen, b_gen, c_gen, length=2048, seed=0):
    gen = StructGen([('a', a_gen),('b', b_gen),('c', c_gen)], nullable=False)
    return gen_df(spark, gen, length=length, seed=seed)

def two_col_df(spark, a_gen, b_gen, length=2048, seed=0):
    gen = StructGen([('a', a_gen),('b', b_gen)], nullable=False)
    return gen_df(spark, gen, length=length, seed=seed)

def binary_op_df(spark, gen, length=2048, seed=0):
    return two_col_df(spark, gen, gen, length=length, seed=seed)

def unary_op_df(spark, gen, length=2048, seed=0):
    return gen_df(spark, StructGen([('a', gen)], nullable=False), length=length, seed=seed)

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
    elif isinstance(spark_type, DecimalType):
        return 'DECIMAL({}, {})'.format(spark_type.precision, spark_type.scale)
    else:
        raise RuntimeError('CAST TO TYPE {} NOT SUPPORTED YET'.format(spark_type))

def get_null_lit_string(spark_type):
    if isinstance(spark_type, NullType):
        return 'null'
    else:
        string_type = to_cast_string(spark_type)
        return 'CAST(null as {})'.format(string_type)

def _convert_to_sql(t, data):
    if isinstance(data, str):
        d = "'" + data.replace("'", "\\'") + "'"
    elif isinstance(data, datetime):
        d = "'" + data.strftime('%Y-%m-%d T%H:%M:%S.%f').zfill(26) + "'"
    elif isinstance(data, date):
        d = "'" + data.strftime('%Y-%m-%d').zfill(10) + "'"
    else:
        d = str(data)

    return 'CAST({} as {})'.format(d, t)

def gen_scalars_for_sql(data_gen, count, seed=0, force_no_nulls=False):
    """Generate scalar values, but strings that can be used in selectExpr or SQL"""
    src = _gen_scalars_common(data_gen, count, seed=seed)
    if isinstance(data_gen, NullGen):
        assert not force_no_nulls
        return ('null' for i in range(0, count))
    string_type = to_cast_string(data_gen.data_type)
    return (_convert_to_sql(string_type, src.gen(force_no_nulls=force_no_nulls)) for i in range(0, count))

byte_gen = ByteGen()
short_gen = ShortGen()
int_gen = IntegerGen()
long_gen = LongGen()
float_gen = FloatGen()
double_gen = DoubleGen()
string_gen = StringGen()
boolean_gen = BooleanGen()
date_gen = DateGen()
timestamp_gen = TimestampGen()
decimal_gen_default = DecimalGen()
decimal_gen_neg_scale = DecimalGen(precision=7, scale=-3)
decimal_gen_scale_precision = DecimalGen(precision=7, scale=3)
decimal_gen_same_scale_precision = DecimalGen(precision=7, scale=7)
decimal_gen_64bit = DecimalGen(precision=12, scale=2)

null_gen = NullGen()

numeric_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen]

integral_gens = [byte_gen, short_gen, int_gen, long_gen]
# A lot of mathematical expressions only support a double as input
# by parametrizing even for a single param for the test it makes the tests consistent
double_gens = [double_gen]
double_n_long_gens = [double_gen, long_gen]
int_n_long_gens = [int_gen, long_gen]
decimal_gens = [decimal_gen_default, decimal_gen_neg_scale, decimal_gen_scale_precision,
        decimal_gen_same_scale_precision, decimal_gen_64bit]

# all of the basic gens
all_basic_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
        string_gen, boolean_gen, date_gen, timestamp_gen, null_gen]

# TODO add in some array generators to this once that is supported for sorting
# a selection of generators that should be orderable (sortable and compareable)
orderable_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
        string_gen, boolean_gen, date_gen, timestamp_gen, null_gen] + decimal_gens

# TODO add in some array generators to this once that is supported for these operations
# a selection of generators that can be compared for equality
eq_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
        string_gen, boolean_gen, date_gen, timestamp_gen, null_gen]

# Include decimal type while testing equalTo and notEqualTo
eq_gens_with_decimal_gen =  eq_gens + decimal_gens

#gen for testing round operator
round_gens = numeric_gens + decimal_gens

date_gens = [date_gen]
date_n_time_gens = [date_gen, timestamp_gen]

boolean_gens = [boolean_gen]

single_level_array_gens = [ArrayGen(sub_gen) for sub_gen in all_basic_gens + decimal_gens + [null_gen]]

single_level_array_gens_no_decimal = [ArrayGen(sub_gen) for sub_gen in all_basic_gens + [null_gen]]

# Be careful to not make these too large of data generation takes for ever
# This is only a few nested array gens, because nesting can be very deep
nested_array_gens_sample = [ArrayGen(ArrayGen(short_gen, max_length=10), max_length=10),
        ArrayGen(ArrayGen(string_gen, max_length=10), max_length=10),
        ArrayGen(StructGen([['child0', byte_gen], ['child1', string_gen], ['child2', float_gen]]))]

# Some array gens, but not all because of nesting
array_gens_sample = single_level_array_gens + nested_array_gens_sample

# all of the basic types in a single struct
all_basic_struct_gen = StructGen([['child'+str(ind), sub_gen] for ind, sub_gen in enumerate(all_basic_gens)])

# Some struct gens, but not all because of nesting
struct_gens_sample = [all_basic_struct_gen,
        StructGen([['child0', byte_gen]]),
        StructGen([['child0', ArrayGen(short_gen)], ['child1', double_gen]])]

simple_string_to_string_map_gen = MapGen(StringGen(pattern='key_[0-9]', nullable=False),
        StringGen(), max_length=10)

# Some map gens, but not all because of nesting
map_gens_sample = [simple_string_to_string_map_gen,
        MapGen(StringGen(pattern='key_[0-9]', nullable=False), ArrayGen(string_gen), max_length=10),
        MapGen(RepeatSeqGen(IntegerGen(nullable=False), 10), long_gen, max_length=10),
        MapGen(BooleanGen(nullable=False), boolean_gen, max_length=2),
        MapGen(StringGen(pattern='key_[0-9]', nullable=False), simple_string_to_string_map_gen)]

allow_negative_scale_of_decimal_conf = {'spark.sql.legacy.allowNegativeScaleOfDecimal': 'true'}

no_nans_conf = {'spark.rapids.sql.hasNans': 'false'}

all_gen = [StringGen(), ByteGen(), ShortGen(), IntegerGen(), LongGen(),
           FloatGen(), DoubleGen(), BooleanGen(), DateGen(), TimestampGen(),
           decimal_gen_default, decimal_gen_scale_precision, decimal_gen_same_scale_precision,
           decimal_gen_64bit]

# Pyarrow will complain the error as below if the timestamp is out of range for both CPU and GPU,
# so narrow down the time range to avoid exceptions causing test failures.
#
#     "pyarrow.lib.ArrowInvalid: Casting from timestamp[us, tz=UTC] to timestamp[ns]
#      would result in out of bounds timestamp: 51496791452587000"
#
# This issue has been fixed in pyarrow by the PR https://github.com/apache/arrow/pull/7169
# However it still requires PySpark to specify the new argument "timestamp_as_object".
arrow_common_gen = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
        string_gen, boolean_gen, date_gen,
        TimestampGen(start=datetime(1970, 1, 1, tzinfo=timezone.utc),
                     end=datetime(2262, 1, 1, tzinfo=timezone.utc))]

arrow_array_gens = [ArrayGen(subGen) for subGen in arrow_common_gen] + nested_array_gens_sample

arrow_one_level_struct_gen = StructGen([
        ['child'+str(i), sub_gen] for i, sub_gen in enumerate(arrow_common_gen)])

arrow_struct_gens = [arrow_one_level_struct_gen,
        StructGen([['child0', ArrayGen(short_gen)], ['child1', arrow_one_level_struct_gen]])]

# This function adds a new column named uniq_int where each row
# has a new unique integer value. It just starts at 0 and
# increments by 1 for each row.
# This can be used to add a column to a dataframe if you need to
# sort on a column with unique values.
# This collects the data to driver though so can be expensive.
def append_unique_int_col_to_df(spark, dataframe):
    def append_unique_to_rows(rows):
        new = []
        for item in range(len(rows)):
            row_dict = rows[item].asDict()
            row_dict['uniq_int'] = item
            new_row = Row(**row_dict)
            new.append(new_row)
        return new

    collected = dataframe.collect()
    if (len(collected) > INT_MAX):
        raise RuntimeError('To many rows to add unique integer values starting from 0 to')
    existing_schema = dataframe.schema
    new_rows = append_unique_to_rows(collected)
    new_schema = StructType(existing_schema.fields + [StructField("uniq_int", IntegerType(), False)])
    return spark.createDataFrame(new_rows, new_schema)
