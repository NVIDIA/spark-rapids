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

from pyspark.sql.types import *
import random
import sre_yield
import struct

class DataGen:
    """
    Base class for data generation
    """

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
        if callable(special_case):
            sc = special_case
        else:
            sc = lambda rand: special_case
        self._special_cases.append((weight, sc))
        return self

    def start(self, rand):
        raise TypeError('Children should implement this method and call _start')

    def _start(self, rand, gen_func):
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
        if not self._gen_func:
            raise RuntimeError('start must be called before generateing any data')
        return self._gen_func()

_MAX_CHOICES = 1 << 64
class StringGen(DataGen):
    def __init__(self, pattern="(.|\n){1,30}", flags=0, charset=sre_yield.CHARSET, nullable=True):
        super().__init__(StringType(), nullable=nullable)
        self.base_strs = sre_yield.AllStrings(pattern, flags=flags, charset=charset, max_count=_MAX_CHOICES)

    def with_special_pattern(self, special_pattern, flags=0, charset=sre_yield.CHARSET, weight=1.0):
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
    def __init__(self, nullable=True):
        super().__init__(ByteType(), nullable=nullable)

    def start(self, rand):
        self._start(rand, lambda : rand.randint(_BYTE_MIN, _BYTE_MAX))

_SHORT_MIN = -(1 << 15)
_SHORT_MAX = (1 << 15) - 1
class ShortGen(DataGen):
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
    def __init__(self, nullable=True):
        super().__init__(FloatType(), nullable=nullable)
        self.with_special_case(_FLOAT_MIN)
        self.with_special_case(_FLOAT_MAX)
        self.with_special_case(0.0)
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

_DOUBLE_MIN = -1.7976931348623157E308
_DOUBLE_MAX = 1.7976931348623157E308
_NEG_DOUBLE_NAN = struct.unpack('d', struct.pack('L', 0xfff0000000000001))[0]
class DoubleGen(DataGen):
    def __init__(self, nullable=True):
        super().__init__(DoubleType(), nullable=nullable)
        self.with_special_case(_DOUBLE_MIN)
        self.with_special_case(_DOUBLE_MAX)
        self.with_special_case(0.0)
        self.with_special_case(1.0)
        self.with_special_case(-1.0)
        self.with_special_case(float('inf'))
        self.with_special_case(float('-inf'))
        self.with_special_case(float('nan'))
        self.with_special_case(_NEG_DOUBLE_NAN)

    def start(self, rand):
        def gen_double():
            i = rand.randint(_LONG_MIN, _LONG_MAX)
            p = struct.pack('l', i)
            return struct.unpack('d', p)[0]
        self._start(rand, gen_double)

class StructGen(DataGen):
    def __init__(self, children, nullable=False):
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

def gen_df(spark, data_gen, length=2048, seed=0):
    if isinstance(data_gen, list):
        src = StructGen(data_gen)
    else:
        src = data_gen
    rand = random.Random(seed)
    src.start(rand)
    data = [src.gen() for index in range(0, length)]
    return spark.createDataFrame(data, src.data_type)
