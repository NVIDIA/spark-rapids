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

import pytest

_incompat = False

@pytest.fixture
def incompat():
    """Marks a test as using an incompat operator"""
    global _incompat 
    _incompat = True
    yield _incompat
    _incompat = False

def is_incompat():
    return _incompat

_ignore_order = False

@pytest.fixture
def ignore_order():
    """Marks a test as producing a different order that the CPU version."""
    global _ignore_order
    _ignore_order = True
    yield _ignore_order
    _ignore_order = False

def is_order_ignored():
    return _ignore_order

