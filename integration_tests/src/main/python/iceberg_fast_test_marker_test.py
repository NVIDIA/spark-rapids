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

"""Exercise fast-suite selection with real pytest collection, without Spark jobs."""

from pathlib import Path

import pytest

pytest_plugins = ["pytester"]


@pytest.fixture
def marker_suite(pytester: pytest.Pytester) -> pytest.Pytester:
    python_dir = Path(__file__).resolve().parent
    pytester.makeini((python_dir.parents[2] / "pytest.ini").read_text())
    pytester.makepyfile(marks=(python_dir / "marks.py").read_text())
    pytester.makepyfile(test_cases='''
import pytest
from marks import iceberg, iceberg_fast_test

@iceberg
@iceberg_fast_test
def test_selected() -> None:
    pass

@iceberg
def test_new_unmarked_case() -> None:
    pass

@iceberg_fast_test
def test_other_suite() -> None:
    pass

@iceberg
@iceberg_fast_test
@pytest.mark.skip(reason="existing catalog/version restriction")
def test_restricted_case() -> None:
    pass
''')
    return pytester


def test_fast_selection_is_an_explicit_allowlist(marker_suite: pytest.Pytester) -> None:
    result = marker_suite.runpytest_subprocess("-q", "-m", "iceberg and iceberg_fast_test")
    result.assert_outcomes(passed=1, skipped=1, deselected=2)


def test_normal_selection_includes_unmarked_cases(marker_suite: pytest.Pytester) -> None:
    result = marker_suite.runpytest_subprocess("-q", "-m", "iceberg")
    result.assert_outcomes(passed=2, skipped=1, deselected=1)


def test_fast_marker_is_registered(marker_suite: pytest.Pytester) -> None:
    result = marker_suite.runpytest_subprocess("--markers")
    assert result.ret == 0
    result.stdout.fnmatch_lines(["@pytest.mark.iceberg_fast_test:*"])
