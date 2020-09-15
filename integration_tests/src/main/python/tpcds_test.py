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

from asserts import assert_gpu_and_cpu_are_equal_collect
from marks import incompat, ignore_order, allow_non_gpu, approximate_float

queries = ['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8', 'q9',
        'q10', 'q11', 'q12', 'q13', 'q14a', 'q14b', 'q15', 'q16', 'q17', 'q18', 'q19',
        'q20', 'q21', 'q22', 'q23a', 'q23b', 'q24a', 'q24b', 'q25', 'q26', 'q27', 'q28', 'q29',
        'q30', 'q31', 'q32', 'q33', 'q34', 'q35', 'q36', 'q37', 'q38', 'q39a', 'q39b',
        'q40', 'q41', 'q42', 'q43', 'q44', 'q45', 'q46', 'q47', 'q48', 'q49',
        'q50', 'q51', 'q52', 'q53', 'q54', 'q55', 'q56', 'q57', 'q58', 'q59',
        'q60', 'q61', 'q62', 'q63', 'q64', 'q65', 'q66',        'q68', 'q69',
               'q71', 'q72', 'q73', 'q74', 'q75', 'q76', 'q77', 'q78', 'q79',
        'q80', 'q81', 'q82', 'q83', 'q84', 'q85', 'q86', 'q87', 'q88', 'q89',
        'q90', 'q91', 'q92', 'q93', 'q94', 'q95', 'q96', 'q97', 'q98', 'q99',
        'ss_max', 'ss_maxb']

@incompat
@ignore_order
@approximate_float
@allow_non_gpu(any=True)
@pytest.mark.parametrize('query', queries)
def test_tpcds(tpcds, query):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpcds.do_test_query(query),
          conf={'spark.rapids.sql.variableFloatAgg.enabled': 'true'})

no_var_agg_queries = ['q67', 'q70']

@incompat
@ignore_order
@approximate_float
@allow_non_gpu(any=True)
@pytest.mark.parametrize('query', no_var_agg_queries)
def test_tpcds_no_var_agg(tpcds, query):
  assert_gpu_and_cpu_are_equal_collect(
          lambda spark : tpcds.do_test_query(query))
