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

import os

confMap = {
  'spark.master':                               'local',
  'spark.ui.showConsoleProgress':               'false',
  'spark.sql.session.timeZone':                 'UTC',
  'spark.sql.shuffle.partitions':               '12',
  'spark.executor.extraJavaOptions':            '-Duser.timezone=GMT',
}

def update_conf_map():
    # 'RAP_TEST_PARALLEL' is num of parallelism for spark pytests, only valid for spark local mode
    # We need to divide GPU mem to RAP_TEST_PARALLEL+1 parts for running num of parallelism
    allowFraction = 1 / (int(os.getenv('RAP_TEST_PARALLEL')) + 2)
    maxFraction = 1 / (int(os.getenv('RAP_TEST_PARALLEL')) + 1)
    confMap['spark.rapids.memory.gpu.allocFraction'] = str(round(allowFraction, 3))
    confMap['spark.rapids.memory.gpu.maxAllocFraction'] = str(round(maxFraction, 3))

    if ('RAP_TEST_EXTRA_CP' in os.environ):
        confMap['spark.driver.extraClassPath'] = os.environ['RAP_TEST_EXTRA_CP']

    _javaOption = '-Duser.timezone=GMT ' + str(os.getenv('COVERAGE_SUBMIT_FLAGS'))
    if ('PYTEST_XDIST_WORKER' in os.environ):
        wid = os.environ['PYTEST_XDIST_WORKER']
        d = "./derby_{}".format(wid)
        if not os.path.exists(d):
            os.makedirs(d)
        confMap['spark.driver.extraJavaOptions'] = _javaOption + ' -Dderby.system.home={}'.format(d)
    else:
        confMap['spark.driver.extraJavaOptions'] = _javaOption
