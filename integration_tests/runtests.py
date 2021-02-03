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

import sys
import os

from pytest import main

#import cProfile

if __name__ == '__main__':
    #cProfile.run('main(sys.argv[1:])', 'test_profile')
    # arguments are the same as for pytest https://docs.pytest.org/en/latest/usage.html
    # or run pytest -h
    iteration = 0
    maxIterEnvName = 'IT_MAX_ITERATIONS'
    maxIterations = int(os.environ[maxIterEnvName]) if maxIterEnvName in os.environ else 1
    itForever = maxIterations < 0

    testArgs = sys.argv[1:]
    while iteration < maxIterations or itForever:
        print('###############################################################')
        print("# Spark RAPIDS PyTest Iteration " + str(iteration))
        exitCode = main(testArgs)
        print(f"# Spark RAPIDS PyTest Iteration {iteration} exited with {exitCode}")
        print('###############################################################')
        iteration += 1
