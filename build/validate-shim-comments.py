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

import json

shimFiles = project.getReference('allShimFilesID')
for f in shimFiles:
  shimLineFound = False
  with open(str(f), 'rt') as shimSourceFile:
    for line in shimSourceFile:
      if line.startswith('// {"spark-distros":'):
        shimLineFound = True
        distroStr = line[len('// '):]
        jsonObj = None
        try:
          jsonObj = json.loads(distroStr)
        except Exception as e:
          raise Exception('Failed to decode ' + distroStr + ' from ' + str(f))
        distros = jsonObj['spark-distros']
        assert len(distros) > 0, "At least one shim expected in {}:{}".format(str(f), distroStr)
        distrosSorted = distros[:]
        distrosSorted.sort()
        if distrosSorted != jsonObj['spark-distros']:
          raise Exception('Shims should be sorted lexicographically in ' + \
            str(f) + ': ' + distroStr + '. Expected order is ' + \
            json.dumps(distrosSorted))
        break
    assert shimLineFound, "no shim comment spark-distros found in {}".format(str(f))
