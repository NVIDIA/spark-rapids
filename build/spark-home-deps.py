# Copyright (c) 2025, NVIDIA CORPORATION.
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

import glob
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--csp', choices=['emr', 'db'], required=True)

args = parser.parse_args()

spark_home = '/usr/lib/spark' if args.csp == 'emr' else '/databricks'
jar_paths = glob.glob(os.path.join(spark_home, 'jars', '*.jar'))
for i in range(len(jar_paths)):
  print(
f"""
                <dependency>
                    <groupId>com.nvidia</groupId>
                    <artifactId>{args.csp}dep-{i}</artifactId>
                    <version>1.0</version>
                    <scope>system</scope>
                    <systemPath>{jar_paths[i]}</systemPath>
                </dependency>
""")

