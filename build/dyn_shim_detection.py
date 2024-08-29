# Copyright (c) 2024, NVIDIA CORPORATION.
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

import logging
import xml.etree.ElementTree as ET
import sys

_log = logging.getLogger("dyn-shim-detection")
# This script is called by maven's antrun plugin. The `project` variable is set by antrun which contains all the
# properties that are set in the pom.xml. For more details checkout the documentation of the `script` task
# https://ant.apache.org/manual/Tasks/script.html
show_version_info = project.getProperty("dyn.shim.trace")
_log.setLevel(logging.DEBUG if show_version_info else logging.INFO)
# Same as shimplify.py
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))
_log.addHandler(ch)
spark_rapids_source_basedir = project.getProperty("spark.rapids.source.basedir")
multi_module_project_dir = project.getProperty("maven.multiModuleProjectDirectory")
expression = project.getProperty("expression")

sys.path.append("{}/build/".format(spark_rapids_source_basedir))
from get_buildvers import _get_expression
value = _get_expression(expression, "{}/pom.xml".format(multi_module_project_dir), _log)
project.setProperty(expression, value)