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
import os
import xml.etree.ElementTree as ET

_log = logging.getLogger("dyn-shim-detection")
show_version_info = project.getProperty("dyn.shim.trace")
_log.setLevel(logging.DEBUG if show_version_info else logging.INFO)
# Same as shimplify.py
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))
_log.addHandler(ch)

multi_module_project_dir = project.getProperty("maven.multiModuleProjectDirectory")
expression = project.getProperty("expression")

def _get_buildver(pom, expression):
    pom = ET.parse(pom)
    ns = {"pom": "http://maven.apache.org/POM/4.0.0"}
    releases = []
    release_prefix = "release"
    for profile in pom.findall("//pom:profile/pom:id", ns):
       if profile.text.startswith(release_prefix):
            releases.append(profile.text[len(release_prefix):])
    snapshots = []
    no_snapshots = []

    for release in releases:
        spark_version = pom.find(".//pom:spark{}.version".format(release), ns)
        if (spark_version.text.endswith("SNAPSHOT")):
            snapshots.append(release)
        else:
            no_snapshots.append(release)
    excluded_shims = project.getProperty("dyn.shim.excluded.releases")
    if (excluded_shims):
        for removed_shim in [x.strip() for x in excluded_shims.split(",")]:
            if (removed_shim not in snapshots and removed_shim not in no_snapshots):
                raise Exception("Shim {} listed in dyn.shim.excluded.releases in pom.xml not present in releases".format(removed_shim))
            try:
                snapshots.remove(removed_shim)
            except ValueError:
                pass
            try:
                no_snapshots.remove(removed_shim)
            except ValueError:
                pass


    if multi_module_project_dir.endswith("scala2.13"):
        no_snapshots = filter(lambda x: not x.endswith("cdh"), no_snapshots)
    db_release = filter(lambda x: x.endswith("db"), no_snapshots)
    no_snapshots = filter(lambda x: not x.endswith("db"), no_snapshots)
    snap_and_no_snap = no_snapshots + snapshots
    all = snap_and_no_snap + db_release
    release_dict={}
    release_dict["databricks.buildvers"]=" ".join(db_release)
    release_dict["snapshots.buildvers"]=" ".join(snapshots)
    release_dict["no_snapshots.buildvers"]=" ".join(no_snapshots)
    release_dict["snap_and_no_snap.buildvers"]=" ".join(snap_and_no_snap)
    release_dict["all.buildvers"]=" ".join(all)
    _log.debug("release_dict: {}".format(release_dict))
    if (expression):
        return release_dict[expression]

value=_get_buildver("{}/pom.xml".format(multi_module_project_dir), expression)
project.setProperty(expression, value)
print(value)