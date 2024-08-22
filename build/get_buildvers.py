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

import sys
import xml.etree.ElementTree as ET

def _get_expression(pomFile, expression, logger = None):
    pom = ET.parse(pomFile)
    ns = {"pom": "http://maven.apache.org/POM/4.0.0"}
    releases = []
    release_prefix = "release"
    for profile in pom.findall(".//pom:profile/pom:id", ns):
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
    excluded_shims = pom.find(".//pom:dyn.shim.excluded.releases", ns).text
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


    if "scala2.13" in pomFile:
        no_snapshots = list(filter(lambda x: not x.endswith("cdh"), no_snapshots))
    db_release = list(filter(lambda x: x.endswith("db"), no_snapshots))
    no_snapshots = list(filter(lambda x: not x.endswith("db"), no_snapshots))
    snap_and_no_snap = no_snapshots + snapshots
    all = snap_and_no_snap + db_release
    release_dict={}
    release_dict["databricks.buildvers"]=" ".join(db_release)
    release_dict["snapshots.buildvers"]=" ".join(snapshots)
    release_dict["no_snapshots.buildvers"]=" ".join(no_snapshots)
    release_dict["snap_and_no_snap.buildvers"]=" ".join(snap_and_no_snap)
    release_dict["all.buildvers"]=" ".join(all)
    if (logger):
        logger.debug("release_dict: {}".format(release_dict))
    if (expression):
        return release_dict[expression]


if __name__ == "__main__":
    print(_get_expression(sys.argv[2], sys.argv[1]))
