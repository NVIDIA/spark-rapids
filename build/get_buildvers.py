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


def _get_buildvers(buildvers, pom_file, logger=None):
    pom = ET.parse(pom_file)
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
        if spark_version.text.endswith("SNAPSHOT"):
            snapshots.append(release)
        else:
            no_snapshots.append(release)
    excluded_shims = pom.find(".//pom:dyn.shim.excluded.releases", ns)
    if excluded_shims:
        for removed_shim in [x.strip() for x in excluded_shims.text.split(",")]:
            if removed_shim in snapshots:
                snapshots.remove(removed_shim)
            elif removed_shim in no_snapshots:
                no_snapshots.remove(removed_shim)
            else:
                raise Exception(
                    "Shim {} listed in dyn.shim.excluded.releases in pom.xml not present in releases".format(
                        removed_shim))

    if "scala2.13" in pom_file:
        no_snapshots = list(filter(lambda x: not x.endswith("cdh"), no_snapshots))

    db_release = list(filter(lambda x: x.endswith("db"), no_snapshots))
    no_snapshots = list(filter(lambda x: not x.endswith("db"), no_snapshots))
    snap_and_no_snap = no_snapshots + snapshots
    all_buildvers = snap_and_no_snap + db_release
    release_dict = {"databricks": " ".join(db_release), "snapshots": " ".join(snapshots),
                    "no_snapshots": " ".join(no_snapshots),
                    "snap_and_no_snap": " ".join(snap_and_no_snap), "all.buildvers": " ".join(all_buildvers)}
    if logger:
        logger.debug("release_dict: {}".format(release_dict))
    if buildvers:
        return release_dict[buildvers]


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("get_buildvers.py needs a pom_file location and an buildvers as arguments")
    else:
        print(_get_buildvers(sys.argv[1], sys.argv[2]))
