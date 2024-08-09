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
import subprocess

_log = logging.getLogger("release-version")
show_version_info = project.getProperty("release.version.trace")
_log.setLevel(logging.INFO if show_version_info else logging.ERROR)
# Same as shimplify.py
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))
_log.addHandler(ch)

pom = attributes.get("pom")
scala_version = "2.12" if pom == "" else "2.13"
section_header = project.getProperty("release.212.section.header") if pom == "" else project.getProperty("release.213.section.header")
overwrite_properties = attributes.get("overwrite_properties")
expression = attributes.get("expression")
release_properties = project.getProperty("spark.rapids.releases")

if (overwrite_properties == "true" or not os.path.exists(release_properties)):
    _log.info("Writing properties at {}...".format(release_properties))

    try:
        output = project.getProperty("release.versions")
        result_strings = map(lambda x: x.split(), output.encode("ASCII", "ignore").split("|"))
        snapshots = result_strings[0]
        no_snapshots = result_strings[1]
        if (scala_version == "2.13"):
            no_snapshots = filter(lambda x: not x.endswith("cdh"), no_snapshots)
        db_release = filter(lambda x: x.endswith("db"), no_snapshots)
        no_snapshots = filter(lambda x: not x.endswith("db"), no_snapshots)
        snap_and_no_snap = no_snapshots + snapshots
        all = snap_and_no_snap + db_release
        f = open(release_properties, "w")
        release_dict={}
        release_dict["databricks.buildvers"]=" ".join(db_release)
        release_dict["snapshots.buildvers"]=" ".join(snapshots)
        release_dict["no_snapshots.buildvers"]=" ".join(no_snapshots)
        release_dict["snap_and_no_snap.buildvers"]=" ".join(snap_and_no_snap)
        release_dict["all.buildvers"]=" ".join(all)
        _log.info("release_dict: {}".format(release_dict))
        try:
            f.write("[{}]\n".format(section_header))
            for key in release_dict.keys():
            	f.write("{}={}\n".format(key,release_dict[key]))
            _log.info("Finished writing properties file at {}".format(release_properties))
            if (expression):
                print(release_dict[expression])
        finally:
            f.close()
    except subprocess.CalledProcessError as e:
        _log.error("Error:", e.output)

else:
    import ConfigParser
    config = ConfigParser.ConfigParser()
    config.read(release_properties)
    print(config.get(section_header, expression))