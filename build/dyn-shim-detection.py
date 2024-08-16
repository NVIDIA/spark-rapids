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

_log = logging.getLogger("dyn-shim-detection")
show_version_info = project.getProperty("dyn.shim.trace")
_log.setLevel(logging.INFO if show_version_info else logging.ERROR)
# Same as shimplify.py
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))
_log.addHandler(ch)

pom = attributes.get("pom")
multi_module_project_dir = project.getProperty("maven.multiModuleProjectDirectory")
section_header = project.getProperty("dyn.shim.section.header")
overwrite_properties = attributes.get("overwrite_properties")
expression = attributes.get("expression")
release_properties = project.getProperty("dyn.shim.properties.location")

if (overwrite_properties == "true" or not os.path.exists(release_properties)):
    _log.info("Writing properties at {}...".format(release_properties))

    try:
        output = project.getProperty("dyn.shim.versions")
        result_strings = map(lambda x: x.split(), output.encode("ASCII", "ignore").split("|"))
        excluded_shims = project.getProperty("dyn.shim.excluded.releases")
        snapshots = result_strings[0]
        no_snapshots = result_strings[1]
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
    _log.info("Reading properties {}...".format(release_properties))
    import ConfigParser
    config = ConfigParser.ConfigParser()
    config.read(release_properties)
    print(config.get(section_header, expression))
