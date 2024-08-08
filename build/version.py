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

import subprocess
import os

pom = attributes.get("pom")
scala_version = "2.12" if pom == "" else "2.13"
overwrite_properties = attributes.get("overwrite_properties")
expression = attributes.get("expression")

if (overwrite_properties == "true" or not os.path.exists("release.properties")):
    # Define the Bash command
    bash_command = """
    export TEMP=$(mvn help:all-profiles -pl . {pom}| sort | uniq | awk "/([^-])release[0-9]/ {{print substr(\$3, 8)}}");
    TEMP=$(echo -n $TEMP)
    [[ -n $TEMP ]] || {{ echo -e "Error setting databricks versions"; }};
    <<< $TEMP read -r -a SPARK_SHIM_VERSIONS_ARR;
    SNAPSHOTS=();
    NO_SNAPSHOTS=()
    for ver in ${{SPARK_SHIM_VERSIONS_ARR[@]}}; do
        TEST=$(mvn -B help:evaluate -q -pl dist {pom} -Dexpression=spark.version -Dbuildver="$ver" -DforceStdout)
        if [[ "$TEST" != *"-SNAPSHOT" ]]; then
            NO_SNAPSHOTS+=(" $ver")
        else
            SNAPSHOTS+=(" $ver")
        fi
    done
    echo "${{SNAPSHOTS[@]}} | ${{NO_SNAPSHOTS[@]}}"
    """.format(pom=pom)

    try:
        # Run the command
        output = subprocess.check_output(bash_command, shell=True, executable="/bin/bash", stderr=subprocess.STDOUT)
        result_strings = map(lambda x: x.split(), output.encode("ASCII", "ignore").split("|"))
        snapshots = result_strings[0]
        no_snapshots = result_strings[1]
        if (scala_version == "2.13"):
            no_snapshots = filter(lambda x: not x.endswith("cdh"), no_snapshots)
        db_release = filter(lambda x: x.endswith("db"), no_snapshots)
        no_snapshots = filter(lambda x: not x.endswith("db"), no_snapshots)
        snap_and_no_snap = no_snapshots + snapshots
        all = snap_and_no_snap + db_release
        f = open("release.properties", "w")
        release_dict={}
        release_dict["databricks.buildvers"]=" ".join(db_release)
        release_dict["snapshots.buildvers"]=" ".join(snapshots)
        release_dict["no_snapshots.buildvers"]=" ".join(no_snapshots)
        release_dict["snap_and_no_snap.buildvers"]=" ".join(snap_and_no_snap)
        release_dict["all.buildvers"]=" ".join(all)
        try:
            if (scala_version == "2.12"):
                f.write("[Scala212ReleaseSection]\n")
            elif (scala_version == "2.13"):
                f.write("[Scala213ReleaseSection]\n")
            else:
                raise Exception("Illegal scala_version")
            for key in release_dict.keys():
            	f.write("{}={}\n".format(key,release_dict[key]))

            if (expression):
                print(release_dict[expression])
        finally:
            f.close()
    except subprocess.CalledProcessError as e:
        print("Error:", e.output)

else:
    import ConfigParser
    config = ConfigParser.ConfigParser()
    config.read("release.properties")
    if (scala_version == "2.12"):
        print(config.get("Scala212ReleaseSection", expression))
    elif (scala_version == "2.13"):
        print(config.get("Scala213ReleaseSection", expression))
    else:
        raise Exception("Illegal scala_version")