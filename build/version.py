import subprocess
import os

pom = attributes.get("pom")
overwrite_properties = attributes.get("overwrite_properties")
buildvers = attributes.get("build_vers")

if (overwrite_properties == "true"):
    # Define the Bash command
    bash_command = """
    export TEMP=$(mvn help:all-profiles -pl . {pom}| sort | uniq | awk "/release[0-9]/ {{print substr(\$3, 8)}}");
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
    # <!-- #if scala-2.13 --><!--
    #     no_snapshots = filter(lambda x: not x.endswith("cdh"), no_snapshots)
    # --><!-- #endif scala-2.13 -->
        db_release = filter(lambda x: x.endswith("db"), no_snapshots)
        no_snapshots = filter(lambda x: not x.endswith("db"), no_snapshots)
        snap_and_no_snap = no_snapshots + snapshots
        all = snap_and_no_snap + db_release
        f = open("release.properties", "w")
        try:
            f.write("[ReleaseSection]\n")
            f.write("databricks.buildvers={}".format(" ".join(db_release)) +
            "\nsnapshots.buildvers={}".format(" ".join(snapshots)) +
            "\nnoSnapshots.buildvers={}".format(" ".join(no_snapshots)) +
            "\nsnapAndNoSnap.buildvers={}".format(" ".join(snap_and_no_snap)) +
            "\nall.buildvers={}".format(" ".join(all)))
        finally:
            f.close()
    except subprocess.CalledProcessError as e:
        print("Error:", e.output)
else:
    import ConfigParser
    config = ConfigParser.ConfigParser()
    config.read("release.properties")
    print(config.get("ReleaseSection", buildvers))

