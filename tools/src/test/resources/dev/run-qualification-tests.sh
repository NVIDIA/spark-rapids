#!/usr/bin/env bash
#
# Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
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
#

YELLOW='\e[33;1m'
RED='\e[31;1m'
GREEN='\e[32;1m'
ENDCOLOR='\e[0m'

MODULE_NAME="tools"
RELATIVE_QUAL_LOG_PATH="src/test/resources/spark-events-qualification"
RELATIVE_PROF_LOG_PATH="src/test/resources/spark-events-profiling"
RELATIVE_QUAL_REF_PATH="src/test/resources/QualificationExpectations"
RELATIVE_TOOLS_JAR_PATH="tools/target/spark311/"
RELATIVE_COMMON_JAR_PATH="common/target/spark330/"


PROJECT_ROOT=""
MODULE_PATH=""
PROF_LOG_DIR=""
QUAL_LOG_DIR=""
QUAL_REF_DIR=""
JVM_DEFAULT_HEAP_SIZE="10g"

# get the directory of the script
WORK_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
BATCH_OUT_DIR="${WORK_DIR}/qualification-output"
CSV_OUT_DIR="${BATCH_OUT_DIR}/csv"
RUNS_OUT_DIR="${BATCH_OUT_DIR}/runs-output"

java_cp=""
rapids_jar_home=""
script_out_dir=""
qual_events_dir=""
prof_events_dir=""
qual_expectations_dir=""

heap_size=$JVM_DEFAULT_HEAP_SIZE
declare -A qualification_path_map

show_help()
{
   # Display Help
   log_msg
   log_msg "Run the Qualification tool to generate CSV files to update/replace Qualification expectations set."
   log_msg "The output folder of the CSV files is test/resources/dev/qualification-output/csv"
   log_msg
   log_msg "The script compares the CSV files. If pairs of files do not match or the directories are"
   log_msg "not identical, the script will fail."
   log_msg "Please make sure to update the CSV files for the failing unit tests."
   log_msg
   log_msg
   log_msg "Optional Arguments:"
   log_msg "  --rapids-jars-dir=<arg>       - Directory containing RAPIDS jars. By default the script sets it to the"
   log_msg "                                  target directory of tools."
   log_msg "  --cp=<arg>                    - Classpath required as dependencies to run the qualification tool."
   log_msg "                                  For example, if \$SPARK_HOME/jars are not in the directory <rapids-jars>, then"
   log_msg "                                  pass it as --cp=\$SPARK_HOME/jars/*:\$CLASS_PATH"
   log_msg "  --out-dir=<arg>               - Output directory passed to the qualification tool runs."
   log_msg "                                  By default is is set to test/resources/dev/qualification-output"
   log_msg "                                  The directory has two subdirectories: csv/ which has the CSV files; and"
   log_msg "                                  runs-output/ which has the actual output of the tool."
   log_msg "                                  target directory of tools."
   log_msg "  --qual-expectations-dir=<arg> - Path of the reference CSV files."
   log_msg "                                  By default is is set to test/resources/QualificationExpectations"
   log_msg "  --qual-events-dir=<arg>       - Path of the Spark events logs used as input for the unit tests"
   log_msg "                                  By default is is set to test/resources/spark-events-qualification"
   log_msg "  --prof-log-dir=<arg>          - Some the unit tests reads files located in another directory."
   log_msg "                                  By default is is set to test/resources/spark-events-profiling"
   log_msg "  --heap=<arg>                  - optional heap size. Default is 10g."
   log_msg "  --help|-h                     - Shows Help."
   log_msg
   log_msg "Example Usage:"
   log_msg "  run-qualification-tests.sh --cp=\$CLASS_PATH --heap=5g"
   log_msg "  This is equivalent to:"
   log_msg "    java -Xmx5g \\"
   log_msg "         -cp rapids-4-spark-tools_2.12-<version>-SNAPSHOT.jar:\$CLASS_PATH \\"
   log_msg "         com.nvidia.spark.rapids.tool.qualification.QualificationMain \\"
   log_msg "         --no-html-report --output-directory file:qualification-output/csv \\"
   log_msg "         \$LOGFILES"
   log_msg
   log_msg "How to Add New Test:"
   log_msg " 1- add a dummy csv file in qual-expectations-dir <new_unit_test.csv>"
   log_msg " 2- add the eventlog into the prof-log-dir <new_unit_log>"
   log_msg " 3- update the definition of the hash in define_qualification_tests_map() to map between"
   log_msg "    the expected file and the unit test name."
   log_msg " 4- run teh script. It is expected that the script fails because the dummy csv is incorrect."
   log_msg " 5- copy the content of the generated output dev/qualification-output/csv/new_unit_test.csv"
   log_msg "    into the expectation folders."
   log_msg
}

log_error()
{
  log_msg "${RED}$*"
}

log_result()
{
  log_msg "${GREEN}$*"
}

log_info() {
  log_msg "${YELLOW}$*"
}

log_msg()
{
  echo -e "$* $ENDCOLOR"
}

print_banner()
{
  printf '%80s\n' | tr ' ' -
  log_info "$*"
  printf '%80s\n' | tr ' ' -
}

set_rapids_jars_from_work_dir()
{
  rapids_tools_jar_file=( "$( find "${MODULE_PATH}" -type f \( -iname "*.jar" ! -iname "*tests.jar" ! -iname "original-rapids-4*.jar" \) )" )
  # get the parent directory
  rapids_jar_home="$(dirname "${rapids_tools_jar_file}")"
}

set_rapids_jar_home()
{
  if [ "${rapids_jar_home}" ]; then
    log_msg "RAPIDS_JARS are passed through the arguments: ${rapids_jar_home}/*"
  else
    set_rapids_jars_from_work_dir
    log_msg "RAPIDS_JARS are pulled from : ${rapids_jar_home}/*"
  fi
}

set_rapids_tools_classpath()
{
  tools_cp="${rapids_jar_home}/*"
  log_msg "tools_cp:\n ${tools_cp}"
  # check that the java class path is set correctly
  if [ "$java_cp" ]; then
    RAPIDS_CLASS_PATH="${tools_cp}:${java_cp}"
  else
    log_msg "The run did not define CP Dependencies"
    RAPIDS_CLASS_PATH="${tools_cp}"
  fi

  log_msg "RAPIDS CLASS_PATH is:\n ${RAPIDS_CLASS_PATH}"
}

set_script_output()
{
  if [ "${script_out_dir}" ]; then
    log_msg "Script output is set through the arguments: ${script_out_dir}"
    # reset default values
    # Note that it is safer to use the output-directory as a parent. This avoids the mistake of
    # deleting user's data if the directory has other subdirectories.
    BATCH_OUT_DIR="${script_out_dir}/qualification-output"
    CSV_OUT_DIR="${BATCH_OUT_DIR}/csv"
    RUNS_OUT_DIR="${BATCH_OUT_DIR}/runs-output"
  else
    log_msg "Script output is set as default: ${BATCH_OUT_DIR}"
  fi

  # cleanup output folder before running tests
  if [ -d "$BATCH_OUT_DIR" ]; then
    log_info "Output folder already exists...removing it"
    rm -r "$BATCH_OUT_DIR"
  fi
}

process_events_and_expectations_paths()
{
  MODULE_PATH="${PROJECT_ROOT}/${MODULE_NAME}"

  if [ "${qual_expectations_dir}" ]; then
    QUAL_REF_DIR="${qual_expectations_dir}"
  else
    QUAL_REF_DIR="${MODULE_PATH}/${RELATIVE_QUAL_REF_PATH}"
  fi

  if [ "${qual_events_dir}" ]; then
    QUAL_LOG_DIR="${qual_events_dir}"
  else
    QUAL_LOG_DIR="${MODULE_PATH}/${RELATIVE_QUAL_LOG_PATH}"
  fi

  if [ "${prof_events_dir}" ]; then
    PROF_LOG_DIR="${prof_events_dir}"
  else
    PROF_LOG_DIR="${MODULE_PATH}/${RELATIVE_PROF_LOG_PATH}"
  fi
}

initialize()
{
  arr=( ${WORK_DIR//"/${MODULE_NAME}/"/ } )
  PROJECT_ROOT=${arr[0]}

  process_events_and_expectations_paths
  set_rapids_jar_home
  set_script_output
  set_rapids_tools_classpath

  print_banner "\t\t\tRun Qualifications Tool Tests"
  log_info "Qualification Expectations Path : ${QUAL_REF_DIR}"
  log_info "Qualification Events Path       : ${QUAL_LOG_DIR}"
  log_info "Profiling Events Path           : ${PROF_LOG_DIR}"
  log_info "Output Directory                : ${BATCH_OUT_DIR}"
  log_info "Heap Size                       : ${heap_size}"
  log_info "RAPIDS Jars Home                : ${rapids_jar_home}"
  log_info "Classpath                       : ${java_cp}"
}

define_qualification_tests_map()
{
  qual_log_prefix="file:${QUAL_LOG_DIR}"
  prof_log_prefix="file:${PROF_LOG_DIR}"

  qualification_path_map[nds_q86_test_expectation]="${qual_log_prefix}/nds_q86_test ${qual_log_prefix}/malformed_json_eventlog.zstd"
  qualification_path_map[spark2_expectation]="${prof_log_prefix}/spark2-eventlog.zstd"
  qualification_path_map[qual_test_simple_expectation]="${qual_log_prefix}/dataset_eventlog ${qual_log_prefix}/udf_func_eventlog ${qual_log_prefix}/dsAndDf_eventlog.zstd ${qual_log_prefix}/udf_dataset_eventlog"
  qualification_path_map[qual_test_missing_sql_end_expectation]="${qual_log_prefix}/join_missing_sql_end"
  qualification_path_map[truncated_1_end_expectation]="${qual_log_prefix}/truncated_eventlog"
  qualification_path_map[nds_q86_test_expectation]="${qual_log_prefix}/nds_q86_test"
  qualification_path_map[directory_test_expectation]="${qual_log_prefix}/eventlog_v2_local-1623876083964"
  qualification_path_map[db_sim_test_expectation]="${qual_log_prefix}/db_sim_eventlog"
  qualification_path_map[nds_q86_fail_test_expectation]="${qual_log_prefix}/nds_q86_fail_test"
  qualification_path_map[write_format_expectation]="${qual_log_prefix}/writeformat_eventlog"
  qualification_path_map[nested_type_expectation]="${qual_log_prefix}/nested_type_eventlog"
  qualification_path_map[jdbc_expectation]="${qual_log_prefix}/jdbc_eventlog.zstd"
  qualification_path_map[read_dsv1_expectation]="${prof_log_prefix}/eventlog_dsv1.zstd"
  qualification_path_map[read_dsv2_expectation]="${prof_log_prefix}/eventlog_dsv2.zstd"
  qualification_path_map[complex_dec_expectation]="${qual_log_prefix}/complex_dec_eventlog.zstd"
  qualification_path_map[nested_dsv2_expectation]="${qual_log_prefix}/eventlog_nested_dsv2"
}

process_qualification_output()
{
  qual_out_dir="$1/rapids_4_spark_qualification_output"

  # check if directory exists in case the caller did not check that the run was successful.
  if [ ! -d "$qual_out_dir" ]
  then
    log_error "Qualification tool did not generate the output for $2"
    exit 1
  fi

  mkdir -p "${CSV_OUT_DIR}"
  output_file="$qual_out_dir/rapids_4_spark_qualification_output.csv"
  csv_output_file="${CSV_OUT_DIR}/${2}.csv"
  log_info "Start copying Output of ${2}"
  cp "$output_file" "$csv_output_file"
  log_info "\tSource: $output_file \n\tDestination: $csv_output_file"

  expected_csv_file="${QUAL_REF_DIR}/${key}.csv"
  compare_pair_files "$expected_csv_file" "$csv_output_file"
}

compare_pair_files()
{
  ref_file=$1
  cand_file=$2
  # shellcheck disable=SC2086
  diff "$ref_file" "$cand_file"
  if [ $? -ne 0 ]; then
    log_error "Error: The two files are not identical\n\tGenerated file:$cand_file\n\tReference file:$ref_file";
  else
    log_result "Run Succeeded"
    log_result "\tGenerated file:$cand_file\n\tReference file:$ref_file"
  fi
}

compare_qualification_csv_folders()
{
  print_banner "\t\tComparing Two Directories"
  log_info "\tGenerated: ${CSV_OUT_DIR}"
  log_info "\tReference: ${QUAL_REF_DIR}"
  diff -q "${QUAL_REF_DIR}/" "${CSV_OUT_DIR}/"
  if [ $? -ne 0 ]; then
    log_error "Batch Error"
    log_error "\tThe two directories do not match."
    log_error "\tGenerated Dir: $CSV_OUT_DIR"
    log_error "\tReference Dir: $QUAL_REF_DIR"
    exit 1
  else
    log_result "The two directories match";
    log_result "Result: SUCCESS";
  fi
}

run_qualification_tool()
{
  for key in "${!qualification_path_map[@]}"; do
    print_banner "Qualification tool:\t\t\t${key}"

    output_dir="${RUNS_OUT_DIR}/${key}"

    mkdir -p "${output_dir}"

    ## set teh arguments
    jvm_args="-Xmx${1} -cp $RAPIDS_CLASS_PATH"
    qual_tool_args="--no-html-report --output-directory file:${output_dir}"

    ## run the tool
    java ${jvm_args} com.nvidia.spark.rapids.tool.qualification.QualificationMain \
         ${qual_tool_args} \
         ${qualification_path_map[$key]}

    # check if the qualification tool failed
    if [ $? -eq 0 ]
    then
    	log_info "OK: Running Qualification tool"
    	process_qualification_output "$output_dir" "$key"
    else
      # we do not need to exit immediately because the two directories are compared any way
      # at the end of the script
    	log_error "FAIL: Running Qualification tool ${key}"
    fi
  done
}

bail()
{
    printf '%s\n' "$1" >&2
    show_help
    exit 1
}

##########################
# Main Script starts here
##########################

# Parse arguments
while :; do
    case $1 in
        -h|-\?|--help)
            show_help    # Display a usage synopsis.
            exit
            ;;
        --heap=?*)
            heap_size=${1#*=} # Delete everything up to "=" and assign the remainder.
            ;;
        --rapids-jars-dir=?*)
            rapids_jar_home=${1#*=} # Delete everything up to "=" and assign the remainder.
            ;;
        --out-dir=?*)
            script_out_dir=${1#*=} # Delete everything up to "=" and assign the remainder.
            ;;
        --qual-expectations-dir=?*)
            qual_expectations_dir=${1#*=} # Delete everything up to "=" and assign the remainder.
            ;;
        --qual-events-dir=?*)
            qual_events_dir=${1#*=} # Delete everything up to "=" and assign the remainder.
            ;;
        --prof-log-dir=?*)
            prof_events_dir=${1#*=} # Delete everything up to "=" and assign the remainder.
            ;;
        --cp=?*)
            java_cp=${1#*=} # Delete everything up to "=" and assign the remainder.
            ;;
        -v|--verbose)
            verbose=$((verbose + 1))  # Each -v adds 1 to verbosity.
            ;;
        --)              # End of all options.
            shift
            break
            ;;
        -?*)
            printf 'WARN: Unknown option (ignored): %s\n' "$1" >&2
            ;;
        *)               # Default case: No more options, so break out of the loop.
            break
    esac

    shift
done

initialize
define_qualification_tests_map
run_qualification_tool "$heap_size"
compare_qualification_csv_folders
