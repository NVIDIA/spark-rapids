# Copyright (c) 2023, NVIDIA CORPORATION.
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

import errno
import json
import logging
import os
import subprocess

log = logging.getLogger('shimplify')
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)

def upsert_shim_json(filename, bv_list):
    with open(filename, 'r') as file:
        contents = file.readlines()
    tag = 'spark-rapids-shim-json-lines'
    shim_comment = []
    shim_comment.append('/*** ' + tag)
    for bv in bv_list:
        shim_comment.append(json.dumps({'spark' :  bv}))
    shim_comment.append(tag + ' ***/')
    delete_prior_comment_if_allowed(contents, tag, filename)
    log.debug("Inserting comment {} to {}".format(shim_comment, filename))
    package_line = next(i for i in xrange(len(contents)) if str(contents[i]).startswith('package'))
    log.debug("filename={} package_line_number={}".format(filename, package_line))
    shim_comment_str = '\n'.join(shim_comment) + '\n'
    contents.insert(package_line, shim_comment_str)
    with open(filename, 'w') as file:
        file.writelines(contents)

def delete_prior_comment_if_allowed(contents, tag, filename):
    opening_shim_comment_line = None
    closing_shim_comment_line = None
    overShimplify = project.getProperty("overShimplify")
    overwrite = (str(True).lower() == overShimplify)
    try:
        opening_shim_comment_line = next(i for i in xrange(len(contents)) if tag in str(contents[i]))
        closing_shim_comment_line = next(i for i in xrange(opening_shim_comment_line + 1, len(contents)) if tag in str(contents[i]))
    except StopIteration:
        if not (opening_shim_comment_line is None) and (closing_shim_comment_line is None):
            raise Exception("no closing comment for {}:{}".format(filename, opening_shim_comment_line))
        pass
    # no work
    if opening_shim_comment_line is None:
        return
    if not overwrite:
        raise Exception(
            "found shim comment from prior execution at {}:{}, use -DoverShimplify=true to overwrite"\
                .format(filename, opening_shim_comment_line))
    assert not (opening_shim_comment_line is None) and not (closing_shim_comment_line is None)
    log.debug("removing comments {}:{}:{}".format(filename, opening_shim_comment_line, closing_shim_comment_line))
    del contents[opening_shim_comment_line:(closing_shim_comment_line + 1)]

def git_rename(shim_file, first_shim):
    log.debug("git rename {} to the dir of {}".format(shim_file, first_shim))
    parent_pom_dir = str(project.getProperty('spark.rapids.source.basedir'))
    # sql-plugin/src/main/320+-nondb/scala/org/apache/spark/sql/delta/rapids/shims/GpuCheckDeltaInvariant.scala
    rel_path = os.path.relpath(shim_file, parent_pom_dir)
    log.debug("spark-rapids root dir: {}".format(parent_pom_dir))
    log.debug("shim file path relative to root dir: {}".format(rel_path))
    path_parts = rel_path.split(os.sep)
    path_parts[3] = first_shim
    new_shim_file = os.sep.join([parent_pom_dir] + path_parts)
    if shim_file == new_shim_file:
        log.info("{} is already at the right location, skipping git rename")
        return
    new_shim_dir = os.path.dirname(new_shim_file)
    log.debug("creating new shim path {}".format(new_shim_dir))
    try:
        os.makedirs(new_shim_dir)
    except OSError as ose:
        if ose.errno == errno.EEXIST and os.path.isdir(new_shim_dir):
            pass
        else:
            raise
    git_cmd = ['git', 'mv', shim_file, new_shim_file]
    ret_code = subprocess.call(git_cmd)
    if ret_code != 0:
        raise Exception('git rename failed')


def task_impl():
    """Simplifies the old version range directory system

    Each shim Scala/Java file receives a comment describing all Spark builds it
    it belongs to. Lines are sorted by the Spark buildver lexicographically.
    Each line is assumed to be a JSON to keep it extensible.
    /*** spark-rapids-shim-json-lines
    {"spark": "312"}
    {"spark": "323"}
    spark-rapids-shim-json-lines ***/

    The canonical location of a source file shared by multiple shims is the
    first src/main/<top_buildver_in_the_comment>

    You can find all shim files for a particular shim easily by executing:
    git grep '{"spark": "312"}'
    """
    log.info('############# Starting Jython Task Shimplify #######')
    log.info("review changes and `git restore` if necessary")
    if_attr = attributes.get('if')
    if_val = None if if_attr is None else project.getProperty(if_attr)
    should_skip = if_val is None or if_val != str(True).lower()
    if should_skip:
        log.info('Skipping shimplify! Set -Dshimplify=true to convert old shims')
        return
    shims_attr = attributes.get('shims')
    # remove whitespace
    shims_attr_csv = str(shims_attr).translate(None, ' \n\r')
    shims_arr = shims_attr_csv.split(',')
    shims_arr.sort()

    # map file -> [shims it's part of]
    files2bv = {}
    for build_ver in shims_arr:
        shim_source_files = project.getReference("spark{}.fileset".format(build_ver))
        log.debug("build_ver={} shim_src.size={}".format(build_ver, shim_source_files.size()))
        for resource in shim_source_files:
            shim_file = str(resource)
            if shim_file in files2bv.keys():
                files2bv[shim_file] += [build_ver]
            else:
                files2bv[shim_file] = [build_ver]

    for shim_file, bv_list in files2bv.items():
        log.debug("calling upsert_shim_json shim_file={} bv_list={}".format(shim_file, bv_list))
        upsert_shim_json(shim_file, bv_list)
        git_rename(shim_file, bv_list[0])

task_impl()