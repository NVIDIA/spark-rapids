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

"""implementation of shimplify Ant task """

import errno
import json
import logging
import os
import subprocess

# wrap Ant interactions to reduce linting warnings
# TODO pass only Python types if possible


def __project():
    return project


def __attributes():
    return attributes


# get Ant project property as string
def __ant_proj_prop(name):
    return str(__project().getProperty(name))


def __ant_attr(name):
    return str(__attributes().get(name))


def __is_enabled_property(prop_name):
    assert not (prop_name is None), "Invalid property: None"
    prop_val = __ant_proj_prop(prop_name)
    return str(True).lower() == prop_val


def __is_enabled_attr(attr):
    assert not (attr is None), "Invalid attribute: None"
    attr_val = __ant_attr(attr)
    return not (attr_val is None) and __is_enabled_property(attr_val)


task_enabled = __is_enabled_attr('if')

# should we move files?
move_files = __is_enabled_property('shimplifyMove')

# allowed to overwrite the existing comment
over_shimplify = __is_enabled_property('overShimplify')

# enable log tracing
shimplify_trace = __is_enabled_property('traceShimplify')


log = logging.getLogger('shimplify')
log.setLevel(logging.DEBUG if shimplify_trace else logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))
log.addHandler(ch)


def __upsert_shim_json(filename, bv_list):
    with open(filename, 'r') as file:
        contents = file.readlines()
    tag = 'spark-rapids-shim-json-lines'
    shim_comment = []
    shim_comment.append('/*** ' + tag)
    for build_ver in bv_list:
        shim_comment.append(json.dumps({'spark':  build_ver}))
    shim_comment.append(tag + ' ***/')
    __delete_prior_comment_if_allowed(contents, tag, filename)
    log.debug("Inserting comment %s to %s", shim_comment, filename)
    package_line = next(i for i in range(len(contents)) if str(contents[i]).startswith('package'))
    log.debug("filename=%s package_line_number=%d", filename, package_line)
    shim_comment_str = '\n'.join(shim_comment) + '\n'
    contents.insert(package_line, shim_comment_str)
    with open(filename, 'w') as file:
        file.writelines(contents)


def __delete_prior_comment_if_allowed(contents, tag, filename):
    opening_shim_comment_line = None
    closing_shim_comment_line = None
    try:
        opening_shim_comment_line = next(i for i in range(len(contents))
                                         if tag in str(contents[i]))
        shim_comment_and_below = range(opening_shim_comment_line + 1, len(contents))
        closing_shim_comment_line = next(i for i in shim_comment_and_below
                                         if tag in str(contents[i]))
    except StopIteration as si_exc:
        if not (opening_shim_comment_line is None) and (closing_shim_comment_line is None):
            raise Exception("%s: no closing comment for %s:%d"
                            % (si_exc, filename, opening_shim_comment_line))

    # no work
    if opening_shim_comment_line is None:
        return
    if not over_shimplify:
        raise Exception("found shim comment from prior execution at %s:%d, use -DoverShimplify=true"
                        "to overwrite" % (filename, opening_shim_comment_line))
    assert not (opening_shim_comment_line is None) and not (closing_shim_comment_line is None)
    log.debug("removing comments %s:%d:%d", filename, opening_shim_comment_line,
              closing_shim_comment_line)
    del contents[opening_shim_comment_line:(closing_shim_comment_line + 1)]


def __git_rename(shim_file, first_shim):
    log.debug("git rename %s to the canonical dir of %s", shim_file, first_shim)
    parent_pom_dir = __ant_proj_prop('spark.rapids.source.basedir')
    # sql-plugin/src/main/320+-nondb/scala/org/apache/spark/...
    rel_path = os.path.relpath(shim_file, parent_pom_dir)
    log.debug("spark-rapids root dir: %s", parent_pom_dir)
    log.debug("shim file path relative to root dir: %s", rel_path)
    path_parts = rel_path.split(os.sep)
    path_parts[3] = first_shim
    new_shim_file = os.sep.join([parent_pom_dir] + path_parts)
    if shim_file == new_shim_file:
        log.info("%s is already at the right location, skipping git rename", shim_file)
        return
    new_shim_dir = os.path.dirname(new_shim_file)
    log.debug("creating new shim path %s", new_shim_dir)
    try:
        os.makedirs(new_shim_dir)
    except OSError as ose:
        if not (ose.errno == errno.EEXIST and os.path.isdir(new_shim_dir)):
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

    The canonical location of a source file shared by multiple shims is
    src/main/<top_buildver_in_the_comment>

    You can find all shim files for a particular shim easily by executing:
    git grep '{"spark": "312"}'
    """
    log.info('############# Starting Jython Task Shimplify #######')
    log.info("config: task_enabled=%s over_shimplify=%s move_files=%s",
             task_enabled, over_shimplify, move_files)
    log.info("review changes and `git restore` if necessary")

    shims_attr = __ant_attr('shims')
    # remove whitespace
    shims_attr_csv = str(shims_attr).translate(None, ' \n\r')
    shims_arr = shims_attr_csv.split(',')
    shims_arr.sort()

    for prop_pattern in ["spark%s.sources", "spark%s.test.sources"]:
        __warn_shims_with_multiple_dedicated_dirs(shims_arr, prop_pattern)

    if not task_enabled:
        log.info('Skipping shimplify! Set -Dshimplify=true to convert old shims')
        return

    # map file -> [shims it's part of]
    files2bv = {}
    for build_ver in shims_arr:
        # alternatively we can use range dirs instead of files, which is more efficient
        # keeping at file level for flexibility until/unless the shim layer becomes unexpectedly
        # large
        shim_source_files = __project().getReference("spark%s.fileset" % build_ver)
        log.debug("build_ver=%s shim_src.size=%i", build_ver, shim_source_files.size())
        for resource in shim_source_files:
            shim_file = str(resource)
            if shim_file in files2bv.keys():
                files2bv[shim_file] += [build_ver]
            else:
                files2bv[shim_file] = [build_ver]

    for shim_file, bv_list in files2bv.items():
        log.debug("calling upsert_shim_json shim_file=%s bv_list=%s", shim_file, bv_list)
        __upsert_shim_json(shim_file, bv_list)
        if move_files:
            __git_rename(shim_file, bv_list[0])


def __warn_shims_with_multiple_dedicated_dirs(shims_arr, prop_pattern):
    dirs2bv = {}
    for build_ver in shims_arr:
        log.debug("updating dirs2bv for %s", build_ver)
        src_dirs = __ant_proj_prop(prop_pattern % build_ver)
        shim_dirs = [] if len(src_dirs) == 0 else src_dirs.split(',')
        for dir in shim_dirs:
            if dir not in dirs2bv.keys():
                dirs2bv[dir] = [build_ver]
            else:
                dirs2bv[dir] += [build_ver]
    log.debug("Map build_ver -> shim_dirs %s" % dirs2bv)
    # each shim has at least one dedicated dir, report shims
    # with multiple dedicated dirs because they can be merged
    single_shim_dirs = {dir: shims[0] for dir, shims in dirs2bv.items() if len(shims) == 1}
    log.debug("shims with exclusive dirs %s", single_shim_dirs)
    multi_dir_shims = {}
    for dir, single_shim in single_shim_dirs.items():
        if single_shim in multi_dir_shims.keys():
            multi_dir_shims[single_shim].append(dir)
        else:
            multi_dir_shims[single_shim] = [dir]
    for shim, dirs in multi_dir_shims.items():
        if len(dirs) > 1:
            log.warning("Consider consolidating %s, it spans multiple dedicated directories %s",
                        shim, dirs)


task_impl()
