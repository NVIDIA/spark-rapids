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

"""implementation of shimplify Ant task in Python2.7 for Jython"""

import errno
import json
import logging
import os
import re
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
    assert prop_name is not None, "Invalid property: None"
    prop_val = __ant_proj_prop(prop_name)
    return str(True).lower() == prop_val


def __is_enabled_attr(attr):
    assert attr is not None, "Invalid attribute: None"
    attr_val = __ant_attr(attr)
    return attr_val is not None and __is_enabled_property(attr_val)


def __csv_ant_prop_as_arr(name):
    prop_val = __ant_proj_prop(name)
    return __csv_as_arr(prop_val)


def __csv_attr_as_arr(name):
    attr_val = __ant_attr(name)
    return __csv_as_arr(attr_val)


def __csv_as_arr(str_val):
    if str_val in (None, ''):
        return []
    else:
        return str_val.translate(None, ' \n\r').split(',')


should_add_comment = __is_enabled_attr('if')

# should we move files?
should_move_files = __is_enabled_property('shimplifyMove')

# allowed to overwrite the existing comment
should_overwrite = __is_enabled_property('overShimplify')

# enable log tracing
should_trace = __is_enabled_property('traceShimplify')

__shim_comment_tag = 'spark-rapids-shim-json-lines'
__opening_shim_tag = '/*** ' + __shim_comment_tag
__closing_shim_tag = __shim_comment_tag + ' ***/'
__shims_arr = sorted(__csv_attr_as_arr('shims'))
__all_shims_arr = sorted(__csv_ant_prop_as_arr('all.buildvers'))

log = logging.getLogger('shimplify')
log.setLevel(logging.DEBUG if should_trace else logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))
log.addHandler(ch)


def __upsert_shim_json(filename, bv_list):
    with open(filename, 'r') as file:
        contents = file.readlines()

    shim_comment = []
    shim_comment.append(__opening_shim_tag)
    for build_ver in bv_list:
        shim_comment.append(json.dumps({'spark':  build_ver}))
    shim_comment.append(__closing_shim_tag)
    __delete_prior_comment_if_allowed(contents, __shim_comment_tag, filename)
    log.debug("Inserting comment %s to %s", shim_comment, filename)
    package_line = next(i for i in range(len(contents)) if str(contents[i]).startswith('package'))
    log.debug("filename %s package_line_number=%d", filename, package_line)
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
        if (opening_shim_comment_line is not None) and (closing_shim_comment_line is None):
            raise Exception("%s: no closing comment for %s:%d"
                            % (si_exc, filename, opening_shim_comment_line))

    # no work
    if opening_shim_comment_line is None:
        return
    if not should_overwrite:
        raise Exception("found shim comment from prior execution at %s:%d, use -DoverShimplify=true"
                        "to overwrite" % (filename, opening_shim_comment_line))
    assert (opening_shim_comment_line is not None) and (closing_shim_comment_line is not None)
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
    # to enable both builds at the same time the original location should change too
    path_parts[3] = 'spark%s' % first_shim
    new_shim_file = os.sep.join([parent_pom_dir] + path_parts)
    if shim_file == new_shim_file:
        log.info("%s is already at the right location, skipping git rename", shim_file)
        return
    new_shim_dir = os.path.dirname(new_shim_file)
    log.debug("creating new shim path %s", new_shim_dir)
    __makedirs(new_shim_dir)

    git_cmd = ['git', 'mv', shim_file, new_shim_file]
    ret_code = subprocess.call(git_cmd)
    if ret_code != 0:
        raise Exception('git rename failed')


def __makedirs(new_dir):
    try:
        log.debug("__makedirs %s", new_dir)
        os.makedirs(new_dir)
    except OSError as ose:
        if not (ose.errno == errno.EEXIST and os.path.isdir(new_dir)):
            raise


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
    log.info('# Starting Jython Task Shimplify #')
    log.info("config: task_enabled=%s over_shimplify=%s move_files=%s",
             should_add_comment, should_overwrite, should_move_files)
    log.info("review changes and `git restore` if necessary")

    for prop_pattern in ["spark%s.sources", "spark%s.test.sources"]:
        __warn_shims_with_multiple_dedicated_dirs(prop_pattern)
    if should_add_comment:
        __shimplify_layout()
    else:
        log.info('Skipping shimplify! Set -Dshimplify=true to convert old shims')

    __generate_symlinks()


def __generate_symlinks():
    """
    link
    <module>/src/<main|test>/<buildver>/scala/<package_path>/SomeClass.scala
    <module>/target/<buildver>/generated/src/<main|test>/scala/<package_path>/SomeClass.scala
    """
    buildver = __ant_proj_prop('buildver')
    shim_dir_pattern = re.compile(r'spark\d{3}')
    shim_comment_pattern = re.compile(re.escape(__opening_shim_tag) +
                                      r'\n(.*)\n' +
                                      re.escape(__closing_shim_tag), re.DOTALL)
    for src_type in ['main', 'test']:
        __traverse_source_tree(buildver, src_type, shim_dir_pattern, shim_comment_pattern)


def __traverse_source_tree(buildver, src_type, shim_dir_pattern, shim_comment_pattern):
    base_dir = str(__project().getBaseDir())
    src_root = os.path.join(base_dir, 'src', src_type)
    target_root = os.path.join(base_dir, 'target', "spark%s" % buildver, 'generated', 'src',
                               src_type)
    log.info("# generating symlinks for shim %s %s files under %s", buildver, src_type, target_root)
    for dir, subdirs, files in os.walk(src_root, topdown=True):
        if dir == src_root:
            subdirs[:] = [d for d in subdirs if re.match(shim_dir_pattern, d)]
        for f in files:
            shim_file_path = os.path.join(dir, f)
            log.debug("processing shim comment at %s", shim_file_path)
            with open(shim_file_path, 'r') as shim_file:
                shim_file_txt = shim_file.read()
                shim_match = shim_comment_pattern.search(shim_file_txt)
                assert shim_match is not None and shim_match.groups(), \
                    "no shim comment located in %s" % shim_file_path
                shim_arr = shim_match.group(1).split('\n')
                assert len(shim_arr) > 0, "invalid empty shim comment,"\
                    "orphan shim files should be deleted"
                build_ver_arr = map(lambda s: str(json.loads(s).get('spark')), shim_arr)
                log.debug("extracted shims %s", build_ver_arr)
                assert build_ver_arr == sorted(build_ver_arr),\
                    "%s shim list is not properly sorted" % shim_file_path

                if buildver in build_ver_arr:
                    first_build_ver = build_ver_arr[0]
                    log.debug("top shim comment %s", first_build_ver)
                    shim_file_rel_path = os.path.relpath(shim_file_path, src_root)
                    expected_prefix = "spark%s%s" % (first_build_ver, os.sep)
                    assert shim_file_rel_path.startswith(expected_prefix),\
                        "Unexpected path %s is not prefixed by %s" % (shim_file_rel_path,
                                                                      expected_prefix)
                    shim_file_rel_path_parts = shim_file_rel_path.split(os.sep)
                    # drop spark3XY from spark3XY/scala/com/nvidia
                    target_file_parts = shim_file_rel_path_parts[1:]
                    target_rel_path = os.sep.join(target_file_parts)
                    target_shim_file_path = os.path.join(target_root, target_rel_path)
                    log.debug("creating symlink %s -> %s", target_shim_file_path, shim_file_path)
                    __makedirs(os.path.dirname(target_shim_file_path))
                    if should_overwrite:
                        __remove_file(target_shim_file_path)
                    os.symlink(shim_file_path, target_shim_file_path)


def __remove_file(target_shim_file_path):
    try:
        os.remove(target_shim_file_path)
    except OSError as ose:
        # ignore non-exisiting files
        if ose.errno != errno.ENOENT:
            raise


def __shimplify_layout():
    # map file -> [shims it's part of]
    files2bv = {}
    for build_ver in __all_shims_arr:
        src_roots = __csv_ant_prop_as_arr("spark%s.sources" % build_ver)
        test_src_roots = __csv_ant_prop_as_arr("spark%s.test.sources" % build_ver)
        log.debug("check %s sources: %s", build_ver, src_roots)
        log.debug("check %s test sources: %s", build_ver, test_src_roots)
        main_and_test_roots = src_roots + test_src_roots
        # alternatively we can use range dirs instead of files, which is more efficient.
        # file level provides flexibility until/unless the shim layer becomes unexpectedly
        # large
        for src_root in main_and_test_roots:
            log.debug("os.walk looking for shim files from %s", src_root)
            for dir, _, shim_source_files in os.walk(src_root):
                for shim_file in shim_source_files:
                    shim_path = os.path.join(dir, shim_file)
                    log.debug("updating files2bv %s -> %s", shim_path, build_ver)
                    if shim_path in files2bv.keys():
                        files2bv[shim_path] += [build_ver]
                    else:
                        files2bv[shim_path] = [build_ver]
    for shim_file, bv_list in files2bv.items():
        log.debug("calling upsert_shim_json on shim_file %s bv_list=%s", shim_file, bv_list)
        __upsert_shim_json(shim_file, bv_list)
        if should_move_files:
            __git_rename(shim_file, bv_list[0])


def __warn_shims_with_multiple_dedicated_dirs(prop_pattern):
    dirs2bv = {}
    for build_ver in __all_shims_arr:
        log.debug("updating dirs2bv for %s", build_ver)
        shim_dirs = __csv_ant_prop_as_arr(prop_pattern % build_ver)
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
