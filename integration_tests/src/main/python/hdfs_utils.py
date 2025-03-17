# Copyright (c) 2025, NVIDIA CORPORATION.
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

import os
from pathlib import Path

def hdfs_glob(path_str, pattern):
    """
    Finds hdfs files by checking the input path with glob pattern

    :param path_str: hdfs path to check
    :type path_str: str
    :return: generator of matched files
    """
    from spark_init_internal import get_spark_i_know_what_i_am_doing
    full_pattern = os.path.join(path_str, pattern)
    sc = get_spark_i_know_what_i_am_doing().sparkContext
    config = sc._jsc.hadoopConfiguration()
    fs_path = sc._jvm.org.apache.hadoop.fs.Path(full_pattern)
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(fs_path.toUri(), config)
    statuses = fs.globStatus(fs_path)
    for status in statuses:
        yield status.getPath().toString()

def glob(path_str, pattern):
    """
    Finds files by checking the input path with glob pattern.
    Support local file system and hdfs

    :param path_str: input path to check
    :type path_str: str
    :return: generator of matched files
    """
    if not path_str.startswith('hdfs:'):
        path_list = Path(path_str).glob(pattern)
        return [path.as_posix() for path in path_list]
