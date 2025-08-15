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

def resource_bounded_multithreaded_reader_conf(file_type, specialized_conf = {}):
    base_conf = specialized_conf.copy()
    base_conf['spark.rapids.sql.multiThreadedRead.stageLevelPool'] = 'true'
    base_conf['spark.sql.sources.useV1SourceList'] = file_type

    combine_size = ('spark.rapids.sql.reader.multithreaded.combine.sizeBytes', [0, 4 << 20])
    keep_order = ('spark.rapids.sql.reader.multithreaded.read.keepOrder', [False, True])
    reader_type = ('spark.rapids.sql.format.%s.reader.type' % file_type,
                   ['MULTITHREADED', 'COALESCING'])
    pool_size = ('spark.rapids.sql.multiThreadedRead.numThreads', [16, 128])
    memory_limit = ('spark.rapids.sql.multiThreadedRead.memoryLimit', [
        4 << 20,   # 4MB
        32 << 20,   # 32MB
        128 << 20,  # 128MB
    ])
    task_timeout = ('spark.rapids.sql.multiThreadedRead.taskTimeout', [0, 1000])

    conf_matrix = [base_conf]
    for conf_branch in [combine_size, keep_order, reader_type, pool_size, memory_limit, task_timeout]:
        branch_key, branch_values = conf_branch
        updated_matrix = []
        for conf in conf_matrix:
            for value in branch_values:
                conf_copy = conf.copy()
                conf_copy[branch_key] = value
                updated_matrix.append(conf_copy)
        conf_matrix = updated_matrix

    return conf_matrix
