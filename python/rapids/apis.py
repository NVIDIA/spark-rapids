##
# Copyright (c) 2020, NVIDIA CORPORATION.
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
##

import os
from pyspark.taskcontext import TaskContext

# Initilize the GPU context for cuDF, including:
#   - Set the CUDA_VISIBLE_DEVICES per gpuID from TaskContext for cuDF.
#   - <RMM things, such as the maximum size of GPU memory>
def initGpuContext():
    # To avoid duplicate initialization
    if "GPU_INITED" not in os.environ:
        res = TaskContext.get().resources()
        if 'gpu' not in res:
            raise Exception("Can not find GPU in task resources.")
        gpu_addrs = res['gpu'].addresses
        if len(gpu_addrs) > 1:
            raise Exception("Spark GPU Plugin only supports 1 gpu per python worker.")
        # Always override if this API is called
        os.environ['CUDA_VISIBLE_DEVICES'] = gpu_addrs[0]
        print("Set CUDA visible devices to {}".format(os.environ.get('CUDA_VISIBLE_DEVICES')))
        os.environ['GPU_INITED'] = 'true'
