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
from pyspark.worker import local_connect_and_auth, main as worker_main


def initialize_gpu_mem():
    # CUDA device(s) info
    print("INFO: Found CUDA visible device(s): {}".format(os.environ.get('CUDA_VISIBLE_DEVICES')))

    # SQL plugin info
    if 'RAPIDS_SQL_ENABLED' in os.environ:
        sql_enabled = os.environ.get('RAPIDS_SQL_ENABLED').lower() == 'true'
        print("INFO: rapids python worker is running with sql plugin {}".format(
            'enabled' if sql_enabled else 'disabled'))
    else:
        print("INFO: rapids python worker is running without sql plugin.")

    # Initialize RMM only when requiring to enable pooled or managed memory.
    pool_enabled = os.environ.get('RAPIDS_POOLED_MEM_ENABLED', 'false').lower() == 'true'
    uvm_enabled = os.environ.get('RAPIDS_UVM_ENABLED', 'false').lower() == 'true'
    if pool_enabled:
        from cudf import rmm
        '''
        RMM will be initialized with default configures (pool disabled) when importing cudf
        as above. So overwrite the initialization when asking for pooled memory,
        along with a pool size and max pool size.
        Meanwhile, the above `import` precedes the `import` in UDF, make our initialization
        not be overwritten again by the `import` in UDF, since Python will ignore duplicated
        `import`.
        '''
        pool_size = int(os.environ.get('RAPIDS_POOLED_MEM_SIZE', 0))
        if 'RAPIDS_POOLED_MEM_MAX_SIZE' in os.environ:
            pool_max_size = int(os.environ['RAPIDS_POOLED_MEM_MAX_SIZE'])
        else:
            pool_max_size = pool_size
        base_t = rmm.mr.ManagedMemoryResource if uvm_enabled else rmm.mr.CudaMemoryResource
        rmm.mr.set_default_resource(rmm.mr.PoolMemoryResource(base_t(), pool_size, pool_max_size))
        print("DEBUG: Pooled memory, pool size: {}, max size: {}".format(pool_size,
                                                                         pool_max_size))
    elif uvm_enabled:
        # Will this really be needed for Python ?
        from cudf import rmm
        rmm.mr.set_default_resource(rmm.mr.ManagedMemoryResource())
    else:
        # Do nothing, whether to use RMM (default mode) or not depends on UDF definition.
        pass


if __name__ == '__main__':
    # GPU context setup
    initialize_gpu_mem()

    # Code below is all copied from Pyspark/worker.py
    java_port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
    auth_secret = os.environ["PYTHON_WORKER_FACTORY_SECRET"]
    (sock_file, sock) = local_connect_and_auth(java_port, auth_secret)
    # Use the `sock_file` as both input and output will cause EOFException in JVM side,
    # So open a new file object on the same socket as output, similar behavior
    # with that in `pyspark/daemon.py`.
    buffer_size = int(os.environ.get("SPARK_BUFFER_SIZE", 65536))
    outfile = os.fdopen(os.dup(sock.fileno()), "wb", buffer_size)
    worker_main(sock_file, outfile)
