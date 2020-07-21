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

# Test print the CUDA_VISIBLE_DEVICES
print("Found CUDA visible devices: {}".format(os.environ.get('CUDA_VISIBLE_DEVICES')))

if __name__ == '__main__':
    java_port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
    auth_secret = os.environ["PYTHON_WORKER_FACTORY_SECRET"]
    (sock_file, sock) = local_connect_and_auth(java_port, auth_secret)
    # Use the `sock_file` as both input and ouput will cause EOFException in JVM side,
    # So open a new file object on the same socket as output, similar hehavior
    # with that in `pyspark.daemon.py`.
    buffer_size = int(os.environ.get("SPARK_BUFFER_SIZE", 65536))
    outfile = os.fdopen(os.dup(sock.fileno()), "wb", buffer_size)
    worker_main(sock_file, outfile)
