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

from pyspark.worker import main as worker_main
from pyspark.java_gateway import local_connect_and_auth

# Test print the CUDA_VISIBLE_DEVICES
print("CUDA visible devices: {}".format(os.environ.get('CUDA_VISIBLE_DEVICES')))

if __name__ == '__main__':
    print("main 1")
    # Copied from pyspark/worker.py
    # Read information about how to connect back to the JVM from the environment.
    java_port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
    auth_secret = os.environ["PYTHON_WORKER_FACTORY_SECRET"]
    (sock_file, _) = local_connect_and_auth(java_port, auth_secret)
    print("main 2")
    worker_main(sock_file, sock_file)
    print("main 3")
