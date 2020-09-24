#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import signal
import select
import socket
import sys
import traceback
import time
import gc
from errno import EINTR, EAGAIN
from socket import AF_INET, SOCK_STREAM, SOMAXCONN
from signal import SIGHUP, SIGTERM, SIGCHLD, SIG_DFL, SIG_IGN

from pyspark.serializers import read_int, write_int
from pyspark.daemon import worker

from rapids.worker import initialize_gpu_mem


def manager():
    # Create a new process group to corral our children
    os.setpgid(0, 0)

    # Create a listening socket on the AF_INET loopback interface
    listen_sock = socket.socket(AF_INET, SOCK_STREAM)
    listen_sock.bind(('127.0.0.1', 0))
    listen_sock.listen(max(1024, SOMAXCONN))
    listen_host, listen_port = listen_sock.getsockname()

    # re-open stdin/stdout in 'wb' mode
    stdin_bin = os.fdopen(sys.stdin.fileno(), 'rb', 4)
    stdout_bin = os.fdopen(sys.stdout.fileno(), 'wb', 4)
    write_int(listen_port, stdout_bin)
    stdout_bin.flush()

    def shutdown(code):
        signal.signal(SIGTERM, SIG_DFL)
        # Send SIGHUP to notify workers of shutdown
        os.kill(0, SIGHUP)
        sys.exit(code)

    def handle_sigterm(*args):
        shutdown(1)
    signal.signal(SIGTERM, handle_sigterm)  # Gracefully exit on SIGTERM
    signal.signal(SIGHUP, SIG_IGN)  # Don't die on SIGHUP
    signal.signal(SIGCHLD, SIG_IGN)

    reuse = os.environ.get("SPARK_REUSE_WORKER")

    # Initialization complete
    try:
        while True:
            try:
                ready_fds = select.select([0, listen_sock], [], [], 1)[0]
            except select.error as ex:
                if ex[0] == EINTR:
                    continue
                else:
                    raise

            if 0 in ready_fds:
                try:
                    worker_pid = read_int(stdin_bin)
                except EOFError:
                    # Spark told us to exit by closing stdin
                    shutdown(0)
                try:
                    os.kill(worker_pid, signal.SIGKILL)
                except OSError:
                    pass  # process already died

            if listen_sock in ready_fds:
                try:
                    sock, _ = listen_sock.accept()
                except OSError as e:
                    if e.errno == EINTR:
                        continue
                    raise

                # Launch a worker process
                try:
                    pid = os.fork()
                except OSError as e:
                    if e.errno in (EAGAIN, EINTR):
                        time.sleep(1)
                        pid = os.fork()  # error here will shutdown daemon
                    else:
                        outfile = sock.makefile(mode='wb')
                        write_int(e.errno, outfile)  # Signal that the fork failed
                        outfile.flush()
                        outfile.close()
                        sock.close()
                        continue

                if pid == 0:
                    # in child process
                    listen_sock.close()

                    # It should close the standard input in the child process so that
                    # Python native function executions stay intact.
                    #
                    # Note that if we just close the standard input (file descriptor 0),
                    # the lowest file descriptor (file descriptor 0) will be allocated,
                    # later when other file descriptors should happen to open.
                    #
                    # Therefore, here we redirects it to '/dev/null' by duplicating
                    # another file descriptor for '/dev/null' to the standard input (0).
                    # See SPARK-26175.
                    devnull = open(os.devnull, 'r')
                    os.dup2(devnull.fileno(), 0)
                    devnull.close()

                    try:
                        # GPU context setup
                        initialize_gpu_mem()

                        # Acknowledge that the fork was successful
                        outfile = sock.makefile(mode="wb")
                        write_int(os.getpid(), outfile)
                        outfile.flush()
                        outfile.close()
                        authenticated = False
                        while True:
                            code = worker(sock, authenticated)
                            if code == 0:
                                authenticated = True
                            if not reuse or code:
                                # wait for closing
                                try:
                                    while sock.recv(1024):
                                        pass
                                except Exception:
                                    pass
                                break
                            gc.collect()
                    except:
                        traceback.print_exc()
                        os._exit(1)
                    else:
                        os._exit(0)
                else:
                    sock.close()

    finally:
        shutdown(1)


if __name__ == '__main__':
    manager()
