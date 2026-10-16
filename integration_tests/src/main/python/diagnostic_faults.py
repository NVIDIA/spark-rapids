# Copyright (c) 2026, NVIDIA CORPORATION.
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
"""TEST ONLY: explicit fault cases; filename intentionally avoids normal pytest discovery."""

import os
import signal

import pytest
from py4j.protocol import Py4JError

import spark_init_internal as spark_init


def announce_fault(name):
    message = f"TEST ONLY: injecting {name}; {spark_init._spark_process_details}"
    spark_init.logger.error(message)
    print(message, flush=True)


def test_assertion_failure():
    announce_fault("ordinary assertion failure")
    pytest.fail("TEST ONLY: expected assertion failure with a healthy JVM")


def test_pytest_worker_exit():
    assert os.environ.get("PYTEST_XDIST_WORKER"), "Requires an isolated xdist worker"
    announce_fault("pytest worker os._exit(88)")
    os._exit(88)


def test_driver_exit():
    announce_fault("driver Runtime.halt(87)")
    spark = spark_init.get_spark_i_know_what_i_am_doing()
    try:
        spark._jvm.java.lang.Runtime.getRuntime().halt(87)
    except (Py4JError, OSError):
        # The existing fixture detects the dead JVM after this test and logs cached identity.
        return
    pytest.fail("TEST ONLY: driver halt unexpectedly returned")


def test_native_driver_crash():
    assert os.environ.get("PYTEST_XDIST_WORKER"), "Requires an isolated xdist worker"
    process = spark_init._spark_gateway_process
    assert process is not None and process.poll() is None, "Requires this worker's live JVM child"
    announce_fault("SIGSEGV to the worker-owned driver JVM; expect hs_err header")
    process.send_signal(signal.SIGSEGV)
    assert process.wait(timeout=30) != 0
