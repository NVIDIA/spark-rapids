# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Shared test utilities for JVM skill integration tests.
"""

import re
import subprocess
import sys
from typing import Optional


def run_mvn(
    work_dir: str,
    *goals: str,
    extra_args: Optional[list[str]] = None,
    timeout: int = 300,
) -> subprocess.CompletedProcess:
    """Run Maven in work_dir with the given goals."""
    cmd = ["mvn", *goals, "-q"]
    if extra_args:
        cmd.extend(extra_args)
    result = subprocess.run(
        cmd,
        cwd=work_dir,
        capture_output=True,
        text=True,
        timeout=timeout,
    )

    # Write output to stdout/stderr so it is visible via pytest -s (and on errors)
    if result.stdout:
        sys.stdout.write(result.stdout)
    if result.stderr:
        sys.stderr.write(result.stderr)
    return result


def run_script(
    script_path: str,
    args: Optional[list[str]] = None,
    timeout: int = 300,
) -> subprocess.CompletedProcess:
    """Run a bash script with the given arguments."""
    cmd = ["bash", script_path]
    if args:
        cmd.extend(args)
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )

    # Write output to stdout/stderr so it is visible via pytest -s (and on errors)
    if result.stdout:
        sys.stdout.write(result.stdout)
    if result.stderr:
        sys.stderr.write(result.stderr)
    return result


def replace_java_todo_method(source: str, method_name: str, new_body: str) -> str:
    """
    Replace a TODO method in a Java source file with a real implementation.
    """
    pattern = re.compile(r"    public static \S+ " + re.escape(method_name) + r"\b")
    match = pattern.search(source)
    if not match:
        raise ValueError(f"Could not find TODO method '{method_name}' in source")

    start = match.start()
    brace_pos = source.index("{", start)
    end_pos = source.index("}", brace_pos + 1) + 1
    return source[:start] + new_body + source[end_pos:]


def replace_scala_todo_method(source: str, method_name: str, new_body: str) -> str:
    """
    Replace a TODO method stub in a Scala source file with a real implementation.
    Assumes stubs look like "def foo(...) = ???"
    """
    pattern = re.compile(
        r"  def " + re.escape(method_name) + r"\b.*?\?\?\?",
        re.DOTALL,
    )
    result = pattern.sub(new_body, source, count=1)
    if result == source:
        raise ValueError(f"Could not find TODO method '{method_name}' in source")
    return result
