# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Integration tests for the JVM export directories.
"""

import os
import shutil
import stat
import tempfile
from pathlib import Path

import pytest

from . import cuda_fixtures, java_fixtures, scala_fixtures
from .utils import run_mvn, run_script

pytestmark = pytest.mark.slow

SKILLS_DIR = Path(__file__).resolve().parents[2]
TEMPLATES_DIR = SKILLS_DIR / "udf-gen-test" / "templates"
CUDA_TEMPLATES_DIR = (
    SKILLS_DIR
    / "udf-convert-to-cuda"
    / "templates"
    / "cuda"
)

LANG_CONFIGS = [java_fixtures, scala_fixtures]
TARGETS = ["cudf", "sql", "cuda"]
LANG_TARGET_PARAMS = [(cfg, target) for cfg in LANG_CONFIGS for target in TARGETS]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _comparison_test_methods(cfg, target):
    """Return the comparison test method stubs for the given target."""
    if target == "cudf":
        return {"registerRapidsUDF": cfg.RAPIDS_UDF_REGISTER}
    if target == "cuda":
        return {"registerRapidsUDF": cfg.NATIVE_RAPIDS_UDF_REGISTER}
    # target == "sql" - no additional methods
    return {}


def _bench_utils_methods(cfg, target):
    """Return the BenchUtils method stubs for the given target."""
    methods = {
        "generateSyntheticData": cfg.BENCH_GENERATE,
        "executeCpu": cfg.BENCH_CPU,
    }
    if target == "cudf":
        methods["executeGpu"] = cfg.BENCH_GPU_CUDF
    elif target == "cuda":
        methods["executeGpu"] = cfg.BENCH_GPU_CUDA
    else:
        methods["executeGpu"] = cfg.BENCH_GPU_SQL
    return methods


def _micro_bench_methods(cfg, target):
    """Return the MicroBenchRunner method stubs."""
    methods = {
        "prepareCpuData": cfg.MICRO_PREPARE_CPU,
        "executeCpu": cfg.MICRO_EXECUTE_CPU,
    }
    if target == "cuda":
        methods["executeGpu"] = cfg.MICRO_EXECUTE_GPU_CUDA
    else:
        methods["executeGpu"] = cfg.MICRO_EXECUTE_GPU
    return methods


def _build_project_dir(cfg):
    """Copy export directory to a temp directory and resolve pom.xml."""
    export_dir = TEMPLATES_DIR / cfg.NAME
    tmp_dir = tempfile.mkdtemp(prefix=f"test_{cfg.NAME}_")
    project_dir = os.path.join(tmp_dir, cfg.NAME)
    shutil.copytree(str(export_dir), project_dir)

    return tmp_dir, project_dir


def _copy_cuda_templates(project_dir):
    """Copy CUDA add-on templates and replace placeholders with fixture sources."""
    shutil.copytree(str(CUDA_TEMPLATES_DIR), project_dir, dirs_exist_ok=True)

    extract_script = Path(project_dir) / "native" / "scripts" / "extract-cudf-libs.sh"
    extract_script.chmod(
        extract_script.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
    )

    project_path = Path(project_dir)
    for rel_path in cuda_fixtures.PLACEHOLDER_FILES:
        (project_path / rel_path).unlink(missing_ok=True)

    for rel_path, source in cuda_fixtures.NATIVE_SOURCE_FILES.items():
        path = project_path / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source)

    cmake_path = project_path / "native" / "src" / "main" / "cpp" / "CMakeLists.txt"
    cmake = cmake_path.read_text()
    cmake = cmake.replace(
        """\
set(SOURCE_FILES
  "src/PlaceholderUDFNameJni.cpp"
  "src/placeholder_udf_name.cu"
)
""",
        cuda_fixtures.CMAKE_SOURCE_FILES,
    )
    cmake_path.write_text(cmake)


def _fill_stubs(cfg, project_dir, target):
    """Write UDF sources and fill in all TODO stubs in the project."""
    ext = f".{cfg.NAME}"

    def _replace_stubs(path, methods):
        with open(path, "r") as f:
            source = f.read()
        for method_name, impl in methods.items():
            source = cfg.REPLACE_TODO_FN(source, method_name, impl)
        with open(path, "w") as f:
            f.write(source)

    src_dir = os.path.join(project_dir, "src", "main", cfg.NAME, "com", "udf")
    test_dir = os.path.join(project_dir, "src", "test", cfg.NAME, "com", "udf")

    # Write CPU UDF source.
    with open(os.path.join(src_dir, f"IntegerMultiplyBy2UDF{ext}"), "w") as f:
        f.write(cfg.UDF_SOURCE)

    # Fill in unit test stubs.
    _replace_stubs(os.path.join(test_dir, f"UnitTest{ext}"), cfg.UNIT_TEST_METHODS)

    if target == "cudf":
        # Write RapidsUDF source.
        with open(os.path.join(src_dir, f"IntegerMultiplyBy2RapidsUDF{ext}"), "w") as f:
            f.write(cfg.RAPIDS_UDF_SOURCE)

        # Fill comparison test stubs.
        _replace_stubs(
            os.path.join(test_dir, f"CudfComparisonTest{ext}"),
            _comparison_test_methods(cfg, "cudf"),
        )

        # Fill MicroBenchRunner stubs.
        _replace_stubs(
            os.path.join(src_dir, "bench", f"MicroBenchRunner{ext}"),
            _micro_bench_methods(cfg, target),
        )
    elif target == "cuda":
        _copy_cuda_templates(project_dir)

        # Fill comparison test stubs.
        _replace_stubs(
            os.path.join(test_dir, f"CudfComparisonTest{ext}"),
            _comparison_test_methods(cfg, "cuda"),
        )

        # Fill MicroBenchRunner stubs.
        _replace_stubs(
            os.path.join(src_dir, "bench", f"MicroBenchRunner{ext}"),
            _micro_bench_methods(cfg, target),
        )
    else:
        # Write SQL file.
        resources_dir = os.path.join(project_dir, "src", "main", "resources")
        os.makedirs(resources_dir, exist_ok=True)
        with open(os.path.join(resources_dir, "integer_multiply_by_2.sql"), "w") as f:
            f.write(cfg.SQL_SOURCE)

        # Replace SQL file path placeholder in the comparison test.
        sql_test_path = os.path.join(test_dir, f"SqlComparisonTest{ext}")
        with open(sql_test_path, "r") as f:
            content = f.read()
        content = content.replace("placeholder_udf_name", "integer_multiply_by_2")
        with open(sql_test_path, "w") as f:
            f.write(content)

        # Fill comparison test stubs.
        _replace_stubs(sql_test_path, _comparison_test_methods(cfg, "sql"))

    # Fill bench utils stubs.
    _replace_stubs(
        os.path.join(src_dir, "bench", f"BenchUtils{ext}"),
        _bench_utils_methods(cfg, target),
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", params=LANG_CONFIGS, ids=lambda c: c.NAME)
def project_dir(request):
    """Clean copy of the export template with resolved pom.xml (stubs not filled)."""
    cfg = request.param
    tmp_dir, proj = _build_project_dir(cfg)
    yield (proj, cfg)
    shutil.rmtree(tmp_dir, ignore_errors=True)


@pytest.fixture(
    scope="module",
    params=LANG_TARGET_PARAMS,
    ids=lambda p: f"{p[0].NAME}-{p[1]}",  # "language-target"
)
def project_with_fixtures(request):
    """Clean copy with resolved pom.xml and all stubs filled in."""
    cfg, target = request.param
    tmp_dir, proj = _build_project_dir(cfg)
    _fill_stubs(cfg, proj, target)
    yield (proj, cfg, target)
    shutil.rmtree(tmp_dir, ignore_errors=True)


@pytest.fixture(
    scope="class",
    params=LANG_TARGET_PARAMS,
    ids=lambda p: f"{p[0].NAME}-{p[1]}",  # "language-target"
)
def project_with_broken_gpu(request):
    """Project with deliberately broken GPU implementation."""
    cfg, target = request.param
    tmp_dir, proj = _build_project_dir(cfg)
    _fill_stubs(cfg, proj, target)

    def _break_gpu_source(source: str) -> str:
        # Change multiplier from 2 to 3
        source = source.replace("fromInt(2)", "fromInt(3)")
        source = source.replace("* 2", "* 3")
        return source

    def _insert_memory_leak(source: str) -> str:
        # Inject an unclosed Scalar.
        idx = source.index("evaluateColumnar")
        brace = source.index("{", idx)
        return (
            source[: brace + 1] + '\nScalar.fromString("LEAKED");' + source[brace + 1 :]
        )

    if target == "cudf":
        path = os.path.join(
            proj,
            "src",
            "main",
            cfg.NAME,
            "com",
            "udf",
            f"IntegerMultiplyBy2RapidsUDF.{cfg.NAME}",
        )
    elif target == "cuda":
        path = os.path.join(
            proj,
            "native",
            "src",
            "main",
            "cpp",
            "src",
            "integer_multiply_by_2.cu",
        )
    else:
        path = os.path.join(
            proj, "src", "main", "resources", "integer_multiply_by_2.sql"
        )

    # Read and overwrite with the broken source.
    with open(path, "r") as f:
        content = f.read()

    broken = _break_gpu_source(content)
    if target == "cudf":
        broken = _insert_memory_leak(broken)

    with open(path, "w") as f:
        f.write(broken)

    yield (proj, cfg, target)
    shutil.rmtree(tmp_dir, ignore_errors=True)


@pytest.fixture(scope="class", params=LANG_CONFIGS, ids=lambda c: c.NAME)
def project_with_broken_schema(request):
    """Project with wrong column name in generateSyntheticData."""
    cfg = request.param
    tmp_dir, proj = _build_project_dir(cfg)
    _fill_stubs(cfg, proj, "cudf")

    def _break_bench_source(source: str) -> str:
        # Cause a schema error due to unresolved column.
        return source.replace('.alias("value")', '.alias("wrong_column")')

    ext = f".{cfg.NAME}"
    bench_path = os.path.join(
        proj,
        "src",
        "main",
        cfg.NAME,
        "com",
        "udf",
        "bench",
        f"BenchUtils{ext}",
    )

    # Read and overwrite with the broken source.
    with open(bench_path, "r") as f:
        source = f.read()
    with open(bench_path, "w") as f:
        f.write(_break_bench_source(source))

    yield (proj, cfg)
    shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCompilation:
    """Verify the export directory compiles."""

    def test_compile_smoke(self, project_dir):
        """
        Smoke test: compile the project as-is with TODO stubs, without
        completing any of the methods, so we can catch any simple compile errors.
        """
        proj, cfg = project_dir
        result = run_mvn(proj, "clean", "compile")
        assert result.returncode == 0, f"{cfg.NAME} smoke compile failed"

    def test_compile_with_fixtures(self, project_with_fixtures):
        """
        Compile after writing UDF sources and filling in TODO stubs, to make
        sure our fixtures are valid source code.
        """
        proj, cfg, _target = project_with_fixtures
        # test-compile compiles both main and test sources.
        result = run_mvn(proj, "clean", "test-compile")
        assert result.returncode == 0, f"{cfg.NAME} compile with fixtures failed"


class TestComparisonTest:
    """Run the comparison test suite."""

    def test_run_comparison_test(self, project_with_fixtures):
        """Execute the comparison test (CudfComparisonTest or SqlComparisonTest)."""
        proj, cfg, target = project_with_fixtures
        if target == "sql":
            suite = "com.udf.SqlComparisonTest"
        else:
            suite = "com.udf.CudfComparisonTest"

        result = run_mvn(
            proj,
            "test",
            extra_args=[
                *(["-Pcuda-native-udf"] if target == "cuda" else []),
                f"{cfg.TEST_SELECTOR_FLAG}={suite}",
            ],
        )
        assert result.returncode == 0, f"{suite} failed"


class TestBench:
    """Test the benchmark pipeline (GenData + BenchRunner)."""

    def test_validate(self, project_with_fixtures):
        """GenData validation: generate a small dataset and run validation."""
        proj, _, target = project_with_fixtures
        result = run_mvn(
            proj,
            "compile",
            "exec:java",
            extra_args=[
                *(["-Pcuda-native-udf"] if target == "cuda" else []),
                "-Dexec.mainClass=com.udf.bench.GenData",
                "-Dexec.classpathScope=compile",
                "-Dexec.args=--rows 1000 --validate --spark-conf spark.master=local[*]",
            ],
        )
        assert result.returncode == 0, "GenData validate failed"

    def test_spark_e2e(self, project_with_fixtures):
        """End-to-end: GenData generates data, SparkBenchRunner benchmarks CPU/GPU."""
        proj, _, target = project_with_fixtures

        data_dir = os.path.join(proj, "data", "bench_input")
        result_path = os.path.join(proj, "results", "bench_result.json")
        mvn_args = ["--mvn-arg", "-Pcuda-native-udf"] if target == "cuda" else []
        try:
            # GenData: generate parquet
            gen_result = run_script(
                os.path.join(proj, "run_gen_data.sh"),
                args=["--rows", "1000", "--output-path", data_dir, *mvn_args],
            )
            assert gen_result.returncode == 0, "run_gen_data.sh failed"

            # BenchRunner: run both benchmarks
            for mode in ["cpu", "gpu"]:
                bench_result = run_script(
                    os.path.join(proj, "run_spark_benchmark.sh"),
                    args=[
                        "--mode",
                        mode,
                        "--data-path",
                        data_dir,
                        "--result-path",
                        result_path,
                        *mvn_args,
                    ],
                )
                assert (
                    bench_result.returncode == 0
                ), f"run_spark_benchmark.sh --mode {mode} failed"
                assert os.path.isfile(
                    result_path
                ), f"Result file not created: {result_path}"
        finally:
            shutil.rmtree(data_dir, ignore_errors=True)
            if os.path.isfile(result_path):
                os.remove(result_path)

    def test_micro_e2e(self, project_with_fixtures):
        """End-to-end: GenData generates data, MicroBenchRunner benchmarks CPU/GPU."""
        proj, _, target = project_with_fixtures
        if target not in {"cudf", "cuda"}:
            pytest.skip("MicroBenchRunner only applies to RapidsUDF targets")

        data_dir = os.path.join(proj, "data", "micro_input")
        mvn_args = ["--mvn-arg", "-Pcuda-native-udf"] if target == "cuda" else []
        try:
            # GenData: generate parquet
            gen_result = run_script(
                os.path.join(proj, "run_gen_data.sh"),
                args=["--rows", "1000", "--output-path", data_dir, *mvn_args],
            )
            assert gen_result.returncode == 0, "run_gen_data.sh failed"

            # MicroBenchRunner: run both benchmarks
            bench_result = run_script(
                os.path.join(proj, "run_micro_benchmark.sh"),
                args=["--mode", "all", "--data-path", data_dir, *mvn_args],
            )
            assert bench_result.returncode == 0, (
                "run_micro_benchmark.sh failed:\n"
                + bench_result.stdout
                + bench_result.stderr
            )
        finally:
            shutil.rmtree(data_dir, ignore_errors=True)


class TestErrors:
    """Verify that errors are caught by the test harness."""

    def test_comparison_catches_gpu_error(self, project_with_broken_gpu):
        """Comparison test should fail when GPU implementation produces wrong results."""
        proj, cfg, target = project_with_broken_gpu
        if target == "sql":
            suite = "com.udf.SqlComparisonTest"
        else:
            suite = "com.udf.CudfComparisonTest"

        result = run_mvn(
            proj,
            "test",
            extra_args=[
                *(["-Pcuda-native-udf"] if target == "cuda" else []),
                f"{cfg.TEST_SELECTOR_FLAG}={suite}",
            ],
        )
        assert (
            result.returncode != 0
        ), f"{suite} should have failed with broken GPU implementation"
        combined = result.stdout + result.stderr
        assert (  # 123 * 2 vs. 123 * 3, since we swapped multiplier
            "246" in combined and "369" in combined
        ), "Expected to see mismatch in test output"

        if target == "cudf":
            # Re-run with debug.memory.leaks=true to verify memory leak is detected
            leak_result = run_mvn(
                proj,
                "test",
                extra_args=[
                    f"{cfg.TEST_SELECTOR_FLAG}={suite}",
                    "-Ddebug.memory.leaks=true",
                ],
            )
            assert (
                leak_result.returncode != 0
            ), f"{suite} should have failed with broken GPU implementation"
            leak_output = leak_result.stdout + leak_result.stderr
            assert "A SCALAR WAS LEAKED" in leak_output, "Expected memory leak"

    def test_bench_validate_catches_error(self, project_with_broken_schema):
        """GenData --validate should fail when synthetic data has wrong schema."""
        proj, cfg = project_with_broken_schema
        result = run_mvn(
            proj,
            "compile",
            "exec:java",
            extra_args=[
                "-Dexec.mainClass=com.udf.bench.GenData",
                "-Dexec.classpathScope=compile",
                "-Dexec.args=--rows 1000 --validate --spark-conf spark.master=local[*]",
            ],
        )
        assert (
            result.returncode != 0
        ), "GenData --validate should have failed with wrong schema"
        assert (
            "org.apache.spark.sql.AnalysisException" in result.stderr
        ), "Expected to see schema error message"
