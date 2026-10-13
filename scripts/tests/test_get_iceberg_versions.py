#!/usr/bin/env python3

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

import contextlib
import copy
import importlib.util
import io
import json
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "get_iceberg_versions.py"
SPEC = importlib.util.spec_from_file_location("get_iceberg_versions", SCRIPT)
MATRIX_READER = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MATRIX_READER
SPEC.loader.exec_module(MATRIX_READER)

# Only the properties, profiles, and dependencies consumed by the loader are needed. Keep tests
# independent of the live release profiles, Spark shims, and Iceberg version pins.
FIXTURE_DIR = Path(__file__).resolve().parent / "resources" / "iceberg"


class IcebergVersionMatrixSuite(unittest.TestCase):
    def setUp(self):
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.pom_path = Path(temp_dir.name) / "pom.xml"
        self.pom_path.write_text((FIXTURE_DIR / "spark-shims.xml").read_text(), encoding="utf-8")
        for resource in FIXTURE_DIR.glob("iceberg/*/runtime-dependencies.xml"):
            module_pom = self.pom_path.parent / resource.parent.relative_to(FIXTURE_DIR) / "pom.xml"
            module_pom.parent.mkdir(parents=True, exist_ok=True)
            module_pom.write_text(resource.read_text(), encoding="utf-8")
        self.matrix_path = Path(temp_dir.name) / "matrix.json"
        self.document = json.loads(
            (FIXTURE_DIR / "iceberg-versions.json").read_text(encoding="utf-8"))
        self.matrix = self._load_document(self.document)

    def _load_document(self, document):
        self.matrix_path.write_text(json.dumps(document), encoding="utf-8")
        return MATRIX_READER.IcebergVersionMatrix.load(self.matrix_path, self.pom_path)

    def _run_cli(self, command, *arguments):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exit_code = MATRIX_READER.main([
                command, "--matrix", str(self.matrix_path),
                "--pom", str(self.pom_path), *arguments])
        return exit_code, stdout.getvalue(), stderr.getvalue()

    def test_selects_representative_versions(self):
        selections = {
            "3.5.1": ["1.6.1"],
            "3.5.6": ["1.9.2", "1.10.1"],
            "4.0.1": ["1.10.1"],
        }
        for spark_version, expected_versions in selections.items():
            with self.subTest(spark=spark_version):
                self.assertEqual(
                    expected_versions,
                    self.matrix.supported_iceberg_versions(spark_version))

    def test_minimum_baselines_intentionally_exclude_older_patches(self):
        for spark_version in ("3.5.0", "3.5.4"):
            with self.subTest(spark=spark_version):
                self.assertEqual([], self.matrix.supported_iceberg_versions(spark_version))
        self.assertEqual(["1.9.2"], self.matrix.supported_iceberg_versions("3.5.5"))

    def test_valid_requested_versions_preserve_caller_order(self):
        requested = ["1.10.1", "1.9.2"]
        self.assertEqual(
            requested,
            self.matrix.validate_requested_versions("3.5.6", requested))

    def test_unknown_requested_version_fails(self):
        with self.assertRaisesRegex(
                MATRIX_READER.MatrixError, "not present in the test matrix"):
            self.matrix.validate_requested_versions("3.5.6", ["9.9.9"])

    def test_requested_versions_outside_baseline_policy_fail(self):
        for spark_version in ("3.5.5", "4.2.0"):
            with self.subTest(spark=spark_version):
                with self.assertRaisesRegex(MATRIX_READER.MatrixError, "minimum-baseline"):
                    self.matrix.validate_requested_versions(spark_version, ["1.10.1"])

    def test_packaging_unsupported_requested_version_fails_with_reason(self):
        with self.assertRaisesRegex(
                MATRIX_READER.MatrixError, "Iceberg 1.6.x is not packaged"):
            self.matrix.validate_requested_versions("3.5.6", ["1.6.1"])

    def test_rejects_malformed_top_level_metadata(self):
        self.document["unexpected"] = True
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "must contain only"):
            self._load_document(self.document)

    def test_rejects_unsupported_entry_without_reason(self):
        del self.document["iceberg_versions"][0]["spark_versions"][1]["reason"]
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "needs a reason"):
            self._load_document(self.document)

    def test_rejects_missing_pom_derived_spark_mapping(self):
        entries = self.document["iceberg_versions"][0]["spark_versions"]
        entries[:] = [entry for entry in entries if entry["version"] != "3.5.6"]
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "missing 3.5.6"):
            self._load_document(self.document)

    def test_cli_fails_for_missing_mapping_instead_of_skipping(self):
        self.document["iceberg_versions"][1]["spark_versions"].pop(0)
        self.matrix_path.write_text(json.dumps(self.document), encoding="utf-8")
        exit_code, stdout, stderr = self._run_cli("list", "--spark-version", "3.5.5")
        self.assertEqual(1, exit_code)
        self.assertEqual("", stdout)
        self.assertIn("missing 3.5.5", stderr)

    def test_cli_explains_empty_selections(self):
        # 4.2.0 represents the stub family. Below-baseline patches are also valid
        # empty selections under issue #15875, even if their POM packages Iceberg.
        for spark_version in ("4.2.0", "3.5.0", "3.5.4"):
            with self.subTest(spark=spark_version):
                exit_code, stdout, stderr = self._run_cli("list", "--spark-version", spark_version)
                self.assertEqual(0, exit_code)
                self.assertEqual("\n", stdout)
                self.assertIn(f"No Iceberg versions selected for Spark {spark_version}", stderr)
                self.assertIn("minimum-baseline and packaging test policy", stderr)

    def test_cli_skips_snapshot_without_iceberg_family(self):
        exit_code, stdout, stderr = self._run_cli("list", "--spark-version", "5.0.0-SNAPSHOT")
        self.assertEqual(0, exit_code)
        self.assertEqual("\n", stdout)
        self.assertIn("No Iceberg versions selected for Spark 5.0.0-SNAPSHOT", stderr)

    def test_cli_rejects_snapshot_in_matrix_family(self):
        # Do not silently skip or select the corresponding release for a family
        # whose Iceberg coverage is defined by the matrix.
        exit_code, stdout, stderr = self._run_cli("list", "--spark-version", "3.5.6-SNAPSHOT")
        self.assertEqual(1, exit_code)
        self.assertEqual("", stdout)
        self.assertIn("invalid version", stderr)

    def test_cli_rejects_malformed_snapshot_versions(self):
        for spark_version in ("5.0-SNAPSHOT", "5.0.0-SNAPSHOT-SNAPSHOT", "5.0.0-preview"):
            with self.subTest(spark=spark_version):
                exit_code, stdout, stderr = self._run_cli("list", "--spark-version", spark_version)
                self.assertEqual(1, exit_code)
                self.assertEqual("", stdout)
                self.assertIn("invalid version", stderr)

    def test_cli_validates_requested_versions(self):
        exit_code, stdout, stderr = self._run_cli(
            "validate-requested", "--spark-version", "3.5.6",
            "--requested-versions", "1.10.1, 1.9.2")
        self.assertEqual(0, exit_code)
        self.assertEqual("1.10.1 1.9.2\n", stdout)
        self.assertEqual("", stderr)

    def test_cli_reports_malformed_json(self):
        self.matrix_path.write_text("{", encoding="utf-8")
        exit_code, stdout, stderr = self._run_cli("validate")
        self.assertEqual(1, exit_code)
        self.assertEqual("", stdout)
        self.assertIn("Iceberg test matrix error", stderr)

    def test_cli_validates_matrix_without_query_arguments(self):
        self.assertEqual((0, "", ""), self._run_cli("validate"))

    def test_cli_validate_fails_for_missing_pom_derived_mapping(self):
        self.document["iceberg_versions"][1]["spark_versions"].pop(0)
        self.matrix_path.write_text(json.dumps(self.document), encoding="utf-8")
        exit_code, stdout, stderr = self._run_cli("validate")
        self.assertEqual(1, exit_code)
        self.assertEqual("", stdout)
        self.assertIn("missing 3.5.5", stderr)

    def test_cli_validate_fails_for_malformed_pom(self):
        pom_path = self.matrix_path.parent / "malformed-pom.xml"
        pom_path.write_text("<project>", encoding="utf-8")
        exit_code, stdout, stderr = self._run_cli("validate", "--pom", str(pom_path))
        self.assertEqual(1, exit_code)
        self.assertEqual("", stdout)
        self.assertIn("Iceberg test matrix error", stderr)

    def test_cli_requested_version_failures_return_nonzero(self):
        cases = (
            ("3.5.6", "9.9.9", "not present in the test matrix"),
            ("3.5.6", "1.6.1", "Iceberg 1.6.x is not packaged"),
            ("3.5.5", "1.10.1", "minimum-baseline"),
        )
        for spark_version, iceberg_version, diagnostic in cases:
            with self.subTest(spark=spark_version, iceberg=iceberg_version):
                exit_code, stdout, stderr = self._run_cli(
                    "validate-requested", "--spark-version", spark_version,
                    "--requested-versions", iceberg_version)
                self.assertEqual(1, exit_code)
                self.assertEqual("", stdout)
                self.assertIn(diagnostic, stderr)

    def test_cli_lists_versions_in_matrix_order(self):
        self.assertEqual(
            (0, "1.9.2 1.10.1\n", ""),
            self._run_cli("list", "--spark-version", "3.5.6"))

    def test_cli_requires_command_and_its_arguments(self):
        cases = (
            ([], "command"),
            (["list"], "--spark-version"),
            (["validate-requested", "--requested-versions", "1.10.1"], "--spark-version"),
            (["validate-requested", "--spark-version", "3.5.6"], "--requested-versions"),
        )
        for arguments, missing in cases:
            with self.subTest(arguments=arguments):
                stderr = io.StringIO()
                with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit) as error:
                    MATRIX_READER.parse_args(arguments)
                self.assertEqual(2, error.exception.code)
                self.assertIn("required", stderr.getvalue())
                self.assertIn(missing, stderr.getvalue())

    def test_cli_rejects_arguments_from_other_commands(self):
        cases = (
            ["validate", "--spark-version", "3.5.6"],
            ["validate", "--requested-versions", "1.10.1"],
            ["list", "--spark-version", "3.5.6", "--requested-versions", "1.10.1"],
        )
        for arguments in cases:
            with self.subTest(arguments=arguments):
                stderr = io.StringIO()
                with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit) as error:
                    MATRIX_READER.parse_args(arguments)
                self.assertEqual(2, error.exception.code)
                self.assertIn("unrecognized arguments", stderr.getvalue())

    def test_json_decoder_constructs_nested_dataclasses(self):
        self.assertIsInstance(self.matrix, MATRIX_READER.IcebergVersionMatrix)
        iceberg_support = self.matrix.iceberg_supports[0]
        self.assertIsInstance(iceberg_support, MATRIX_READER.IcebergSupport)
        spark_support = iceberg_support.spark_supports[0]
        self.assertIsInstance(spark_support, MATRIX_READER.SparkSupport)
        self.assertIsNone(spark_support.reason)
        self.assertEqual({"3.5": "3.5.1"}, iceberg_support.spark_minor_to_patch)

    def test_infers_spark357_runtime_combinations_from_module_poms(self):
        packaging = MATRIX_READER.MavenPackaging.load(self.pom_path)
        self.assertEqual({"1.9.2", "1.10.1"}, packaging.iceberg_versions_by_spark["3.5.7"])
        self.assertEqual(["1.9.2", "1.10.1"], self.matrix.supported_iceberg_versions("3.5.7"))
        # Maven packaging still does not admit patches below the test baselines.
        self.assertTrue(packaging.supports("3.5.4", "1.9.2"))
        self.assertEqual([], self.matrix.supported_iceberg_versions("3.5.4"))
        self.assertEqual(set(), packaging.iceberg_versions_by_spark["4.2.0"])

    def test_rejects_support_flags_that_disagree_with_maven_packaging(self):
        for iceberg_version, supported in (("1.9.2", False), ("1.6.1", True)):
            with self.subTest(iceberg=iceberg_version, supported=supported):
                document = copy.deepcopy(self.document)
                iceberg_support = next(entry for entry in document["iceberg_versions"]
                                       if entry["version"] == iceberg_version)
                spark_support = next(entry for entry in iceberg_support["spark_versions"]
                                     if entry["version"] == "3.5.7")
                spark_support["supported"] = supported
                if supported:
                    del spark_support["reason"]
                else:
                    spark_support["reason"] = "Incorrect fixture flag"
                with self.assertRaisesRegex(MATRIX_READER.MatrixError, "Maven packaging"):
                    self._load_document(document)

    def test_module_removal_fails_even_when_spark_shims_are_unchanged(self):
        tree = ET.parse(self.pom_path)
        namespace = MATRIX_READER.POM_NAMESPACE
        profile = tree.getroot().find("pom:profiles/pom:profile[pom:id='release357']", namespace)
        modules = profile.find("pom:modules", namespace)
        modules.remove(next(module for module in modules
                            if module.text == "iceberg/iceberg-1-10-x"))
        tree.write(self.pom_path, encoding="utf-8", xml_declaration=True)
        exit_code, stdout, stderr = self._run_cli("validate")
        self.assertEqual(1, exit_code)
        self.assertEqual("", stdout)
        self.assertIn("Iceberg 1.10.1, Spark 3.5.7", stderr)
        self.assertIn("Maven packaging (expected False)", stderr)

    def test_rejects_packaged_runtime_missing_from_matrix(self):
        self.document["iceberg_versions"].pop(1)
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "missing from the matrix: 1.9.2"):
            self._load_document(self.document)

    def test_module_runtime_property_pin_is_used_instead_of_module_name(self):
        tree = ET.parse(self.pom_path)
        property_node = tree.getroot().find("pom:properties/pom:iceberg.19x.version",
                                            MATRIX_READER.POM_NAMESPACE)
        property_node.text = "1.9.3"
        tree.write(self.pom_path, encoding="utf-8", xml_declaration=True)
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "missing from the matrix: 1.9.3"):
            self._load_document(self.document)

    def test_module_properties_override_parent_runtime_pin(self):
        module_pom = self.pom_path.parent / "iceberg" / "iceberg-1-9-x" / "pom.xml"
        tree = ET.parse(module_pom)
        ns = "{http://maven.apache.org/POM/4.0.0}"
        properties = ET.SubElement(tree.getroot(), ns + "properties")
        ET.SubElement(properties, ns + "iceberg.19x.version").text = "1.9.3"
        tree.write(module_pom, encoding="utf-8", xml_declaration=True)
        self.document["iceberg_versions"][1]["version"] = "1.9.3"
        matrix = self._load_document(self.document)
        self.assertEqual(["1.9.3", "1.10.1"], matrix.supported_iceberg_versions("3.5.7"))

    def test_rejects_unresolved_or_cyclic_runtime_pin_properties(self):
        for value in ("${missing.version}", "${iceberg.19x.version}"):
            with self.subTest(value=value):
                tree = ET.parse(self.pom_path)
                property_node = tree.getroot().find("pom:properties/pom:iceberg.19x.version",
                                                    MATRIX_READER.POM_NAMESPACE)
                property_node.text = value
                tree.write(self.pom_path, encoding="utf-8", xml_declaration=True)
                with self.assertRaisesRegex(MATRIX_READER.MatrixError, "unresolved or cyclic"):
                    self._load_document(self.document)

    def test_json_decoder_rejects_unknown_record_fields(self):
        for record in ("iceberg", "spark"):
            with self.subTest(record=record):
                document = copy.deepcopy(self.document)
                entry = document["iceberg_versions"][0]
                if record == "spark":
                    entry = entry["spark_versions"][0]
                entry["unexpected"] = True
                with self.assertRaises(MATRIX_READER.MatrixError):
                    self._load_document(document)

    def test_rejects_release_profile_without_declared_spark_shim(self):
        tree = ET.parse(self.pom_path)
        namespace = MATRIX_READER.POM_NAMESPACE
        version = tree.getroot().find(
            "pom:profiles/pom:profile[pom:id='release357']/pom:properties/pom:spark.version",
            namespace)
        version.text = "3.5.9"
        tree.write(self.pom_path, encoding="utf-8", xml_declaration=True)
        with self.assertRaisesRegex(MATRIX_READER.MatrixError, "3.5.9 has no declared shim property"):
            self._load_document(self.document)


if __name__ == "__main__":
    unittest.main()
