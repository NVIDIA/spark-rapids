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

"""Read and validate the Iceberg integration-test matrix defined by issue #15875.

Iceberg's Spark dependency pins are minimum test baselines by cudf-spark policy,
not upstream compatibility guarantees. Packaged combinations below those baselines
are intentionally excluded: https://github.com/NVIDIA/cudf-spark/issues/15875.
"""

import argparse
import json
import re
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MATRIX = REPO_ROOT / "scripts" / "iceberg-versions.json"
DEFAULT_POM = REPO_ROOT / "scala2.13" / "pom.xml"
VERSION_PATTERN = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
SPARK_PROPERTY_PATTERN = re.compile(r"^spark[0-9]+\.version$")


class MatrixError(ValueError):
    """Invalid matrix metadata or a query that violates the test-selection policy.

    The message identifies the offending version, field, or missing mapping.
    File access, JSON decoding, and XML parsing errors retain their native types;
    the CLI reports all of these failures on stderr and exits with status 1.
    """


def _version_tuple(version):
    if not isinstance(version, str) or not VERSION_PATTERN.fullmatch(version):
        raise MatrixError(f"invalid version: {version!r}")
    return tuple(int(part) for part in version.split("."))


def _spark_family(version):
    return ".".join(version.split(".")[:2])


def read_spark_shims(pom_path):
    """Return the set of release Spark versions declared in a Maven POM.

    Args:
        pom_path: Path to a Maven XML file whose top-level properties contain
            ``spark<digits>.version`` entries, such as ``spark356.version``.

    Returns:
        Unique ``major.minor.patch`` version strings. Snapshot values, properties
        with other names, and values without three numeric components are ignored.

    Raises:
        MatrixError: The POM has no top-level properties element.
        OSError: The POM cannot be read.
        ET.ParseError: The POM is not well-formed XML.
    """
    root = ET.parse(pom_path).getroot()
    namespace = {"pom": "http://maven.apache.org/POM/4.0.0"}
    properties = root.find("pom:properties", namespace)
    if properties is None:
        raise MatrixError(f"properties not found in {pom_path}")

    versions = set()
    for child in properties:
        name = child.tag.rsplit("}", 1)[-1]
        value = (child.text or "").strip()
        if SPARK_PROPERTY_PATTERN.fullmatch(name) and VERSION_PATTERN.fullmatch(value):
            versions.add(value)
    return versions


@dataclass(frozen=True)
class SparkSupport:
    """One Spark release's test eligibility for a particular Iceberg release.

    Use ``from_dict`` to validate serialized entries before constructing a record.
    This records cudf-spark test policy, not an upstream compatibility guarantee.

    Attributes:
        version: Exact Spark release string in ``major.minor.patch`` form.
        supported: Whether the matrix enables this combination. For entries at
            or above the test baseline, this reflects the packaged integration.
        reason: Non-blank explanation required when ``supported`` is false;
            otherwise None. It is included in rejected-request diagnostics.
    """

    version: str
    supported: bool
    reason: Optional[str] = None

    @classmethod
    def from_dict(cls, entry, iceberg_version):
        """Validate a Spark JSON entry and return its ``SparkSupport`` record.

        Args:
            entry: Mapping with ``version`` and boolean ``supported`` fields,
                and an optional ``reason`` field. No other keys are accepted.
            iceberg_version: Parent Iceberg version, used to identify errors.

        Raises:
            MatrixError: The entry has invalid keys, a malformed version, a
                non-boolean support flag, or a reason inconsistent with that flag.
        """
        if not isinstance(entry, dict):
            raise MatrixError(f"invalid Spark entry for Iceberg {iceberg_version}")
        required_keys = {"version", "supported"}
        if not required_keys.issubset(entry) or not set(entry).issubset(
                required_keys | {"reason"}):
            raise MatrixError(
                f"Spark entries for Iceberg {iceberg_version} require version and supported")

        version = entry["version"]
        _version_tuple(version)
        supported = entry["supported"]
        if type(supported) is not bool:
            raise MatrixError(
                f"supported must be boolean for Iceberg {iceberg_version}, Spark {version}")
        reason = entry.get("reason")
        if not supported and (not isinstance(reason, str) or not reason.strip()):
            raise MatrixError(
                f"unsupported Iceberg {iceberg_version}, Spark {version} needs a reason")
        if supported and reason is not None:
            raise MatrixError(
                f"supported Iceberg {iceberg_version}, Spark {version} cannot have a reason")
        return cls(version, supported, reason)


@dataclass(frozen=True)
class IcebergSupport:
    """Test baselines and Spark eligibility records for one Iceberg runtime.

    Attributes:
        version: Exact Iceberg runtime release in ``major.minor.patch`` form.
        spark_minor_to_patch: Mapping from a Spark family to its full minimum
            test version, for example ``{"3.5": "3.5.6"}``. Serialized as
            ``upstream_minimums``, these are Iceberg dependency pins interpreted
            as minimum test baselines by cudf-spark policy.
        spark_supports: Spark records in JSON order, including unsupported
            combinations and their reasons. When loaded through ``from_dict``,
            this list contains every POM shim at or above a declared family's
            baseline, exactly once, and no other versions.
    """

    version: str
    spark_minor_to_patch: Dict[str, str]
    spark_supports: List[SparkSupport]

    @classmethod
    def from_dict(cls, entry, spark_shims):
        """Validate an Iceberg JSON entry against the POM shims and return it.

        Args:
            entry: Mapping containing exactly ``version``, ``upstream_minimums``,
                and ``spark_versions``. Baselines and Spark entries must be nonempty.
            spark_shims: Collection of release Spark version strings, normally the
                set returned by ``read_spark_shims``.

        Raises:
            MatrixError: Metadata is malformed, a baseline belongs to the wrong
                family, a Spark entry is invalid or duplicated, or the entry list
                differs from the shims selected by the minimum baselines.

        Packaging flags are read from the JSON; this method does not infer them
        from Maven release profiles.
        """
        if not isinstance(entry, dict) or set(entry) != {
                "version", "upstream_minimums", "spark_versions"}:
            raise MatrixError(
                "each Iceberg entry requires version, upstream_minimums, and spark_versions")

        version = entry["version"]
        _version_tuple(version)
        spark_minor_to_patch = entry["upstream_minimums"]
        spark_entries = entry["spark_versions"]
        if not isinstance(spark_minor_to_patch, dict) or not spark_minor_to_patch:
            raise MatrixError(f"upstream_minimums for Iceberg {version} must be a mapping")
        if not isinstance(spark_entries, list) or not spark_entries:
            raise MatrixError(f"spark_versions for Iceberg {version} must be a list")

        # Issue #15875 uses upstream build/test pins as minimum test baselines.
        # Maven packaging alone does not qualify a below-baseline Spark patch.
        expected_versions = set()
        for family, minimum in spark_minor_to_patch.items():
            minimum_tuple = _version_tuple(minimum)
            if not isinstance(family, str) or _spark_family(minimum) != family:
                raise MatrixError(
                    f"minimum {minimum!r} does not belong to Spark family {family!r}")
            expected_versions.update(
                spark_version for spark_version in spark_shims
                if _spark_family(spark_version) == family and
                _version_tuple(spark_version) >= minimum_tuple)

        spark_supports = [SparkSupport.from_dict(item, version) for item in spark_entries]
        actual_versions = set()
        for spark_support in spark_supports:
            if spark_support.version in actual_versions:
                raise MatrixError(
                    f"duplicate Spark version {spark_support.version} for Iceberg {version}")
            actual_versions.add(spark_support.version)

        missing = expected_versions - actual_versions
        extra = actual_versions - expected_versions
        if missing or extra:
            details = []
            if missing:
                details.append(f"missing {', '.join(sorted(missing, key=_version_tuple))}")
            if extra:
                details.append(f"unexpected {', '.join(sorted(extra, key=_version_tuple))}")
            raise MatrixError(f"Iceberg {version} Spark entries: {'; '.join(details)}")
        return cls(version, spark_minor_to_patch, spark_supports)

    def support_for(self, spark_version):
        """Return the record for an exact Spark version string, or None if absent.

        Unsupported records are returned too, so callers can inspect their reason.
        The input is neither normalized nor validated by this lookup.
        """
        return next(
            (support for support in self.spark_supports if support.version == spark_version), None)

    def supports(self, spark_version):
        """Return whether an exact Spark version has an enabled matrix entry.

        Both a missing record and a record marked unsupported return false.
        As with ``support_for``, this lookup does not validate the input string.
        """
        spark_support = self.support_for(spark_version)
        return spark_support is not None and spark_support.supported


@dataclass(frozen=True)
class IcebergVersionMatrix:
    """Collection of Iceberg releases used to select CI test combinations.

    Use ``load`` to validate both the JSON schema and its coverage of POM shims.

    Attributes:
        iceberg_supports: Iceberg records in JSON order. ``load`` requires a
            nonempty list of unique Iceberg versions; automatic selection
            preserves this order.
    """

    iceberg_supports: List[IcebergSupport]

    @classmethod
    def load(cls, matrix_path=DEFAULT_MATRIX, pom_path=DEFAULT_POM):
        """Read the matrix and POM, validate all entries, and return the matrix.

        Args:
            matrix_path: JSON file with a single ``iceberg_versions`` list.
                Defaults to the repository's ``scripts/iceberg-versions.json``.
            pom_path: Maven POM supplying the Spark shim properties. Defaults
                to the repository's ``scala2.13/pom.xml``.

        Raises:
            MatrixError: The schema, a version entry, shim coverage, or Iceberg
                version uniqueness is invalid.
            OSError: Either input file cannot be read.
            json.JSONDecodeError: The matrix is not valid JSON.
            ET.ParseError: The POM is not well-formed XML.

        Validation covers the entire matrix before any individual version query.
        """
        with open(matrix_path, encoding="utf-8") as stream:
            document = json.load(stream)
        if not isinstance(document, dict) or set(document) != {"iceberg_versions"}:
            raise MatrixError("matrix must contain only an iceberg_versions list")
        entries = document["iceberg_versions"]
        if not isinstance(entries, list) or not entries:
            raise MatrixError("iceberg_versions must be a non-empty list")

        spark_shims = read_spark_shims(pom_path)
        iceberg_supports = [IcebergSupport.from_dict(entry, spark_shims) for entry in entries]
        versions = [support.version for support in iceberg_supports]
        if len(set(versions)) != len(versions):
            duplicate = next(version for index, version in enumerate(versions)
                             if version in versions[:index])
            raise MatrixError(f"duplicate Iceberg version: {duplicate}")
        return cls(iceberg_supports)

    def supported_iceberg_versions(self, spark_version):
        """Return enabled Iceberg versions for Spark, preserving matrix order.

        Args:
            spark_version: Exact ``major.minor.patch`` Spark release, or a
                well-formed ``major.minor.patch-SNAPSHOT`` in a family absent
                from every Iceberg baseline mapping.

        Returns:
            A list of Iceberg version strings. No enabled match yields an empty
            list, including the accepted snapshot case, so CI can skip the run.

        Raises:
            MatrixError: The Spark version is malformed or is a snapshot in a
                family covered by the matrix. Snapshots never select release entries.
        """
        # Spark master can use a snapshot in a family with no Iceberg integration.
        # Preserve the callers' skip for that family without treating snapshots
        # in covered families as their corresponding releases or silently skipping them.
        if isinstance(spark_version, str) and spark_version.endswith("-SNAPSHOT"):
            _version_tuple(spark_version[:-len("-SNAPSHOT")])
            if not any(_spark_family(spark_version) in support.spark_minor_to_patch
                       for support in self.iceberg_supports):
                return []
        _version_tuple(spark_version)
        return [
            iceberg_support.version
            for iceberg_support in self.iceberg_supports
            if iceberg_support.supports(spark_version)
        ]

    def validate_requested_versions(self, spark_version, requested_versions):
        """Validate caller-selected Iceberg versions and return the original list.

        Args:
            spark_version: Exact Spark release in ``major.minor.patch`` form.
                Explicit requests do not accept Spark snapshots.
            requested_versions: List of exact Iceberg version strings. Each must
                have an enabled entry for Spark; caller order and duplicates are
                preserved, and an empty list remains empty.

        Raises:
            MatrixError: Spark is malformed, an Iceberg version is not in the
                matrix, or a combination is absent or marked unsupported. The
                latter error includes the entry's exclusion reason.
        """
        _version_tuple(spark_version)
        by_version = {support.version: support for support in self.iceberg_supports}
        for iceberg_version in requested_versions:
            if iceberg_version not in by_version:
                raise MatrixError(
                    f"Iceberg version {iceberg_version} is not present in the test matrix")
            spark_support = by_version[iceberg_version].support_for(spark_version)
            if spark_support is None:
                raise MatrixError(
                    f"Iceberg {iceberg_version} with Spark {spark_version} is excluded "
                    "by the minimum-baseline test policy (issue #15875)")
            if not spark_support.supported:
                raise MatrixError(
                    f"Iceberg {iceberg_version} is not supported with Spark {spark_version}: "
                    f"{spark_support.reason}")
        return requested_versions


def parse_args(arguments=None):
    """Parse command-specific CLI options into an ``argparse.Namespace``.

    ``arguments`` is a sequence of argument strings; None reads ``sys.argv[1:]``.
    Each required subcommand owns its input-file options and accepts only the
    query arguments relevant to that operation. Invalid usage raises SystemExit
    with status 2; requesting help raises SystemExit with status 0.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    inputs = argparse.ArgumentParser(add_help=False)
    inputs.add_argument(
        "--matrix", type=Path, default=DEFAULT_MATRIX,
        help="matrix JSON file (default: scripts/iceberg-versions.json)")
    inputs.add_argument(
        "--pom", type=Path, default=DEFAULT_POM,
        help="Spark shim POM (default: scala2.13/pom.xml)")

    subparsers.add_parser(
        "validate", parents=[inputs], help="validate the entire matrix against the POM")

    list_parser = subparsers.add_parser(
        "list", parents=[inputs], help="list matrix-selected Iceberg versions for Spark")
    list_parser.add_argument(
        "--spark-version", required=True, help="exact Spark version to query")

    requested_parser = subparsers.add_parser(
        "validate-requested", parents=[inputs],
        help="validate and echo caller-selected Iceberg versions for Spark")
    requested_parser.add_argument(
        "--spark-version", required=True, help="exact Spark release to query")
    requested_parser.add_argument(
        "--requested-versions", required=True,
        help="comma- or whitespace-separated Iceberg versions to validate and echo")
    return parser.parse_args(arguments)


def main(arguments=None):
    """Run one CLI command and return 0 on success or 1 for matrix/query errors.

    ``arguments`` follows ``parse_args``. The ``validate`` command is silent on
    success; ``list`` and ``validate-requested`` print space-separated versions
    followed by a newline. Empty selections also explain the skip on stderr.
    File, parse, and validation failures print a diagnostic on stderr. Argument
    parsing retains argparse's SystemExit behavior for usage errors and help.
    """
    args = parse_args(arguments)
    try:
        matrix = IcebergVersionMatrix.load(args.matrix, args.pom)
        if args.command == "validate":
            return 0
        if args.command == "validate-requested":
            requested = [
                version for version in re.split(r"[\s,]+", args.requested_versions) if version]
            versions = matrix.validate_requested_versions(args.spark_version, requested)
        else:
            versions = matrix.supported_iceberg_versions(args.spark_version)
        if not versions:
            # A valid matrix can intentionally exclude even a packaged combination.
            # Missing entries for shims at or above a baseline fail during load().
            print(
                f"No Iceberg versions selected for Spark {args.spark_version}: "
                "the minimum-baseline and packaging test policy excludes all entries "
                "(https://github.com/NVIDIA/cudf-spark/issues/15875).",
                file=sys.stderr)
        print(" ".join(versions))
        return 0
    except (MatrixError, ET.ParseError, OSError, json.JSONDecodeError) as error:
        print(f"Iceberg test matrix error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
