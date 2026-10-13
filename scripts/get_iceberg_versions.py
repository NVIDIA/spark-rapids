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
from dataclasses import MISSING, dataclass, field, fields
from pathlib import Path
from typing import Dict, List, Optional, Set

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MATRIX = REPO_ROOT / "scripts" / "iceberg-versions.json"
DEFAULT_POM = REPO_ROOT / "scala2.13" / "pom.xml"
VERSION_PATTERN = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
SPARK_PROPERTY_PATTERN = re.compile(r"^spark[0-9]+\.version$")
PROPERTY_REFERENCE_PATTERN = re.compile(r"^\$\{([^}]+)\}$")
POM_NAMESPACE = {"pom": "http://maven.apache.org/POM/4.0.0"}


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


def _pom_properties(element):
    properties = element.find("pom:properties", POM_NAMESPACE)
    if properties is None:
        return {}
    return {child.tag.rsplit("}", 1)[-1]: (child.text or "").strip()
            for child in properties}


def _resolve_property(value, properties):
    seen = set()
    while True:
        match = PROPERTY_REFERENCE_PATTERN.fullmatch(value)
        if match is None:
            return value
        name = match.group(1)
        if name in seen or name not in properties:
            raise MatrixError(f"unresolved or cyclic Maven property: {name}")
        seen.add(name)
        value = properties[name]


def _spark_shims(properties):
    return {value for name, value in properties.items()
            if SPARK_PROPERTY_PATTERN.fullmatch(name) and VERSION_PATTERN.fullmatch(value)}


@dataclass(frozen=True)
class MavenPackaging:
    """Spark × Iceberg runtime combinations inferred from Maven release profiles.

    Attributes:
        spark_versions: Release Spark shims declared by the POM's properties,
            including versions that have no packaged Iceberg integration.
        iceberg_versions_by_spark: Exact Iceberg runtime versions packaged for
            each release Spark profile, read from its included module POMs.
            Stub modules contribute no runtime version.
    """

    spark_versions: Set[str]
    iceberg_versions_by_spark: Dict[str, Set[str]]

    @classmethod
    def load(cls, pom_path):
        """Read release profiles and the runtime dependencies of their Iceberg modules.

        Args:
            pom_path: Parent POM, normally ``scala2.13/pom.xml``. Included
                ``iceberg/`` module POMs are resolved relative to this file.

        Returns:
            Packaging records after resolving Spark and Iceberg version property
            references. Parent properties are overridden by profile properties
            and then by the included module's properties. Non-release Spark
            profiles, including snapshots and Databricks versions, are ignored.

        Raises:
            MatrixError: Parent properties are absent, version properties cannot
                be resolved, a release profile has no declared Spark shim, or an
                included runtime version is malformed.
            OSError, ET.ParseError: An input POM cannot be read or parsed.
        """
        pom_path = Path(pom_path)
        root = ET.parse(pom_path).getroot()
        if root.find("pom:properties", POM_NAMESPACE) is None:
            raise MatrixError(f"properties not found in {pom_path}")
        properties = _pom_properties(root)
        spark_versions = _spark_shims(properties)
        packaged = {}
        module_roots = {}
        for profile in root.findall("pom:profiles/pom:profile", POM_NAMESPACE):
            profile_values = _pom_properties(profile)
            profile_properties = {**properties, **profile_values}
            version = profile_values.get("spark.version")
            if version is None:
                continue
            spark_version = _resolve_property(version, profile_properties)
            if not VERSION_PATTERN.fullmatch(spark_version):
                continue
            if spark_version not in spark_versions:
                raise MatrixError(f"profile Spark version {spark_version} has no declared shim property")
            versions = packaged.setdefault(spark_version, set())
            for module in profile.findall("pom:modules/pom:module", POM_NAMESPACE):
                module_name = (module.text or "").strip()
                if not module_name.startswith("iceberg/"):
                    continue
                module_path = pom_path.parent / module_name / "pom.xml"
                if module_path not in module_roots:
                    module_roots[module_path] = ET.parse(module_path).getroot()
                module_root = module_roots[module_path]
                module_properties = {**profile_properties, **_pom_properties(module_root)}
                for dependency in module_root.findall(
                        "pom:dependencies/pom:dependency", POM_NAMESPACE):
                    group = dependency.findtext("pom:groupId", "", POM_NAMESPACE).strip()
                    artifact = dependency.findtext("pom:artifactId", "", POM_NAMESPACE).strip()
                    if group == "org.apache.iceberg" and artifact.startswith("iceberg-spark-runtime-"):
                        runtime = dependency.findtext("pom:version", "", POM_NAMESPACE).strip()
                        runtime = _resolve_property(runtime, module_properties)
                        _version_tuple(runtime)
                        versions.add(runtime)
        return cls(spark_versions, packaged)

    def supports(self, spark_version, iceberg_version):
        """Return whether a release profile packages the exact runtime combination.

        Args:
            spark_version: Exact release Spark version string.
            iceberg_version: Exact Iceberg runtime version string.

        Returns:
            A boolean; missing profiles and stub-only profiles return false.
            Minimum test baselines are applied separately by the matrix validator.
        """
        return iceberg_version in self.iceberg_versions_by_spark.get(spark_version, set())


@dataclass(frozen=True)
class SparkSupport:
    """One Spark release's test eligibility for a particular Iceberg release.

    Construction validates the record, including through the JSON decoder hook.
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

    def __post_init__(self):
        """Reject invalid versions, support flags, and exclusion reasons."""
        _version_tuple(self.version)
        if type(self.supported) is not bool:
            raise MatrixError(f"supported must be boolean for Spark {self.version}")
        if not self.supported and (not isinstance(self.reason, str) or not self.reason.strip()):
            raise MatrixError(f"unsupported Spark {self.version} needs a reason")
        if self.supported and self.reason is not None:
            raise MatrixError(f"supported Spark {self.version} cannot have a reason")


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
            combinations and their reasons. ``validate`` checks that this list
            contains every POM shim at or above a declared family's baseline
            exactly once, and that each support flag agrees with Maven packaging.
    """

    version: str
    spark_minor_to_patch: Dict[str, str] = field(metadata={"json_name": "upstream_minimums"})
    spark_supports: List[SparkSupport] = field(metadata={"json_name": "spark_versions"})

    def __post_init__(self):
        """Validate the version, baselines, and decoded Spark records at construction."""
        _version_tuple(self.version)
        if not isinstance(self.spark_minor_to_patch, dict) or not self.spark_minor_to_patch:
            raise MatrixError(f"upstream_minimums for Iceberg {self.version} must be a mapping")
        if not isinstance(self.spark_supports, list) or not self.spark_supports:
            raise MatrixError(f"spark_versions for Iceberg {self.version} must be a list")
        for family, minimum in self.spark_minor_to_patch.items():
            _version_tuple(minimum)
            if not isinstance(family, str) or _spark_family(minimum) != family:
                raise MatrixError(
                    f"minimum {minimum!r} does not belong to Spark family {family!r}")
        versions = set()
        for support in self.spark_supports:
            if not isinstance(support, SparkSupport):
                raise MatrixError(
                    f"Spark entries for Iceberg {self.version} require version and supported")
            if support.version in versions:
                raise MatrixError(f"duplicate Spark version {support.version} for Iceberg {self.version}")
            versions.add(support.version)

    def validate(self, packaging):
        """Compare eligible Spark records and support flags with inferred Maven combinations.

        Args:
            packaging: ``MavenPackaging`` loaded from the parent and module POMs.

        Returns:
            None after the expected Spark entries and every support flag agree
            with the baseline policy and Maven packaging.

        Raises:
            MatrixError: The Spark entry list differs from the eligible POM shims,
                or a support flag disagrees with the included modules' runtime pins.

        Packaged combinations below the minimum test baselines remain excluded
        under issue #15875; all combinations at or above a baseline are checked.
        """
        # Issue #15875 uses upstream build/test pins as minimum test baselines.
        # Maven packaging alone does not qualify a below-baseline Spark patch.
        expected_versions = set()
        for family, minimum in self.spark_minor_to_patch.items():
            minimum_tuple = _version_tuple(minimum)
            expected_versions.update(
                spark_version for spark_version in packaging.spark_versions
                if _spark_family(spark_version) == family and
                _version_tuple(spark_version) >= minimum_tuple)

        actual_versions = {support.version for support in self.spark_supports}

        missing = expected_versions - actual_versions
        extra = actual_versions - expected_versions
        if missing or extra:
            details = []
            if missing:
                details.append(f"missing {', '.join(sorted(missing, key=_version_tuple))}")
            if extra:
                details.append(f"unexpected {', '.join(sorted(extra, key=_version_tuple))}")
            raise MatrixError(f"Iceberg {self.version} Spark entries: {'; '.join(details)}")
        for support in self.spark_supports:
            inferred = packaging.supports(support.version, self.version)
            if support.supported != inferred:
                raise MatrixError(
                    f"Iceberg {self.version}, Spark {support.version}: supported={support.supported} "
                    f"disagrees with Maven packaging (expected {inferred})")

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

    Use ``load`` to decode the JSON records and validate their Maven combinations.

    Attributes:
        iceberg_supports: Iceberg records in JSON order. ``load`` requires a
            nonempty list of unique Iceberg versions; automatic selection
            preserves this order.
    """

    iceberg_supports: List[IcebergSupport] = field(metadata={"json_name": "iceberg_versions"})

    def __post_init__(self):
        """Require decoded Iceberg records with unique runtime versions."""
        if not isinstance(self.iceberg_supports, list) or not self.iceberg_supports:
            raise MatrixError("iceberg_versions must be a non-empty list")
        if any(not isinstance(support, IcebergSupport) for support in self.iceberg_supports):
            raise MatrixError(
                "each Iceberg entry requires version, upstream_minimums, and spark_versions")
        versions = [support.version for support in self.iceberg_supports]
        if len(set(versions)) != len(versions):
            duplicate = next(version for index, version in enumerate(versions)
                             if version in versions[:index])
            raise MatrixError(f"duplicate Iceberg version: {duplicate}")

    @classmethod
    def load(cls, matrix_path=DEFAULT_MATRIX, pom_path=DEFAULT_POM):
        """Decode JSON dataclasses, validate the inferred Maven combinations, and return them.

        Args:
            matrix_path: JSON file with a single ``iceberg_versions`` list.
                Defaults to the repository's ``scripts/iceberg-versions.json``.
            pom_path: Maven POM supplying the Spark shim properties and profiles.
                Defaults to the repository's ``scala2.13/pom.xml``. Its included Iceberg
                module POMs supply the packaged runtime versions.

        Raises:
            MatrixError: The schema, an entry, or its inferred Maven support is
                invalid, or a packaged Iceberg version is missing from the matrix.
            OSError: The JSON, parent POM, or an included module POM cannot be read.
            json.JSONDecodeError: The matrix is not valid JSON.
            ET.ParseError: The POM is not well-formed XML.

        The standard-library JSON ``object_hook`` constructs the dataclasses using
        their field metadata and constructor validation. Maven comparison covers
        the entire matrix before any individual version query.
        """
        with open(matrix_path, encoding="utf-8") as stream:
            matrix = json.load(stream, object_hook=_decode_matrix_record)
        if not isinstance(matrix, cls):
            raise MatrixError("matrix must contain only an iceberg_versions list")
        packaging = MavenPackaging.load(pom_path)
        versions = {support.version for support in matrix.iceberg_supports}
        packaged_versions = set().union(*packaging.iceberg_versions_by_spark.values())
        missing = packaged_versions - versions
        if missing:
            raise MatrixError(
                f"packaged Iceberg versions missing from the matrix: {', '.join(sorted(missing))}")
        for support in matrix.iceberg_supports:
            support.validate(packaging)
        return matrix

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


def _decode_matrix_record(record):
    """JSON object hook constructing records from dataclass fields and JSON aliases.

    The decoder visits nested objects first. Required fields and optional defaults
    come from ``dataclasses.fields``; no per-class dictionary resolver is needed.
    Other dictionaries, such as Spark baseline mappings, remain dictionaries.
    """
    for record_type in (SparkSupport, IcebergSupport, IcebergVersionMatrix):
        record_fields = fields(record_type)
        aliases = {item.metadata.get("json_name", item.name): item.name for item in record_fields}
        required = {item.metadata.get("json_name", item.name) for item in record_fields
                    if item.default is MISSING and item.default_factory is MISSING}
        if required <= record.keys() <= aliases.keys():
            return record_type(**{aliases[name]: value for name, value in record.items()})
    return record


def _print_versions(spark_version, versions):
    """Print shell-readable versions and explain an empty selection on stderr."""
    if not versions:
        # A valid matrix can intentionally exclude even a packaged combination.
        # Missing entries for shims at or above a baseline fail during load().
        print(
            f"No Iceberg versions selected for Spark {spark_version}: "
            "the minimum-baseline and packaging test policy excludes all entries "
            "(https://github.com/NVIDIA/cudf-spark/issues/15875).",
            file=sys.stderr)
    print(" ".join(versions))


def command_validate(args):
    """Check the entire matrix against the POM without selecting test versions.

    Args:
        args: Parsed ``validate`` arguments. ``matrix`` is the JSON input path
            and ``pom`` is the Maven POM declaring Spark release profiles.
            Included Iceberg module POMs are read relative to the parent POM.

    Returns:
        None after both inputs have been read and all matrix entries validated.
        Success produces no output; ``main`` translates it to exit status 0.

    Raises:
        MatrixError: The schema, version entries, or inferred Maven combinations
            are invalid. Validation includes duplicates and unsupported-entry reasons.
        OSError: Either input cannot be read.
        json.JSONDecodeError: The matrix is not valid JSON.
        ET.ParseError: The POM is not well-formed XML.

    These failures reach ``main``, which reports them and returns exit status 1.
    """
    IcebergVersionMatrix.load(args.matrix, args.pom)


def command_list(args):
    """Validate the inputs and print matrix-selected Iceberg versions for Spark.

    Args:
        args: Parsed ``list`` arguments containing the ``matrix`` JSON path,
            the ``pom`` path, and the exact ``spark_version`` to query.

    Returns:
        None after printing space-separated versions in matrix order and a
        newline. An empty selection prints a newline and a policy diagnostic
        on stderr; it is successful and lets CI skip that Spark combination.

    Raises:
        MatrixError: Matrix validation fails or the Spark query is malformed,
            including a snapshot in a family covered by the matrix.
        OSError, json.JSONDecodeError, ET.ParseError: An input cannot be read
            or parsed. ``main`` reports these failures and returns status 1.
    """
    matrix = IcebergVersionMatrix.load(args.matrix, args.pom)
    versions = matrix.supported_iceberg_versions(args.spark_version)
    _print_versions(args.spark_version, versions)


def command_validate_requested(args):
    """Validate caller-selected versions for Spark and echo them in caller order.

    Args:
        args: Parsed ``validate-requested`` arguments containing ``matrix`` and
            ``pom`` paths, an exact Spark release in ``spark_version``, and a
            comma- or whitespace-separated ``requested_versions`` string.

    Returns:
        None after printing the requested versions separated by spaces and a
        newline. Input order and duplicates are retained. An empty version list
        prints a newline and the empty-selection diagnostic on stderr.

    Raises:
        MatrixError: The matrix or Spark release is invalid, or a requested
            Iceberg version is unknown, below the test baseline, or unsupported.
        OSError, json.JSONDecodeError, ET.ParseError: An input cannot be read
            or parsed. ``main`` reports these failures and returns status 1.

    The entire matrix is validated before the requested combinations are checked.
    """
    matrix = IcebergVersionMatrix.load(args.matrix, args.pom)
    requested = [
        version for version in re.split(r"[\s,]+", args.requested_versions) if version]
    versions = matrix.validate_requested_versions(args.spark_version, requested)
    _print_versions(args.spark_version, versions)


def parse_args(arguments=None):
    """Parse command-specific CLI options into an ``argparse.Namespace``.

    ``arguments`` is a sequence of argument strings; None reads ``sys.argv[1:]``.
    Each required subcommand owns its input-file options and accepts only the
    query arguments relevant to that operation. The returned namespace includes
    the command's ``handler`` function. Invalid usage raises SystemExit with
    status 2; requesting help raises SystemExit with status 0.
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

    validate_parser = subparsers.add_parser(
        "validate", parents=[inputs], help="validate the entire matrix against the POM")
    validate_parser.set_defaults(handler=command_validate)

    list_parser = subparsers.add_parser(
        "list", parents=[inputs], help="list matrix-selected Iceberg versions for Spark")
    list_parser.add_argument(
        "--spark-version", required=True, help="exact Spark version to query")
    list_parser.set_defaults(handler=command_list)

    requested_parser = subparsers.add_parser(
        "validate-requested", parents=[inputs],
        help="validate and echo caller-selected Iceberg versions for Spark")
    requested_parser.add_argument(
        "--spark-version", required=True, help="exact Spark release to query")
    requested_parser.add_argument(
        "--requested-versions", required=True,
        help="comma- or whitespace-separated Iceberg versions to validate and echo")
    requested_parser.set_defaults(handler=command_validate_requested)
    return parser.parse_args(arguments)


def main(arguments=None):
    """Dispatch to the selected command and translate failures to exit status 1.

    ``arguments`` follows ``parse_args``. A handler that completes successfully
    yields status 0; command handlers document their output and validation rules.
    File, parse, and validation failures print a diagnostic on stderr. Argument
    parsing retains argparse's SystemExit behavior for usage errors and help.
    """
    args = parse_args(arguments)
    try:
        args.handler(args)
        return 0
    except (MatrixError, ET.ParseError, OSError, json.JSONDecodeError) as error:
        print(f"Iceberg test matrix error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
