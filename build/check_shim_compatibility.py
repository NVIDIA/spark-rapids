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

"""Read-only shim preflight. Static coverage is deliberately narrower than compilation."""

import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import re
import subprocess
import sys

from get_buildvers import _get_buildvers


SHIM_TAG = "spark-rapids-shim-json-lines"
VERSIONED = re.compile(r"^(.*)/src/main/spark([^/]+)/(scala|java)/(.+)$")
PACKAGE = re.compile(r"(?m)^\s*package\s+([\w.]+)")
DECLARATION = re.compile(
    r"(?m)^(?:(?:(?:private|protected)(?:\[[\w.]+\])?"
    r"|public|sealed|abstract|final|case)\s+)*"
    r"(?:class|object|trait|interface|enum)\s+(\w+)")
IMPORT = re.compile(r"(?m)^\s*import\s+([^\n;{]+(?:\{[^}]*\}[^\n;]*)?)")


class PreflightError(Exception):
    pass


def run(command, root):
    result = subprocess.run(command, cwd=root, text=True, capture_output=True, timeout=90)
    if result.returncode:
        # Do not echo gh stderr: it may contain authentication diagnostics.
        raise PreflightError("{} failed (exit {})".format(command[0], result.returncode))
    return result.stdout


def git(root, *args):
    return run(["git", *args], root).strip()


def git_paths(root, *args):
    return set(filter(None, run(["git", *args, "-z"], root).split("\0")))


def commit_sha(root, ref):
    try:
        return git(root, "rev-parse", "--verify", ref + "^{commit}")
    except PreflightError as error:
        raise PreflightError("Cannot resolve commit {}; fetch its history and retry".format(ref)) from error


def without_comments(source):
    """Keep strings opaque and preserve lines while removing Scala/Java comments."""
    token = re.compile(r'"""[\s\S]*?"""|"(?:\\.|[^"\\])*"|/\*[\s\S]*?\*/|//[^\n]*')
    return token.sub(lambda m: "\n" * m[0].count("\n") + " "
                     if m[0].startswith(("/", '"')) else m[0], source)


def annotations(source, path, required=False):
    if SHIM_TAG not in source:
        if required:
            raise PreflightError("{}: missing shim annotation".format(path))
        return None
    blocks = re.findall(r"/\*\*\* " + SHIM_TAG + r"\n(.*?)\n" + SHIM_TAG + r" \*\*\*/",
                        source, re.DOTALL)
    if len(blocks) != 1 or source.count(SHIM_TAG) != 2:
        raise PreflightError("{}: malformed shim annotation".format(path))
    try:
        versions = [json.loads(line)["spark"] for line in blocks[0].splitlines()]
    except (ValueError, KeyError, TypeError) as error:
        raise PreflightError("{}: invalid shim JSON".format(path)) from error
    if (not versions or any(not isinstance(v, str) or not v for v in versions)
            or versions != sorted(set(versions))):
        raise PreflightError("{}: shim versions must be nonempty, unique and sorted".format(path))
    return set(versions)


def named_imports(source):
    """Return explicit import paths; wildcard imports are reported as unmodelled."""
    names, wildcards = set(), []
    for match in IMPORT.finditer(without_comments(source)):
        value = match[1].strip().removeprefix("static ")
        if "{" in value:
            prefix, selectors = value.split("{", 1)
            for selector in selectors.split("}", 1)[0].split(","):
                original = re.split(r"\s*(?:=>|\bas\b)\s*", selector.strip())[0]
                if original in ("_", "*"):
                    wildcards.append(prefix + original)
                elif original:
                    names.add(prefix + original)
        elif value.endswith(("._", ".*")):
            wildcards.append(value)
        else:
            names.add(re.split(r"\s+as\s+", value)[0])
    return names, wildcards


def supported_versions(root):
    result = set()
    for relative in ("pom.xml", "scala2.13/pom.xml"):
        pom = root / relative
        if pom.is_file():
            try:
                result.update(v.strip() for v in _get_buildvers("all.buildvers", str(pom)).split(",")
                              if v.strip())
            except Exception as error:
                raise PreflightError("Cannot enumerate release profiles in " + relative) from error
    if not result:
        raise PreflightError("No supported release profiles found")
    return result


def coverage(root, modules, requested):
    versions = supported_versions(root)
    if requested:
        unknown = set(requested) - versions
        if unknown:
            raise PreflightError("Unknown or excluded build profiles: " + ", ".join(sorted(unknown)))
        versions &= set(requested)
    findings, skipped, checked = [], set(), set()
    for module in modules:
        main = root / module / "src/main"
        if not main.is_dir():
            raise PreflightError("Missing module source directory: " + str(main))
        providers, destinations, callers = defaultdict(list), defaultdict(list), []
        versioned_symbols = set()
        # Source roots only: never inspect target/, generated links or dependencies.
        for source_root in sorted(main.iterdir()):
            if source_root.name not in ("scala", "java") and not re.fullmatch(
                    r"spark[0-9][\w]*", source_root.name):
                continue
            for path in sorted(source_root.rglob("*")):
                if path.is_symlink() or path.suffix not in (".scala", ".java") or not path.is_file():
                    continue
                relative = path.relative_to(root).as_posix()
                source = path.read_text(encoding="utf-8")
                versioned = VERSIONED.fullmatch(relative)
                enabled = annotations(source, relative, required=True) if versioned else None
                active = versions if enabled is None else versions & enabled
                callers.append((relative, source, active))
                code = without_comments(source)
                package = PACKAGE.search(code)
                if package:
                    for symbol in set(DECLARATION.findall(code)):
                        qualified = package[1] + "." + symbol
                        providers[qualified].append((relative, active))
                        if versioned:
                            versioned_symbols.add(qualified)
                if versioned:
                    if versioned[2] != min(enabled):
                        raise PreflightError("{}: owner directory must be spark{}".format(
                            relative, min(enabled)))
                    destination = versioned[3] + "/" + versioned[4]
                    destinations[destination].append((relative, active))
        for destination, implementations in sorted(destinations.items()):
            for version in sorted(versions):
                selected = [p for p, active in implementations if version in active]
                if len(selected) > 1:
                    findings.append(dict(kind="duplicate_source", module=module,
                                         buildver=version, source=destination, providers=selected))
        for path, source, active in callers:
            if not active:
                continue
            imports, wildcards = named_imports(source)
            skipped.update((path, wildcard) for wildcard in wildcards if "shim" in wildcard.lower())
            for imported in sorted(imports):
                # Imports of members (Foo.method) also require Foo's implementation.
                symbols = [s for s in versioned_symbols
                           if imported == s or imported.startswith(s + ".")]
                for symbol in symbols:
                    available = set().union(*(enabled for _, enabled in providers[symbol]))
                    checked.update((module, symbol, v) for v in active)
                    for version in sorted(active - available):
                        findings.append(dict(kind="missing_implementation", module=module,
                                             buildver=version, symbol=symbol, caller=path,
                                             providers=[p for p, _ in providers[symbol]]))
    return dict(status="BLOCKED" if findings else "PASS", scope="explicit imports of module-local "
                "versioned declarations and generated source collisions; not a compile result",
                modules=modules, buildvers=sorted(versions), checked_requirements=len(checked),
                findings=findings, unmodelled_wildcard_imports=[dict(caller=p, imported=i)
                                                            for p, i in sorted(skipped)])


def graphql(root, query, **variables):
    command = ["gh", "api", "graphql", "-f", "query=" + query]
    for name, value in variables.items():
        if value is not None:
            command.extend(["-F", "{}={}".format(name, value)])
    payload = json.loads(run(command, root))
    if payload.get("errors") or not payload.get("data"):
        raise PreflightError("GitHub returned incomplete GraphQL data")
    return payload["data"]["repository"]


PR_FIELDS = ("number title url state baseRefName headRefOid isDraft mergeCommit { oid } "
             "baseRepository { nameWithOwner }")


def next_cursor(page, previous):
    cursor = page["pageInfo"].get("endCursor")
    if not cursor or cursor == previous:
        raise PreflightError("Pagination did not advance; scan incomplete")
    return cursor


def remote_base(root, owner, name, branch):
    query = """query($owner:String!, $name:String!, $ref:String!) {
      repository(owner:$owner, name:$name) { ref(qualifiedName:$ref) { target { oid } } }
    }"""
    ref = graphql(root, query, owner=owner, name=name, ref="refs/heads/" + branch)["ref"]
    if not ref:
        raise PreflightError("Target branch does not exist")
    return ref["target"]["oid"]


def open_prs(root, owner, name, branch):
    query = """query($owner:String!, $name:String!, $branch:String!, $cursor:String) {
      repository(owner:$owner, name:$name) {
        pullRequests(states:OPEN, baseRefName:$branch, first:100, after:$cursor) {
          nodes { %s changedFiles files(first:100) {
            nodes { path } pageInfo { hasNextPage endCursor }
          } } pageInfo { hasNextPage endCursor }
        }
      }
    }""" % PR_FIELDS
    result, cursor = [], None
    while True:
        page = graphql(root, query, owner=owner, name=name, branch=branch,
                       cursor=cursor)["pullRequests"]
        result.extend(page["nodes"])
        if not page["pageInfo"]["hasNextPage"]:
            return result
        cursor = next_cursor(page, cursor)


def merged_prs(root, owner, name, commits, branch):
    result = {}
    for start in range(0, len(commits), 40):
        fields = []
        for index, sha in enumerate(commits[start:start + 40]):
            fields.append('c%d: object(oid:"%s") { ... on Commit { associatedPullRequests'
                          '(first:100) { nodes { %s } pageInfo { hasNextPage } } } }'
                          % (index, sha, PR_FIELDS))
        query = ('query($owner:String!, $name:String!) { repository(owner:$owner,name:$name) {'
                 + " ".join(fields) + "} }")
        data = graphql(root, query, owner=owner, name=name)
        for commit in data.values():
            if commit is None:
                raise PreflightError("GitHub could not read a base commit")
            prs = commit["associatedPullRequests"]
            if prs["pageInfo"]["hasNextPage"]:
                raise PreflightError("Commit has more than 100 associated PRs; scan incomplete")
            for pr in prs["nodes"]:
                merge = pr.get("mergeCommit")
                if (pr["state"] == "MERGED" and pr["baseRefName"] == branch
                        and pr["baseRepository"]["nameWithOwner"].lower() == (owner + "/" + name).lower()
                        and merge and merge["oid"] in commits):
                    result[pr["number"]] = pr
    return list(result.values())


def pr_files(root, owner, name, number):
    query = """query($owner:String!, $name:String!, $number:Int!, $cursor:String) {
      repository(owner:$owner, name:$name) { pullRequest(number:$number) {
        headRefOid changedFiles files(first:100, after:$cursor) {
          nodes { path } pageInfo { hasNextPage endCursor }
        }
      } }
    }"""
    paths, cursor, head = [], None, None
    while True:
        pr = graphql(root, query, owner=owner, name=name, number=number,
                     cursor=cursor)["pullRequest"]
        if head is not None and head != pr["headRefOid"]:
            raise PreflightError("PR head changed during file pagination")
        head = pr["headRefOid"]
        page = pr["files"]
        paths.extend(node["path"] for node in page["nodes"])
        if not page["pageInfo"]["hasNextPage"]:
            if len(set(paths)) != pr["changedFiles"]:
                raise PreflightError("PR file list is truncated or changed during scan")
            return head, set(paths)
        cursor = next_cursor(page, cursor)


def candidate_files(root, owner, name, pr):
    page = pr.get("files")
    if page is not None and not page["pageInfo"]["hasNextPage"]:
        paths = {node["path"] for node in page["nodes"]}
        if len(paths) != pr["changedFiles"]:
            raise PreflightError("PR file list is truncated")
        return pr["headRefOid"], paths
    return pr_files(root, owner, name, pr["number"])


def path_signals(paths):
    families = {m[3] + "/" + m[4] for p in paths if (m := VERSIONED.fullmatch(p))}
    matrix = any(p in ("pom.xml", "scala2.13/pom.xml", "build/get_buildvers.py",
                       "build/shimplify.py", "build/dyn_shim_detection.py", "build/buildall",
                       "jenkins/version-def.sh", "jenkins/spark-premerge-build.sh",
                       ".github/workflows/mvn-verify-check.yml")
                 or p.startswith("shim-deps/") or p.startswith("scala2.13/shim-deps/")
                 or (p.startswith("jenkins/") and "databricks" in p)
                 for p in paths)
    return families, matrix


def imported_families(source):
    names, _ = named_imports(source)
    result = set()
    for name in names:
        parts = name.split(".")
        # A member import also requires its containing declaration. The peer's
        # actual file path determines which of these prefixes is a family.
        for end in range(1, len(parts) + 1):
            for language, extension in (("scala", ".scala"), ("java", ".java")):
                result.add(language + "/" + "/".join(parts[:end]) + extension)
    return result


def changed_imports(root, paths, head, dirty):
    result = set()
    for path in sorted(paths):
        if not re.search(r"/src/main/(?:scala|java|spark[^/]+/(?:scala|java))/", path):
            continue
        if not path.endswith((".scala", ".java")):
            continue
        if dirty:
            source = root / path
            if source.is_file() and not source.is_symlink():
                result.update(imported_families(source.read_text(encoding="utf-8")))
        elif git(root, "ls-tree", "--name-only", head, "--", path):
            result.update(imported_families(git(root, "show", head + ":" + path)))
    return result


def overlap_reasons(current, other, references=()):
    families, matrix = path_signals(current)
    families.update(references)
    peer_families, peer_matrix = path_signals(other)
    reasons = []
    if current & other:
        reasons.append("same files: " + ", ".join(sorted(current & other)))
    if families & peer_families:
        reasons.append("same shim family: " + ", ".join(sorted(families & peer_families)))
    if (families and peer_matrix) or (peer_families and matrix) or (matrix and peer_matrix):
        reasons.append("shim implementation and/or build matrix changes; inspect actual diffs")
    # Peer source is not fetched or executed. A shared caller can depend on our
    # shim/profile without changing a versioned path, so require manual review
    # rather than declaring these combinations unrelated.
    peer_shared = any(re.search(r"/src/main/(?:scala|java)/", path)
                      and path.endswith((".scala", ".java")) for path in other)
    if (families or matrix) and peer_shared:
        reasons.append("shared-source peer may call changed shims or use changed profiles; "
                       "inspect actual diffs")
    return reasons


def related_prs(root, repo, base_ref, head_ref, validated_base, branch, own_pr):
    owner, name = repo.split("/")
    base, head = commit_sha(root, base_ref), commit_sha(root, head_ref)
    if remote_base(root, owner, name, branch) != base:
        raise PreflightError("Fetched base differs from GitHub; fetch the target branch and retry")
    start = commit_sha(root, validated_base) if validated_base else git(
        root, "merge-base", base, head)
    git(root, "merge-base", "--is-ancestor", start, base)
    common = git(root, "merge-base", base, head)
    current = git_paths(root, "diff", "--name-only", common, head)
    # Include work in progress when preparing a new PR; record that it is not an exact-head scan.
    is_worktree_head = head == git(root, "rev-parse", "HEAD")
    dirty = is_worktree_head and bool(git(root, "status", "--porcelain", "--untracked-files=normal"))
    if dirty:
        current.update(git_paths(root, "diff", "--name-only", "HEAD"))
        current.update(git_paths(root, "ls-files", "--others", "--exclude-standard"))
    references = changed_imports(root, current, head, dirty)
    commits = git(root, "rev-list", start + ".." + base).splitlines()
    candidates = list(open_prs(root, owner, name, branch))
    open_heads = {(pr["number"], pr["headRefOid"]) for pr in candidates}
    candidates += merged_prs(root, owner, name, commits, branch)
    related = []
    candidates = sorted((p for p in candidates if p["number"] != own_pr), key=lambda p: p["number"])
    # Each read is independent; keep API concurrency bounded.
    with ThreadPoolExecutor(max_workers=4) as pool:
        files = pool.map(lambda p: candidate_files(root, owner, name, p), candidates)
        for pr, (live_head, paths) in zip(candidates, files):
            if live_head != pr["headRefOid"]:
                raise PreflightError("PR head changed during scan; retry")
            reasons = overlap_reasons(current, paths, references)
            if reasons:
                related.append(dict(number=pr["number"], title=pr["title"], url=pr["url"],
                                    state=pr["state"], head=live_head, draft=pr["isDraft"],
                                    reasons=reasons, files=sorted(paths)))
    if git(root, "rev-parse", base_ref) != base or git(root, "rev-parse", head_ref) != head:
        raise PreflightError("Local base/head refs changed during scan; retry")
    live_open = open_prs(root, owner, name, branch)
    if (remote_base(root, owner, name, branch) != base
            or {(pr["number"], pr["headRefOid"]) for pr in live_open} != open_heads):
        raise PreflightError("GitHub base or open PR heads changed during scan; retry")
    return dict(status="NEEDS_REVIEW" if related else "NO_RELATED_PRS", base=base, head=head,
                history_start=start, history_commits=len(commits), dirty=dirty,
                changed_files=sorted(current), candidates_scanned=len(candidates), related=related,
                scope="local fetched base and observed PR heads; association is not a confirmed defect")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    subparsers = parser.add_subparsers(dest="mode", required=True)
    static = subparsers.add_parser("coverage", help="offline source-selection checks")
    static.add_argument("--module", action="append", help="default: sql-plugin")
    static.add_argument("--buildver", action="append", help="default: all supported release profiles")
    online = subparsers.add_parser("related-prs", help="read-only GitHub PR association scan")
    online.add_argument("--repo", default="NVIDIA/cudf-spark")
    online.add_argument("--base-ref", default="origin/main")
    online.add_argument("--head-ref", default="HEAD")
    online.add_argument("--base-branch", default="main")
    online.add_argument("--validated-base", help="base SHA used in the last relevant validation")
    online.add_argument("--pr", type=int, help="exclude this PR from candidates")
    args = parser.parse_args(argv)
    try:
        root = args.root.resolve()
        if args.mode == "coverage":
            result = coverage(root, args.module or ["sql-plugin"], args.buildver)
        else:
            result = related_prs(root, args.repo, args.base_ref, args.head_ref,
                                 args.validated_base, args.base_branch, args.pr)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 1 if result["status"] == "BLOCKED" else 0
    except (PreflightError, OSError, ValueError, KeyError, TypeError,
            subprocess.TimeoutExpired) as error:
        print(json.dumps(dict(status="INCOMPLETE", error=str(error)), indent=2))
        return 2


if __name__ == "__main__":
    sys.exit(main())
