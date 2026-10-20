---
name: check-shim-compatibility
description: Check cudf-spark shim compatibility before publishing, updating, or merging a PR that changes versioned sources, shared shim callers, or Spark/Databricks build profiles. Inspect related open and intervening merged PRs, check source selection, and identify the required combined-tree validation.
---

# Check shim compatibility

Run from the cudf-spark repository root. This is a read-only preflight, not
authorization to publish, comment, push, or merge. Preserve the calling workflow's
validation, review, and mutation requirements.

The repository helper is `build/check_shim_compatibility.py` (Python 3.10+,
standard library only). The `related-prs` mode also needs authenticated `gh` and
Git history. It never posts comments or changes GitHub state.

## Establish the proposed combination

1. Fetch the target branch from the upstream remote. Verify that it is the
   intended `NVIDIA/cudf-spark` branch; the examples below use `origin/main`.
2. Record the PR head and fetched base SHAs. If earlier validation exists,
   identify the **base SHA actually used by that run**, not today's PR base SHA.
   Record the build profiles it exercised too. Head-only green checks are not
   evidence for a changed base or matrix.
3. Inspect the diff for versioned sources, new/changed imports or other shim
   calls in shared source, release profiles, dependency selection, and CI matrix
   changes. For a genuinely unrelated change, report that scope and stop this
   specialized preflight; normal publication/merge requirements still apply.

## Find related PRs

For an existing PR (replace `12345`), run:

```bash
python3 build/check_shim_compatibility.py related-prs \
  --base-ref origin/main --pr 12345
```

For a new PR, omit `--pr`. Add `--validated-base SHA` when the previous
validation's exact base is known. The default starts at the head/base common
ancestor. History is commit-based, not limited to the last 24 hours: #16291
merged about 56 hours before #16037 and their combination caused #16404.

The helper paginates **all authors' open PRs**, including Drafts, and examines
PRs merged in that base-commit range. It compares actual changed paths, explicit
imports in our changed sources, shim families across version directories, and
matrix/implementation interactions.
A matrix path match is deliberately broad; inspect the actual diff to determine
whether it changes applicable profiles. The scan records the observed head of
each related PR and refuses a clean result on API failure, truncated file lists,
or base/open-head movement during the scan. It does not fetch other PR branches
or execute their code.

`NEEDS_REVIEW` is an association report, not a confirmed incompatibility.
For each match, determine whether it introduces a profile requiring our shim,
adds a shim required by our profile, changes the same API, or is independent.
Use source evidence and applicable validation to clear an independent match.
Do not delay a PR simply because another related PR remains open or Draft.

## Check source selection and validate

Check the **proposed combination with the current target branch**, using a
separate disposable worktree/snapshot when needed. Do not silently change the
author's worktree. Conflicting combinations need resolution and validation
before merge.

```bash
python3 build/check_shim_compatibility.py coverage
python3 build/check_shim_compatibility.py coverage --buildver 410db183
```

The default checks `sql-plugin` against the union of supported release profiles
in `pom.xml` and `scala2.13/pom.xml`, using `build/get_buildvers.py`. It honors
excluded releases and includes Databricks. Repeat `--module` to select additional
modules; use their applicable `--buildver` values for modules not built in every
profile. Module selection replaces the default, so explicitly include
`--module sql-plugin` when selecting it alongside another module.

The first version checks explicit imports of known module-local versioned
Scala/Java declarations, annotation validity/owner layout, and duplicate
generated source destinations. Shared implementations count as providers too.
Wildcard imports are listed for manual inspection. Fully qualified calls,
same-package uses, inherited APIs, signatures, cross-module providers,
Scala-specific source roots, and Maven's complete effective classpath are not
modelled. **A static `PASS` establishes only those source-selection checks.**
It is neither compilation nor CPU/GPU semantic evidence.

For new shim code, check every applicable profile in current main. For a new
profile, check all required shim families in that module, beyond the PR's file
list. Compile the affected profiles on the combined tree, including newly added
Databricks targets. Use the repository's build instructions and matching runtime
dependencies. Run applicable CPU/GPU tests when behavior changes; annotations
alone cannot select the correct behavioral implementation.

## Disposition and evidence

- `BLOCKED` (exit 1): an explicit import lacks a selected implementation, or
  multiple files select the same generated destination. Inspect and fix the
  finding, then repeat the check and matching compilation. Compare with the
  target branch to distinguish a new regression from existing base debt.
  Report base debt separately; do not change unrelated code to silence it.
- `INCOMPLETE` (exit 2): missing/malformed input, API/permission failure,
  truncated pagination, or moved refs. Report the reason; do not interpret it as
  no related PRs or permission to merge.
- A dependency on an open PR needs an explicit merge order and validation after
  that dependency lands. A justified Draft can describe the remaining work.
- Related PRs with complete coverage and valid combined-tree evidence may
  proceed through normal review and CI gates without an extra human approval.

Exit 0 means the requested check/scan completed; a `NEEDS_REVIEW` report still
requires disposition of its matches. No exit code alone establishes merge readiness.

Report head/base SHAs, profile matrix, related PR links and dispositions,
static findings, actual build/test evidence, and remaining gaps. A report with
`dirty: true` includes work in progress and must be rerun on the final committed
head before merge. Immediately before publication or merge, refresh the base,
current head, related PR states, and matrix. Relevant movement invalidates prior
evidence. This skill is advisory until the repository wires a deterministic
check into required CI; it cannot prevent concurrent human merges.

## Manual verification

For a real-source #16404 replay, export `pom.xml`, `scala2.13/pom.xml`, and
`sql-plugin/src/main` from these commits into separate temporary directories.
Run the current helper with `--root DIRECTORY coverage --buildver 410db183`:

- `c5516e87ea261dd05b94117d1647820a0e85bb77`: must report missing
  `com.nvidia.spark.rapids.shims.ParquetMissingStructShims`.
- `1a20c46043b45c464809311e9e8fb55b8e7e0443`: that gap must be absent.

These are source-selection regressions. Do not describe them as a Databricks
build or runtime test.
