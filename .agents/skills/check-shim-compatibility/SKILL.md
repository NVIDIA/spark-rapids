---
name: check-shim-compatibility
description: Check cudf-spark shim source selection on a PR combined with its current target branch and identify required profile validation. Use for versioned sources, shared shim callers, or Spark/Databricks build profiles; cross-PR discovery is a separate optional advisory step.
---

# Check shim compatibility

Run from the cudf-spark repository root. This is a read-only preflight, not
authorization to publish, comment, push, or merge. Preserve the calling workflow's
validation, review, and mutation requirements.

The repository helper is `build/check_shim_compatibility.py` (Python 3.10+,
standard library only). The `related-prs` and `review` modes need authenticated
`gh` and Git history; `review` requires Git 2.38+ for `merge-tree --write-tree`.
The helper never posts comments or changes GitHub state.

## Automated review evidence

The `Shim compatibility` workflow runs the `shim-compatibility` check on PR
events, using read-only repository/PR permissions. It exports immutable Git
source blobs, checks `sql-plugin` on current base and the proposed combined tree,
without depending on unrelated PR activity. No product code, Maven build, or
GPU test is executed. Cross-PR discovery is available separately through
`related-prs` when useful; the automated check does not run it.

```bash
python3 build/check_shim_compatibility.py review \
  --base-ref origin/main --pr 12345 --output-dir /tmp/shim-review
```

`report.json` and `report.md` record exact head/base/combined-tree identities,
the checked matrix and new versus unchanged base findings.
The workflow publishes a summary and full artifact, with up to ten error
annotations for new static findings. Unchanged base debt is reported without
new-error annotations. `INCOMPLETE` fails
the check, retaining any static evidence already collected. Unrelated diffs are
reported as `NOT_APPLICABLE`. The automated source-selection scope is currently
`sql-plugin`; inspect other affected modules separately.

Copilot and Greptile reviewers should read this evidence,
then report confirmed new gaps with file/symbol/profile and
observed output. If no execution/API tools or current report are available,
state that limitation. Never fabricate a successful run or report missing
access as a product defect. A green check is not merge authorization.

The workflow runs on PR opening, head updates, reopening, ready-for-review, and
target-branch changes. Title/body-only edits skip the job and use an isolated
concurrency group so they cannot cancel a useful scan. It does not run on every
merge to the target branch. Before
merge, compare the report's base/head to current refs and rerun when relevant
changes have landed. Required-check branch protection remains a repository
setting; this change does not enable it or the reviewers' execution features.

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

## Optional related-PR discovery

This advisory command is separate from `review` and is not required for the
core check to pass. Use it when investigating concurrent changes or when a
reviewer asks for cross-PR analysis. Its API failures or moving peer refs affect
only that advisory result, not a completed core report.

For an existing PR (replace `12345`), run:

```bash
python3 build/check_shim_compatibility.py related-prs \
  --base-ref origin/main --pr 12345
```

For a new PR, omit `--pr`. Search PRs targeting the same repository and branch:

- **Open PRs:** all authors, including Drafts, without an age limit; exclude the
  PR being checked.
- **Merged PRs:** changes after the base actually covered by the latest
  applicable successful validation, through the fetched current target SHA.
  Pass that base as `--validated-base SHA` only when the run's exact base is
  known and it covered the affected profiles/shim APIs. Otherwise use the
  default head/target-branch common ancestor. The helper does not discover or
  verify prior CI coverage automatically.

Prioritize related merges from the last **48 hours** during inspection, while
retaining older changes in that commit range. An earlier green premerge check
only justifies narrowing the range if it included those changes and covered the
affected profiles/shim APIs. #16291 merged about 56 hours before #16037 and their
combination caused #16404. The goal is to catch concurrent PRs that each pass
their own checks but fail when combined.

The helper paginates the open PRs and examines PRs merged in that base-commit
range. It compares actual changed paths, explicit
imports in our changed sources, shim families across version directories, and
matrix/implementation interactions.
A matrix path match is deliberately broad. A shim/profile change also flags
peer shared-source changes conservatively because their imports are not read;
inspect those diffs to establish whether a caller actually depends on the change.
The scan records each related PR's observed head and refuses a clean result on
API failure, truncated file lists,
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

Exit 0 means the requested check completed. The optional `related-prs` command
can return `NEEDS_REVIEW`, requiring disposition of its matches. No exit code
alone establishes merge readiness.

Report head/base SHAs, profile matrix, static findings, actual build/test
evidence, and remaining gaps. Include related PR links and dispositions only
when the optional advisory scan was requested. A report with
`dirty: true` includes work in progress and must be rerun on the final committed
head before merge. Immediately before publication or merge, refresh the base,
current head and matrix. Relevant movement invalidates prior
evidence. The CI check is advisory until made required by repository settings;
it cannot prevent concurrent human merges.

## Manual verification

For a real-source #16404 replay, export `pom.xml`, `scala2.13/pom.xml`, and
`sql-plugin/src/main` from these commits into separate temporary directories.
Run the current helper with `--root DIRECTORY coverage --buildver 410db183`:

- `c5516e87ea261dd05b94117d1647820a0e85bb77`: must report missing
  `com.nvidia.spark.rapids.shims.ParquetMissingStructShims`.
- `1a20c46043b45c464809311e9e8fb55b8e7e0443`: that gap must be absent.

These are source-selection regressions. Do not describe them as a Databricks
build or runtime test.
