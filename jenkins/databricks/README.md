# Databricks premerge validation

This draft tests provider fallback through
`@Library('blossom-lib@main')` in the Databricks child Jenkinsfile. The shared-library
change has merged; its implementation is not stored in this repository. There is
no `DATABRICKS_CI_MODE` parameter.

## Manual validation

1. Ensure the parent and child jobs load the Jenkinsfiles from this draft. The
   child job must allow the specified shared-library version override.
2. Trigger the parent premerge for this PR. Both CI parts receive the same full
   source SHA and build independently; CI_PART2 does not wait for a workspace
   tarball from CI_PART1. For direct child runs, supply the same full `REF` SHA
   and PR metadata for both parts.
3. Check both runtime cells (14.3 and 17.3) and both CI parts. The preferred profile
   is still selected by the existing `DATABRICKS_TYPE` job setting (`aws` or
   `azure3`). A classified preparation or Spark-readiness failure can switch to
   the other profile once, after confirmed cluster termination.
4. Inspect the per-attempt `databricks-attempt-*.json` diagnostics. Filenames
   include the child build number; state is bound to the build, runtime, provider
   and workspace. A successful first attempt does not validate the fallback path.

`SPARK_STARTUP_CHECK` is an internal CI-only hook, installed and supplied by the
Jenkins shared library through the existing `EXTRA_ENVS`. Leave it unset for local
usage; local runs do not need to install or provide this shared-library script.
After environment setup, it runs a plugin-free Spark job on the Databricks runtime.
CI_PART1 also checks the upstream Spark local-cluster launch used by the two-shim
smoke test. These checks run before product tests, with a ten-minute limit each.
The merged library also verifies that a successful test callback left a successful
readiness result. Missing or malformed results cannot silently skip readiness.

Compilation and product-test failures never trigger fallback. A failed readiness
check needs concrete infrastructure evidence, such as a disk-full/disk-I/O error
or an allowlisted cluster termination. Unknown errors, authentication/configuration
errors, unclassified Spark-startup timeouts, OOM, and executor loss fail closed.
In particular, `Master removed our application` alone is insufficient. Failed or unavailable diagnostics
also fail closed. SSH loss during remote build/test execution and failures after
readiness passes are not recovered by this pilot.

For manual validation, confirm successful readiness checks for both runtime cells
and both CI parts, a classified readiness failure followed by fallback, and a
product-test failure that stops without fallback. No builds are triggered by the
local validation procedure.

The runtime matrix and test selection are unchanged. Independent builds resolve
Maven SNAPSHOT dependencies separately, so identical source SHAs do not guarantee
identical dependency bytes. CI duration and resource usage need live validation.

The consumer now uses the merged library main branch. Before marking ready,
complete live Jenkins/CPS and Databricks validation. To stop testing, use the
pre-pilot Jenkinsfiles again; reverting only the library is insufficient because
the revised child calls the new shared-library step.

## Temporary Validation-Only Commit

Do not merge the validation-only commit. The parent temporarily runs only the
two Databricks CI parts, skipping Docker builds, Linux Maven/UT/Scala jobs and
Skills stages before they allocate agents. Both Databricks runtime cells remain.
Duplicate-run cancellation and its delay are disabled to allow overlapping runs
of the same PR. The separate incident commit also disables fail-fast across CI
parts and runtime cells so one failed experiment does not cancel the others.

There are no added case-selection parameters. Individual test commits are prepared
separately when requested. Manual triggering remains with the operator. Check each
Jenkins build independently; concurrent runs can overwrite the shared GitHub PR status.

Before marking ready, remove the validation-only commit to restore ordinary stage
selection and duplicate-run cancellation. Keep the merged-library adoption commit
and normal fallback wiring.

## Current Incidents: Four Parallel Cells

This separate, temporary test commit assigns a different incident to each of the
four runtime/CI-part cells. Only the preferred profile selected by
`DATABRICKS_TYPE` (`aws` by default) is affected. No case-selection job parameter
is added; the alternate profile always runs the normal workload.

| Runtime | CI part | Synthetic incident | Expected reason |
| --- | --- | --- | --- |
| 14.3 | CI_PART1 | SSH connection timed out | `SSH_UNREACHABLE` |
| 14.3 | CI_PART2 | SSH connection refused | `SSH_UNREACHABLE` |
| 17.3 | CI_PART1 | Spark startup disk full | `SPARK_STARTUP_DISK_FULL` |
| 17.3 | CI_PART2 | Spark startup disk I/O error | `SPARK_STARTUP_DISK_IO` |

The SSH wrapper intercepts only the preferred profile's `ssh ... true` readiness
probe and returns the selected error with exit code 255. The library's real
six-probe loop and classifier are unchanged. Other SSH commands use the real
client. Spark cases substitute only the initial Databricks readiness probe's
`spark-shell`; the real library readiness script writes and classifies its result.
Builds are real, so a compilation failure prevents that Spark incident from being
reached. The upstream readiness hook remains unmodified on successful attempts.

These are synthetic failure signals, not real host/network/disk faults. No
credentials, firewall rules, cluster state JSON or shared-library source are
modified. The SSH wrapper returns immediately instead of reproducing elapsed
network timeouts. Cluster creation and cleanup are real.

For each cell, confirm the expected reason and `retryable: true`, then termination
and `cleanup_complete: true` before the alternate starts. Confirm the alternate
completes real builds, Spark readiness and product tests. Check archived attempt
JSON and logs, not just the final build status. A different failure does not prove
the assigned incident ran. Four cells can create up to eight clusters in total,
with cleanup required before each cell allocates its alternate.

Remove this incident commit before another experiment or merging. Restore normal
fail-fast behavior and direct readiness invocation, retaining the CI-only comments.
