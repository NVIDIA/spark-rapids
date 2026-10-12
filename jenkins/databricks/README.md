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

The runtime matrix is unchanged. Outside the temporary incident injections below,
the product-test selection is unchanged. Independent builds resolve
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

## Current Incidents: No-Fallback Boundaries

This round replaces the preceding four positive fallback signals with four
negative cases. Both runtime cells and both CI parts remain selected. Only the
preferred profile selected by `DATABRICKS_TYPE` (`aws` by default) is affected;
no case-selection job parameter is added.

| Runtime | CI part | Synthetic incident | Expected evidence |
| --- | --- | --- | --- |
| 14.3 | CI_PART1 | Build-command exit 42 | Build callback fails before compilation, outside startup classification |
| 14.3 | CI_PART2 | Test-command exit 43 | Real Databricks readiness is `READY` before the test command fails |
| 17.3 | CI_PART1 | Ambiguous Spark startup error | Readiness reason `SPARK_STARTUP_UNCLASSIFIED`, with no allowlisted cluster termination |
| 17.3 | CI_PART2 | SSH authentication denied | Preparation reason `SSH_READINESS_FAILED` |

All four cells and the overall run are expected to FAIL, with no alternate
provider attempt. A red build alone is not sufficient: verify the assigned
injection message, `retryable: false`, confirmed termination and
`cleanup_complete: true` in the preferred attempt, and no second attempt in logs
or archived JSON. The test/startup cases should retain the top-level classification
`NOT_A_CLASSIFIED_SPARK_STARTUP_FAILURE`; the build case retains its prepared
state because no startup classifier should run after a build error.

The build/test command exits simulate failure propagation, not actual compiler
or product defects. The test case requires a real build and successful plugin-free
Databricks readiness first, then stops before product tests. The unknown-startup
case also requires a real build; its replacement `spark-shell` emits only
`Master removed our application: FAILED`, which is not infrastructure evidence.
The real readiness script still writes and classifies the result. The SSH case
returns `Permission denied (publickey)` with exit 255 for the preferred profile's
readiness probe only; it does not change credentials or contact that SSH endpoint.

No host, network, credentials, attempt-state JSON or shared-library source is
modified. Cluster creation, readiness where applicable, classification and
cleanup are real. Fail-fast remains disabled so a negative result in one cell
does not cancel another. A genuine preparation/termination fault before or during
the experiment can invalidate a case and must be distinguished from the injection.

The previous positive matrix remains available at commit `471d1408dc`.
Reverting only this follow-up restores those positive injections, not production
behavior. Before merging, remove all temporary incident code, restore normal
fail-fast and direct readiness invocation, and retain the CI-only comments.
