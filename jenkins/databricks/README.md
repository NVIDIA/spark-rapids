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

The shared library supplies `SPARK_STARTUP_CHECK` through the existing `EXTRA_ENVS`.
After environment setup, it runs a plugin-free Spark job on the Databricks runtime.
CI_PART1 also checks the upstream Spark local-cluster launch used by the two-shim
smoke test. These checks run before product tests, with a ten-minute limit each.
The merged library also verifies that a successful test callback left a successful
readiness result. Missing or malformed results cannot silently skip readiness.

Compilation and product-test failures never trigger fallback. A failed readiness
check needs concrete infrastructure evidence, such as a disk-full/disk-I/O error
or an allowlisted cluster termination. Unknown errors, authentication/configuration
errors, bare timeouts, OOM, and executor loss fail closed. In particular, `Master
removed our application` alone is insufficient. Failed or unavailable diagnostics
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
