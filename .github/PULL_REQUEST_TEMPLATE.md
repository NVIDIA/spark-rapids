<!--

Thank you for contributing to RAPIDS Accelerator for Apache Spark!

Please read https://github.com/NVIDIA/spark-rapids/blob/HEAD/CONTRIBUTING.md#creating-a-pull-request before making this PR.

The following are the guidelines to help the review process go smoothly. Please read them carefully and fill out relevant information as much as possible.

Thank you for your cooperation!

-->

<!--
Please replace #xxxx with the ID of the issue fixed in this PR. If such issue does not exist, please consider filing one and link it here.
-->
Fixes #xxxx.

### Description

<!--
Please provide a description of the changes proposed in this pull request. Here are some questions to help you fill out the description:

- What is the problem you are trying to solve? Describe it from the user's perspective. If you have an existing github issue, please add a summary of the issue here.
- After this change, what will the user experience be like? Please describe any user-facing changes, such as new configurations or new behaviors. If you are introducing new configurations, please add some guidelines on how to use them.
- How are you fixing the problem? Please provide a technical description of your solution. You can add or link your design doc if it exists.
- How are the new features/behaviors tested? Please describe the test cases you added or modified. If they are tested in a cluster, please describe it as well.
-->

### Why are the changes needed?

<!--
Please clarify the motivation for these changes. For instance:
  1. If you fix a bug, describe the user-visible symptoms and root cause.
  2. If you add a new feature, describe the use case and why it benefits users.
  3. If you refactor, explain what problem the current code structure causes.
-->

### Does this PR introduce any user-facing change?

<!--
"User-facing" includes any change to behavior, configuration, error messages, or public APIs.
If yes, describe the before/after behavior. If no, write "No".
-->

### Checklists

<!--
Check the items below by putting "x" in the brackets for what is done.
Not all of these items may be relevant to every PR, so please check only those that apply.
-->

- [ ] This PR has added documentation for new or modified features or behaviors.
- [ ] This PR has added new tests or modified existing tests to cover new code paths.
      (Please explain in the PR description how the new code paths are tested, such as names of the new/existing tests that cover them.)
- [ ] Performance testing has been performed and its results are added in the PR description. Or, an issue has been filed with a link in the PR description.

#### GPU & Compatibility Checklist

- [ ] All GPU resources (`ColumnarBatch`, `GpuColumnVector`, etc.) use `withResource`/`closeOnExcept` — no bare `.close()`.
- [ ] If shim files were modified, all related Spark version shims are updated consistently.
- [ ] If `pom.xml` was modified, `./build/make-scala-version-build-files.sh 2.13` has been run to sync Scala 2.13 pom files.
- [ ] If new `RapidsConf` keys were added, they have documentation and sensible defaults.
- [ ] If new GPU operators were added, they are registered in `GpuOverrides` with proper fallback declarations.
- [ ] If this PR affects Databricks-specific code, `[databricks]` is in the PR title.
- [ ] If SNAPSHOT dependency versions were changed, the rationale is described above.

### Was this patch authored or co-authored using generative AI tooling?

<!--
If generative AI tooling has been used in the process of authoring this patch, please include the
phrase: 'Generated-by: ' followed by the name of the tool and its version.
If no, write 'No'.
Please refer to the ASF Generative Tooling Guidance (https://www.apache.org/legal/generative-tooling.html) for details.
-->
