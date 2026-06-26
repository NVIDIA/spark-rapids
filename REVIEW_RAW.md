## Code Review: spark-rapids — (codex/legacy-date-format-millis)

**📊 Found across 2 files:**
- 🔴 1 must-fix
- 🟡 1 should-fix
- 🟢 0 suggestions

---

### 📄 `integration_tests/src/main/python/date_time_test.py`

#### 1. [🟡 SHOULD FIX] Can you add fallback coverage for the new LEGACY millisecond-format allowlist gates? The new positive tests at /home/haoyangl/cc_exp/spark-rapids/integration_tests/src/main/python/date_time_test.py:968 and /home/haoyangl/cc_exp/spark-rapids/integration_tests/src/main/python/date_time_test.py:989 only exercise the path where `spark.rapids.sql.incompatibleDateFormats.enabled` is true and ANSI is disabled, but /home/haoyangl/cc_exp/spark-rapids/sql-plugin/src/main/scala/com/nvidia/spark/rapids/DateUtils.scala:234 still routes LEGACY-compatible formats through two fallback gates at /home/haoyangl/cc_exp/spark-rapids/sql-plugin/src/main/scala/com/nvidia/spark/rapids/DateUtils.scala:240 and /home/haoyangl/cc_exp/spark-rapids/sql-plugin/src/main/scala/com/nvidia/spark/rapids/DateUtils.scala:242

**Line:** 968
**Issue:** Can you add fallback coverage for the new LEGACY millisecond-format allowlist gates? The new positive tests at /home/haoyangl/cc_exp/spark-rapids/integration_tests/src/main/python/date_time_test.py:968 and /home/haoyangl/cc_exp/spark-rapids/integration_tests/src/main/python/date_time_test.py:989 only exercise the path where `spark.rapids.sql.incompatibleDateFormats.enabled` is true and ANSI is disabled, but /home/haoyangl/cc_exp/spark-rapids/sql-plugin/src/main/scala/com/nvidia/spark/rapids/DateUtils.scala:234 still routes LEGACY-compatible formats through two fallback gates at /home/haoyangl/cc_exp/spark-rapids/sql-plugin/src/main/scala/com/nvidia/spark/rapids/DateUtils.scala:240 and /home/haoyangl/cc_exp/spark-rapids/sql-plugin/src/main/scala/com/nvidia/spark/rapids/DateUtils.scala:242. Without tests for those gates, this change can accidentally enable `date_format` or `from_unixtime` for `yyyy-MM-dd HH:mm:ss.SSS` when ANSI is on or when incompatible date formats are not explicitly enabled.
**Confidence:** 🟣❗ CERTAIN
**Code:**
```python
/home/haoyangl/cc_exp/spark-rapids/integration_tests/src/main/python/date_time_test.py:968 sets `spark.sql.legacy.timeParserPolicy` and `spark.rapids.sql.incompatibleDateFormats.enabled`, and /home/haoyangl/cc_exp/spark-rapids/integration_tests/src/main/python/date_time_test.py:972 verifies GPU/CPU equality for `date_format(timestamp_millis(a), 'yyyy-MM-dd HH:mm:ss.SSS')`; /home/haoyangl/cc_exp/spark-rapids/integration_tests/src/main/python/date_time_test.py:989 sets the same success-path config, and /home/haoyangl/cc_exp/spark-rapids/integration_tests/src/main/python/date_time_test.py:993 verifies GPU/CPU equality for `from_unixtime(a, 'yyyy-MM-dd HH:mm:ss.SSS')`.
```
**Fix:** Add `assert_gpu_fallback_collect` cases for `date_format(timestamp_millis(a), 'yyyy-MM-dd HH:mm:ss.SSS')` and `from_unixtime(a, 'yyyy-MM-dd HH:mm:ss.SSS')` with `spark.sql.legacy.timeParserPolicy=LEGACY` when incompatible date formats are not enabled, and again when ANSI is enabled with incompatible date formats enabled.
**Diff:**
```diff
--- /home/haoyangl/cc_exp/spark-rapids/integration_tests/src/main/python/date_time_test.py
+++ /home/haoyangl/cc_exp/spark-rapids/integration_tests/src/main/python/date_time_test.py
@@ -996,6 +996,28 @@ def test_from_unixtime_legacy_millisecond_format():
         conf)


+@allow_non_gpu('ProjectExec')
+@pytest.mark.parametrize('expr,cpu_fallback_class', [
+    ("date_format(timestamp_millis(a), 'yyyy-MM-dd HH:mm:ss.SSS')", "DateFormatClass"),
+    ("from_unixtime(a, 'yyyy-MM-dd HH:mm:ss.SSS')", "FromUnixTime")
+], ids=idfn)
+@pytest.mark.parametrize('conf', [
+    {
+        "spark.sql.legacy.timeParserPolicy": "LEGACY"
+    },
+    {
+        "spark.sql.legacy.timeParserPolicy": "LEGACY",
+        "spark.sql.ansi.enabled": "true",
+        "spark.rapids.sql.incompatibleDateFormats.enabled": "true"
+    }
+], ids=idfn)
+def test_legacy_millisecond_format_fallback_gates(expr, cpu_fallback_class, conf):
+    gen = SetValuesGen(LongType(), [0])
+    assert_gpu_fallback_collect(
+        lambda spark: unary_op_df(spark, gen, length=1).selectExpr(expr),
+        cpu_fallback_class,
+        conf)
+
+
 @disable_ansi_mode
 @allow_non_gpu('ProjectExec')
 def test_to_timestamp_legacy_millisecond_format_still_fallback():
```
**Reference:** /home/haoyangl/cc_exp/spark-rapids/.review/conventions.md:388 says test coverage should include CPU/GPU fallback paths; /home/haoyangl/cc_exp/spark-rapids/.review/conventions.md:410 lists `assert_gpu_fallback_collect`; /home/haoyangl/cc_exp/spark-rapids/integration_tests/src/main/python/date_time_test.py:504 and /home/haoyangl/cc_exp/spark-rapids/integration_tests/src/main/python/date_time_test.py:508 show the existing fallback-test pattern for `from_unixtime`.
*(Reviewer: test-coverage)*

---

### 📄 `sql-plugin/src/main/scala/com/nvidia/spark/rapids/DateUtils.scala`

#### 2. [🔴 MUST FIX] Can you wrap the new `correctedCompatibleFormats` parameter before `): String = {`? The changed line is 104 characters, while /home/haoyangl/cc_exp/spark-rapids/scalastyle-config.xml:43 enables `FileLineLengthChecker` at error level and /home/haoyangl/cc_exp/spark-rapids/scalastyle-config.xml:45 sets `maxLineLength` to 100

**Line:** 225
**Issue:** Can you wrap the new `correctedCompatibleFormats` parameter before `): String = {`? The changed line is 104 characters, while /home/haoyangl/cc_exp/spark-rapids/scalastyle-config.xml:43 enables `FileLineLengthChecker` at error level and /home/haoyangl/cc_exp/spark-rapids/scalastyle-config.xml:45 sets `maxLineLength` to 100. That makes this a style-check failure on a changed line rather than only a readability issue.
**Confidence:** 🟣❗ CERTAIN
**Code:**
```scala
      correctedCompatibleFormats: Set[String] = GpuToTimestamp.CORRECTED_COMPATIBLE_FORMATS): String = {
```
**Fix:** Put the return type on the next line so both lines stay under the enforced 100-character limit.
**Diff:**
```diff
-      correctedCompatibleFormats: Set[String] = GpuToTimestamp.CORRECTED_COMPATIBLE_FORMATS): String = {
+      correctedCompatibleFormats: Set[String] = GpuToTimestamp.CORRECTED_COMPATIBLE_FORMATS)
+      : String = {
```
**Reference:** /home/haoyangl/cc_exp/spark-rapids/scalastyle-config.xml:43 configures `org.scalastyle.file.FileLineLengthChecker` with `level="error"`, /home/haoyangl/cc_exp/spark-rapids/scalastyle-config.xml:45 sets `maxLineLength` to `100`, and `awk 'length($0)>100 ...'` measured /home/haoyangl/cc_exp/spark-rapids/sql-plugin/src/main/scala/com/nvidia/spark/rapids/DateUtils.scala:225 as 104 characters.
*(Reviewer: correctness)*

---

### Skipped Domains
architecture (no matching signals), refactor (no matching signals), memory-stream (no matching signals), gpu-kernel-correctness (no matching signals), gpu-kernel-optimization (no matching signals), comment-compliance (no matching signals), impact-surface (no unchanged callers for changed public API), security-taint (no matching signals), jni-boundary (no matching signals), refactor-completeness (no matching signals)

---

### 🔍 Validation Notes

⚠️ **2 findings corrected** (2 rejected, 0 downgraded)

| # | Finding | Verdict | Detail |
|---|---------|---------|--------|
| ? | 🟡 compliant_copy0-F1 | ❌ REJECTED | The cited source exists and /home/haoyangl/cc_exp/spark-rapids/sql-plugin/src/main/scala/com/nvidia/ |
| ? | 🟡 compliant_copy0-F1 | ❌ REJECTED | The cited source exists and /home/haoyangl/cc_exp/spark-rapids/sql-plugin/src/main/scala/com/nvidia/ |

### Files Reviewed
- 📄 `integration_tests/src/main/python/date_time_test.py`
- 📄 `sql-plugin/src/main/scala/com/nvidia/spark/rapids/DateUtils.scala`
