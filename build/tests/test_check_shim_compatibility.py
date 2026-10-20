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

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


BUILD = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BUILD))
SPEC = importlib.util.spec_from_file_location(
    "check_shim_compatibility", BUILD / "check_shim_compatibility.py")
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


def shim(versions, symbol="ExampleShims"):
    return ("/*** spark-rapids-shim-json-lines\n"
            + "\n".join(json.dumps({"spark": v}) for v in versions)
            + "\nspark-rapids-shim-json-lines ***/\npackage example\nobject " + symbol + " {}\n")


def write(root, path, text):
    target = root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def pom(versions, excluded=""):
    properties = "".join("<spark{0}.version>{0}.0</spark{0}.version>".format(v) for v in versions)
    profiles = "".join("<profile><id>release{}</id></profile>".format(v) for v in versions)
    return ('<project xmlns="http://maven.apache.org/POM/4.0.0"><properties>' + properties
            + "<dyn.shim.excluded.releases>" + excluded + "</dyn.shim.excluded.releases>"
            + "</properties><profiles>" + profiles + "</profiles></project>")


class CoverageTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        write(self.root, "pom.xml", pom(["330", "410db183", "411"]))
        self.shared = "sql-plugin/src/main/scala/example/Caller.scala"
        self.old = "sql-plugin/src/main/spark330/scala/example/ExampleShims.scala"
        self.new = "sql-plugin/src/main/spark411/scala/example/ExampleShims.scala"
        write(self.root, self.shared, "package example\nimport example.ExampleShims\nobject Caller {}")
        write(self.root, self.old, shim(["330"]))
        write(self.root, self.new, shim(["411"]))

    def scan(self, versions=None):
        return CHECK.coverage(self.root, ["sql-plugin"], versions)

    def test_missing_new_profile_matches_16404(self):
        report = self.scan()
        self.assertEqual("BLOCKED", report["status"])
        self.assertEqual([("missing_implementation", "410db183")],
                         [(f["kind"], f["buildver"]) for f in report["findings"]])

    def test_registering_and_moving_shim_repairs_gap(self):
        (self.root / self.new).unlink()
        write(self.root, self.new.replace("spark411", "spark410db183"), shim(["410db183", "411"]))
        self.assertEqual("PASS", self.scan()["status"])

    def test_selected_profile_only(self):
        self.assertEqual("PASS", self.scan(["330"])["status"])

    def test_unknown_profile_does_not_pass(self):
        with self.assertRaises(CHECK.PreflightError):
            self.scan(["not-a-profile"])

    def test_unannotated_shared_provider_is_valid(self):
        write(self.root, "sql-plugin/src/main/scala/example/ExampleShims.scala",
              "package example\nobject ExampleShims {}")
        self.assertEqual("PASS", self.scan()["status"])

    def test_versioned_callers_only_require_their_profiles(self):
        (self.root / self.shared).unlink()
        write(self.root, "sql-plugin/src/main/spark330/scala/example/Caller.scala",
              shim(["330"], "Caller") + "import example.ExampleShims\n")
        self.assertEqual("PASS", self.scan()["status"])

    def test_duplicate_generated_destination(self):
        write(self.root, self.old, shim(["330", "410db183"]))
        (self.root / self.new).unlink()
        write(self.root, self.new.replace("spark411", "spark410db183"), shim(["410db183", "411"]))
        report = self.scan()
        self.assertEqual(["duplicate_source"], [f["kind"] for f in report["findings"]])
        self.assertEqual("410db183", report["findings"][0]["buildver"])

    def test_wrong_owner_is_incomplete(self):
        write(self.root, self.new, shim(["410db183", "411"]))
        with self.assertRaisesRegex(CHECK.PreflightError, "owner"):
            self.scan()

    def test_missing_header_is_incomplete(self):
        write(self.root, self.old, "package example\nobject ExampleShims {}")
        with self.assertRaisesRegex(CHECK.PreflightError, "annotation"):
            self.scan()

    def test_invalid_json_is_incomplete(self):
        write(self.root, self.old, shim(["330"]).replace('"330"', '"330",'))
        with self.assertRaisesRegex(CHECK.PreflightError, "JSON"):
            self.scan()

    def test_repeated_annotation_version_is_rejected(self):
        write(self.root, self.old, shim(["330", "330"]))
        with self.assertRaises(CHECK.PreflightError):
            self.scan()

    def test_scala_213_profiles_are_included(self):
        write(self.root, "pom.xml", pom(["330"]))
        write(self.root, "scala2.13/pom.xml", pom(["410db183", "411"]))
        self.assertEqual("410db183", self.scan()["findings"][0]["buildver"])

    def test_excluded_profiles_are_not_requirements(self):
        write(self.root, "pom.xml", pom(["330", "410db183", "411"], "410db183"))
        self.assertEqual("PASS", self.scan()["status"])

    def test_braced_aliased_member_import(self):
        write(self.root, self.shared, "import example.{\n ExampleShims => Local\n}\nobject Caller {}")
        self.assertEqual("BLOCKED", self.scan()["status"])
        write(self.root, self.shared, "import example.ExampleShims.method\nobject Caller {}")
        self.assertEqual("BLOCKED", self.scan()["status"])

    def test_java_static_import(self):
        (self.root / self.shared).unlink()
        write(self.root, "sql-plugin/src/main/java/example/Caller.java",
              "package example;\nimport static example.ExampleShims.method;\npublic class Caller {}")
        self.assertEqual("BLOCKED", self.scan()["status"])

    def test_wildcard_is_reported_as_unmodelled(self):
        write(self.root, self.shared, "import example.shims._\nobject Caller {}")
        report = self.scan()
        self.assertTrue(report["unmodelled_wildcard_imports"])
        self.assertIn("not a compile result", report["scope"])

    def test_comments_and_strings_do_not_create_imports(self):
        write(self.root, self.shared, '// import example.ExampleShims\n'
              '/*\nimport example.ExampleShims\n*/\n'
              'val s = """\nimport example.ExampleShims\n"""\n')
        self.assertEqual(0, self.scan()["checked_requirements"])

    def test_generated_sources_and_test_sources_are_ignored(self):
        write(self.root, "sql-plugin/target/spark330/generated/src/main/scala/Broken.scala", "broken")
        write(self.root, "sql-plugin/src/test/spark330/scala/Broken.scala", "broken")
        self.assertEqual(1, len(self.scan()["findings"]))

    def test_missing_module_is_incomplete(self):
        with self.assertRaises(CHECK.PreflightError):
            CHECK.coverage(self.root, ["missing"], None)

    def test_invalid_pom_is_incomplete(self):
        write(self.root, "pom.xml", "<not-xml")
        with contextlib.redirect_stdout(io.StringIO()) as output:
            code = CHECK.main(["--root", str(self.root), "coverage"])
        self.assertEqual(2, code)
        self.assertEqual("INCOMPLETE", json.loads(output.getvalue())["status"])

    def test_cli_exit_codes_and_json(self):
        for arguments, code, status in [([], 1, "BLOCKED"),
                                        (["--buildver", "330"], 0, "PASS"),
                                        (["--buildver", "999"], 2, "INCOMPLETE")]:
            with contextlib.redirect_stdout(io.StringIO()) as output:
                result = CHECK.main(["--root", str(self.root), "coverage", *arguments])
            self.assertEqual(code, result)
            self.assertEqual(status, json.loads(output.getvalue())["status"])


class RelatedPRTest(unittest.TestCase):
    def test_shared_caller_relates_to_new_profile(self):
        references = CHECK.imported_families("import example.ExampleShims.method")
        self.assertTrue(CHECK.overlap_reasons(
            {"sql-plugin/src/main/scala/example/Caller.scala"}, {"pom.xml"}, references))

    def test_shared_caller_relates_to_another_prs_shim(self):
        references = CHECK.imported_families("import example.{ExampleShims => Alias}")
        self.assertTrue(CHECK.overlap_reasons(
            {"sql-plugin/src/main/scala/example/Caller.scala"},
            {"sql-plugin/src/main/spark330/scala/example/ExampleShims.scala"}, references))

    def test_profile_change_relates_to_new_shim_without_shared_file(self):
        reasons = CHECK.overlap_reasons({"pom.xml", "scala2.13/pom.xml"},
                                       {"sql-plugin/src/main/spark330/scala/example/NewShims.scala"})
        self.assertTrue(reasons)
        self.assertIn("build matrix", reasons[0])

    def test_same_family_in_different_version_directories(self):
        reasons = CHECK.overlap_reasons(
            {"sql-plugin/src/main/spark330/scala/example/Foo.scala"},
            {"sql-plugin/src/main/spark411/scala/example/Foo.scala"})
        self.assertTrue(any("same shim family" in r for r in reasons))

    def test_unrelated_changes_do_not_block(self):
        self.assertEqual([], CHECK.overlap_reasons({"docs/dev/README.md"}, {"pom.xml"}))

    def test_open_pr_pagination_has_no_author_filter(self):
        first = dict(nodes=[dict(number=1)], pageInfo=dict(hasNextPage=True, endCursor="next"))
        last = dict(nodes=[dict(number=2)], pageInfo=dict(hasNextPage=False, endCursor=None))
        with patch.object(CHECK, "graphql", side_effect=[dict(pullRequests=first),
                                                       dict(pullRequests=last)]) as mock:
            result = CHECK.open_prs(Path("."), "NVIDIA", "cudf-spark", "main")
        self.assertEqual([1, 2], [pr["number"] for pr in result])
        self.assertEqual("next", mock.call_args.kwargs["cursor"])
        self.assertNotIn("author", mock.call_args.args[1])

    def test_file_pagination_beyond_100(self):
        first = dict(headRefOid="abc", changedFiles=101, files=dict(
            nodes=[dict(path=str(i)) for i in range(100)],
            pageInfo=dict(hasNextPage=True, endCursor="next")))
        last = dict(headRefOid="abc", changedFiles=101, files=dict(nodes=[dict(path="last")],
                    pageInfo=dict(hasNextPage=False, endCursor=None)))
        with patch.object(CHECK, "graphql", side_effect=[dict(pullRequest=first), dict(pullRequest=last)]):
            head, paths = CHECK.pr_files(Path("."), "NVIDIA", "cudf-spark", 123)
        self.assertEqual("abc", head)
        self.assertEqual(101, len(paths))

    def test_truncated_files_do_not_produce_clean_result(self):
        pr = dict(headRefOid="abc", changedFiles=2, files=dict(nodes=[dict(path="one")],
                  pageInfo=dict(hasNextPage=False)))
        with patch.object(CHECK, "graphql", return_value=dict(pullRequest=pr)):
            with self.assertRaisesRegex(CHECK.PreflightError, "truncated"):
                CHECK.pr_files(Path("."), "NVIDIA", "cudf-spark", 123)

    def test_complete_open_page_avoids_per_pr_request(self):
        pr = dict(number=123, headRefOid="abc", changedFiles=1,
                  files=dict(nodes=[dict(path="pom.xml")], pageInfo=dict(hasNextPage=False)))
        with patch.object(CHECK, "pr_files") as mock:
            self.assertEqual(("abc", {"pom.xml"}),
                             CHECK.candidate_files(Path("."), "NVIDIA", "cudf-spark", pr))
        mock.assert_not_called()

    def test_large_open_pr_falls_back_to_full_pagination(self):
        pr = dict(number=123, files=dict(nodes=[], pageInfo=dict(hasNextPage=True)))
        with patch.object(CHECK, "pr_files", return_value=("abc", {"all-files"})) as mock:
            self.assertEqual(("abc", {"all-files"}),
                             CHECK.candidate_files(Path("."), "NVIDIA", "cudf-spark", pr))
        mock.assert_called_once()

    def test_truncated_open_page_is_incomplete(self):
        pr = dict(number=123, headRefOid="abc", changedFiles=2,
                  files=dict(nodes=[dict(path="one")], pageInfo=dict(hasNextPage=False)))
        with self.assertRaisesRegex(CHECK.PreflightError, "truncated"):
            CHECK.candidate_files(Path("."), "NVIDIA", "cudf-spark", pr)

    def test_changed_head_during_pagination_is_incomplete(self):
        pages = [dict(headRefOid=head, changedFiles=2, files=dict(nodes=[dict(path=head)],
                      pageInfo=dict(hasNextPage=more, endCursor="next")))
                 for head, more in [("old", True), ("new", False)]]
        with patch.object(CHECK, "graphql", side_effect=[dict(pullRequest=p) for p in pages]):
            with self.assertRaisesRegex(CHECK.PreflightError, "head changed"):
                CHECK.pr_files(Path("."), "NVIDIA", "cudf-spark", 123)

    def test_merged_pr_lookup_uses_commit_membership_not_day_window(self):
        pr = dict(number=16291, state="MERGED", baseRefName="main", mergeCommit=dict(oid="a"),
                  baseRepository=dict(nameWithOwner="NVIDIA/cudf-spark"))
        unrelated = dict(pr, number=999, mergeCommit=dict(oid="elsewhere"))
        foreign = dict(pr, number=888, baseRepository=dict(nameWithOwner="someone/fork"))
        data = dict(c0=dict(associatedPullRequests=dict(nodes=[pr, unrelated, foreign],
                           pageInfo=dict(hasNextPage=False))))
        with patch.object(CHECK, "graphql", return_value=data) as mock:
            result = CHECK.merged_prs(Path("."), "NVIDIA", "cudf-spark", ["a"], "main")
        self.assertEqual([16291], [p["number"] for p in result])
        self.assertNotIn("merged:", mock.call_args.args[1])

    def test_nonadvancing_pagination_is_incomplete(self):
        page = dict(nodes=[], pageInfo=dict(hasNextPage=True, endCursor="same"))
        with patch.object(CHECK, "graphql", return_value=dict(pullRequests=page)):
            with self.assertRaisesRegex(CHECK.PreflightError, "did not advance"):
                CHECK.open_prs(Path("."), "NVIDIA", "cudf-spark", "main")

    def test_graphql_errors_fail_closed(self):
        with patch.object(CHECK, "run", return_value=json.dumps(dict(errors=[dict(message="bad")]))):
            with self.assertRaises(CHECK.PreflightError):
                CHECK.graphql(Path("."), "query {}")

    def test_command_errors_do_not_echo_authentication_diagnostics(self):
        completed = subprocess.CompletedProcess([], 1, "", "secret-token")
        with patch.object(CHECK.subprocess, "run", return_value=completed):
            with self.assertRaises(CHECK.PreflightError) as caught:
                CHECK.run(["gh", "api"], Path("."))
        self.assertNotIn("secret-token", str(caught.exception))


class RelatedGitTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.git("init", "-q")
        write(self.root, "pom.xml", pom(["330"]))
        self.start = self.commit("initial")
        self.git("branch", "feature")
        write(self.root, "pom.xml", pom(["330", "410db183"]))
        self.base = self.commit("add profile")
        self.git("update-ref", "refs/remotes/origin/main", self.base)
        self.git("checkout", "-q", "feature")
        self.shim = "sql-plugin/src/main/spark330/scala/example/NewShims.scala"
        write(self.root, self.shim, shim(["330"], "NewShims"))
        self.head = self.commit("add shim")
        self.open = dict(number=12, headRefOid="open-head", state="OPEN", isDraft=True,
                         title="Draft profile support", url="https://example.invalid/12")
        self.merged = dict(number=16291, headRefOid="merged-head", state="MERGED", isDraft=False,
                           title="Profile support", url="https://example.invalid/16291")
        for method, value in [("remote_base", self.base), ("open_prs", [self.open]),
                              ("merged_prs", [self.merged])]:
            mock = patch.object(CHECK, method, return_value=value)
            setattr(self, method, mock.start())
            self.addCleanup(mock.stop)
        mock = patch.object(CHECK, "pr_files", side_effect=lambda root, owner, name, number:
                            (("open-head" if number == 12 else "merged-head"), {"pom.xml"}))
        self.files = mock.start()
        self.addCleanup(mock.stop)

    def git(self, *args):
        return CHECK.git(self.root, *args)

    def commit(self, message):
        self.git("add", ".")
        self.git("-c", "user.name=Shim Preflight Test", "-c", "user.email=preflight@example.invalid",
                 "commit", "-q", "--signoff", "-m", message)
        return self.git("rev-parse", "HEAD")

    def scan(self, validated=None, own=None):
        return CHECK.related_prs(self.root, "NVIDIA/cudf-spark", "origin/main", "HEAD",
                                 validated, "main", own)

    def test_end_to_end_commit_range_and_related_draft(self):
        report = self.scan()
        self.assertEqual("NEEDS_REVIEW", report["status"])
        self.assertEqual(self.start, report["history_start"])
        self.assertEqual(1, report["history_commits"])
        self.assertEqual([12, 16291], [p["number"] for p in report["related"]])
        self.assertTrue(report["related"][0]["draft"])
        self.assertEqual([self.base], self.merged_prs.call_args.args[3])
        self.assertFalse(report["dirty"])

    def test_validated_base_overrides_common_ancestor(self):
        report = self.scan(validated=self.base)
        self.assertEqual(0, report["history_commits"])
        self.assertEqual([], self.merged_prs.call_args.args[3])

    def test_own_pr_is_not_a_dependency(self):
        report = self.scan(own=12)
        self.assertEqual([16291], [p["number"] for p in report["related"]])

    def test_stale_fetched_base_is_incomplete(self):
        self.remote_base.return_value = "new-main"
        with self.assertRaisesRegex(CHECK.PreflightError, "fetch"):
            self.scan()
        self.open_prs.assert_not_called()

    def test_base_movement_during_scan_is_incomplete(self):
        self.remote_base.side_effect = [self.base, "new-main"]
        with self.assertRaisesRegex(CHECK.PreflightError, "changed"):
            self.scan()

    def test_open_head_movement_is_incomplete(self):
        self.open_prs.side_effect = [[self.open], [dict(self.open, headRefOid="new-head")]]
        with self.assertRaisesRegex(CHECK.PreflightError, "changed"):
            self.scan()

    def test_untracked_work_is_marked_dirty(self):
        write(self.root, "notes.txt", "work in progress")
        report = self.scan()
        self.assertTrue(report["dirty"])
        self.assertIn("notes.txt", report["changed_files"])

    def test_missing_head_has_actionable_error(self):
        with self.assertRaisesRegex(CHECK.PreflightError, "Cannot resolve commit"):
            CHECK.related_prs(self.root, "NVIDIA/cudf-spark", "origin/main", "not-a-ref",
                              None, "main", None)


if __name__ == "__main__":
    unittest.main()
