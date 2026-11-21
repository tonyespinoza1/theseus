from __future__ import annotations

import datetime
import hashlib
import io
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import tempfile
import unittest
from contextlib import closing, redirect_stderr, redirect_stdout
import itertools
import random
from unittest import mock

from aim_next import admin, cli, outcome_rules, pipeline_state, supervisor as runner_supervisor
from aim_next.contract import Refusal
from tools import deploy, known_reds, release_check, releases
import test_agent_cli
from test_pipeline_steering import PipelineCase

SHA = "c" * 40
# The policy of expected browser failures (item 481), read at the live commit: the deploy's event base.
POLICY = ".github/check-baseline.json"
# os.kill before any test replaces it with a recorder.
REAL_KILL = os.kill
HEAD = "b" * 40


def log(*commits):
    """`git log` output as the deploy asks for it: one entry per (subject, its Pipeline trailer values[, its Train-Item
    trailer values]), oldest first; commit n's sha is n + 1 in hex.

    Which lines are trailers is git's to read (tests/test_tools_releases.py runs it)."""
    return "".join("%s\x1f%s\x1f%s\x1f%s\x1e\n" % (("%040x" % (n + 1)), subject, "\x1d".join(values), "\x1d".join(train[0] if train else ()))
                   for n, (subject, values, *train) in enumerate(commits))


SHIPPED = log(("One change", ["88"]))


def release(number, commit, items=(), **more):
    return dict({"release": number, "commit": commit, "previous": None, "deployed_at": "2026-11-06T05:28:35-08:00",
                 "items": list(items), "measurements": None, "source": "backfill", "evidence": None}, **more)


def page(*records):
    """The CLI's --json page; a bare number is a record with no items, the first of them at the live HEAD."""
    records = [record if isinstance(record, dict) else release(record, HEAD if index == 0 else "%040x" % record)
               for index, record in enumerate(records)]
    return json.dumps({"instance_id": "i-1", "items": records, "next_cursor": None}) + "\n"


def summary(commit=SHA, failed=()):
    steps = [{"name": name, "exit": int(name in failed), "passed": name not in failed} for name in release_check.REQUIRED]
    return {"tool": release_check.TOOL, "commit": commit, "tree_unchanged": True, "passed": not failed,
            "required": list(release_check.REQUIRED), "steps": steps}


class FakeHost:
    """Answers the deploy's ps and git calls from a table and records every call; nothing real runs."""

    def __init__(self, config, servers=(111,), runners=(222,), supervisors=(), children=(), dirty="", ff=True, backup=0,
                 merge=0, migrate=0, spelled_as_deployed=False, changed=(),
                 log=SHIPPED, keeps=True, releases=(page(19), 0), cli=0, recorded=0, on_cli=None, logs=None,
                 kept=(), listed=0, at=SHA, line=(HEAD,), others=(), policy=None, policies=None, head=HEAD):
        """runners are bare runners (as started before supervision); children are (pid, parent) runner processes,
        spelled the way the real supervisor starts them (its own interpreter and its checkout's resolved path), or
        exactly as the deploy spells a runner when those are the same."""
        self.spelled_as_deployed = spelled_as_deployed
        # changed: the paths git says differ between the live HEAD and the commit; None when git cannot say. No file
        # names a unit module, and the replay imports no tools module.
        self.changed = changed
        self.config, self.calls = config, []
        self.servers, self.runners = list(servers), list(runners)
        self.supervisors, self.children = list(supervisors), list(children)
        self.dirty, self.ff, self.backup, self.merge, self.migrate = dirty, ff, backup, merge, migrate
        self.log, self.keeps, self.releases, self.cli, self.recorded = log, keeps, releases, cli, recorded
        self.written, self.on_cli = [], on_cli
        # keeps: whether the live code keeps release records, None when git cannot say. git log answers per range
        # (default: self.log), an (output, exit) pair for a range git cannot read. kept: the records
        # tools/pipeline/releases holds at `at`, the commit deployed, listed with exit `listed`. line: the live
        # checkout's history, oldest first; others: commits git knows on another line. Any other commit is one git
        # cannot read (exit 128).
        self.logs, self.kept, self.listed, self.at = logs or {}, list(kept), listed, at
        self.line, self.others = list(line), set(others)
        # policy: the .github/check-baseline.json every commit keeps (item 481): None for none, a JSON object, or
        # "unreadable" for a file git lists but cannot show; policies: {commit: that}, each commit its own, in its place.
        # head: the live checkout's HEAD.
        self.policy, self.policies, self.head = policy, policies, head

    def __call__(self, argv, **kw):
        self.calls.append(argv)
        out, code = "", 0
        if argv[:2] == ["ps", "-axo"]:
            server = " ".join(deploy.server_argv(self.config))
            runner = " ".join(deploy.runner_argv(self.config))
            supervisor = " ".join(deploy.supervisor_argv(self.config))
            out = "".join("%d 1 %s\n" % (pid, server) for pid in self.servers)
            out += "".join("%d 1 %s\n" % (pid, runner) for pid in self.runners)
            out += "".join("%d 1 %s\n" % (pid, supervisor) for pid in self.supervisors)
            child = runner if self.spelled_as_deployed else " ".join(runner_supervisor.child_argv(deploy.runner_argv(self.config)[3:]))
            out += "".join("%d %d %s\n" % (pid, parent, child) for pid, parent in self.children)
            out += "333 1 /usr/bin/python3 -I unrelated\n"
        elif argv[:2] == ["ps", "-o"]:
            out = "Thu Nov  5 15:02:31 2026     process %s" % argv[-1]
        elif argv[0] == "git" and "rev-parse" in argv and argv[-1].endswith("^{tree}"):
            # Item 588: a tree is read by real git, from the live path: a real repository where a case makes one, and
            # no repository at all elsewhere, so git cannot read it there.
            return subprocess.run(argv, capture_output=True, text=True)
        elif argv[0] == "git" and "rev-parse" in argv:
            out = self.head + "\n"
        elif argv[0] == "git" and "diff" in argv:
            out, code = ("\0".join(self.changed) + "\0", 0) if self.changed is not None else ("", 128)
        elif argv[0] == "git" and "grep" in argv:
            code = 1
        elif argv[0] == "git" and "status" in argv:
            out = self.dirty
        elif argv[0] == "git" and "merge-base" in argv and argv[-1] == SHA:
            code = 0 if self.ff else 1
        elif argv[0] == "git" and "merge-base" in argv:
            older, newer = argv[-2:]
            if not {older, newer} <= set(self.line) | self.others:
                code = 128
            else:
                code = 0 if older == newer or (older in self.line and newer in self.line
                                               and self.line.index(older) < self.line.index(newer)) else 1
        elif argv[0] == "git" and "merge" in argv:
            code = self.merge
        elif argv[0] == "git" and "--format=%(trailers:key=Pipeline,valueonly,separator=%x1d)" in argv:
            # Admission's separate trailer-only read; these host cases own no pending pipeline items.
            out = ""
        elif argv[0] == "git" and "log" in argv:
            out = self.logs.get(argv[-1], self.log)
            if isinstance(out, tuple):
                out, code = out
        elif argv[0] == "git" and "ls-tree" in argv and argv[-1] == POLICY:
            out = POLICY + "\n" if self.policy_at(argv[-3]) is not None else ""
        elif argv[0] == "git" and "show" in argv and argv[-1].endswith(":" + POLICY):
            found = self.policy_at(argv[-1][:-len(":" + POLICY)])
            out, code = ("", 128) if found in ("unreadable", None) else (json.dumps(found), 0)
        elif argv[0] == "git" and "ls-tree" in argv and argv[-1] == "aim_next/releases.py":
            out, code = ("aim_next/releases.py\n" if self.keeps else "", 0) if self.keeps is not None else ("", 128)
        elif argv[0] == "git" and "ls-tree" in argv:
            out = "".join("tools/pipeline/releases/release-%s.json\n" % record["release"] for record in self.kept)
            code = self.listed
        elif argv[0] == "git" and "show" in argv:
            out = json.dumps([record for record in self.kept
                              if argv[-1] == "%s:tools/pipeline/releases/release-%s.json" % (self.at, record["release"])][0])
        elif "releases" in argv:
            out, code = self.releases
        elif "record-release" in argv:
            # What the admin verb was handed, read while the file is there.
            self.written.append(json.loads(Path(argv[argv.index("--record") + 1]).read_text()))
            out, code = ('{"ok":true}\n', 0) if self.recorded == 0 else ('{"ok":false,"error":{"code":"RELEASE_EXISTS"}}\n', 2)
        elif "-c" in argv:
            code = self.backup
        elif "migrate" in argv:
            code = self.migrate
        elif "--broker" in argv and argv[-1] == "binding":
            out = json.dumps({"binding": {"instance_id": "i-1"}})
        elif "--broker" in argv and argv[-2:] == ["view", "-"]:
            request = json.loads(kw["input"])
            assert request["view"]["name"] == "pipeline"
            out = json.dumps({"instance_id": "i-1", "workspace_id": "scratch-pipeline", "relevant_versions": {"pipeline": 1},
                "pipeline": {"enabled": True, "ships_in_releases": True}, "pieces": [], "items": [],
                "facts": {"approved_waiting": [], "next_release": None}, "next_cursor": None})
        elif argv[-1] == "binding":
            if self.on_cli:
                self.on_cli()
            out, code = "i-1\n", self.cli
        else:
            raise AssertionError("unexpected command %r" % argv)
        return subprocess.CompletedProcess(argv, code, out, "")

    def ran(self, word):
        return [argv for argv in self.calls if word in argv]

    def policy_at(self, commit):
        return self.policy if self.policies is None else self.policies.get(commit)


class DeployCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        base = Path(self.tmp.name)
        self.instance = base / "instance"
        for name, pid, last in (("run-server-03", 111, None), ("run-runner-01", 9, None), ("run-runner-02", 222, '{"status": "stopped"}')):
            (self.instance / name).mkdir(parents=True)
            (self.instance / name / "pid.json").write_text(json.dumps({"pid": pid}))
            if last:
                (self.instance / name / "stdout.log").write_text('{"event": "acquired"}\n' + last + "\n")
        (self.instance / "tony.profile.json").write_text(json.dumps({"instance_id": "i-1"}))
        self.raw = {"live": str(base / "live"), "instance": str(self.instance), "server_profile": "tony.profile.json",
                    "port": 5192, "tailnet_origin": "https://host.example:5192", "runner_profile": "agent.profile.json",
                    "runner_id": "runner-01", "routes": "routes.json",
                    "pipeline_outcome": "scratch-pipeline", "pipeline_instance": "i-1",
                    "pipeline_broker": str(self.instance / "admission.sock")}
        self.config_path = base / "deploy.json"
        self.config_path.write_text(json.dumps(self.raw))
        self.config = deploy.load_config(self.config_path)
        self.evidence = base / "evidence"
        self.kills = []
        self.summary = base / "summary.json"
        self.summary.write_text(json.dumps(summary()))

    def tearDown(self):
        self.tmp.cleanup()

    def deploy(self, host, dry=False, sha=SHA, **release):
        # What the deploy printed, kept for the lines the operator reads, a refusal or not.
        self.printed = io.StringIO()
        with mock.patch.object(deploy, "run", host), \
                mock.patch.object(deploy.os, "kill", lambda pid, sig: self.kills.append((pid, sig))), \
                mock.patch.object(deploy, "wait_exit", return_value="exited"), \
                mock.patch.object(deploy, "start", side_effect=AssertionError("must not start a process")), \
                redirect_stdout(self.printed):
            deploy.deploy(self.config, sha, self.evidence, self.summary, dry, **release)

    def known_lines(self):
        """The lines the deploy printed about known reds, in order."""
        return [line for line in self.printed.getvalue().splitlines() if line.startswith("known red")]

    def planned(self, host, **release):
        """A dry run that must not refuse; a refusal fails the test rather than erroring it."""
        try:
            self.deploy(host, dry=True, **release)
        except deploy.Refused as refusal:
            self.fail("the deploy refused: %s" % refusal)

    def refused(self, host, dry=False, sha=SHA, **release):
        """The deploy's refusal; a crash is not one."""
        try:
            self.deploy(host, dry, sha, **release)
        except deploy.Refused as refusal:
            return str(refusal)
        except Exception as error:  # the failure under test: anything but a refusal
            self.fail("the deploy did not refuse, it raised %r" % error)
        self.fail("the deploy was not refused")

    def records(self):
        return sorted(path.stem for path in self.evidence.glob("*.json"))

    def complete_measurements(self, record):
        """Read the real sidecar, its identity and exact compact projection."""
        path = self.evidence.resolve() / ("release-%d-measurements.json" % record["release"])
        raw = path.read_bytes()
        full = json.loads(raw)
        self.assertEqual(raw, json.dumps(full, sort_keys=True, separators=(',', ':')).encode())
        self.assertEqual(record['measurements']['full_measurements'],
                         dict(version=1, path=str(path), sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw)))
        self.assertEqual(releases.verify_record(dict(record, evidence=str(self.evidence.resolve())), self.evidence.resolve()), full)
        return full


class Config(DeployCase):
    def test_the_config_names_every_machine_path_and_the_argv_is_built_from_it(self):
        base = Path(self.raw["live"])
        self.assertEqual(deploy.server_argv(self.config), [
            "/usr/bin/python3", "-I", str(base / "bin/aim-next-server"), "--profile", str(self.instance / "tony.profile.json"),
            "--port", "5192", "--tailnet-origin", "https://host.example:5192"])
        self.assertEqual(deploy.runner_argv(self.config), [
            "/usr/bin/python3", "-I", str(base / "bin/aim-next-runner"), "--profile", str(self.instance / "agent.profile.json"),
            "--runner-id", "runner-01", "--routes", str(self.instance / "routes.json")])
        # The deploy starts the runner under its supervisor, which runs it with exactly these arguments.
        self.assertEqual(deploy.supervisor_argv(self.config),
                         deploy.runner_argv(self.config)[:3] + ["supervise"] + deploy.runner_argv(self.config)[3:])
        del self.raw["tailnet_origin"]
        self.raw.update(python="/opt/python3", routes="/elsewhere/routes.json")
        self.config_path.write_text(json.dumps(self.raw))
        config = deploy.load_config(self.config_path)
        self.assertEqual(deploy.server_argv(config)[-2:], ["--port", "5192"])
        self.assertEqual(deploy.runner_argv(config)[0], "/opt/python3")
        self.assertEqual(deploy.runner_argv(config)[-1], "/elsewhere/routes.json")

    def test_a_malformed_config_is_refused(self):
        for change in ({"port": "5192"}, {"port": 0}, {"live": "relative/live"}, {"python": "python3"},
                       {"runner_id": ""}, {"extra": 1}):
            with self.subTest(change):
                raw = dict(self.raw, **change)
                self.config_path.write_text(json.dumps(raw))
                with self.assertRaises(deploy.Refused):
                    deploy.load_config(self.config_path)
        for key in deploy.REQUIRED_KEYS:
            with self.subTest(missing=key):
                self.config_path.write_text(json.dumps({k: v for k, v in self.raw.items() if k != key}))
                with self.assertRaises(deploy.Refused):
                    deploy.load_config(self.config_path)
        self.config_path.write_text("[]")
        with self.assertRaises(deploy.Refused):
            deploy.load_config(self.config_path)
        with self.assertRaises(deploy.Refused):
            deploy.load_config(self.config_path.with_name("absent.json"))


class Refusals(DeployCase):
    def test_a_dry_run_reads_and_records_the_plan_and_changes_nothing(self):
        host = FakeHost(self.config)
        self.deploy(host, dry=True)
        self.assertEqual(self.records(), ["00-plan", "00-release-check", "01-live", "02-checkout", "02-release", "release-20-measurements"])
        planned = json.loads((self.evidence / '02-release.json').read_text())['record']
        self.complete_measurements(planned)
        self.assertFalse((self.evidence / 'release-20.json').exists())
        self.assertEqual(self.kills, [])
        self.assertEqual(host.ran("merge") + host.ran("migrate") + host.ran("-c"), [])
        live = json.loads((self.evidence / "01-live.json").read_text())
        self.assertEqual([pid for pid, _ in live["server"]], [111])
        self.assertEqual([pid for pid, _ in live["runner"]], [222])

    def test_unexpected_live_processes_are_refused_before_anything_stops(self):
        for host, reason in ((FakeHost(self.config, servers=()), "expected one server"),
                             (FakeHost(self.config, servers=(111, 112)), "expected one server"),
                             (FakeHost(self.config, runners=(222, 223)), "expected one server"),
                             (FakeHost(self.config, servers=(110,)), "not the ones their run records started"),
                             (FakeHost(self.config, runners=(221,)), "not the ones their run records started")):
            with self.subTest(reason=reason, servers=host.servers, runners=host.runners):
                self.assertIn(reason, self.refused(host))
                self.assertEqual(self.kills, [])

    def test_a_supervised_runner_is_stopped_through_its_supervisor(self):
        for spelled in (False, True):
            with self.subTest(spelled_as_deployed=spelled):
                self.kills = []
                kw = dict(runners=(), supervisors=(222,), children=((224, 222),), spelled_as_deployed=spelled)
                self.assertIn("migration failed", self.refused(FakeHost(self.config, migrate=1, **kw)))
                self.assertEqual(self.kills, [(222, 15), (111, 15)])
                self.kills = []
                self.deploy(FakeHost(self.config, **kw), dry=True)
                live = json.loads((self.evidence / "01-live.json").read_text())
                self.assertEqual(([pid for pid, _ in live["supervisor"]], [pid for pid, _ in live["runner"]]), ([222], [224]))

    def test_a_supervised_runner_that_did_not_stop_cleanly_is_caught_however_its_path_is_spelled(self):
        # The supervisor starts its child by its own interpreter and resolved checkout path, not the deploy's spelling.
        (self.instance / "run-runner-02" / "stdout.log").write_text('{"status": "paused", "reason": "storage_failure"}\n')
        host = FakeHost(self.config, runners=(), supervisors=(222,), children=((224, 222),))
        self.assertIn("did not stop cleanly", self.refused(host))
        self.assertEqual(self.kills, [(222, 15), (111, 15)])

    def test_a_supervisor_between_runs_is_stopped_without_a_runner_line(self):
        (self.instance / "run-runner-02" / "stdout.log").write_text('{"status": "idle_lease_lost"}\n')
        host = FakeHost(self.config, runners=(), supervisors=(222,), migrate=1)
        self.assertIn("migration failed", self.refused(host))
        self.assertEqual(self.kills, [(222, 15), (111, 15)])

    def test_runners_that_are_not_the_supervisors_own_are_refused_before_anything_stops(self):
        for kw, reason in ((dict(runners=(224,), supervisors=(222,), children=((225, 222),)), "not its supervisor's"),
                           (dict(runners=(224,), supervisors=(222,)), "not its supervisor's"),
                           (dict(runners=(), supervisors=(222,), children=((224, 222), (225, 222))), "at most one runner"),
                           (dict(runners=(), supervisors=(222, 223)), "at most one runner"),
                           (dict(runners=(), supervisors=(221,), children=((224, 221),)), "not the ones their run records started")):
            with self.subTest(**{k: str(v) for k, v in kw.items()}):
                self.assertIn(reason, self.refused(FakeHost(self.config, **kw)))
                self.assertEqual(self.kills, [])

    def test_a_runner_left_behind_by_a_supervisor_that_died_is_refused_however_its_path_is_spelled(self):
        # A killed supervisor leaves its runner to pid 1, spelled the way the supervisor started it, not the deploy's way.
        for kw, reason in ((dict(runners=(), supervisors=(), children=((224, 1),)), "not the ones their run records started"),
                           (dict(runners=(), supervisors=(222,), children=((224, 1), (225, 222))), "not its supervisor's")):
            for spelled in (False, True):
                with self.subTest(spelled_as_deployed=spelled, **{k: str(v) for k, v in kw.items()}):
                    self.kills = []
                    self.assertIn(reason, self.refused(FakeHost(self.config, spelled_as_deployed=spelled, **kw)))
                    self.assertEqual(self.kills, [])

    def test_a_dirty_or_diverged_live_checkout_is_refused_before_anything_stops(self):
        for host in (FakeHost(self.config, dirty=" M aim_next/store.py\n"), FakeHost(self.config, ff=False)):
            self.assertIn("fast-forward", self.refused(host))
            self.assertEqual(self.kills, [])
            self.assertEqual(json.loads((self.evidence / "02-checkout.json").read_text())["target"], SHA)

    def test_a_runner_that_did_not_stop_cleanly_stops_the_deploy_before_the_backup(self):
        (self.instance / "run-runner-02" / "stdout.log").write_text('{"status": "paused"}\n')
        host = FakeHost(self.config)
        self.assertIn("did not stop cleanly", self.refused(host))
        self.assertEqual(self.kills, [(222, 15), (111, 15)])
        self.assertEqual(host.ran("-c"), [])

    def test_a_failed_backup_stops_before_the_fast_forward(self):
        host = FakeHost(self.config, backup=1)
        self.assertIn("backup failed", self.refused(host))
        self.assertEqual([argv for argv in host.ran("merge") if "--ff-only" in argv], [])

    def test_a_failed_fast_forward_or_migration_stops_before_any_restart(self):
        host = FakeHost(self.config, merge=1)
        self.assertIn("fast-forward failed", self.refused(host))
        self.assertEqual(host.ran("migrate"), [])
        host = FakeHost(self.config, migrate=1)
        self.assertIn("migration failed", self.refused(host))
        migrate = host.ran("migrate")[0]
        self.assertEqual(migrate[migrate.index("--db") + 1], str(self.instance / "data.sqlite3"))
        self.assertEqual(migrate[migrate.index("--instance") + 1], "i-1")

    def test_main_exits_one_on_a_refusal(self):
        host = FakeHost(self.config, ff=False)
        with mock.patch.object(deploy, "run", host), redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()) as err:
            code = deploy.main(["--config", str(self.config_path), "--release-check", str(self.summary), SHA,
                                str(self.evidence), "--dry-run"])
        self.assertEqual(code, 1)
        self.assertIn("refused:", err.getvalue())


class ReleasePlan(DeployCase):
    """Before anything stops, the deploy works out the release record it will write and checks it can be written."""

    def plan(self):
        return json.loads((self.evidence / "02-release.json").read_text())

    def test_the_items_come_from_every_commit_shipped_in_order_and_the_number_follows_the_latest(self):
        shipped = log(("Close keeps the writing", ["87, 2"]), ("A check for the saving sign", []),
                      ("Merge branch 'next-90'", ["2", "90"]))
        host = FakeHost(self.config, log=shipped, releases=(page(19, 18), 0))
        self.deploy(host, dry=True)
        plan = self.plan()
        self.assertEqual({key: plan["record"][key] for key in ("release", "commit", "previous", "items", "source")},
                         {"release": 20, "commit": SHA, "previous": HEAD, "items": [87, 2, 90], "source": "deploy"})
        self.assertEqual([commit["items"] for commit in plan["commits"]], [[87, 2], [], [2, 90]])
        self.assertEqual(plan["latest_recorded"], 19)
        self.assertIsNone(plan["restart_of"], "a deploy that ships a release restarts none")
        log_call = host.ran("log")[0]
        self.assertEqual(log_call[-1], "%s..%s" % (HEAD, SHA))
        self.assertIn("--reverse", log_call)
        self.assertEqual(self.kills, [])

    def test_the_record_carries_the_release_checks_counts(self):
        written = summary()
        for step in written["steps"]:
            step["counts"] = ({"tests": 1106, "status": "OK"} if step["name"] == "python-units" else
                              {"tests": 740, "pass": 740} if step["name"] == "node-units" else {"tests": 3, "pass": 2})
        self.summary.write_text(json.dumps(written))
        self.deploy(FakeHost(self.config), dry=True)
        measured = self.plan()["record"]["measurements"]
        natives = len(release_check.NATIVE_STEPS)
        self.assertEqual((measured["python_units"], measured["node_units"]), ({"tests": 1106}, {"tests": 740, "pass": 740}))
        self.assertEqual((measured["native"]["pass"], measured["native"]["total"]), (2 * natives, 3 * natives))
        self.assertEqual(measured["native"]["steps"]["journal"], {"pass": 2, "total": 3})
        self.assertEqual(set(measured["native"]["steps"]), {step.name for step in release_check.NATIVE_STEPS})

    def test_a_pipeline_trailer_that_names_no_item_numbers_stops_the_deploy_before_anything_stops(self):
        for value in ("next", "87 and 2", "", "087", "2,"):
            with self.subTest(value):
                host = FakeHost(self.config, log=log(("Something", ["3", value])))
                self.assertIn("does not name pipeline item numbers", self.refused(host))
                self.assertEqual((self.kills, host.ran("-c")), ([], []))

    def test_a_record_the_store_would_refuse_stops_the_deploy_before_anything_stops(self):
        # 501 items: more than a record holds, so the record could never be written once the release is live.
        host = FakeHost(self.config, log=log(("Everything", [", ".join(str(n) for n in range(1, 502))])))
        self.assertIn("the release record could not be prepared: items is a list of at most 500", self.refused(host))
        self.assertEqual((self.kills, host.ran("-c"), self.records()[-1]), ([], [], "02-checkout"))

    def test_a_release_that_names_no_item_says_so_or_stops_before_anything_stops(self):
        quiet = log(("A check", []), ("Another check", []))
        host = FakeHost(self.config, log=quiet)
        self.assertIn("--no-items", self.refused(host))
        self.assertEqual((self.kills, host.ran("-c")), ([], []))
        self.deploy(FakeHost(self.config, log=quiet), dry=True, no_items=True)
        self.assertEqual(self.plan()["record"]["items"], [])

    def test_the_first_record_needs_its_number_and_a_named_number_must_follow_the_latest(self):
        # Live code from before release records: the migration starts an empty table.
        host = FakeHost(self.config, keeps=False)
        self.assertIn("--release", self.refused(host))
        self.assertEqual((self.kills, host.ran("releases")), ([], []))
        self.deploy(FakeHost(self.config, keeps=False), dry=True, number=20)
        self.assertEqual(self.plan()["record"]["release"], 20)
        # Live code that keeps records, none written yet.
        self.assertIn("--release", self.refused(FakeHost(self.config, releases=(page(), 0))))
        self.assertIn("would not follow the latest, 19", self.refused(FakeHost(self.config), number=21))
        self.deploy(FakeHost(self.config), dry=True, number=20)
        self.assertEqual(self.plan()["record"]["release"], 20)
        self.assertEqual(self.kills, [])

    def launched(self, revision, at="2026-11-07T05:00:00-08:00"):
        """The live server's run record, as the deploy that started it wrote it."""
        (self.instance / "run-server-03" / "launch.json").write_text(json.dumps({"at": at, "revision": revision}))

    def test_a_live_commit_no_record_names_is_recorded_first_from_its_servers_run_record(self):
        # Release 20 went live at HEAD, but its deploy stopped before writing the record.
        X = "a" * 40
        logs = {"%s..%s" % (X, HEAD): log(("Search", ["89"]), ("Ring", ["74"])), "%s..%s" % (HEAD, SHA): SHIPPED}
        answer = (page(release(19, X, [87])), 0)
        self.launched(HEAD)
        self.deploy(FakeHost(self.config, releases=answer, logs=logs, line=[X, HEAD]), dry=True)
        plan = self.plan()
        self.assertEqual(plan["before"], [{"release": 20, "commit": HEAD, "previous": X, "items": [89, 74],
                                           "deployed_at": "2026-11-07T05:00:00-08:00", "measurements": None,
                                           "source": "backfill", "evidence": None}])
        self.assertEqual({key: plan["record"][key] for key in ("release", "previous", "items")},
                         {"release": 21, "previous": HEAD, "items": [88]})
        self.assertEqual(plan["latest_recorded"], 20)
        self.assertEqual(self.kills, [])

    def test_when_no_run_record_says_when_the_live_commit_went_live_the_deploy_needs_that_time(self):
        X = "a" * 40
        answer = (page(release(19, X, [87])), 0)
        host = FakeHost(self.config, releases=answer, logs={"%s..%s" % (X, HEAD): log()}, line=["d" * 40, X, HEAD])
        self.assertIn("name that time with --live-since", self.refused(host))
        self.launched("d" * 40)  # the server's run record names a commit before the latest record
        self.assertIn("name that time with --live-since", self.refused(host))
        self.deploy(host, dry=True, live_since="2026-11-07T04:00:00-08:00")
        self.assertEqual([(record["release"], record["deployed_at"]) for record in self.plan()["before"]],
                         [(20, "2026-11-07T04:00:00-08:00")])
        self.launched(HEAD)
        self.assertIn("--live-since is not needed", self.refused(host, live_since="2026-11-07T04:00:00-08:00"))
        self.assertIn("--live-since names when an unrecorded live commit went live",
                      self.refused(FakeHost(self.config), live_since="2026-11-07T04:00:00-08:00"))
        # The time it is given is checked like any record's.
        (self.instance / "run-server-03" / "launch.json").unlink()
        self.assertIn("the release record would be refused: deployed_at", self.refused(host, live_since="yesterday"))
        self.assertEqual(self.kills, [])

    def test_a_live_commit_that_does_not_follow_the_latest_record_stops_the_deploy(self):
        answer = (page(release(19, "a" * 40, [87])), 0)
        self.launched(HEAD)
        self.assertIn("does not follow", self.refused(FakeHost(self.config, releases=answer, others=["a" * 40])))
        self.assertEqual(self.kills, [])

    def test_an_instance_with_no_records_starts_from_the_records_the_repository_keeps(self):
        kept = [release(18, "1" * 40, [1, 4]), release(19, HEAD, [87, 2])]
        # Live code from before release records keeps none; the records kept at SHA name the live HEAD.
        host = FakeHost(self.config, keeps=False, kept=kept, line=["1" * 40, HEAD])
        self.deploy(host, dry=True)
        plan = self.plan()
        self.assertEqual((plan["before"], plan["record"]["release"], plan["latest_recorded"]), (kept, 20, 19))
        self.assertIn("would not follow the latest, 19", self.refused(FakeHost(self.config, keeps=False, kept=kept, line=["1" * 40, HEAD]), number=2))
        # An instance whose live commit the kept records do not reach starts from the number it is given.
        elsewhere = [release(18, "1" * 40, [1, 4]), release(19, "2" * 40, [87, 2])]
        self.assertIn("name this one's number with --release", self.refused(FakeHost(self.config, keeps=False, kept=elsewhere, others=["1" * 40, "2" * 40])))
        self.deploy(FakeHost(self.config, keeps=False, kept=elsewhere, others=["1" * 40, "2" * 40]), dry=True, number=1)
        self.assertEqual((self.plan()["before"], self.plan()["record"]["release"]), ([], 1))
        # A kept record that is not one, or names no number, stops the deploy.
        for broken, reason in ((dict(kept[1], items="87"), "is not one: items"), (dict(kept[1], release=None), "names no release number")):
            with self.subTest(reason):
                self.assertIn(reason, self.refused(FakeHost(self.config, keeps=False, kept=[kept[0], broken], line=["1" * 40, HEAD])))
        # Records already on the instance are not written again.
        host = FakeHost(self.config, kept=kept, line=["1" * 40, HEAD], releases=(page(*reversed(kept)), 0))
        self.deploy(host, dry=True)
        self.assertEqual((self.plan()["before"], self.plan()["record"]["release"]), ([], 20))
        self.assertEqual(self.kills, [])

    def test_an_item_an_earlier_release_shipped_stops_the_deploy_before_anything_stops(self):
        host = FakeHost(self.config, releases=(page(release(19, HEAD, [88])), 0))
        self.assertIn("pipeline item 88 shipped in release 19; release 20 names it again", self.refused(host))
        kept = [release(18, "1" * 40, [88]), release(19, HEAD, [87])]
        self.assertIn("pipeline item 88 shipped in release 18; release 20 names it again",
                      self.refused(FakeHost(self.config, keeps=False, kept=kept, line=["1" * 40, HEAD])))
        X = "a" * 40
        self.launched(HEAD)
        recovered = FakeHost(self.config, releases=(page(release(19, X, [87])), 0), line=[X, HEAD],
                             logs={"%s..%s" % (X, HEAD): log(("Again", ["87"]))})
        self.assertIn("pipeline item 87 shipped in release 19; release 20 names it again", self.refused(recovered))
        self.assertEqual(self.kills, [])

    # The shape of e6b29056..af10137c, where release 76's record had to name 464 alone (codex #4651, #4654): 412, then
    # 464, then 398 and 399 on a merge; 398 and 399 reverted by a commit from before the trailer rule, 412 by a trailer.
    REVERTED = log(("Dossiers drawn as a stack", ["412"]), ("The composer grows", ["464"]),
                   ("Merge train 4", ["398, 399"]), ("Train 5: revert 398/399", []),
                   ("Train 8: revert 412", [], ["Revert 412"]))

    def test_a_reverted_item_ships_nothing_and_the_record_says_how_it_was_taken_back(self):
        # Each scenario is a distinct plan; its published evidence is immutable.
        self.evidence = Path(self.tmp.name) / "reverted-by-flag"
        self.deploy(FakeHost(self.config, log=self.REVERTED), dry=True, revert=[398, 399])
        record = self.plan()["record"]
        self.assertEqual(record["items"], [464])
        self.assertEqual(record["measurements"]["reverts"], [{"item": 412, "commit": "%040x" % 5, "by": "trailer"},
                                                              {"item": 398, "commit": None, "by": "flag"},
                                                              {"item": 399, "commit": None, "by": "flag"}])
        # Without the flag the trailer still takes 412 back, and only 412.
        self.evidence = Path(self.tmp.name) / "reverted-by-trailer"
        self.deploy(FakeHost(self.config, log=self.REVERTED), dry=True)
        self.assertEqual(self.plan()["record"]["items"], [464, 398, 399])
        # A release with nothing reverted says nothing of it.
        self.evidence = Path(self.tmp.name) / "nothing-reverted"
        self.deploy(FakeHost(self.config), dry=True)
        self.assertNotIn("reverts", self.plan()["record"]["measurements"])
        self.assertEqual(self.kills, [])

    def test_a_flag_names_only_an_item_the_range_ships_and_no_trailer_took_back(self):
        for flagged, why in (([500], "--revert names [500]"), ([412], "--revert names [412]"), ([88], "--revert names [88]")):
            with self.subTest(flagged):
                host = FakeHost(self.config, log=self.REVERTED)
                self.assertIn(why, self.refused(host, revert=flagged))
                self.assertEqual((self.kills, host.ran("-c")), ([], []))

    def test_a_reverted_item_comes_back_in_a_later_release_and_a_shipped_one_still_cannot(self):
        # Release 19 is the one the revert ran in: it names 464 alone. A later range that ships 412 again is a release.
        earlier = page(release(19, HEAD, [464]))
        self.deploy(FakeHost(self.config, log=log(("Dossiers again", ["412"])), releases=(earlier, 0)), dry=True)
        self.assertEqual(self.plan()["record"]["items"], [412])
        self.assertIn("pipeline item 464 shipped in release 19; release 20 names it again",
                      self.refused(FakeHost(self.config, log=log(("The composer again", ["464"])), releases=(earlier, 0))))
        # Had release 19 named what its range claimed, 412's return would be refused: the record is what lets it back.
        claimed = page(release(19, HEAD, [412, 464, 398, 399]))
        self.assertIn("pipeline item 412 shipped in release 19; release 20 names it again",
                      self.refused(FakeHost(self.config, log=log(("Dossiers again", ["412"])), releases=(claimed, 0))))
        self.assertEqual(self.kills, [])

    def test_a_revert_trailer_that_names_no_item_numbers_stops_the_deploy_before_anything_stops(self):
        for value in ("Revert next", "Revert 412 and 413", "Revert"):
            with self.subTest(value):
                host = FakeHost(self.config, log=log(("Something", ["3"]), ("Take it back", [], [value])))
                self.assertIn("does not name pipeline item numbers", self.refused(host))
                self.assertEqual((self.kills, host.ran("-c")), ([], []))

    def started(self, *runs):
        """Server run records after run-server-03, one per (commit, time), as the deploys that started them wrote them."""
        for number, (revision, at) in enumerate(runs, 4):
            run = self.instance / ("run-server-%02d" % number)
            run.mkdir()
            (run / "launch.json").write_text(json.dumps({"at": at, "revision": revision}))
            (run / "pid.json").write_text(json.dumps({"pid": 111}))

    def summarised(self):
        return [(record["release"], record["commit"], record["previous"], record["items"], record["deployed_at"])
                for record in self.plan()["before"]]

    def test_the_records_the_repository_keeps_fill_in_what_the_instance_lacks_and_continue_from_them(self):
        X, Y = "a" * 40, "e" * 40
        # Release 18 was recorded by hand; release 19's command was refused, or never run.
        kept = [release(18, X, [1, 4]), release(19, HEAD, [87, 2])]
        self.planned(FakeHost(self.config, kept=kept, releases=(page(kept[0]), 0), line=[X, HEAD]))
        self.assertEqual((self.plan()["before"], self.plan()["record"]["release"]), ([kept[1]], 20))
        # Only 19 was recorded by hand: 18 is written, below it.
        self.planned(FakeHost(self.config, kept=kept, releases=(page(kept[1]), 0), line=[X, HEAD]))
        self.assertEqual((self.plan()["before"], self.plan()["record"]["release"]), ([kept[0]], 20))
        # Then release 20 went live and its record was refused: it follows 19, from its own commits and first start.
        kept = [release(18, X, [1, 4]), release(19, Y, [87, 2])]
        self.started((Y, "2026-11-06T05:28:11-08:00"), (HEAD, "2026-11-07T05:00:00-08:00"))
        logs = {"%s..%s" % (Y, HEAD): log(("Release 20's change", ["88"])),
                "%s..%s" % (X, HEAD): log(("Release 19's change", ["87"]), ("Release 20's change", ["88"])),
                "%s..%s" % (HEAD, SHA): log(("Release 21's change", ["89"]))}
        self.planned(FakeHost(self.config, kept=kept, releases=(page(kept[0]), 0), line=[X, Y, HEAD], logs=logs))
        self.assertEqual(self.plan()["before"][0], kept[1])
        self.assertEqual(self.summarised()[1:], [(20, HEAD, Y, [88], "2026-11-07T05:00:00-08:00")])
        self.assertEqual({key: self.plan()["record"][key] for key in ("release", "items")}, {"release": 21, "items": [89]})
        self.assertEqual(self.kills, [])

    def test_each_commit_a_server_started_after_the_latest_record_is_a_release_of_its_own(self):
        # Releases 20 and 21 went live and neither verified; each server was restarted at its own commit.
        W, X, R = "9" * 40, "a" * 40, "d" * 40
        self.started((W, "2026-11-04T10:00:00-08:00"), (X, "2026-11-05T10:00:00-08:00"), (R, "2026-11-06T10:00:00-08:00"),
                     (R, "2026-11-06T11:00:00-08:00"), (HEAD, "2026-11-07T10:00:00-08:00"), (HEAD, "2026-11-07T12:00:00-08:00"))
        logs = {"%s..%s" % (X, R): log(("Twenty", ["89"])), "%s..%s" % (R, HEAD): log(("Twenty-one", ["74"])),
                "%s..%s" % (X, HEAD): log(("Twenty", ["89"]), ("Twenty-one", ["74"])), "%s..%s" % (HEAD, SHA): SHIPPED}
        host = FakeHost(self.config, releases=(page(release(19, X, [87])), 0), line=[W, X, R, HEAD], logs=logs)
        self.planned(host)
        self.assertEqual(self.summarised(), [(20, R, X, [89], "2026-11-06T10:00:00-08:00"),
                                             (21, HEAD, R, [74], "2026-11-07T10:00:00-08:00")])
        self.assertEqual((self.plan()["record"]["release"], self.plan()["latest_recorded"]), (22, 21))
        # A commit no run record started, between them, is not a release: the next start at a later commit includes it.
        self.assertEqual(self.kills, [])

    def test_a_live_commit_no_run_record_started_takes_the_time_it_is_given_and_only_it_does(self):
        X, R = "a" * 40, "d" * 40
        self.started((R, "2026-11-06T10:00:00-08:00"))
        logs = {"%s..%s" % (X, R): log(("Twenty", ["89"])), "%s..%s" % (R, HEAD): log(("Twenty-one", ["74"])),
                "%s..%s" % (HEAD, SHA): SHIPPED}
        host = FakeHost(self.config, releases=(page(release(19, X, [87])), 0), line=[X, R, HEAD], logs=logs)
        self.assertIn("name that time with --live-since, and the deploy records it as release 21", self.refused(host))
        self.planned(host, live_since="2026-11-07T04:00:00-08:00")
        self.assertEqual(self.summarised(), [(20, R, X, [89], "2026-11-06T10:00:00-08:00"),
                                             (21, HEAD, R, [74], "2026-11-07T04:00:00-08:00")])
        self.assertEqual(self.kills, [])

    def test_records_and_run_records_off_the_instances_line_stop_the_deploy_before_anything_stops(self):
        X, Y, Z = "a" * 40, "e" * 40, "f" * 40
        live = (page(release(19, HEAD, [87]), release(18, X, [1])), 0)
        cases = (
            # The repository keeps another commit under a number the instance records.
            ("keeps release 18 at %s" % Y[:12], dict(kept=[release(18, Y, [1]), release(19, HEAD, [87])], releases=live,
                                                    line=[X, Y, HEAD])),
            # A kept record that leaves a number out.
            ("numbered 18, 20", dict(kept=[release(18, X, [1]), release(20, HEAD, [87])], releases=(page(release(18, X, [1])), 0),
                                     line=[X, HEAD])),
            # A kept record whose commit comes before the release under it.
            ("release 19 at %s does not come after release 18" % Y[:12],
             dict(kept=[release(18, X, [1]), release(19, Y, [87])], releases=(page(release(18, X, [1])), 0), line=[Y, X, HEAD])),
            # A kept record the live commit does not include.
            ("does not follow %s" % Z[:12], dict(kept=[release(18, X, [1]), release(19, Z, [87])],
                                                releases=(page(release(18, X, [1])), 0), line=[X, HEAD, Z])))
        for reason, setup in cases:
            with self.subTest(reason):
                self.assertIn(reason, self.refused(FakeHost(self.config, **setup)))
        # A server run record at a commit off the line from the latest record to the live HEAD.
        self.started((Z, "2026-11-06T10:00:00-08:00"), (HEAD, "2026-11-07T10:00:00-08:00"))
        host = FakeHost(self.config, releases=(page(release(19, X, [87])), 0), line=[X, HEAD], others=[Z],
                        logs={"%s..%s" % (X, HEAD): log(("Twenty", ["89"]))})
        self.assertIn("a server run record started %s" % Z[:12], self.refused(host))
        # Run records that start a commit after one it comes before.
        for run in (self.instance / "run-server-04", self.instance / "run-server-05"):
            for name in ("launch.json", "pid.json"):
                (run / name).unlink()
            run.rmdir()
        self.started((HEAD, "2026-11-06T10:00:00-08:00"), (Y, "2026-11-07T10:00:00-08:00"))
        host = FakeHost(self.config, releases=(page(release(19, X, [87])), 0), line=[X, Y, HEAD],
                        logs={"%s..%s" % (X, Y): log(("Twenty", ["89"]))})
        self.assertIn("after %s, which it comes before" % HEAD[:12], self.refused(host))
        self.assertEqual(self.kills, [])

    def test_records_the_repository_keeps_for_another_instance_are_not_this_ones(self):
        # This instance numbers its own releases from 1; the kept records share none of them.
        kept = [release(18, "1" * 40, [1, 4]), release(19, "2" * 40, [87, 2])]
        host = FakeHost(self.config, kept=kept, releases=(page(release(1, HEAD, [5])), 0), others=["1" * 40, "2" * 40])
        self.planned(host)
        self.assertEqual((self.plan()["before"], self.plan()["record"]["release"]), ([], 2))

    def test_a_commit_git_cannot_read_or_a_run_record_that_cannot_be_read_stops_the_deploy(self):
        # Kept records whose commits the live checkout lacks are not therefore another instance's.
        kept = [release(18, "1" * 40, [1, 4]), release(19, "2" * 40, [87, 2])]
        self.assertIn("whether 222222222222 comes before %s could not be read" % HEAD[:12],
                      self.refused(FakeHost(self.config, keeps=False, kept=kept)))
        X = "a" * 40
        host = FakeHost(self.config, releases=(page(release(19, X, [87])), 0), line=[X, HEAD])
        launch = self.instance / "run-server-03" / "launch.json"
        for text, reason in (("not json", "could not be read"), ('{"at": "2026-11-07T05:00:00-08:00"}', "does not say which commit"),
                             ('["%s"]' % HEAD, "does not say which commit")):
            with self.subTest(text):
                launch.write_text(text)
                self.assertIn("the run record %s %s" % (launch, reason), self.refused(host))
        self.assertEqual(self.kills, [])

    def test_live_records_that_cannot_be_read_stop_the_deploy_rather_than_count_as_none(self):
        for answer in (("", 1), ('{"instance_id":"i-1","error":{"code":"DATABASE_BUSY"}}\n', 75), ("not json\n", 0),
                       (page(19) + '{"instance_id":"i-1","error":{"code":"DATABASE_BUSY"}}\n', 75),
                       ('{"instance_id":"i-1","items":[{"release":19}],"next_cursor":null}\n', 0), (page(19), 1)):
            with self.subTest(answer):
                host = FakeHost(self.config, releases=answer)
                self.assertIn("could not be read", self.refused(host, number=20))
                self.assertEqual(self.kills, [])

    def test_whether_the_live_code_keeps_records_that_git_cannot_say_stops_the_deploy(self):
        self.assertIn("whether the live code keeps release records could not be read",
                      self.refused(FakeHost(self.config, keeps=None), number=20))
        # Code that does not keep them is read as keeping none, and never asked for them.
        host = FakeHost(self.config, keeps=False)
        self.planned(host, number=20)
        self.assertEqual(host.ran("releases"), [])
        self.assertEqual(self.kills, [])

    def test_kept_records_or_commits_that_git_cannot_list_stop_the_deploy(self):
        self.assertIn("the release records kept at %s could not be listed" % SHA[:12],
                      self.refused(FakeHost(self.config, listed=128)))
        unread = {"%s..%s" % (HEAD, SHA): ("", 128)}
        for no_items in (False, True):
            with self.subTest(no_items=no_items):
                self.assertIn("the commits from %s to %s could not be read" % (HEAD, SHA),
                              self.refused(FakeHost(self.config, logs=unread), no_items=no_items))
        # A release recovered from its run record names its items from git, too.
        X = "a" * 40
        self.launched(HEAD)
        host = FakeHost(self.config, releases=(page(release(19, X, [87])), 0), line=[X, HEAD],
                        logs={"%s..%s" % (X, HEAD): ("", 128)})
        self.assertIn("the commits from %s to %s could not be read" % (X, HEAD), self.refused(host))
        self.assertEqual(self.kills, [])

    def test_live_since_without_a_record_that_places_the_live_commit_is_refused(self):
        self.assertIn("no record places %s" % HEAD[:12],
                      self.refused(FakeHost(self.config, keeps=False), number=20, live_since="2026-11-07T04:00:00-08:00"))
        self.assertEqual(self.kills, [])

    def restarting(self):
        """The release check's summary for the live HEAD, the commit a restart deploys."""
        self.summary.write_text(json.dumps(summary(commit=HEAD)))
        return {"%s..%s" % (HEAD, HEAD): log()}

    def test_a_deploy_of_the_live_commit_is_a_restart_that_writes_no_record_of_its_own(self):
        logs = self.restarting()
        for given in ({}, {"no_items": True}):
            with self.subTest(given):
                self.planned(FakeHost(self.config, logs=logs), sha=HEAD, **given)
                plan = self.plan()
                self.assertEqual((plan["record"], plan["before"], plan["restart_of"], plan["latest_recorded"]),
                                 (None, [], 19, 19))
        # The records the instance lacks are still written; the restart adds none.
        kept = [release(18, "1" * 40, [1, 4]), release(19, HEAD, [87, 2])]
        self.planned(FakeHost(self.config, keeps=False, kept=kept, at=HEAD, line=["1" * 40, HEAD], logs=logs), sha=HEAD)
        self.assertEqual((self.plan()["record"], self.plan()["before"], self.plan()["restart_of"]), (None, kept, 19))
        # Nothing records the live commit: still a restart, and it needs no number.
        self.planned(FakeHost(self.config, keeps=False, logs=logs), sha=HEAD)
        self.assertEqual((self.plan()["record"], self.plan()["restart_of"]), (None, None))
        self.assertIn("a deploy of the live commit restarts release 19 and writes no record of its own",
                      self.refused(FakeHost(self.config, logs=logs), sha=HEAD, number=20))
        self.assertEqual(self.kills, [])

    def test_a_deploy_run_again_after_its_release_went_live_unverified_records_that_release_once(self):
        # Release 20 went live at HEAD and did not verify; the deploy is run again at the same commit.
        X = "a" * 40
        logs = dict(self.restarting(), **{"%s..%s" % (X, HEAD): log(("Twenty", ["89"]))})
        self.launched(HEAD)
        self.planned(FakeHost(self.config, releases=(page(release(19, X, [87])), 0), line=[X, HEAD], logs=logs), sha=HEAD)
        plan = self.plan()
        self.assertEqual(self.summarised(), [(20, HEAD, X, [89], "2026-11-07T05:00:00-08:00")])
        self.assertEqual((plan["record"], plan["restart_of"], plan["latest_recorded"]), (None, 20, 20))
        self.assertEqual(self.kills, [])

    def test_a_commit_already_recorded_as_another_release_stops_the_deploy_before_anything_stops(self):
        X, Y = "a" * 40, "e" * 40
        # The instance records the commit being deployed under an earlier number.
        host = FakeHost(self.config, releases=(page(release(20, HEAD, [87]), release(19, SHA, [1])), 0))
        self.assertIn("%s is release 19 and release 21; a commit is one release" % SHA[:12], self.refused(host))
        # The repository keeps two numbers at one commit.
        self.launched(HEAD)
        kept = [release(18, X, [1]), release(19, X, [4])]
        host = FakeHost(self.config, kept=kept, releases=(page(kept[0]), 0), line=[X, HEAD],
                        logs={"%s..%s" % (X, HEAD): log(("Twenty", ["89"]))})
        self.assertIn("%s is release 18 and release 19" % X[:12], self.refused(host))
        # It keeps a number at a commit the instance records under another.
        kept = [release(18, X, [1]), release(19, Y, [4])]
        host = FakeHost(self.config, kept=kept, releases=(page(release(20, Y, [87]), kept[0]), 0), line=[X, Y, HEAD],
                        logs={"%s..%s" % (Y, HEAD): log(("Twenty-one", ["89"]))})
        self.assertIn("%s is release 19 and release 20" % Y[:12], self.refused(host))
        self.assertEqual(self.kills, [])


# Stand-ins for bin/aim-next-server and bin/aim-next-runner: each prints its first line as the real one does
# (the server only after binding --port) and then stays up until it is signalled.
STUB_SERVER = """import http.server, json, sys
port = int(sys.argv[sys.argv.index("--port") + 1])
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(%(root)d if self.path == "/" else 200)
        self.end_headers()
        self.wfile.write(json.dumps({"stub": %(nonce)r, "path": self.path}).encode())
    def log_message(self, *args):
        pass
server = http.server.HTTPServer(("127.0.0.1", port), Handler)
print(json.dumps({"status": %(status)r, "argv": sys.argv[1:]}), flush=True)
server.serve_forever()
"""
STUB_RUNNER = """import json, signal, sys
print(json.dumps({"event": %(event)r, "argv": sys.argv[1:]}), flush=True)
signal.pause()
"""


class Restarting(DeployCase):
    """The deploy past migrate: real stand-in server and runner processes on a free loopback port.

    ps, git, the backup, the migration and the CLI are still answered by FakeHost, and every
    process the deploy starts is stopped by the PID start() returned."""

    def setUp(self):
        super().setUp()
        self.started = []
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            self.port = probe.getsockname()[1]
        self.assertNotEqual(self.port, 5192)  # unlike the constant the old script had
        self.raw["port"] = self.port
        self.config_path.write_text(json.dumps(self.raw))
        self.config = deploy.load_config(self.config_path)
        self.nonce = os.urandom(8).hex()
        self.stubs()

    def stubs(self, status="ready", event="acquired", root=200):
        (self.config.live / "bin").mkdir(parents=True, exist_ok=True)
        (self.config.live / "bin/aim-next-server").write_text(STUB_SERVER % {"nonce": self.nonce, "status": status,
                                                                             "root": root})
        (self.config.live / "bin/aim-next-runner").write_text(STUB_RUNNER % {"event": event})

    def tearDown(self):
        for proc in self.started:
            proc.kill()
            proc.wait()
        super().tearDown()

    def run_deploy(self, host, sha=SHA, known_reds=None):
        real_start, real_urlopen, opened = deploy.start, deploy.urllib.request.urlopen, []

        def start(*args):
            proc = real_start(*args)
            self.started.append(proc)
            return proc

        def urlopen(url):
            # Only the stand-in server: a wrong port must fail here, never reach a live server.
            opened.append(url)
            self.assertTrue(url.startswith("http://127.0.0.1:%d/" % self.port), url)
            return real_urlopen(url)

        with mock.patch.object(deploy, "run", host), \
                mock.patch.object(deploy.os, "kill", lambda pid, sig: self.kills.append((pid, sig))), \
                mock.patch.object(deploy, "wait_exit", return_value="exited"), \
                mock.patch.object(deploy, "start", start), \
                mock.patch.object(deploy.urllib.request, "urlopen", urlopen), \
                redirect_stdout(io.StringIO()):
            deploy.deploy(self.config, sha, self.evidence, self.summary, False, known_reds=known_reds)
        return opened

    def completed(self, host, sha=SHA):
        """A deploy that must complete; a refusal fails the test rather than erroring it."""
        try:
            return self.run_deploy(host, sha=sha)
        except deploy.Refused as refusal:
            self.fail("the deploy refused: %s" % refusal)

    def first_line(self, run):
        return json.loads((self.instance / run / "stdout.log").read_text().splitlines()[0])


class FullDeploy(Restarting):
    def test_a_deploy_restarts_both_from_the_config_and_verifies_the_new_server(self):
        host = FakeHost(self.config)
        try:
            opened = self.run_deploy(host)
        except Exception as error:  # a refusal here is the failure under test, not an error
            self.fail("the deploy did not complete: %r" % error)
        self.assertEqual(self.records(), ["00-plan", "00-release-check", "01-live", "02-checkout", "02-release", "03-stopped",
                                          "04-backup", "05-fast-forward", "06-migrate", "07-server", "09-runner",
                                          "10-verify", "11-release", "12-release-branch", "release-20", "release-20-measurements"])
        self.assertEqual(self.kills, [(222, 15), (111, 15)])
        # The backup reads the server's profile, the one bound to the database.
        backup = host.ran("-c")[0][-1]
        self.assertIn(repr(str(self.config.server_profile)), backup)
        self.assertNotIn(str(self.config.runner_profile), backup)
        server, runner = self.started
        self.assertEqual(self.first_line("run-server-04")["argv"], deploy.server_argv(self.config)[3:])
        self.assertEqual(self.first_line("run-runner-03")["argv"], deploy.supervisor_argv(self.config)[3:])
        for run, proc in (("run-server-04", server), ("run-runner-03", runner)):
            self.assertEqual(json.loads((self.instance / run / "pid.json").read_text())["pid"], proc.pid)
            self.assertEqual(json.loads((self.instance / run / "launch.json").read_text())["revision"], SHA)
            # Each runs in its own session, so it outlives the deploy and the terminal it ran in.
            self.assertEqual(os.getsid(proc.pid), proc.pid)
        verify = json.loads((self.evidence / "10-verify.json").read_text())
        self.assertEqual((verify["root_status"], verify["server_alive"], verify["runner_alive"], verify["cli_exit"]),
                         (200, True, True, 0))
        self.assertIn(self.nonce, verify["bootstrap_head"])
        self.assertEqual(opened, ["http://127.0.0.1:%d/api/bootstrap" % self.port, "http://127.0.0.1:%d/" % self.port])

    def test_a_server_that_does_not_report_ready_stops_the_deploy_before_the_runner_starts(self):
        self.stubs(status="starting")
        with self.assertRaises(deploy.Refused) as caught:
            self.run_deploy(FakeHost(self.config))
        self.assertIn("did not report ready", str(caught.exception))
        self.assertEqual(len(self.started), 1)
        self.assertFalse((self.instance / "run-runner-03").exists())
        self.assertEqual(self.records()[-2:], ["07-server", "release-20-measurements"])
        self.assertFalse((self.evidence / 'release-20.json').exists())

    def test_a_runner_that_does_not_acquire_its_guard_stops_the_deploy_before_the_verify(self):
        self.stubs(event="waiting")
        with self.assertRaises(deploy.Refused) as caught:
            self.run_deploy(FakeHost(self.config))
        self.assertIn("did not acquire its guard", str(caught.exception))
        self.assertEqual(len(self.started), 2)
        self.assertEqual(self.records()[-2:], ["09-runner", "release-20-measurements"])
        self.assertFalse((self.evidence / 'release-20.json').exists())

    def test_a_verified_release_is_recorded_in_the_instance_and_the_evidence(self):
        host = FakeHost(self.config)
        before = datetime.datetime.now().astimezone()
        self.run_deploy(host)
        [written] = host.written
        self.complete_measurements(written)
        self.assertEqual({key: written[key] for key in ("release", "commit", "previous", "items", "source", "evidence")},
                         {"release": 20, "commit": SHA, "previous": HEAD, "items": [88], "source": "deploy",
                          "evidence": str(self.evidence.resolve())})
        self.assertEqual(json.loads((self.evidence / "11-release.json").read_text())["records"], [written])
        # Written by the new code's admin verb, with the server's profile, after the verify read the new server.
        argv = host.ran("record-release")[0]
        self.assertEqual(argv[3:5], [str(self.config.live / "bin/aim-next-admin"), "record-release"])
        self.assertEqual(argv[argv.index("--profile") + 1], str(self.config.server_profile))
        order = [index for index, call in enumerate(host.calls) if call[-1] == "binding" or "record-release" in call]
        self.assertEqual([("record-release" in host.calls[index]) for index in order], [False, True])
        self.assertGreaterEqual(datetime.datetime.fromisoformat(written["deployed_at"]), before)

    def unverified(self, host):
        with self.assertRaises(deploy.Refused) as caught:
            self.run_deploy(host)
        message = str(caught.exception)
        self.assertIn("did not verify, so no release record was written", message)
        self.assertEqual(host.ran("record-release"), [])
        # The unverified plan and full measurements remain; no deployed record
        # or ready-to-record command can turn failed verification into a release.
        kept = self.evidence / "release-20.json"
        self.assertFalse(kept.exists())
        planned = json.loads((self.evidence / '02-release.json').read_text())['record']
        self.assertEqual(planned['commit'], SHA)
        self.complete_measurements(planned)
        self.assertIn("%s is live; coordinator recovery required" % SHA[:12], message)
        self.assertNotIn('record it with:', message)
        verified = json.loads((self.evidence / "10-verify.json").read_text())
        self.assertIsNone(verified['deployed_at'])
        return verified

    def test_a_release_whose_cli_fails_is_not_recorded(self):
        self.assertEqual(self.unverified(FakeHost(self.config, cli=2))["cli_exit"], 2)

    def test_a_release_whose_reader_does_not_answer_200_is_not_recorded(self):
        self.stubs(root=500)
        self.assertEqual(self.unverified(FakeHost(self.config))["root_status"], 500)

    def stop(self, index):
        """Stop a started stand-in by its captured PID and reap it, so the deploy's check sees it gone.

        The deploy's os.kill is a recorder during the run, so this signals with the real one."""
        REAL_KILL(self.started[index].pid, signal.SIGKILL)
        self.started[index].wait()

    def test_a_release_whose_server_stops_before_the_check_ends_is_not_recorded(self):
        verify = self.unverified(FakeHost(self.config, on_cli=lambda: self.stop(0)))
        self.assertEqual((verify["server_alive"], verify["runner_alive"], verify["cli_exit"]), (False, True, 0))

    def test_a_release_whose_runner_stops_before_the_check_ends_is_not_recorded(self):
        verify = self.unverified(FakeHost(self.config, on_cli=lambda: self.stop(1)))
        self.assertEqual((verify["server_alive"], verify["runner_alive"], verify["root_status"]), (True, False, 200))

    def test_the_records_the_instance_lacks_are_written_first_in_order(self):
        kept = [release(18, "1" * 40, [1, 4]), release(19, HEAD, [87, 2])]
        host = FakeHost(self.config, keeps=False, kept=kept, line=["1" * 40, HEAD])
        self.run_deploy(host)
        self.assertEqual([written["release"] for written in host.written], [18, 19, 20])
        self.assertEqual(host.written[:2], kept)
        self.assertEqual([write["exit"] for write in json.loads((self.evidence / "11-release.json").read_text())["writes"]],
                         [0, 0, 0])

    def test_when_a_record_is_refused_nothing_after_it_is_tried_and_every_command_left_is_given(self):
        kept = [release(18, "1" * 40, [1, 4]), release(19, HEAD, [87, 2])]
        host = FakeHost(self.config, keeps=False, kept=kept, recorded=2, line=["1" * 40, HEAD])
        with self.assertRaises(deploy.Refused) as caught:
            self.run_deploy(host)
        commands = [" ".join(argv) for argv in host.ran("record-release")]
        self.assertEqual(len(commands), 1)
        self.assertIn("release 20 is live but the record of release 18 was not written", str(caught.exception))
        self.assertIn(commands[0] + " && ", str(caught.exception))
        self.assertIn(str((self.evidence / "release-20.json").resolve()), str(caught.exception))

    def test_a_restart_of_the_live_release_writes_no_record(self):
        self.summary.write_text(json.dumps(summary(commit=HEAD)))
        logs = {"%s..%s" % (HEAD, HEAD): log()}
        host = FakeHost(self.config, logs=logs)
        self.completed(host, sha=HEAD)
        self.assertEqual((host.written, host.ran("record-release")), ([], []))
        self.assertEqual(json.loads((self.evidence / "11-release.json").read_text()), {"records": [], "writes": []})
        self.assertNotIn("release-20", self.records())

    def test_a_deploy_run_again_after_its_release_went_live_unverified_writes_that_record_once(self):
        # Release 20 went live at HEAD without its record; the deploy run again records it, and only it.
        self.summary.write_text(json.dumps(summary(commit=HEAD)))
        logs = {"%s..%s" % (HEAD, HEAD): log()}
        X = "a" * 40
        (self.instance / "run-server-03" / "launch.json").write_text(json.dumps({"at": "2026-11-07T05:00:00-08:00",
                                                                                "revision": HEAD}))
        host = FakeHost(self.config, releases=(page(release(19, X, [87])), 0), line=[X, HEAD],
                        logs=dict(logs, **{"%s..%s" % (X, HEAD): log(("Twenty", ["89"]))}))
        self.completed(host, sha=HEAD)
        self.assertEqual([(written["release"], written["commit"], written["items"], written["source"])
                          for written in host.written], [(20, HEAD, [89], "backfill")])

    def test_a_restart_that_does_not_verify_says_so_and_gives_no_record_command(self):
        self.summary.write_text(json.dumps(summary(commit=HEAD)))
        host = FakeHost(self.config, cli=2, logs={"%s..%s" % (HEAD, HEAD): log()})
        with self.assertRaises(deploy.Refused) as caught:
            self.run_deploy(host, sha=HEAD)
        self.assertIn("the restart of release 19 did not verify, so no release record was written: ", str(caught.exception))
        self.assertNotIn("record-release", str(caught.exception))
        self.assertNotIn("record it with", str(caught.exception), "no offer of commands when there are none")
        self.assertTrue(str(caught.exception).endswith("%s is live; coordinator recovery required" % HEAD[:12]), str(caught.exception)[-120:])
        self.assertEqual(host.ran("record-release"), [])

    def test_a_record_the_instance_refuses_says_the_release_is_live_and_how_to_record_it(self):
        host = FakeHost(self.config, recorded=2)
        with self.assertRaises(deploy.Refused) as caught:
            self.run_deploy(host)
        message = str(caught.exception)
        self.assertIn("release 20 is live but the record of release 20 was not written", message)
        self.assertIn(" ".join(host.ran("record-release")[0]), message)
        self.assertEqual(json.loads((self.evidence / "11-release.json").read_text())["writes"][-1]["exit"], 2)


class DeployRefuses(DeployCase):
    def test_a_summary_that_is_not_of_this_commit_is_refused_before_anything_is_read(self):
        # 7.2 step 1: its counts do not describe this commit, so there is nothing to measure. Item 588: a summary of another
        # commit is read only for its tree, which git cannot read here (no repository at the live path), so it is refused.
        cases = ((None, "no readable release-check summary", []),
                 (summary(commit="d" * 40), "ran on %s, not %s, and the tree of %s could not be read" % ("d" * 40, SHA, "d" * 40),
                  ["d" * 40 + "^{tree}"]),
                 (dict(summary(), tree_unchanged=False), "the checkout changed while the release check ran", []))
        for written, reason, trees in cases:
            with self.subTest(reason):
                if written is None:
                    self.summary.unlink()
                else:
                    self.summary.write_text(json.dumps(written))
                host = FakeHost(self.config)
                self.assertIn(reason, self.refused(host))
                self.assertEqual([call[-1] for call in host.calls], trees, "nothing but a tree is read")
                self.assertEqual(self.kills, [])
                self.assertEqual(self.records(), ["00-release-check"])
                self.assertFalse(json.loads((self.evidence / "00-release-check.json").read_text())["passed"])

    def test_a_summary_of_this_commit_that_did_not_pass_is_read_and_refused_before_anything_stops(self):
        # 7.2 steps 1 and 6: it goes on to be measured, and its refusal comes after the checks, before the plan.
        for written, reason in ((summary(failed=("native-colors",)), "native-colors failed"), (dict(summary(), passed=False), "did not pass")):
            with self.subTest(reason):
                self.summary.write_text(json.dumps(written))
                host = FakeHost(self.config)
                refusal = self.refused(host)
                self.assertIn("the release check has not passed for %s: " % SHA, refusal)
                self.assertIn(reason, refusal)
                self.assertEqual(self.records(), ["00-plan", "00-release-check", "01-live", "02-checkout"])
                self.assertEqual((self.kills, host.ran("merge"), host.ran("-c")), ([], [], []))

    def test_a_passed_release_check_for_the_commit_lets_the_deploy_go_on(self):
        host = FakeHost(self.config, ff=False)
        self.assertIn("fast-forward", self.refused(host))
        self.assertTrue(json.loads((self.evidence / "00-release-check.json").read_text())["passed"])
        self.assertTrue(host.ran("ps"))

    def test_a_dry_run_shows_the_plan_but_still_fails_without_a_passed_release_check(self):
        self.summary.write_text(json.dumps(summary(failed=("node-units",))))
        host = FakeHost(self.config)
        self.assertIn("node-units failed", self.refused(host, dry=True))
        self.assertEqual(self.records(), ["00-plan", "00-release-check", "01-live", "02-checkout"])
        self.assertEqual(self.kills, [])

    def skipped(self, live=HEAD, steps=release_check.SKIPPABLE):
        """A passed summary that skipped the browser modes and the wake replay beside live (item 166)."""
        written = summary()
        written["steps"] = [step for step in written["steps"] if step["name"] not in release_check.SKIPPABLE]
        written["skipped"] = {"steps": list(steps), "live": live, "changed": ["docs/tools.md"], "why": release_check.skipped_words(live)}
        self.summary.write_text(json.dumps(written))

    def test_a_summary_that_skipped_what_the_change_cannot_reach_is_taken_for_that_change_alone(self):
        self.skipped()
        host = FakeHost(self.config, ff=False, changed=["docs/tools.md", "tools/checks.py", "tests/test_tools_deploy.py"])
        self.assertIn("fast-forward", self.refused(host), "taken: the deploy goes on to the live checkout")
        written = json.loads((self.evidence / "00-release-check.json").read_text())
        self.assertEqual((written["passed"], written["reasons"]), (True, []))
        self.assertIn("skipped, not passed", written["skipped"])
        admission = [argv for argv in host.ran("diff") if "--no-ext-diff" in argv]
        self.assertTrue(admission, "admission independently reads the candidate diff")
        self.assertTrue(all(argv[-2:] == [HEAD, SHA] for argv in admission))
        [diff] = [argv for argv in host.ran("diff") if "--no-ext-diff" not in argv]
        self.assertEqual(diff[-3:], [HEAD, SHA, "--"], "the paths are read from the live HEAD to the commit, by the deploy itself")

    def test_a_summary_that_skipped_steps_is_refused_for_any_other_change_before_anything_stops(self):
        cases = (("web", dict(changed=["docs/tools.md", "web/app.mjs"]), {}, "but web/app.mjs changed since then, which they could see"),
                 ("aim_next", dict(changed=["aim_next/store.py"]), {}, "but aim_next/store.py changed since then"),
                 ("a harness", dict(changed=[release_check.READER]), {}, "but %s changed since then" % release_check.READER),
                 ("another live commit", dict(changed=["docs/tools.md"]), dict(live="a" * 40), "but %s is live; run the release check again" % HEAD),
                 ("unreadable", dict(changed=None), {}, "cannot be taken: what changed since the live commit"),
                 ("other steps", dict(changed=["docs/tools.md"]), dict(steps=[release_check.WAKE_REPLAY]), "skipped steps it never skips together"))
        for case, host_args, skip, words in cases:
            with self.subTest(case):
                self.skipped(**skip)
                host = FakeHost(self.config, **host_args)
                refusal = self.refused(host)
                self.assertIn("the release check has not passed for %s" % SHA, refusal)
                self.assertIn(words, refusal)
                self.assertEqual(self.records(), ["00-release-check"])
                self.assertFalse(host.ran("ps"), "refused before the live processes are read")
                self.assertEqual(self.kills, [])

    def test_a_deploy_that_measures_an_outcome_refuses_a_summary_that_skipped_steps(self):
        self.config_path.write_text(json.dumps(dict(self.raw, outcome="building-theseus", reporter_profile="agent-b.profile.json")))
        self.config = deploy.load_config(self.config_path)
        self.skipped()
        host = FakeHost(self.config, changed=["docs/tools.md"])
        self.assertIn("measuring building-theseus reads every step, so run the release check without --live", self.refused(host))
        self.assertEqual((host.calls, self.records()), ([], ["00-release-check"]))

    def test_only_a_full_commit_is_accepted(self):
        host = FakeHost(self.config)
        for sha in ("c" * 7, "C" * 40, "HEAD", ""):
            with self.subTest(sha):
                self.assertIn("full 40-character commit", self.refused(host, sha=sha))
        self.assertEqual(host.calls, [])

    def test_main_requires_the_release_check(self):
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as caught:
            deploy.main(["--config", str(self.config_path), SHA, str(self.evidence)])
        self.assertEqual(caught.exception.code, 2)


class GitHost(FakeHost):
    """FakeHost, with the live checkout's own git answering what the deploy reads of the change before anything else:
    the live commit, what changed since it, and the files tools/steps_for.py reads at the commit (item 608)."""

    def __call__(self, argv, **kw):
        if argv[0] == "git" and argv[3] in ("rev-parse", "diff", "show", "ls-tree"):
            self.calls.append(argv)
            return subprocess.run(argv, capture_output=True, text=True, errors="replace")
        return super().__call__(argv, **kw)


class DeployRefusesScoped(DeployCase):
    """Item 608: a small fix's scoped summary is taken only when the deploy, choosing the steps the change touches again
    from its own live checkout, finds each chosen step run whole and passed, the units, its refusals and the wake replay
    passed, and the change inside the small-fix bound; otherwise it is refused before anything stops."""

    def setUp(self):
        super().setUp()
        import test_tools_release_check as checks
        self.checks = checks
        self.live = checks.scoped_repository(self.config.live)

    def scoped(self, files, chosen=("native-health",), remove=(), **more):
        """A commit changing files on top of the live commit, and a passed summary of it scoped to chosen."""
        commit = self.checks.scoped_change(self.config.live, self.live, files, remove)
        written = self.checks.scoped_summary(commit, self.live, list(chosen), sorted(files), **more)
        self.summary.write_text(json.dumps(written))
        return commit, written

    def test_a_small_fix_whose_steps_the_deploy_chooses_again_and_finds_passed_is_taken(self):
        commit, _ = self.scoped(self.checks.HEALTH_FIX)
        host = GitHost(self.config, ff=False)
        self.assertIn("fast-forward", self.refused(host, sha=commit), "taken: the deploy goes on to the live checkout")
        written = json.loads((self.evidence / "00-release-check.json").read_text())
        self.assertEqual((written["passed"], written["reasons"]), (True, []))
        self.assertEqual(written["scoped"], "a small fix")
        admission = [argv for argv in host.ran("diff") if "--no-ext-diff" in argv]
        self.assertTrue(admission, "admission independently reads the candidate diff")
        self.assertTrue(all(argv[-2:] == [self.live, commit] for argv in admission))
        [diff] = [argv for argv in host.ran("diff") if "--name-only" in argv and "--no-ext-diff" not in argv]
        self.assertEqual(diff[-3:], [self.live, commit, "--"], "the paths are read from the live HEAD to the commit, by the deploy itself")
        self.assertTrue([argv for argv in host.ran("show") if argv[-1] == "%s:%s" % (commit, release_check.PERSISTENCE)],
                        "the steps are chosen from the harness at the commit, read by the deploy")

    def test_a_change_that_touches_a_step_the_summary_did_not_run_is_refused_before_anything_stops(self):
        # The fix touches the reply's colour too, which native-colors' case reads; the summary ran native-health alone,
        # and says it chose only that.
        files = dict(self.checks.HEALTH_FIX, **{"web/aim.css": self.checks.SCOPED_CSS.replace("color:black", "color:#222").replace("color:blue", "color:navy")})
        commit, _ = self.scoped(files)
        host = GitHost(self.config)
        refusal = self.refused(host, sha=commit)
        self.assertIn("the release check has not passed for %s" % commit, refusal)
        self.assertIn("but what changed since then touches native-colors, which it did not run", refusal)
        self.assertEqual(self.records(), ["00-release-check"])
        self.assertFalse(host.ran("ps"), "refused before the live processes are read")

    def test_a_change_outside_the_small_fix_bound_is_refused_before_anything_stops(self):
        many = {"docs/n%d.md" % n: "# %d\n" % n for n in range(release_check.SMALL_FIX_FILES)}
        cases = (("a tool", dict(self.checks.HEALTH_FIX, **{"tools/checks.py": "# trials, again\n"}), "tools/checks.py changed, which a small fix never changes"),
                 ("a migration", dict(self.checks.HEALTH_FIX, **{"aim_next/migrations.py": "# steps\n"}), "aim_next/migrations.py changed"),
                 ("too many files", dict(self.checks.HEALTH_FIX, **many), "more than a small fix's %d" % release_check.SMALL_FIX_FILES))
        for case, files, words in cases:
            with self.subTest(case):
                commit, _ = self.scoped(files)
                host = GitHost(self.config)
                refusal = self.refused(host, sha=commit)
                self.assertIn(words, refusal)
                self.assertEqual(self.records(), ["00-release-check"])
                self.assertFalse(host.ran("ps"))

    def test_a_chosen_step_run_for_one_case_or_a_missing_replay_or_refusals_step_is_refused(self):
        commit, written = self.scoped(self.checks.HEALTH_FIX)
        [health] = [step for step in written["steps"] if step["name"] == "native-health"]
        health["argv"].append("health the release line")
        self.summary.write_text(json.dumps(written))
        self.assertIn("native-health did not run whole", self.refused(GitHost(self.config), sha=commit))
        for name in (release_check.WAKE_REPLAY, release_check.DEPLOY_REFUSALS, "python-units"):
            with self.subTest(name):
                commit, written = self.scoped(self.checks.HEALTH_FIX)
                written["steps"] = [step for step in written["steps"] if step["name"] != name]
                self.summary.write_text(json.dumps(written))
                # It describes the commit, so it is refused as not passed, after the checks and before anything stops.
                host = GitHost(self.config, line=(self.live, commit))
                refusal = self.refused(host, sha=commit)
                self.assertIn("the release check has not passed for %s: the release check did not run %s" % (commit, name), refusal)
                self.assertEqual((self.kills, host.ran("merge"), host.ran("-c")), ([], [], []))

    def test_a_live_commit_the_deploy_cannot_read_takes_no_scoped_summary(self):
        commit, _ = self.scoped(self.checks.HEALTH_FIX)
        host = GitHost(self.config)
        real = host.__call__

        def headless(argv, **kw):
            if argv[0] == "git" and "rev-parse" in argv:
                host.calls.append(argv)
                return subprocess.CompletedProcess(argv, 128, "", "fatal: not a git repository")
            return real(argv, **kw)
        refusal = self.refused(headless, sha=commit)
        self.assertIn("ran only the browser steps a small fix touches beside the live commit %s, and that cannot be taken: "
                      "the live commit cannot be read: fatal: not a git repository" % self.live, refusal)
        self.assertFalse([argv for argv in host.calls if "diff" in argv], "no change is read beside a live commit it could not read")
        self.assertFalse(host.ran("ps"))

    def test_a_summary_scoped_beside_another_live_commit_or_with_an_outcome_is_refused(self):
        commit, written = self.scoped(self.checks.HEALTH_FIX)
        written["scoped"]["live"] = "a" * 40
        self.summary.write_text(json.dumps(written))
        self.assertIn("but %s is live; run the release check again" % self.live, self.refused(GitHost(self.config), sha=commit))
        self.config_path.write_text(json.dumps(dict(self.raw, outcome="building-theseus", reporter_profile="agent-b.profile.json")))
        self.config = deploy.load_config(self.config_path)
        commit, _ = self.scoped(self.checks.HEALTH_FIX)
        host = GitHost(self.config)
        self.assertIn("measuring building-theseus reads every step, so run the whole release check", self.refused(host, sha=commit))
        self.assertEqual(host.calls, [])

# Item 178: a step of tests/reader_persistence_native.mjs, whose native.json records its fixture's exit, one of
# tests/reader_native.mjs (results.json, its page served in-process) and the composer's, whose native.json records its
# fixture's exit and signal and which of its planned phases completed.
RED_STEP, READER_STEP, COMPOSE_STEP = "native-colors", "native-reader-keyboard", "native-compose"
RED_CASE = "a reply keeps its colour"
# The composer's planned phases, in order: each of its three runs in Chrome, then in Safari's engine.
COMPOSE_PHASES = ["run:chromium", "run:webkit", "runAnswering:chromium", "runAnswering:webkit", "runSending:chromium", "runSending:webkit"]
MISSING = object()


def native_run(out, name, failed=(RED_CASE,), passed=("the rail keeps its colours",), exit=1, closed=True, fixture=0,
               ended=True, cases=None, printed=None, last=None, results=None, log=True, more=None):
    """One native step's evidence as tools/release_check.py leaves it in out (its results file and log), and the step's
    entry in that summary, its counts read from that evidence by the release check's own reader.

    printed: fields the reader harness's last printed JSON has in place of its results file's; last: the log's last
    lines as written; results: the results file's text as written, which the release check's reader is not given;
    log: False leaves no log; more: fields native.json has beside its results, fixture exit and browser, MISSING
    leaving one out."""
    step = known_reds.NATIVE[name]
    evidence = out / "native" / step.name
    evidence.mkdir(parents=True)
    cases = cases if cases is not None else ([{"name": case, "passed": True} for case in passed]
                                             + [{"name": case, "passed": False, "error": "expected 1, got 2"} for case in failed])
    if step.results == "results.json":
        report = {"closed": closed, "results": cases}
        # It prints a JSON object as it goes as well; only the last one is its results.
        tail = ("{\n  \"opened\": true\n}\n" + json.dumps(dict(report, **(printed or {})), indent=2) + "\n"
                + ("" if ended else "TypeError: cannot read properties of undefined\n"))
    else:
        report = {key: value for key, value in dict({"results": cases, "fixtureExitCode": fixture, "browserClosed": closed},
                                                    **(more or {})).items() if value is not MISSING}
        tail = (known_reds.END_LINE if ended else "AssertionError [ERR_ASSERTION]: the fixture is ready") + "\n"
    (evidence / step.results).write_text(json.dumps(report) if results is None else results)
    if log:
        (out / (name + ".log")).write_text("node %s\n%s" % (step.harness, tail if last is None else last))
    counts, judged = (release_check.native_counts(step, evidence) if results is None
                      else ({"error": "results the release check cannot read"}, False))
    return {"name": name, "exit": exit, "passed": exit == 0 and judged, "counts": counts, "log": str(out / (name + ".log"))}


class KnownRedsFiles:
    """A candidate summary with its evidence, live summaries with theirs, and the --known-reds file naming them."""

    def known_setup(self):
        base = Path(self.tmp.name)
        self.candidate = base / "candidate"
        self.candidate.mkdir()
        self.summary = self.candidate / "summary.json"
        self.known = base / "known-reds.json"

    def candidate_fails(self, *runs, **written):
        """The candidate's summary: every required step passed but runs, each (name, native_run kwargs) or a unit name."""
        result = summary(failed=tuple(run if isinstance(run, str) else run[0] for run in runs))
        made = {run[0]: native_run(self.candidate, run[0], **run[1]) for run in runs if not isinstance(run, str)}
        result["steps"] = [made.get(step["name"], step) for step in result["steps"]]
        result.update(written)
        self.summary.write_text(json.dumps(result))

    def live_summary(self, folder, *runs, commit=HEAD, **written):
        """A release-check --only summary of the live commit in its own folder, with each run's evidence."""
        out = Path(self.tmp.name) / folder
        out.mkdir()
        steps = [native_run(out, name, **args) for name, args in runs]
        result = dict({"tool": release_check.TOOL, "commit": commit, "tree_unchanged": True, "passed": False,
                       "required": list(release_check.REQUIRED), "only": [name for name, _ in runs], "steps": steps}, **written)
        (out / "summary.json").write_text(json.dumps(result))
        return out / "summary.json"

    def whole_run(self, folder, *runs, commit=SHA, shards=None, named=None, downloaded="all"):
        """The Checks workflow's whole run of commit, its browser runs each (name, native_run kwargs), downloaded as
        docs/tools.md ("A merged head its pull request's run judged") says: `gh run download RUN_ID -D folder` keeps each
        artifact in a folder of its own, the whole-run summary in release-check/ and each shard's summary and evidence in
        check-N/ beside it; downloaded="release-check" keeps the summary alone, as `-n release-check` does. The browser runs
        are check-1's, every other step check-0's. shards(their summaries) changes the shards as written; named, the
        shards the whole-run summary names. Returns the whole-run summary's path."""
        top = Path(self.tmp.name) / folder
        combined = summary(commit=commit, failed=tuple(name for name, _ in runs))
        made = {name: native_run(top / "check-1", name, **args) for name, args in runs}
        combined["steps"] = [made.get(step["name"], step) for step in combined["steps"]]
        written = {"check-%d" % number: [step for step in combined["steps"] if (step["name"] in made) == bool(number)]
                   for number in (0, 1)}
        written = {name: {"tool": release_check.TOOL, "commit": commit, "tree_unchanged": True, "required": list(release_check.REQUIRED),
                          "only": [step["name"] for step in steps], "steps": steps} for name, steps in written.items()}
        combined.update(skipped=None, combined_from=named or [name + "/summary.json" for name in sorted(written)])
        if downloaded == "release-check":
            top = Path(self.tmp.name) / (folder + "-summary-alone")
            top.mkdir()
            (top / "summary.json").write_text(json.dumps(combined))
            return top / "summary.json"
        for name, shard in (shards(written) if shards else written).items():
            (top / name).mkdir(exist_ok=True)
            (top / name / "summary.json").write_text(json.dumps(shard))
        (top / "release-check").mkdir()
        (top / "release-check" / "summary.json").write_text(json.dumps(combined))
        return top / "release-check" / "summary.json"

    def listing(self, *reds, live_commit=HEAD):
        """The --known-reds file: each red (step, case, live summary path)."""
        self.known.write_text(json.dumps({"live_commit": live_commit, "reds": [
            {"step": step, "case": case, "live_summary": None if path is None else str(path), "why": "fails on the live release too"}
            for step, case, path in reds]}))

    def red_on_both(self, candidate=None, live=None, listed=(RED_CASE,)):
        """The candidate fails RED_STEP's cases, the live summary fails its own, and the file lists `listed`."""
        self.candidate_fails((RED_STEP, candidate or {}))
        path = self.live_summary("live-check", (RED_STEP, live or {}))
        self.listing(*[(RED_STEP, case, path) for case in listed])
        return path


class DeployScopedKnownReds(KnownRedsFiles, DeployCase):
    """Item 608: a small fix's scoped summary is taken with --known-reds as a whole run is. A browser step the deploy's
    own choice did not touch, which the summary did not run, is not missing there, and a listed red of such a step is
    said not to have run, never that it did not fail; a unit, the deploy's refusals or the wake replay that did not run
    stays missing, and a step the change touches that the summary did not run is refused before any red is read."""

    HEALTH_CASE = "health the release line says its full run"
    # The line the operator reads for a listed red of a step the scoped run did not run (B3).
    NOT_RUN = "known red not run: %s %r: the scoped run did not run %s, which no changed path touches; its full run follows"
    # Reds of steps the health fix does not touch, as a live release's known reds list them.
    ELSEWHERE = (("native-colors", "a reply keeps its colour"), ("native-rail", "the outline opens"),
                 ("native-header", "the header holds at 320px"), ("native-webkit-rail", "the outline opens in Safari"))

    def setUp(self):
        super().setUp()
        self.known_setup()
        import test_tools_release_check as checks
        self.checks = checks
        self.live = checks.scoped_repository(self.config.live)

    def scoped_fails(self, files=None, failed=(HEALTH_CASE,), drop=()):
        """A commit changing files (the health fix) on the live commit, and its scoped summary beside the known reds:
        native-health run whole with failed cases (none: passed), the steps in drop left out."""
        commit = self.checks.scoped_change(self.config.live, self.live, files or self.checks.HEALTH_FIX)
        written = self.checks.scoped_summary(commit, self.live, ["native-health"], sorted(files or self.checks.HEALTH_FIX))
        health = native_run(self.candidate, "native-health", failed=failed, passed=("health a page at a time",), exit=1 if failed else 0)
        health["argv"] = ["/usr/bin/node", release_check.PERSISTENCE, "/x/chrome", str(self.candidate / "native" / "health"), "--health"]
        written["steps"] = [health if step["name"] == "native-health" else step for step in written["steps"] if step["name"] not in drop]
        written["passed"] = not failed and not drop
        self.summary.write_text(json.dumps(written))
        live = self.live_summary("live-check", ("native-health", dict(failed=(self.HEALTH_CASE,), passed=("health a page at a time",))),
                                 commit=self.live)
        self.listing(("native-health", self.HEALTH_CASE, live), *[(step, case, None) for step, case in self.ELSEWHERE],
                     live_commit=self.live)
        return commit

    def written(self):
        return json.loads((self.evidence / "00-release-check.json").read_text())

    def test_a_scoped_summary_whose_failed_cases_are_known_reds_is_taken_and_reds_of_steps_it_did_not_run_are_said_not_run(self):
        commit = self.scoped_fails()
        host = GitHost(self.config, ff=False)
        refusal = self.refused(host, sha=commit, known_reds=self.known)
        self.assertNotIn("known reds", refusal)
        self.assertIn("fast-forward", refusal, "taken: the deploy goes on to the live checkout")
        written = self.written()
        self.assertEqual((written["passed"], written["reasons"]), (False, ["native-health failed (exit 1)"]))
        taken = written["known_reds"]
        self.assertEqual([(red["step"], red["case"], red["taken_by"]) for red in taken["accepted"]],
                         [("native-health", self.HEALTH_CASE, "live")])
        not_run = [self.NOT_RUN % (step, case, step) for step, case in self.ELSEWHERE]
        self.assertEqual(taken["not_run"], [{"step": step, "case": case, "said": said}
                                            for (step, case), said in zip(self.ELSEWHERE, not_run)])
        self.assertEqual(taken["not_needed"], [], "a red of a step that did not run is not one that did not fail")
        # The operator reads each red's recorded line, the one taken first, then those that did not run; none is said
        # not needed.
        self.assertEqual(self.known_lines(), [taken["accepted"][0]["said"]] + not_run)
        self.assertTrue(taken["accepted"][0]["said"].startswith("known red: native-health %r failed, as it does on the live commit "
                                                                % self.HEALTH_CASE))

    def test_a_scoped_summary_that_passed_says_which_reds_did_not_run(self):
        commit = self.scoped_fails(failed=())
        self.refused(GitHost(self.config, ff=False), sha=commit, known_reds=self.known)
        taken = self.written()["known_reds"]
        not_needed = "known red not needed: native-health %r did not fail" % self.HEALTH_CASE
        self.assertEqual((taken["accepted"], taken["not_needed"]),
                         ([], [{"step": "native-health", "case": self.HEALTH_CASE, "said": not_needed}]))
        not_run = [self.NOT_RUN % (step, case, step) for step, case in self.ELSEWHERE]
        self.assertEqual(taken["not_run"], [{"step": step, "case": case, "said": said}
                                            for (step, case), said in zip(self.ELSEWHERE, not_run)])
        # A red of the step that ran and passed is said not needed, never not run; each other is said not run.
        self.assertEqual(self.known_lines(), [not_needed] + not_run)
        self.assertNotIn("known red not run: native-health", self.printed.getvalue())

    def test_a_unit_the_refusals_or_the_replay_it_did_not_run_stays_missing_with_known_reds(self):
        for name in ("python-units", "node-units", release_check.DEPLOY_REFUSALS, release_check.WAKE_REPLAY):
            with self.subTest(name):
                self.tearDown()
                self.setUp()
                commit = self.scoped_fails(drop=(name,))
                host = GitHost(self.config)
                refusal = self.refused(host, sha=commit, known_reds=self.known)
                self.assertIn("known reds: the release check did not run %s; a known red stands only for a case of a step that ran" % name,
                              refusal)
                self.assertFalse(host.ran("ps"), "refused before the live processes are read")

    def test_a_step_the_change_touches_that_it_did_not_run_is_refused_before_any_red_is_read(self):
        # The fix touches the reply's colour too, which native-colors reads; native-colors is a listed known red, and the
        # summary did not run it.
        files = dict(self.checks.HEALTH_FIX, **{"web/aim.css": self.checks.SCOPED_CSS.replace("color:black", "color:#222").replace("color:blue", "color:navy")})
        commit = self.scoped_fails(files=files)
        host = GitHost(self.config)
        refusal = self.refused(host, sha=commit, known_reds=self.known)
        self.assertIn("but what changed since then touches native-colors, which it did not run", refusal)
        self.assertNotIn("known reds", refusal)
        self.assertIsNone(self.written()["known_reds"], "no red is read for a scope the deploy did not confirm")
        self.assertFalse(host.ran("ps"))


class DeployRefusesJudgedTree(KnownRedsFiles, DeployCase):
    """Item 588: a run on a tree already judged is never waited for. A pull request's run judges GitHub's merge of it,
    which is neither the merged head nor the PR's head; --release-check takes that run's summary for a merged head whose
    tree, read from the live checkout's git, is the judged commit's, and records both commits and the tree. The live
    path is a real repository holding the judged commit, the merged head with its tree, and a commit with another."""

    def setUp(self):
        super().setUp()
        self.known_setup()
        self.live = Path(self.raw["live"])
        self.live.mkdir()
        self.git("init", "-q")
        (self.live / "harbour.txt").write_text("the ferry leaves at dawn\n")
        self.commit("the live plan")
        (self.live / "harbour.txt").write_text("the ferry leaves at dusk\n")
        self.judged = self.commit("what the pull request's run checked")
        self.merged = self.commit("the train merged onto the release branch", "--allow-empty")
        (self.live / "harbour.txt").write_text("the ferry stays in port\n")
        self.other = self.commit("a change no run saw")
        self.tree = self.git("rev-parse", self.merged + "^{tree}")

    def git(self, *args):
        return subprocess.run(["git", "-C", str(self.live)] + list(args), check=True, capture_output=True, text=True).stdout.strip()

    def commit(self, message, *more):
        self.git("add", "-A")
        self.git("-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q", "-m", message, *more)
        return self.git("rev-parse", "HEAD")

    def host(self):
        """The live HEAD and the merged head on the live checkout's line; everything else as FakeHost answers it."""
        return FakeHost(self.config, line=(HEAD, self.merged))

    def ran_on(self, commit, **written):
        self.summary.write_text(json.dumps(dict(summary(commit=commit), **written)))

    def evidence_of(self, name):
        # The known reds' own reader of a step's evidence, whose read tools/ci-input-reads.json already declares.
        return DeployRefusesKnownReds.evidence_of(self, name)

    def refused_before_anything(self, host, words, **release):
        refusal = self.refused(host, sha=self.merged, **release)
        self.assertIn("the release check has not passed for %s: the release check ran on " % self.merged, refusal)
        self.assertIn(words, refusal)
        self.assertEqual(self.records(), ["00-release-check"])
        self.assertFalse(host.ran("ps"), "refused before the live processes are read")
        self.assertEqual(self.kills, [])
        written = self.evidence_of("00-release-check")
        self.assertFalse(written["passed"])
        self.assertNotIn("judged", written, "a summary that was not taken records no judged tree")
        return refusal

    def test_a_summary_of_a_commit_whose_tree_is_the_merged_heads_is_taken_and_recorded(self):
        self.assertNotEqual(self.judged, self.merged)
        self.ran_on(self.judged)
        host = self.host()
        self.planned(host, sha=self.merged)
        written = self.evidence_of("00-release-check")
        self.assertEqual((written["commit"], written["passed"], written["reasons"]), (self.merged, True, []))
        self.assertEqual(written.get("judged"), {"judged_commit": self.judged, "commit": self.merged, "tree": self.tree})
        record = self.evidence_of("02-release")["record"]
        self.assertEqual((record["commit"], record["items"]), (self.merged, [88]))
        self.assertEqual(record["measurements"]["release_check"]["commit"], self.judged, "the counts are the judged run's")
        self.assertEqual(record["measurements"].get("judged_tree"),
                         {"judged_commit": self.judged, "commit": self.merged, "tree": self.tree, "judgment": "pending"})
        self.assertEqual([call[-1] for call in host.calls if call[-1].endswith("^{tree}")],
                         [self.judged + "^{tree}", self.merged + "^{tree}"], "both trees are read from the live checkout's git")

    def test_a_summary_of_exactly_the_commit_reads_no_tree_and_records_no_judged_tree(self):
        self.ran_on(self.merged)
        host = self.host()
        self.planned(host, sha=self.merged)
        self.assertNotIn("judged", self.evidence_of("00-release-check"))
        self.assertNotIn("judged_tree", self.evidence_of("02-release")["record"]["measurements"])
        self.assertEqual([call for call in host.calls if call[-1].endswith("^{tree}")], [])

    def test_a_summary_of_a_commit_with_another_tree_is_refused_before_anything_stops(self):
        self.ran_on(self.other)
        tree = self.git("rev-parse", self.other + "^{tree}")
        self.refused_before_anything(self.host(), "%s, not %s, and its tree %s is not %s's tree %s"
                                     % (self.other, self.merged, tree, self.merged, self.tree))

    def test_a_summary_whose_commit_git_cannot_read_is_refused_never_taken_as_the_same_tree(self):
        missing = "e" * 40
        self.ran_on(missing)
        self.refused_before_anything(self.host(), "%s, not %s, and the tree of %s could not be read" % (missing, self.merged, missing))

    def test_a_summary_naming_no_full_commit_is_refused_without_asking_git(self):
        for name in ("HEAD", self.merged[:12]):
            with self.subTest(name):
                self.again()
                self.ran_on(name)
                host = self.host()
                self.refused_before_anything(host, "%s, not %s" % (name, self.merged))
                self.assertEqual(host.calls, [])

    def again(self):
        self.tearDown()
        self.setUp()

    def test_a_deploy_that_measures_an_outcome_takes_only_a_summary_of_exactly_the_commit(self):
        self.config_path.write_text(json.dumps(dict(self.raw, outcome="building-theseus", reporter_profile="agent-b.profile.json")))
        self.config = deploy.load_config(self.config_path)
        self.ran_on(self.judged)
        host = self.host()
        self.refused_before_anything(host, "measuring building-theseus records its run at the summary's own commit, so it takes "
                                     "only a summary of exactly %s" % self.merged)
        self.assertEqual(host.calls, [])

    def test_known_reds_are_matched_against_the_judged_summary_as_against_any(self):
        self.candidate_fails((RED_STEP, dict(failed=(RED_CASE, "a whisper keeps its colour"))), commit=self.judged)
        live = self.live_summary("live-check", (RED_STEP, dict(failed=(RED_CASE, "a whisper keeps its colour"))))
        self.listing((RED_STEP, RED_CASE, live), (RED_STEP, "a whisper keeps its colour", live))
        self.planned(self.host(), sha=self.merged, known_reds=self.known)
        written = self.evidence_of("00-release-check")
        self.assertEqual((written["passed"], written["reasons"]), (False, ["%s failed (exit 1)" % RED_STEP]))
        self.assertEqual([(red["case"], red["taken_by"]) for red in written["known_reds"]["accepted"]],
                         [(RED_CASE, "live"), ("a whisper keeps its colour", "live")])
        measured = self.evidence_of("02-release")["record"]["measurements"]
        self.assertEqual([red["case"] for red in measured["known_reds"]], [RED_CASE, "a whisper keeps its colour"])
        self.assertEqual(measured["native"]["steps"]["colors"], {"pass": 1, "total": 3}, "still counted as failed")
        self.assertEqual((measured.get("judged_tree") or {}).get("judged_commit"), self.judged)

    def test_a_red_the_known_reds_do_not_list_is_refused_for_the_judged_summary_as_for_any(self):
        self.candidate_fails((RED_STEP, dict(failed=(RED_CASE, "a whisper keeps its colour"))), commit=self.judged)
        self.listing((RED_STEP, RED_CASE, self.live_summary("live-check", (RED_STEP, dict(failed=(RED_CASE,))))))
        refusal = self.refused(self.host(), sha=self.merged, known_reds=self.known)
        self.assertIn("known reds: %s failed 'a whisper keeps its colour', which the known reds do not list" % RED_STEP, refusal)
        self.assertEqual((self.records(), self.kills), (["00-release-check"], []))

    RED_RUN = (RED_STEP, dict(failed=(RED_CASE, "a whisper keeps its colour")))

    def test_known_reds_read_a_whole_run_downloaded_as_documented_from_the_shards_beside_its_summary(self):
        # The pull request's run is the Checks workflow's: its whole-run summary keeps no evidence of its own, and each
        # failed step's evidence is in the shard that ran it, which `gh run download RUN_ID -D DIR` puts beside it.
        self.summary = self.whole_run("run-43", self.RED_RUN, commit=self.judged)
        live = self.live_summary("live-check", self.RED_RUN)
        self.listing((RED_STEP, RED_CASE, live), (RED_STEP, "a whisper keeps its colour", live))
        self.planned(self.host(), sha=self.merged, known_reds=self.known)
        written = self.evidence_of("00-release-check")
        self.assertEqual((written["passed"], written["reasons"]), (False, ["%s failed (exit 1)" % RED_STEP]))
        self.assertEqual([(red["case"], red["taken_by"]) for red in written["known_reds"]["accepted"]],
                         [(RED_CASE, "live"), ("a whisper keeps its colour", "live")])
        self.assertEqual(written.get("judged"), {"judged_commit": self.judged, "commit": self.merged, "tree": self.tree},
                         "a summary taken with known reds records the tree it judged")
        measured = self.evidence_of("02-release")["record"]["measurements"]
        self.assertEqual(measured["native"]["steps"]["colors"], {"pass": 1, "total": 3}, "still counted as failed")
        self.assertEqual(measured["judged_tree"]["judged_commit"], self.judged)

    def test_a_live_whole_run_is_read_from_the_shards_beside_its_summary_too(self):
        self.candidate_fails(self.RED_RUN, commit=self.judged)
        live = self.whole_run("live-run", self.RED_RUN, commit=HEAD)
        self.listing((RED_STEP, RED_CASE, live), (RED_STEP, "a whisper keeps its colour", live))
        self.planned(self.host(), sha=self.merged, known_reds=self.known)
        self.assertEqual([(red["case"], red["live_summary"]) for red in self.evidence_of("00-release-check")["known_reds"]["accepted"]],
                         [(RED_CASE, str(live)), ("a whisper keeps its colour", str(live))])

    def test_a_whole_run_whose_shards_do_not_hold_the_step_as_it_says_is_refused(self):
        said = "the candidate's %s" % RED_STEP
        drift = lambda step: dict(step, counts=dict(step["counts"], tests=step["counts"]["tests"] + 1))
        for words, run in (
                # Downloaded as the summary alone: no shard is beside it.
                ("%s has no readable evidence: its run's shard check-0/summary.json" % said, dict(downloaded="release-check")),
                ("%s has no readable evidence: its run's shard check-1/summary.json" % said,
                 dict(shards=lambda written: {"check-0": written["check-0"]})),
                ("%s: its run's shard check-1/summary.json ran on %s, not {judged}" % (said, "d" * 40),
                 dict(shards=lambda written: dict(written, **{"check-1": dict(written["check-1"], commit="d" * 40)}))),
                ("%s: 0 of its run's shards hold the step as the summary has it, not one" % said,
                 dict(shards=lambda written: dict(written, **{"check-1": dict(written["check-1"], steps=[
                     drift(step) for step in written["check-1"]["steps"]])}))),
                ("%s: 2 of its run's shards hold the step as the summary has it, not one" % said,
                 dict(shards=lambda written: dict(written, **{"check-0": dict(written["check-0"], steps=written["check-0"]["steps"]
                                                                              + written["check-1"]["steps"])}))),
                ("%s: its run names the shard '../check-1/summary.json', not check-N/summary.json" % said,
                 dict(named=["check-0/summary.json", "../check-1/summary.json"]))):
            with self.subTest(words):
                self.again()
                self.summary = self.whole_run("run-43", self.RED_RUN, commit=self.judged, **run)
                live = self.live_summary("live-check", self.RED_RUN)
                self.listing((RED_STEP, RED_CASE, live), (RED_STEP, "a whisper keeps its colour", live))
                refusal = self.refused(self.host(), sha=self.merged, known_reds=self.known)
                self.assertIn("known reds: " + words.format(judged=self.judged), refusal)
                self.assertEqual((self.records(), self.kills), (["00-release-check"], []))
                # The summary describes the merged head's tree; it is the known reds that refuse, so the record of the
                # attempt says which commit's run it read.
                self.assertEqual(self.evidence_of("00-release-check").get("judged"),
                                 {"judged_commit": self.judged, "commit": self.merged, "tree": self.tree})

    def test_a_summary_of_the_same_tree_refused_for_another_reason_records_no_judged_tree(self):
        # The tree is read, and is the merged head's, but the summary is still not one the deploy takes.
        for written, words in ((dict(tree_unchanged=False), "the checkout changed while the release check ran"),
                               (dict(only=[RED_STEP]), "the release check ran only %s, so it does not describe " % RED_STEP)):
            with self.subTest(words):
                self.again()
                self.ran_on(self.judged, **written)
                host = self.host()
                refusal = self.refused(host, sha=self.merged)
                self.assertIn("the release check has not passed for %s: %s" % (self.merged, words), refusal)
                self.assertNotIn("ran on " + self.judged, refusal, "the tree is the merged head's")
                self.assertEqual([call[-1] for call in host.calls if call[-1].endswith("^{tree}")],
                                 [self.judged + "^{tree}", self.merged + "^{tree}"], "the tree was read")
                self.assertEqual((self.records(), self.kills), (["00-release-check"], []))
                recorded = self.evidence_of("00-release-check")
                self.assertFalse(recorded["passed"])
                self.assertNotIn("judged", recorded, "a summary that was not taken records no judged tree")


class DeployRefusesKnownReds(KnownRedsFiles, DeployCase):
    """Item 178: --known-reds takes a release check whose only failures are browser cases the live commit fails too."""

    def setUp(self):
        super().setUp()
        self.known_setup()

    def again(self):
        """A fresh instance, evidence and files for the next subtest."""
        self.tearDown()
        self.setUp()

    def evidence_of(self, name):
        return json.loads((self.evidence / (name + ".json")).read_text())

    def refused_known(self, host, words, dry=False):
        """Refused at the release-check step, naming words, with nothing but the live HEAD read and nothing stopped."""
        refusal = self.refused(host, dry=dry, known_reds=self.known)
        self.assertIn("known reds: ", refusal)
        self.assertIn(words, refusal)
        self.assertEqual(self.records(), ["00-release-check"])
        self.assertEqual([call for call in host.calls if "rev-parse" not in call and not any(POLICY in part for part in call)], [],
                         "nothing but the live HEAD and the policy at it is read")
        self.assertEqual(self.kills, [])
        written = self.evidence_of("00-release-check")
        self.assertFalse(written["passed"])
        self.assertIn(words, written["known_reds"]["refused"])
        return refusal

    def test_failed_browser_cases_the_live_commit_fails_too_are_taken_as_known_reds_and_recorded(self):
        live = self.red_on_both(candidate=dict(failed=(RED_CASE, "a whisper keeps its colour")),
                                live=dict(failed=("a whisper keeps its colour", RED_CASE, "an older red")))
        other = self.live_summary("live-journal", ("native-journal", dict(failed=("a journal case",))))
        self.listing((RED_STEP, RED_CASE, live), (RED_STEP, "a whisper keeps its colour", live),
                     ("native-journal", "a journal case", other))
        host = FakeHost(self.config)
        self.planned(host, known_reds=self.known)
        written = self.evidence_of("00-release-check")
        self.assertFalse(written["passed"], "known reds are never described as passes")
        self.assertEqual(written["reasons"], ["%s failed (exit 1)" % RED_STEP])
        taken = written["known_reds"]
        self.assertEqual([(red["step"], red["case"], red["live_summary"], red["live_commit"]) for red in taken["accepted"]],
                         [(RED_STEP, RED_CASE, str(live), HEAD), (RED_STEP, "a whisper keeps its colour", str(live), HEAD)])
        not_needed = "known red not needed: native-journal 'a journal case' did not fail"
        self.assertEqual(taken["not_needed"], [{"step": "native-journal", "case": "a journal case", "said": not_needed}])
        self.assertEqual(taken["not_run"], [], "a whole run leaves no step unrun")
        taken_lines = ["known red: %s %r failed, as it does on the live commit %s (%s): fails on the live release too"
                       % (RED_STEP, case, HEAD[:12], live) for case in (RED_CASE, "a whisper keeps its colour")]
        self.assertEqual([red["said"] for red in taken["accepted"]], taken_lines)
        # The operator reads exactly the lines the record keeps, one per red, and none of a step that did not run.
        self.assertEqual(self.known_lines(), taken_lines + [not_needed])
        record = self.evidence_of("02-release")["record"]
        self.assertEqual(record["measurements"]["known_reds"], [
            {"step": RED_STEP, "case": case, "live_summary": str(live), "live_commit": HEAD, "why": "fails on the live release too",
             "taken_by": "live"} for case in (RED_CASE, "a whisper keeps its colour")])
        self.assertEqual(record["measurements"]["native"]["steps"]["colors"], {"pass": 1, "total": 3}, "still counted as failed")
        self.assertEqual(self.kills, [])

    def test_a_reader_step_is_taken_when_its_harness_printed_its_results_last(self):
        self.candidate_fails((READER_STEP, {}))
        self.listing((READER_STEP, RED_CASE, self.live_summary("live-check", (READER_STEP, {}))))
        self.planned(FakeHost(self.config), known_reds=self.known)
        self.assertEqual([red["case"] for red in self.evidence_of("02-release")["record"]["measurements"]["known_reds"]], [RED_CASE])

    def test_a_failed_case_the_known_reds_do_not_list_is_refused(self):
        self.red_on_both(candidate=dict(failed=(RED_CASE, "a whisper keeps its colour")),
                         live=dict(failed=(RED_CASE, "a whisper keeps its colour")))
        self.refused_known(FakeHost(self.config), "%s failed 'a whisper keeps its colour', which the known reds do not list" % RED_STEP)

    def test_a_case_named_loosely_is_not_the_case_that_failed(self):
        for listed, failed in (("a reply keeps its", RED_CASE), (RED_CASE.upper(), RED_CASE), (RED_CASE + " ", RED_CASE),
                               ("a reply .*", RED_CASE), (RED_CASE, RED_CASE + " [webkit]")):
            with self.subTest(listed=listed, failed=failed):
                self.again()
                self.red_on_both(candidate=dict(failed=(failed,)), live=dict(failed=(failed,)), listed=(listed,))
                self.refused_known(FakeHost(self.config), "%s failed %r, which the known reds do not list" % (RED_STEP, failed))

    def test_a_listed_case_the_live_summary_does_not_show_failing_is_refused(self):
        for live in (dict(failed=("a whisper keeps its colour",)), dict(failed=(RED_CASE + " [webkit]",))):
            with self.subTest(live=live):
                self.again()
                path = self.red_on_both(live=live)
                self.refused_known(FakeHost(self.config), "%s %r is not admitted by %s at the live commit, nor among the cases "
                                   "that failed in %s on the live commit" % (RED_STEP, RED_CASE, POLICY, path))

    def test_a_live_summary_of_another_commit_or_a_changed_checkout_is_refused(self):
        for written, words in ((dict(commit="d" * 40), "ran on %s, not the live commit %s" % ("d" * 40, HEAD)),
                               (dict(tree_unchanged=False), "the checkout changed while it ran"),
                               (dict(tool="something else"), "is not a release-check summary")):
            with self.subTest(words):
                self.again()
                self.candidate_fails((RED_STEP, {}))
                self.listing((RED_STEP, RED_CASE, self.live_summary("live-check", (RED_STEP, {}), **written)))
                self.refused_known(FakeHost(self.config), words)

    def test_a_live_summary_that_cannot_be_read_is_refused(self):
        def copied_away(path):
            # docs/tools.md: the evidence a summary names stays beside it; a copy elsewhere has none.
            away = Path(self.tmp.name) / "copied" / "summary.json"
            away.parent.mkdir()
            away.write_text(path.read_text())
            return away

        def rewritten(text):
            def change(path):
                path.write_text(text(path))
                return path
            return change
        for change, words in ((lambda path: path.with_name("sumary.json"), "cannot be read"),
                              (rewritten(lambda path: "{not json"), "cannot be read"),
                              (rewritten(lambda path: "[]"), "is not a release-check summary"),
                              (rewritten(lambda path: json.dumps(dict(json.loads(path.read_text()), steps=5))), "lists no steps"),
                              (copied_away, "the live %s has no readable evidence" % RED_STEP)):
            with self.subTest(words):
                self.again()
                self.candidate_fails((RED_STEP, {}))
                self.listing((RED_STEP, RED_CASE, change(self.live_summary("live-check", (RED_STEP, {})))))
                self.refused_known(FakeHost(self.config), words)

    def test_a_live_summary_that_did_not_run_the_listed_step_once_is_refused(self):
        for runs, steps in (((("native-journal", dict(failed=("a journal case",))),), lambda steps: steps),
                            # The same evidence listed twice, as a summary that ran the step twice would name it.
                            (((RED_STEP, {}),), lambda steps: steps * 2),
                            # An entry that is not an object is no step.
                            (((RED_STEP, {}),), lambda steps: [5])):
            with self.subTest(runs=[name for name, _ in runs], steps=steps([1])):
                self.again()
                self.candidate_fails((RED_STEP, {}))
                path = self.live_summary("live-check", *runs)
                written = json.loads(path.read_text())
                path.write_text(json.dumps(dict(written, steps=steps(written["steps"]))))
                self.listing((RED_STEP, RED_CASE, path))
                self.refused_known(FakeHost(self.config), "the live summary %s did not run %s once" % (path, RED_STEP))

    def test_a_summary_that_skipped_the_browser_steps_is_still_refused_for_a_unit_red(self):
        self.red_on_both()
        written = summary(failed=("python-units",))
        written["steps"] = [step for step in written["steps"] if step["name"] not in release_check.SKIPPABLE]
        written["skipped"] = {"steps": list(release_check.SKIPPABLE), "live": HEAD, "changed": ["docs/tools.md"],
                              "why": release_check.skipped_words(HEAD)}
        self.summary.write_text(json.dumps(written))
        host = FakeHost(self.config, changed=["docs/tools.md"])
        words = "python-units failed; only a browser step's failed cases can be known reds"
        self.assertIn("known reds: %s" % words, self.refused(host, known_reds=self.known))
        self.assertEqual(self.evidence_of("00-release-check")["known_reds"]["refused"], words)
        # The skip is judged on what the live checkout's git says changed since the live commit; nothing else is read.
        self.assertEqual([call for call in host.calls if call[:3] != ["git", "-C", str(self.config.live)]], [])
        self.assertEqual((self.records(), self.kills), (["00-release-check"], []))

    def test_a_summary_that_did_not_pass_though_no_step_failed_is_refused(self):
        self.red_on_both()
        self.summary.write_text(json.dumps(dict(summary(), passed=False)))
        self.refused_known(FakeHost(self.config), "the release check did not pass, and no step of it failed")

    def test_known_reds_read_beside_a_commit_that_is_not_live_are_refused(self):
        self.candidate_fails((RED_STEP, {}))
        other = "a" * 40
        self.listing((RED_STEP, RED_CASE, self.live_summary("live-check", (RED_STEP, {}), commit=other)), live_commit=other)
        host = FakeHost(self.config)
        self.refused_known(host, "the known reds were read beside %s, but %s is live" % (other, HEAD))
        self.assertEqual(host.ran("rev-parse")[0][:3], ["git", "-C", str(self.config.live)], "the HEAD of the live checkout")

    def test_a_unit_step_red_is_never_a_known_red(self):
        for unit in ("python-units", "node-units", release_check.DEPLOY_REFUSALS, release_check.WAKE_REPLAY):
            with self.subTest(unit):
                self.again()
                self.candidate_fails(unit, (RED_STEP, {}))
                self.listing((RED_STEP, RED_CASE, self.live_summary("live-check", (RED_STEP, {}))))
                self.refused_known(FakeHost(self.config), "%s failed; only a browser step's failed cases can be known reds" % unit)

    def test_a_unit_step_cannot_be_listed(self):
        self.candidate_fails((RED_STEP, {}))
        self.known.write_text(json.dumps({"live_commit": HEAD, "reds": [
            {"step": "python-units", "case": "test_x", "live_summary": str(self.summary), "why": "flaky"}]}))
        self.refused_known(FakeHost(self.config), "a known red names 'python-units'; only a browser step")

    def test_a_step_the_candidate_did_not_run_or_lists_twice_is_refused(self):
        def without_journal(written):
            return [step for step in written["steps"] if step["name"] != "native-journal"]
        for change, words in ((dict(steps=without_journal), "the release check did not run native-journal"),
                              # A step entry that is not an object is passed over, as the release check's judge does.
                              (dict(steps=lambda written: without_journal(written) + [5]), "the release check did not run native-journal"),
                              (dict(steps=without_journal, required=lambda written: []), "the release check did not run native-journal"),
                              (dict(required=lambda written: written["required"] + ["native-later"]),
                               "the release check did not run native-later"),
                              (dict(steps=lambda written: written["steps"] + [step for step in written["steps"] if step["name"] == RED_STEP]),
                               "the release check lists %s more than once" % RED_STEP)):
            with self.subTest(words=words, change=sorted(change)):
                self.again()
                self.candidate_fails((RED_STEP, {}))
                written = json.loads(self.summary.read_text())
                written.update({key: made(written) for key, made in change.items()})
                self.summary.write_text(json.dumps(written))
                self.listing((RED_STEP, RED_CASE, self.live_summary("live-check", (RED_STEP, {}))))
                self.refused_known(FakeHost(self.config), words)

    def harness_cases(self):
        return (("the browser stayed open", dict(closed=False), "the browser did not close"),
                ("the fixture exited 1", dict(fixture=1), "its fixture exited 1, not 0"),
                ("no fixture exit", dict(fixture=None), "its fixture exited None, not 0"),
                ("no failed case with a failing exit", dict(failed=()), "failed, but its results name no failed case"),
                ("the harness never reached its end", dict(ended=False), "the harness did not reach its end"),
                ("a crash", dict(exit=134), "exited 134, not 1"),
                ("killed", dict(exit=-9), "exited -9, not 1"),
                ("a case named twice", dict(cases=[{"name": RED_CASE, "passed": True}, {"name": RED_CASE, "passed": False}]),
                 "the failed case %r is not named once" % RED_CASE),
                ("no cases", dict(cases=[]), "lists no cases"),
                ("a failed case with no name", dict(cases=[{"name": RED_CASE, "passed": False}, {"passed": False}]),
                 "the failed case None is not named once"),
                ("a failed case named ''", dict(cases=[{"name": RED_CASE, "passed": False}, {"name": "", "passed": False}]),
                 "the failed case '' is not named once"),
                ("a failed case named by a number", dict(cases=[{"name": RED_CASE, "passed": False}, {"name": 5, "passed": False}]),
                 "the failed case 5 is not named once"),
                ("its last assertion inside another line", dict(last="console: %s\n" % known_reds.END_LINE),
                 "the harness did not reach its end"),
                ("no log", dict(log=False), "has no readable evidence"),
                ("results that are not JSON", dict(results="{not json"), "has no readable evidence"),
                ("results that are not an object", dict(results="[]"), "lists no cases"),
                ("cases that are not a list", dict(results=json.dumps({"results": 5, "fixtureExitCode": 0, "browserClosed": True})),
                 "lists no cases"),
                ("a case that is not an object", dict(results=json.dumps({"results": [RED_CASE], "fixtureExitCode": 0,
                                                                          "browserClosed": True})), "lists no cases"))

    def test_a_candidate_step_whose_harness_did_not_end_as_it_should_is_refused(self):
        for case, run, words in self.harness_cases():
            with self.subTest(case):
                self.again()
                self.candidate_fails((RED_STEP, run))
                self.listing((RED_STEP, RED_CASE, self.live_summary("live-check", (RED_STEP, {}))))
                self.refused_known(FakeHost(self.config), "the candidate's %s" % RED_STEP)
                self.assertIn(words, self.evidence_of("00-release-check")["known_reds"]["refused"])

    def test_a_live_step_whose_harness_did_not_end_as_it_should_is_refused(self):
        for case, run, words in self.harness_cases():
            with self.subTest(case):
                self.again()
                self.candidate_fails((RED_STEP, {}))
                self.listing((RED_STEP, RED_CASE, self.live_summary("live-check", (RED_STEP, run))))
                self.refused_known(FakeHost(self.config), "the live %s" % RED_STEP)
                self.assertIn(words, self.evidence_of("00-release-check")["known_reds"]["refused"])

    def test_a_reader_step_that_cannot_show_its_run_ended_is_refused(self):
        for name, run, words in ((READER_STEP, dict(ended=False), "the harness did not reach its end"),
                                 (READER_STEP, dict(closed=False), "the browser did not close"),
                                 (READER_STEP, dict(printed=dict(closed=False)), "the harness did not reach its end"),
                                 (READER_STEP, dict(printed=dict(results=[])), "the harness did not reach its end"),
                                 (READER_STEP, dict(last="TypeError: cannot read properties of undefined\n"),
                                  "the harness did not reach its end")):
            with self.subTest(name=name, run=run):
                self.again()
                self.candidate_fails((name, run))
                self.listing((name, RED_CASE, self.live_summary("live-check", (name, {}))))
                self.refused_known(FakeHost(self.config), words)

    # What tests/reader_compose_native.mjs leaves when every planned phase ran, a case failed and its fixture exited 0.
    # Its log is not read for its end: this one does not hold the other harnesses' last assertion.
    ATTESTED = {"fixtureExitSignal": None, "harnessCompleted": True, "plannedPhases": COMPOSE_PHASES, "completedPhases": COMPOSE_PHASES}
    COMPOSED = dict(more=ATTESTED, last="1 passed, 1 failed\n")

    def composed(self, **change):
        """The composer's evidence with native.json's fields changed (MISSING leaves one out), as native_run takes it."""
        run = dict(self.COMPOSED, **{key: value for key, value in change.items() if key != "more"})
        run["more"] = dict(self.ATTESTED, **change.get("more", {}))
        return run

    def test_a_composer_step_whose_run_ended_is_taken(self):
        self.candidate_fails((COMPOSE_STEP, self.COMPOSED))
        self.listing((COMPOSE_STEP, RED_CASE, self.live_summary("live-check", (COMPOSE_STEP, self.COMPOSED))))
        self.planned(FakeHost(self.config), known_reds=self.known)
        self.assertEqual([(red["step"], red["case"]) for red in self.evidence_of("02-release")["record"]["measurements"]["known_reds"]],
                         [(COMPOSE_STEP, RED_CASE)])

    def test_a_composer_step_whose_run_did_not_attest_its_end_is_refused(self):
        # Cleanup closes the composer's browsers and writes native.json however its run ended; only these fields say
        # whether every planned phase ran and its fixture exited 0, with no signal.
        five = COMPOSE_PHASES[:5]
        fixture, phases, completed = "its fixture exited", "its %s are", "the harness did not complete"
        for case, run, words in (
                ("a phase threw after three returned, its browser closed and its fixture exited 0",
                 self.composed(more={"completedPhases": COMPOSE_PHASES[:3], "harnessCompleted": False}), phases % "completedPhases"),
                ("the last phase's browser did not close cleanly", self.composed(more={"completedPhases": five, "harnessCompleted": False}),
                 phases % "completedPhases"),
                ("seeding the fixture failed", self.composed(cases=[], more={"completedPhases": [], "harnessCompleted": False}), "lists no cases"),
                ("harnessCompleted false", self.composed(more={"harnessCompleted": False}), completed),
                ("harnessCompleted missing", self.composed(more={"harnessCompleted": MISSING}), completed),
                ("harnessCompleted not a boolean", self.composed(more={"harnessCompleted": "true"}), completed),
                ("its fixture was killed by a signal", self.composed(fixture=None, more={"fixtureExitSignal": "SIGKILL"}), fixture),
                ("a signal beside exit 0", self.composed(more={"fixtureExitSignal": "SIGTERM"}), fixture),
                ("fixtureExitSignal missing", self.composed(more={"fixtureExitSignal": MISSING}), fixture),
                ("its fixture exited 1", self.composed(fixture=1), fixture),
                ("fixtureExitCode null", self.composed(fixture=None), fixture),
                ("fixtureExitCode missing", self.composed(more={"fixtureExitCode": MISSING}), fixture),
                ("fixtureExitCode false", self.composed(fixture=False), fixture),
                ("fixtureExitCode 0.0", self.composed(fixture=0.0), fixture),
                ("plannedPhases missing", self.composed(more={"plannedPhases": MISSING}), phases % "plannedPhases"),
                ("plannedPhases short of one", self.composed(more={"plannedPhases": five, "completedPhases": five}), phases % "plannedPhases"),
                ("plannedPhases out of order", self.composed(more={"plannedPhases": COMPOSE_PHASES[::-1]}), phases % "plannedPhases"),
                ("completedPhases missing", self.composed(more={"completedPhases": MISSING}), phases % "completedPhases"),
                ("completedPhases out of order", self.composed(more={"completedPhases": COMPOSE_PHASES[1:] + COMPOSE_PHASES[:1]}),
                 phases % "completedPhases"),
                ("completedPhases with one twice", self.composed(more={"completedPhases": COMPOSE_PHASES + COMPOSE_PHASES[-1:]}),
                 phases % "completedPhases"),
                ("the harness was killed", self.composed(exit=-15), "exited -15, not 1"),
                ("a browser stayed open", self.composed(closed=False), "the browser did not close")):
            for side in ("candidate", "live"):
                with self.subTest(case, side=side):
                    self.again()
                    self.candidate_fails((COMPOSE_STEP, run if side == "candidate" else self.COMPOSED))
                    self.listing((COMPOSE_STEP, RED_CASE, self.live_summary("live-check", (COMPOSE_STEP, run if side == "live" else self.COMPOSED))))
                    refusal = self.refused_known(FakeHost(self.config), words)
                    self.assertIn("the %s %s" % ("candidate's" if side == "candidate" else "live", COMPOSE_STEP), refusal)

    def test_the_composers_fields_do_not_stand_for_another_harnesss_last_assertion(self):
        # A native.json harness other than the composer's still has to reach its last assertion.
        run = dict(more=self.ATTESTED, last="1 passed, 1 failed\n")
        self.candidate_fails((RED_STEP, run))
        self.listing((RED_STEP, RED_CASE, self.live_summary("live-check", (RED_STEP, {}))))
        self.refused_known(FakeHost(self.config), "the candidate's %s: the harness did not reach its end" % RED_STEP)

    def test_counts_the_candidate_summary_does_not_share_with_its_evidence_are_refused(self):
        self.candidate_fails((RED_STEP, {}))
        written = json.loads(self.summary.read_text())
        [step] = [step for step in written["steps"] if step["name"] == RED_STEP]
        step["counts"] = dict(step["counts"], failed=[], fail=0)
        self.summary.write_text(json.dumps(written))
        self.listing((RED_STEP, RED_CASE, self.live_summary("live-check", (RED_STEP, {}))))
        self.refused_known(FakeHost(self.config), "the summary's counts are not those of its evidence")

    def test_an_only_candidate_or_one_of_another_commit_is_refused_as_before(self):
        for written, words in ((dict(only=[RED_STEP]), "the release check ran only %s, so it does not describe %s" % (RED_STEP, SHA)),
                               (dict(commit="d" * 40), "the release check ran on %s, not %s, and the tree of %s could not be read"
                                % ("d" * 40, SHA, "d" * 40))):
            with self.subTest(words):
                self.again()
                self.red_on_both()
                self.summary.write_text(json.dumps(dict(json.loads(self.summary.read_text()), **written)))
                host = FakeHost(self.config)
                refusal = self.refused(host, known_reds=self.known)
                self.assertIn("the release check has not passed for %s: " % SHA, refusal)
                self.assertIn(words, refusal)
                # Item 588: a summary of another commit is read for its tree alone, which git cannot read here.
                self.assertEqual([call for call in host.calls if not call[-1].endswith("^{tree}")], [])
                self.assertEqual((self.records(), self.kills), (["00-release-check"], []))

    def test_a_malformed_known_reds_file_is_refused(self):
        self.red_on_both()
        good = json.loads(self.known.read_text())
        red = good["reds"][0]
        without_why = {key: value for key, value in red.items() if key != "why"}
        for written, words in (("not json", "no readable known reds file"),
                               (None, "no readable known reds file"),
                               ([good], "exactly live_commit, reds"),
                               (dict(good, extra=1), "exactly live_commit, reds"),
                               ({"reds": good["reds"]}, "exactly live_commit, reds"),
                               (dict(good, live_commit="b" * 12), "full 40-character commit"),
                               (dict(good, live_commit=None), "full 40-character commit"),
                               (dict(good, reds=[]), "at least one known red"),
                               (dict(good, reds="native-colors"), "at least one known red"),
                               (dict(good, reds=[RED_STEP]), "each known red is a JSON object with exactly step, case, live_summary, why"),
                               (dict(good, reds=[5]), "each known red is a JSON object with exactly step, case, live_summary, why"),
                               (dict(good, reds=[without_why]), "each known red is a JSON object with exactly step, case, live_summary, why"),
                               (dict(good, reds=[dict(red, seen="today")]), "each known red is a JSON object with exactly"),
                               (dict(good, reds=[dict(red, step=[RED_STEP])]), "only a browser step (native-…) can have known reds"),
                               (dict(good, reds=[dict(red, live_summary="live-check/summary.json")]), "is an absolute path"),
                               (dict(good, reds=[dict(red, live_summary=5)]), "is an absolute path"),
                               (dict(good, reds=[dict(red, why="one\ntwo")]), "one line of at most"),
                               (dict(good, reds=[dict(red, why="one\rtwo")]), "one line of at most"),
                               (dict(good, reds=[dict(red, why=" ")]), "one line of at most"),
                               (dict(good, reds=[dict(red, why=5)]), "one line of at most"),
                               (dict(good, reds=[dict(red, why="w" * (known_reds.WHY_LENGTH + 1))]), "one line of at most"),
                               (dict(good, reds=[dict(red, case="")]), "names no case"),
                               (dict(good, reds=[dict(red, case=5)]), "names no case"),
                               (dict(good, reds=[red, red]), "is listed twice")):
            with self.subTest(words=words, written=written):
                self.again()
                self.red_on_both()
                if written is None:
                    self.known.unlink()
                else:
                    self.known.write_text(written if isinstance(written, str) else json.dumps(written))
                self.refused_known(FakeHost(self.config), words)

    def test_a_why_of_the_longest_length_is_taken(self):
        self.red_on_both()
        written = json.loads(self.known.read_text())
        written["reds"][0]["why"] = "w" * known_reds.WHY_LENGTH
        self.known.write_text(json.dumps(written))
        self.planned(FakeHost(self.config), known_reds=self.known)
        self.assertEqual([red["why"] for red in self.evidence_of("00-release-check")["known_reds"]["accepted"]],
                         ["w" * known_reds.WHY_LENGTH])

    def test_a_relative_known_reds_path_is_refused(self):
        self.red_on_both()
        refusal = self.refused(FakeHost(self.config), known_reds=Path("known-reds.json"))
        self.assertIn("known reds: --known-reds is an absolute path", refusal)

    def test_a_live_head_that_moves_after_the_known_reds_were_read_stops_the_deploy_before_anything_stops(self):
        self.red_on_both()
        # Known-red basis, initial admission, then checkout boundary observes the move.
        heads = [HEAD, HEAD, "e" * 40]

        class Moving(FakeHost):
            def __call__(self, argv, **kw):
                if argv[0] == "git" and "rev-parse" in argv:
                    self.calls.append(argv)
                    return subprocess.CompletedProcess(argv, 0, heads.pop(0) + "\n", "")
                return FakeHost.__call__(self, argv, **kw)
        host = Moving(self.config)
        refusal = self.refused(host, known_reds=self.known)
        self.assertIn("the live checkout is at %s now, not %s, which the known reds were read beside" % ("e" * 40, HEAD), refusal)
        self.assertEqual((self.kills, self.records()), ([], ["00-plan", "00-release-check", "01-live", "02-checkout"]))

    def test_without_the_flag_a_red_summary_is_refused_exactly_as_before(self):
        self.red_on_both()
        host = FakeHost(self.config)
        refusal = self.refused(host)
        self.assertEqual(refusal, "the release check has not passed for %s: %s failed (exit 1)" % (SHA, RED_STEP))
        self.assertEqual(self.records(), ["00-plan", "00-release-check", "01-live", "02-checkout"])
        self.assertNotIn("known_reds", self.evidence_of("00-release-check"))
        self.assertEqual(host.ran("rev-parse"), [["git", "-C", str(self.config.live), "rev-parse", "HEAD"]])

    def test_a_passed_summary_with_the_flag_deploys_as_before_and_needs_none_of_them(self):
        self.red_on_both()
        self.summary.write_text(json.dumps(summary()))
        host = FakeHost(self.config)
        self.planned(host, known_reds=self.known)
        written = self.evidence_of("00-release-check")
        self.assertEqual((written["passed"], written["reasons"]), (True, []))
        not_needed = "known red not needed: %s %r did not fail" % (RED_STEP, RED_CASE)
        self.assertEqual(written["known_reds"]["not_needed"], [{"step": RED_STEP, "case": RED_CASE, "said": not_needed}])
        self.assertEqual(written["known_reds"]["accepted"], [])
        self.assertEqual(self.known_lines(), [not_needed])
        self.assertNotIn("known_reds", self.evidence_of("02-release")["record"]["measurements"])

    def test_main_takes_the_known_reds_file(self):
        self.red_on_both()
        with mock.patch.object(deploy, "deploy") as called, redirect_stdout(io.StringIO()):
            self.assertEqual(deploy.main(["--config", str(self.config_path), "--release-check", str(self.summary),
                                          "--known-reds", str(self.known), SHA, str(self.evidence), "--dry-run"]), 0)
        self.assertEqual(called.call_args[0][-1], self.known)


# The candidate's checkout as its summary names it, and a failure of RED_CASE as a harness reports it: its message, its
# values, then its stack through two test sources, which GitHub's classifier reads as the failure's fingerprint.
CHECKOUT = "/verified"
ERROR = ("AssertionError [ERR_ASSERTION]: value\n\n2 !== 3\n\n"
         "    at helper (/verified/tests/reader_inline_native.mjs:2:1)\n"
         "    at check (/verified/tests/reader_persistence_native.mjs:3:1)")
STANDIN = b"// stand-in source\nassert.equal(actual, expected);\nawait helper();\n"


class PolicyRepository:
    """Item 481, rule 2: a real git repository at the live checkout's path holding the test sources GitHub's classifier
    (tools/ci_known_reds.py) reads at the policy's source commit, the live commit and the deployed commit, and a policy
    that classifier's own load_policy() takes. The deploy reads the policy file through `run`, as FakeHost serves it."""

    def policy_setup(self):
        from tools import ci_known_reds as ci
        self.ci = ci
        self.repo = self.config.live
        self.repo.mkdir(parents=True)
        self.git_in("init", "-q")
        self.paths = set(release_check.harnesses()) | {
            "tests/reader_inline_native.mjs", "tools/ci_known_reds.py", "tools/known_reds.py", "tools/release_check.py",
            "tests/test_example.py", "tests/fixtures/at_names.json", "docs/tools.md", "tools/ci_input_reads.py",
            "tools/ci-input-reads.json", ci.PRODUCER_WORKFLOW}
        for path in self.paths:
            target = self.repo / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((ci.ROOT / path).read_bytes() if path.startswith("tools/") or path in (
                release_check.REPLAY_FILE, "tests/fixtures/at_names.json")
                else b"# stand-in source\nassert actual == expected\n" if path.endswith(".py")
                else b"name: Checks\n" if path == ci.PRODUCER_WORKFLOW else STANDIN)
        self.source_commit = self.committed("inventory sources")
        # The live commit, the deploy's event base, where the policy is read: the reviewed sources as they were reviewed.
        self.live = self.source_commit
        (self.repo / "application.txt").write_text("the candidate's change\n")
        # The deployed commit: the reviewed sources unchanged, and a change of its own.
        self.sha = self.committed("candidate")

    def git_in(self, *args):
        done = subprocess.run(["git", "-C", str(self.repo), "-c", "user.name=t", "-c", "user.email=t@example.com",
                               "-c", "commit.gpgsign=false"] + list(args), capture_output=True, text=True)
        self.assertEqual(done.returncode, 0, done.stderr)
        return done.stdout.strip()

    def committed(self, message):
        self.git_in("add", "-A")
        self.git_in("commit", "-q", "-m", message)
        return self.git_in("rev-parse", "HEAD")

    def admission(self, step=RED_STEP, case=RED_CASE, error=ERROR, review="m-admitted", **identity):
        """One red of .github/check-baseline.json as GitHub's classifier takes it: the case's identity and the
        fingerprint of the failure the review read."""
        profile = {"runner_os": "macOS", "runner_arch": "ARM64", "image_os": "macos-26", "image_version": "v1",
                   "scroll_bars": "Always", "reduce_motion": False, "cpus": 3, "chrome": "156", "node": "26",
                   "python": "3.9", "playwright": "1.63"}
        completion = ({"browserClosed": True, "harnessCompleted": True, "fixtureExitCode": 0, "fixtureExitSignal": None,
                       "plannedPhases": COMPOSE_PHASES, "completedPhases": COMPOSE_PHASES}
                      if known_reds.NATIVE[step].harness == release_check.COMPOSE
                      else {"browserClosed": True, "fixtureExitCode": 0, "ended": True})
        return {"step": step, "case": self.ci.identity(dict({"name": case}, **identity)), "owner": "fable",
                "why": "fails on the hosted runners too", "review": review, "observed_profile": profile,
                "target_profile": profile, "source_commit": self.source_commit, "evidence_sha256": "a" * 64,
                "fingerprint": self.ci.fingerprint(error, CHECKOUT, self.repo, self.source_commit), "completion": completion}

    def policy(self, *reds, without=()):
        """The policy file: every reviewed source's hash, every step's inventory (each admitted case in it), the reds.
        without: steps the release check names that this policy's commit does not (item 553), left out of both."""
        inventory = {name: [self.ci.identity({"name": RED_CASE})] for name in known_reds.NATIVE if name not in without}
        for red in reds:
            if red["case"] not in inventory[red["step"]]:
                inventory[red["step"]].append(red["case"])
        inventory.update({name: [self.ci.identity({"name": "test_one (example.Example)"})]
                          for name in ("python-units", "deploy-refusals")})
        inventory["node-units"] = [self.ci.identity({"name": "one"})]
        return {"version": 1, "source_commit": self.source_commit,
                "source_hashes": {path: self.ci.blob(self.ci.source(self.repo, self.source_commit, path)) for path in self.paths},
                "required": [name for name in release_check.REQUIRED if name not in without], "inventory": inventory,
                "reds": list(reds)}

    def fails(self, step=RED_STEP, error=ERROR, **identity):
        """The candidate's summary of the deployed commit, checked out at CHECKOUT, failing RED_CASE of step with error."""
        self.candidate_fails((step, dict(cases=[{"name": "the rail keeps its colours", "passed": True},
                                                dict({"name": RED_CASE, "passed": False, "error": error}, **identity)])),
                             commit=self.sha, checkout=CHECKOUT)
        self.listing((step, RED_CASE, None))

    def host(self, *reds, policy=None, policies=None):
        """The live host at the live commit, every commit keeping the policy of reds (or policy as given), or each commit
        its own (policies)."""
        return FakeHost(self.config, policy=self.policy(*reds) if policy is None else policy, policies=policies, at=self.sha,
                        line=(self.live, self.sha), head=self.live, releases=(page(release(19, self.live)), 0))

    def bare_host(self):
        """The live host at the live commit, keeping no policy, for a deploy of SHA."""
        return FakeHost(self.config, head=self.live, line=(self.live, SHA), releases=(page(release(19, self.live)), 0))

    def listing(self, *reds, live_commit=None):
        return super().listing(*reds, live_commit=live_commit or self.live)

    def live_summary(self, folder, *runs, commit=None, **written):
        return super().live_summary(folder, *runs, commit=commit or self.live, **written)


class DeployRefusesKnownRedsPolicy(PolicyRepository, KnownRedsFiles, DeployCase):
    """Item 481, rule 2: a failed case is known when the merged base's policy admits it, matched as GitHub's classifier
    matches it (the case's identity and its failure's fingerprint), or when live's last release check failed it,
    whatever that run's record format."""

    # The composer's end-of-run record, which a live run from before it lacks entirely.
    END = {"plannedPhases": COMPOSE_PHASES, "completedPhases": COMPOSE_PHASES, "harnessCompleted": True, "fixtureExitSignal": None}
    BEFORE_END = {"fixtureExitCode": MISSING}

    def setUp(self):
        super().setUp()
        self.known_setup()
        self.policy_setup()

    def again(self):
        self.tearDown()
        self.setUp()

    def evidence_of(self, name):
        return json.loads((self.evidence / (name + ".json")).read_text())

    def refused_known(self, host, words, sha=SHA):
        refusal = self.refused(host, sha=sha, known_reds=self.known)
        self.assertIn("known reds: ", refusal)
        self.assertIn(words, refusal)
        self.assertEqual(self.records(), ["00-release-check"])
        self.assertEqual(self.kills, [])
        return refusal

    def refused_known_at(self, host, words):
        """Refused for the deployed commit of the policy's repository."""
        return self.refused_known(host, words, sha=self.sha)

    def taken(self, host):
        """The deploy's dry run of the deployed commit, which must not refuse; its output."""
        with redirect_stdout(io.StringIO()) as out, mock.patch.object(deploy, "run", host), \
                mock.patch.object(deploy, "start", side_effect=AssertionError("must not start a process")):
            try:
                deploy.deploy(self.config, self.sha, self.evidence, self.summary, True, known_reds=self.known)
            except deploy.Refused as refusal:
                self.fail("the deploy refused: %s" % refusal)
        return out.getvalue()

    def test_a_case_the_merged_base_policy_admits_is_a_known_red_without_a_live_summary(self):
        self.fails()
        host = self.host(self.admission())
        out = self.taken(host)
        [taken] = self.evidence_of("00-release-check")["known_reds"]["accepted"]
        self.assertEqual((taken["taken_by"], taken["review"], taken["live_summary"]), ("policy", "m-admitted", None))
        self.assertEqual(self.evidence_of("02-release")["record"]["measurements"]["known_reds"], [
            {"step": RED_STEP, "case": RED_CASE, "live_summary": None, "live_commit": self.live, "why": "fails on the live release too",
             "taken_by": "policy"}])
        self.assertIn("known red: %s %r failed; %s at %s admits it (review m-admitted)" % (RED_STEP, RED_CASE, POLICY, self.live[:12]),
                      out)
        self.assertEqual([line for line in out.splitlines() if line.startswith("known red")], [taken.get("said")],
                         "the line printed is the one recorded with the red")
        # Item 553: a candidate of the policy's own steps adds none beyond it, and the record says nothing of any.
        self.assertFalse({"steps_beyond_policy", "steps_beyond_policy_unread"}
                         & set(self.evidence_of("02-release")["record"]["measurements"]))
        self.assertNotIn("did not review", out)
        policy_reads = [argv for argv in host.calls if any(POLICY in part for part in argv)]
        self.assertEqual(policy_reads, 2 * [["git", "-C", str(self.config.live), "ls-tree", "--name-only", self.live, "--", POLICY],
                                            ["git", "-C", str(self.config.live), "show", "%s:%s" % (self.live, POLICY)]],
                         "read at the live commit, the event base, for the known reds and for the record, and never at the "
                         "deployed commit")

    def test_a_candidates_own_admission_never_counts_for_it(self):
        # B8: GitHub's classifier reads the policy at the pull request's event base, never the candidate's copy; the deploy
        # reads it at the live commit. An admission the candidate adds, to a live policy or where live keeps none, is not
        # read.
        for live in ("policy", "none"):
            with self.subTest(live):
                self.again()
                self.fails()
                policies = {self.sha: self.policy(self.admission())}
                if live == "policy":
                    policies[self.live] = self.policy()
                host = self.host(policies=policies)
                self.refused_known_at(host, "%s %r is not admitted by %s at the live commit, and no live summary names it"
                                      % (RED_STEP, RED_CASE, POLICY))
                self.assertNotIn(["git", "-C", str(self.config.live), "show", "%s:%s" % (self.sha, POLICY)], host.calls)

    def test_an_admission_the_live_policy_holds_stands_though_the_candidate_drops_it(self):
        # The positive beside it: the live commit's policy admits the case and the candidate's copy no longer does.
        self.fails()
        self.taken(self.host(policies={self.live: self.policy(self.admission()), self.sha: self.policy()}))
        [taken] = self.evidence_of("00-release-check")["known_reds"]["accepted"]
        self.assertEqual((taken["taken_by"], taken["review"]), ("policy", "m-admitted"))

    # A test source the live commit has and its policy's plan never reviewed, which the candidate retires.
    RETIRED = "tests/retired_native.mjs"

    def live_with_a_test_the_candidate_retires(self):
        (self.repo / self.RETIRED).write_text("// a test the candidate retires\n")
        self.live = self.committed("the live commit adds a test")
        (self.repo / self.RETIRED).unlink()
        self.sha = self.committed("the candidate retires it")
        self.fails()

    def test_a_plan_whose_retirements_from_the_live_commit_differ_admits_nothing(self):
        # B8: GitHub's classifier picks the plan with the event base as its approved base: what the candidate retires
        # from the base must be the plan's reviewed retirements. The plan here retires nothing, so it admits nothing.
        self.live_with_a_test_the_candidate_retires()
        self.refused_known_at(self.host(self.admission()), "%s %r is not admitted by %s at the live commit, and no live "
                              "summary names it (the tests at %s are not ones it reviewed: reviewed test definition changed: "
                              "no complete matching plan)" % (RED_STEP, RED_CASE, POLICY, self.sha[:12]))

    def test_a_reviewed_transition_that_retires_what_the_candidate_retires_admits_its_reds(self):
        # The positive beside it: a transition whose plan retires exactly that test, reviewed on its own, is the plan.
        self.live_with_a_test_the_candidate_retires()
        plan = self.policy(self.admission())
        self.taken(self.host(policy=dict(plan, transitions=[{"review": "m-retired", "plan": json.loads(json.dumps(plan)),
                                                              "retired_sources": [self.RETIRED]}])))
        [taken] = self.evidence_of("00-release-check")["known_reds"]["accepted"]
        self.assertEqual((taken["taken_by"], taken["review"]), ("policy", "m-admitted"))

    def test_the_policy_admits_a_case_only_by_its_exact_identity(self):
        # The negative: another engine's, phase's or step's case of the same name, a mutation's row, or a name that
        # differs by a space, is another case, as GitHub's classifier keys it.
        for red, row in ((dict(engine="webkit"), {}), (dict(phase="run:webkit"), {}), ({}, dict(engine="webkit")),
                         ({}, dict(phase="run:webkit")), ({}, dict(expectedFailure=True, mutationCaught=False)),
                         (dict(step="native-journal"), {}), (dict(case=RED_CASE + " "), {})):
            with self.subTest(red=red, row=row):
                self.again()
                self.fails(**row)
                self.refused_known_at(self.host(self.admission(**red)), "%s %r is not admitted by %s at the live commit, "
                                   "and no live summary names it" % (RED_STEP, RED_CASE, POLICY))

    def test_a_case_whose_phase_and_engine_the_policy_names_is_admitted(self):
        # The positive beside the identity negative: the same phase and engine, as the real policy's reader and WebKit
        # rows name them.
        self.fails(phase="run:webkit", engine="webkit")
        self.taken(self.host(self.admission(phase="run:webkit", engine="webkit")))
        [taken] = self.evidence_of("00-release-check")["known_reds"]["accepted"]
        self.assertEqual(taken["taken_by"], "policy")

    def test_an_admitted_case_that_fails_for_another_reason_is_refused(self):
        # GitHub's classifier calls each of these "different assertion, values or source chain": another value, another
        # assertion message, another call site, and a failure with no test source in its stack.
        other_values = ERROR.replace("2 !== 3", "2 !== 4")
        other_message = ERROR.replace("value", "every control is 44px")
        other_site = ERROR.replace("reader_inline_native.mjs:2:1", "reader_inline_native.mjs:3:1")
        for error in (other_values, other_message, other_site, "AssertionError: 2 !== 3"):
            with self.subTest(error=error):
                self.again()
                self.fails(error=error)
                self.refused_known_at(self.host(self.admission()), "%s %r: %s at the live commit admits it, but not this "
                                   "failure (different assertion, values or source chain)" % (RED_STEP, RED_CASE, POLICY))

    # A reviewed test source changed at the deployed commit: the assertion the stack names was rewritten, with the same
    # message and line numbers; or a reviewed test source the stack does not name changed. GitHub's classifier matches
    # no plan to either.
    REVIEWED_CHANGES = (("tests/reader_inline_native.mjs", STANDIN.replace(b"assert.equal", b"assert.notEqual")),
                        ("tests/fence_copy_native.mjs", STANDIN + b"await another();\n"))

    def reviewed_test_changed(self, path, text):
        (self.repo / path).write_bytes(text)
        self.sha = self.committed("a reviewed test changed")
        self.fails()

    def test_an_admitted_case_whose_test_source_changed_at_the_deployed_commit_is_refused(self):
        # The policy admits nothing there, so with no live summary naming the case nothing takes it.
        for path, text in self.REVIEWED_CHANGES:
            with self.subTest(path):
                self.again()
                self.reviewed_test_changed(path, text)
                self.refused_known_at(self.host(self.admission()), "%s %r is not admitted by %s at the live commit, and "
                                      "no live summary names it (the tests at %s are not ones it reviewed: reviewed test "
                                      "definition changed: no complete matching plan)" % (RED_STEP, RED_CASE, POLICY, self.sha[:12]))

    def test_at_a_commit_whose_reviewed_tests_changed_a_live_summary_or_an_owners_acceptance_still_takes_the_red(self):
        # Admitting nothing is not refusing: the red falls to the live commit's own failure, or an owner's word, as at a
        # commit that keeps no policy.
        for path, text in self.REVIEWED_CHANGES:
            for route in ("live", "owner_acceptance"):
                with self.subTest(path=path, route=route):
                    self.again()
                    self.reviewed_test_changed(path, text)
                    if route == "live":
                        named = self.live_summary("live-check", (RED_STEP, {}))
                    else:
                        named = Path(self.tmp.name) / "acceptance.json"
                        named.write_text(json.dumps({"owner_acceptance": True, "cases": [{"step": RED_STEP, "case": RED_CASE}],
                                                     "by": "warp", "message": "m-accepted", "why": "taken on the owner's word"}))
                    self.listing((RED_STEP, RED_CASE, named))
                    self.taken(self.host(self.admission()))
                    [taken] = self.evidence_of("00-release-check")["known_reds"]["accepted"]
                    self.assertEqual((taken["taken_by"], taken["live_summary"]), (route, str(named)))

    def test_at_a_commit_whose_reviewed_tests_changed_a_live_summary_that_did_not_fail_the_case_is_refused(self):
        # The negative beside it: the live route still asks that the live commit failed this very case.
        for path, text in self.REVIEWED_CHANGES:
            with self.subTest(path):
                self.again()
                self.reviewed_test_changed(path, text)
                named = self.live_summary("live-check", (RED_STEP, dict(failed=("a whisper keeps its colour",))))
                self.listing((RED_STEP, RED_CASE, named))
                self.refused_known_at(self.host(self.admission()), "%s %r is not admitted by %s at the live commit, nor "
                                      "among the cases that failed in %s on the live commit (the tests at %s are not ones it "
                                      "reviewed" % (RED_STEP, RED_CASE, POLICY, named, self.sha[:12]))

    def test_a_deployed_commit_that_changes_no_reviewed_test_is_matched_against_the_policy(self):
        # The negative beside it: a change outside the reviewed sources leaves the plan as it was.
        (self.repo / "aim_next").mkdir()
        (self.repo / "aim_next/work.py").write_text("# the release's own change\n")
        self.sha = self.committed("a product change")
        self.fails()
        self.taken(self.host(self.admission()))
        [taken] = self.evidence_of("00-release-check")["known_reds"]["accepted"]
        self.assertEqual(taken["taken_by"], "policy")

    def test_a_policy_github_would_not_load_admits_nothing_and_refuses(self):
        # GitHub's classifier refuses each of these before it matches a case, so the deploy does too.
        def changed(**fields):
            found = self.policy(self.admission())
            found.update(fields)
            return found
        # Each policy is made in the repository of its own case, which again() makes anew.
        for policy, words in ((lambda: changed(version=2), "unsupported baseline schema"),
                              (lambda: changed(source_hashes={path: "0" * 40 for path in self.paths}), "inventory source hash differs"),
                              (lambda: changed(inventory={}), "incomplete reviewed check inventory"),
                              (lambda: {"version": 1, "reds": [self.admission()]}, "invalid baseline fields")):
            with self.subTest(words):
                self.again()
                self.fails()
                self.refused_known_at(self.host(policy=policy()), "%s at %s is not a policy GitHub's classifier takes (%s"
                                      % (POLICY, self.live[:12], words))

    def test_a_policy_that_cannot_be_read_refuses_rather_than_admitting_nothing(self):
        self.fails()
        self.refused_known_at(self.host(policy="unreadable"), "%s at %s could not be read" % (POLICY, self.live[:12]))

    def test_a_case_neither_the_policy_admits_nor_the_live_summary_failed_is_refused(self):
        self.candidate_fails((RED_STEP, {}), commit=self.sha, checkout=CHECKOUT)
        path = self.live_summary("live-check", (RED_STEP, dict(failed=("a whisper keeps its colour",))))
        self.listing((RED_STEP, RED_CASE, path))
        self.refused_known_at(self.host(self.admission(case="a whisper keeps its colour")),
                           "%s %r is not admitted by %s at the live commit, nor among the cases that failed in %s on the "
                           "live commit" % (RED_STEP, RED_CASE, POLICY, path))

    def test_a_live_compose_run_from_before_its_end_record_counts_when_it_names_the_case_failed_and_closed(self):
        self.candidate_fails((COMPOSE_STEP, dict(more=self.END)))
        self.listing((COMPOSE_STEP, RED_CASE, self.live_summary("live-check", (COMPOSE_STEP, dict(more=self.BEFORE_END)))))
        self.planned(self.bare_host(), known_reds=self.known)
        [taken] = self.evidence_of("00-release-check")["known_reds"]["accepted"]
        self.assertEqual((taken["step"], taken["case"], taken["taken_by"]), (COMPOSE_STEP, RED_CASE, "live"))

    def test_an_old_compose_run_that_did_not_close_its_browser_or_fail_the_case_is_refused(self):
        for live, words in ((dict(more=self.BEFORE_END, closed=False), "the live %s: the browser did not close" % COMPOSE_STEP),
                            (dict(more=self.BEFORE_END, failed=("another case",)), "nor among the cases that failed")):
            with self.subTest(words):
                self.again()
                self.candidate_fails((COMPOSE_STEP, dict(more=self.END)))
                self.listing((COMPOSE_STEP, RED_CASE, self.live_summary("live-check", (COMPOSE_STEP, live))))
                self.refused_known(self.bare_host(), words)

    def test_the_candidates_own_compose_run_still_needs_its_end_record(self):
        # Only the live run may come from before the record: the candidate's harness writes it.
        self.candidate_fails((COMPOSE_STEP, dict(more=self.BEFORE_END)))
        self.listing((COMPOSE_STEP, RED_CASE, self.live_summary("live-check", (COMPOSE_STEP, dict(more=self.BEFORE_END)))))
        self.refused_known(self.bare_host(), "the candidate's %s: its fixture exited None" % COMPOSE_STEP)


class DeployRefusesKnownRedsPolicyOfFewerSteps(PolicyRepository, KnownRedsFiles, DeployCase):
    """Item 553: a candidate that adds a required step to the release check. The policy at the live commit, the event
    base, is checked against its own source commit's steps (that commit's tools/release_check.py, read there; here the
    live commit is the policy's source commit); a policy that
    reviewed those but not the candidate's admits nothing and refuses nothing, so each red falls to its live summary or an
    owner's acceptance, and the record names each step the candidate adds beyond it for the next policy to cover."""

    ADDED = "native-card-marks"
    # The live commit's release check: the candidate's without the added step.
    FEWER = ('\nNATIVE_STEPS = tuple(step for step in NATIVE_STEPS if "native-" + step.name != "%s")\n'
             'REQUIRED = tuple(name for name in REQUIRED if name != "%s")\n' % (ADDED, ADDED))

    def setUp(self):
        super().setUp()
        self.known_setup()
        self.policy_setup()
        self.live_without_the_added_step()

    def again(self):
        self.tearDown()
        self.setUp()

    def live_without_the_added_step(self, base=None):
        """The live commit's tools/release_check.py lacks ADDED (or reads as base), the policy's sources reviewed there;
        the candidate's is this checkout's, which runs it."""
        checked = self.repo / "tools/release_check.py"
        candidate = checked.read_text()
        checked.write_text(candidate + self.FEWER if base is None else base)
        self.source_commit = self.live = self.committed("the live commit's release check, one step fewer")
        checked.write_text(candidate)
        self.sha = self.committed("the candidate adds a step")

    def host(self, *reds, policy=None, policies=None):
        return FakeHost(self.config, policy=self.policy(*reds, without=(self.ADDED,)) if policy is None else policy,
                        policies=policies, at=self.sha, line=(self.live, self.sha), head=self.live,
                        releases=(page(release(19, self.live)), 0))

    def listing(self, *reds, live_commit=None):
        return super().listing(*reds, live_commit=live_commit or self.live)

    def live_summary(self, folder, *runs, commit=None, **written):
        return super().live_summary(folder, *runs, commit=commit or self.live, **written)

    def evidence_of(self, name):
        return json.loads((self.evidence / (name + ".json")).read_text())

    def taken(self, host):
        with redirect_stdout(io.StringIO()) as out, mock.patch.object(deploy, "run", host), \
                mock.patch.object(deploy, "start", side_effect=AssertionError("must not start a process")):
            try:
                deploy.deploy(self.config, self.sha, self.evidence, self.summary, True, known_reds=self.known)
            except deploy.Refused as refusal:
                self.fail("the deploy refused: %s" % refusal)
        return out.getvalue()

    def refused_known(self, host, words):
        refusal = self.refused(host, sha=self.sha, known_reds=self.known)
        self.assertIn("known reds: ", refusal)
        self.assertIn(words, refusal)
        self.assertEqual(self.records(), ["00-release-check"])
        self.assertEqual(self.kills, [])
        return refusal

    def acceptance(self, *cases):
        named = Path(self.tmp.name) / "acceptance.json"
        named.write_text(json.dumps({"owner_acceptance": True, "cases": [{"step": step, "case": case} for step, case in cases],
                                     "by": "warp", "message": "m-accepted", "why": "taken on the owner's word"}))
        return named

    def test_a_policy_of_the_live_commits_steps_takes_a_candidate_that_adds_one_with_no_admissions(self):
        # Each red falls to its live summary or an owner's acceptance, the added step's own red too; the policy's own
        # admission of the case is not taken, since the policy did not review the candidate's steps.
        self.candidate_fails((RED_STEP, {}), (self.ADDED, {}), commit=self.sha, checkout=CHECKOUT)
        live = self.live_summary("live-check", (RED_STEP, {}))
        accepted = self.acceptance((self.ADDED, RED_CASE))
        self.listing((RED_STEP, RED_CASE, live), (self.ADDED, RED_CASE, accepted))
        out = self.taken(self.host(self.admission()))
        known = self.evidence_of("00-release-check")["known_reds"]
        self.assertEqual([(red["step"], red["taken_by"], red["live_summary"]) for red in known["accepted"]],
                         [(self.ADDED, "owner_acceptance", str(accepted)), (RED_STEP, "live", str(live))])
        self.assertEqual(self.evidence_of("02-release")["record"]["measurements"].get("steps_beyond_policy"), [self.ADDED])
        self.assertIn("the release check runs %s, which %s at %s did not review; the next policy covers it"
                      % (self.ADDED, POLICY, self.live[:12]), out)
        self.assertNotIn("admits it", out)

    def test_a_red_with_no_live_summary_is_refused_naming_the_steps_the_policy_did_not_review(self):
        # The negative beside it: admitting nothing is not admitting the case; with no live summary nothing takes it.
        self.fails()
        self.refused_known(self.host(self.admission()), "%s %r is not admitted by %s at the live commit, and no live summary "
                           "names it (the steps at %s are not the ones it reviewed: it does not review %s)"
                           % (RED_STEP, RED_CASE, POLICY, self.sha[:12], self.ADDED))

    def test_a_policy_malformed_in_itself_still_refuses(self):
        # Against its own source commit's steps (here the live commit's): a policy that names the added step, or leaves
        # out one its source commit has, is incomplete there; so is a bad schema, a duplicate identity or a source hash
        # that differs. A policy whose source commit is older than the live commit is the next class's.
        def changed(**fields):
            found = self.policy(self.admission(), without=(self.ADDED,))
            found.update(fields)
            return found

        def duplicated():
            found = self.policy(self.admission(), without=(self.ADDED,))
            found["inventory"][RED_STEP].append(dict(found["inventory"][RED_STEP][0]))
            return found
        for policy, words in ((lambda: self.policy(self.admission()), "incomplete reviewed steps"),
                              (lambda: self.policy(self.admission(), without=(self.ADDED, "native-journal")),
                               "incomplete reviewed steps"),
                              (lambda: changed(version=2), "unsupported baseline schema"),
                              (lambda: changed(source_hashes={path: "0" * 40 for path in self.paths}), "inventory source hash differs"),
                              (duplicated, "%s: duplicate reviewed identity" % RED_STEP),
                              (lambda: {"version": 1, "reds": [self.admission()]}, "invalid baseline fields")):
            with self.subTest(words):
                self.again()
                self.fails()
                self.refused_known(self.host(policy=policy()), "%s at %s is not a policy GitHub's classifier takes (%s"
                                   % (POLICY, self.live[:12], words))

    def test_the_live_commits_steps_that_cannot_be_read_refuse(self):
        # Not read is not "no steps": a live commit whose release check cannot be read refuses.
        self.live_without_the_added_step(base="raise SystemExit(3)\n")
        self.fails()
        self.refused_known(self.host(policy=self.policy(self.admission())),
                           "the release check's steps at %s could not be read" % self.live[:12])

    def test_a_policy_of_a_step_the_candidate_no_longer_runs_admits_nothing_either(self):
        # The same rule the other way: the live commit runs a step the candidate dropped, and the policy reviewed it.
        extra = "native-retired"
        checked = self.repo / "tools/release_check.py"
        self.live_without_the_added_step(base=checked.read_text() + (
            '\nNATIVE_STEPS = NATIVE_STEPS + (NativeStep("retired", PERSISTENCE, ("--retired",), "native.json"),)\n'
            'REQUIRED = REQUIRED + ("%s",)\n' % extra))
        self.fails()
        # It admits a case of the dropped step too: a browser step at the policy's source commit, though not the
        # candidate's, so the policy is one the classifier takes there.
        policy = self.policy(self.admission())
        policy["required"].append(extra)
        policy["inventory"][extra] = [self.ci.identity({"name": RED_CASE})]
        policy["reds"].append(dict(self.admission(), step=extra))
        self.refused_known(self.host(policy=policy), "no live summary names it (the steps at %s are not the ones it "
                           "reviewed: it reviews %s, which %s does not run)" % (self.sha[:12], extra, self.sha[:12]))

    def test_the_candidates_own_policy_naming_its_added_step_never_counts(self):
        self.fails()
        host = self.host(policies={self.live: self.policy(without=(self.ADDED,)), self.sha: self.policy(self.admission())})
        self.refused_known(host, "%s %r is not admitted by %s at the live commit, and no live summary names it"
                           % (RED_STEP, RED_CASE, POLICY))
        self.assertNotIn(["git", "-C", str(self.config.live), "show", "%s:%s" % (self.sha, POLICY)], host.calls)


class DeployRefusesKnownRedsPolicyOfAnEarlierSourceCommit(PolicyRepository, KnownRedsFiles, DeployCase):
    """Item 553, the deploy after the one that adds a step: the policy file is the one reviewed at its source commit,
    whose release check lacks ADDED and its harness; the live commit added both and left the policy as it was, as train 21
    leaves .github/check-baseline.json; the candidate runs the live commit's steps. The policy has a reviewed transition,
    as the real one has four. It is checked against its own source commit's steps, never the live commit's or the
    candidate's: it is a policy the deploy takes, it did not review the candidate's steps, so it admits nothing and
    refuses nothing, and every deploy's record names the steps beyond it."""

    ADDED = DeployRefusesKnownRedsPolicyOfFewerSteps.ADDED
    FEWER = DeployRefusesKnownRedsPolicyOfFewerSteps.FEWER

    def setUp(self):
        super().setUp()
        self.known_setup()
        self.policy_setup()
        checked, harness = self.repo / "tools/release_check.py", self.repo / release_check.CARD_MARKS
        candidate, kept = checked.read_text(), harness.read_bytes()
        checked.write_text(candidate + self.FEWER)
        harness.unlink()
        self.paths.discard(release_check.CARD_MARKS)
        self.source_commit = self.committed("the policy's source commit: one step fewer, and no harness for it")
        checked.write_text(candidate)
        harness.write_bytes(kept)
        self.live = self.committed("the live commit adds the step and its harness, and leaves the policy as it was")
        (self.repo / "application.txt").write_text("the next train's change\n")
        self.sha = self.committed("the candidate runs the live commit's steps")

    def again(self):
        self.tearDown()
        self.setUp()

    def reviewed(self, *reds, without=None):
        """The policy at the live commit: reviewed at its source commit, with one reviewed transition."""
        plan = self.policy(*reds, without=(self.ADDED,) if without is None else without)
        return dict(plan, transitions=[{"review": "m-transition", "plan": json.loads(json.dumps(plan))}])

    def host(self, *reds, policy=None):
        return FakeHost(self.config, policy=self.reviewed(*reds) if policy is None else policy, at=self.sha,
                        line=(self.live, self.sha), head=self.live, releases=(page(release(19, self.live)), 0))

    def listing(self, *reds, live_commit=None):
        return super().listing(*reds, live_commit=live_commit or self.live)

    def live_summary(self, folder, *runs, commit=None, **written):
        return super().live_summary(folder, *runs, commit=commit or self.live, **written)

    def evidence_of(self, name):
        return json.loads((self.evidence / (name + ".json")).read_text())

    def dry(self, host, **release):
        """The deploy's dry run of the candidate, which must not refuse; its output."""
        with redirect_stdout(io.StringIO()) as out, mock.patch.object(deploy, "run", host), \
                mock.patch.object(deploy, "start", side_effect=AssertionError("must not start a process")):
            try:
                deploy.deploy(self.config, self.sha, self.evidence, self.summary, True, **release)
            except deploy.Refused as refusal:
                self.fail("the deploy refused: %s" % refusal)
        return out.getvalue()

    def refused_known(self, host, words):
        refusal = self.refused(host, sha=self.sha, known_reds=self.known)
        self.assertIn("known reds: %s" % words, refusal)
        self.assertEqual((self.records(), self.kills), (["00-release-check"], []))
        return refusal

    def acceptance(self, *cases):
        named = Path(self.tmp.name) / "acceptance.json"
        named.write_text(json.dumps({"owner_acceptance": True, "cases": [{"step": step, "case": case} for step, case in cases],
                                     "by": "warp", "message": "m-accepted", "why": "taken on the owner's word"}))
        return named

    def beyond_said(self):
        return ("the release check runs %s, which %s at %s did not review; the next policy covers it"
                % (self.ADDED, POLICY, self.live[:12]))

    def test_the_policy_admits_nothing_refuses_nothing_and_each_red_falls_to_its_live_summary_or_acceptance(self):
        # B1: checked against the live commit's steps this policy would be "incomplete reviewed steps", and every deploy
        # with known reds after train 21 refused. B2: its transition's plan and its harness sources are checked against
        # the same source commit's steps, never the candidate's, which add a step and a harness the policy never hashed.
        self.candidate_fails((RED_STEP, {}), (self.ADDED, {}), commit=self.sha, checkout=CHECKOUT)
        live = self.live_summary("live-check", (RED_STEP, {}))
        accepted = self.acceptance((self.ADDED, RED_CASE))
        self.listing((RED_STEP, RED_CASE, live), (self.ADDED, RED_CASE, accepted))
        out = self.dry(self.host(self.admission()), known_reds=self.known)
        self.assertEqual([(red["step"], red["taken_by"], red["live_summary"])
                          for red in self.evidence_of("00-release-check")["known_reds"]["accepted"]],
                         [(self.ADDED, "owner_acceptance", str(accepted)), (RED_STEP, "live", str(live))])
        self.assertEqual(self.evidence_of("02-release")["record"]["measurements"].get("steps_beyond_policy"), [self.ADDED])
        self.assertIn(self.beyond_said(), out)
        self.assertNotIn("admits it", out)

    def test_a_red_with_no_live_summary_is_refused_naming_the_steps_the_policy_did_not_review(self):
        # The negative beside it: the policy's admission of the case is not taken.
        self.fails()
        self.refused_known(self.host(self.admission()), "%s %r is not admitted by %s at the live commit, and no live summary "
                           "names it (the steps at %s are not the ones it reviewed: it does not review %s)"
                           % (RED_STEP, RED_CASE, POLICY, self.sha[:12], self.ADDED))

    def test_a_policy_whose_steps_are_not_its_own_source_commits_still_refuses(self):
        # Malformed in itself: the live commit's steps (naming ADDED, which its source commit lacks), or its source
        # commit's without a step that commit has.
        for without, words in (((), "incomplete reviewed steps"), ((self.ADDED, "native-journal"), "incomplete reviewed steps")):
            with self.subTest(without=without):
                self.again()
                self.fails()
                self.refused_known(self.host(policy=self.reviewed(self.admission(), without=without)),
                                   "%s at %s is not a policy GitHub's classifier takes (%s)" % (POLICY, self.live[:12], words))

    def test_a_passed_summary_records_the_steps_beyond_the_live_policy_with_or_without_the_flag(self):
        # B3: a summary that passed never reaches the known reds' judge, and still the record names the added step.
        self.summary.write_text(json.dumps(summary(commit=self.sha)))
        self.listing((RED_STEP, RED_CASE, None))
        for flag in ({}, {"known_reds": self.known}):
            with self.subTest(flag=sorted(flag)):
                for path in self.evidence.glob("*.json"):
                    path.unlink()
                out = self.dry(self.host(), **flag)
                measured = self.evidence_of("02-release")["record"]["measurements"]
                self.assertEqual(measured.get("steps_beyond_policy"), [self.ADDED])
                self.assertNotIn("steps_beyond_policy_unread", measured)
                self.assertIn(self.beyond_said(), out)

    def test_a_record_says_nothing_of_steps_beyond_a_policy_that_reviews_every_step_or_where_live_keeps_none(self):
        # The negative: no field and no line when nothing is beyond the policy, or there is no policy to be beyond.
        self.summary.write_text(json.dumps(summary(commit=self.sha)))
        for policy in (self.policy(), None):
            with self.subTest(policy=policy is not None):
                for path in self.evidence.glob("*.json"):
                    path.unlink()
                host = FakeHost(self.config, policy=policy, at=self.sha, line=(self.live, self.sha), head=self.live,
                                releases=(page(release(19, self.live)), 0))
                out = self.dry(host)
                measured = self.evidence_of("02-release")["record"]["measurements"]
                self.assertFalse({"steps_beyond_policy", "steps_beyond_policy_unread"} & set(measured), measured)
                self.assertNotIn("did not review", out)
                self.assertEqual([argv for argv in host.calls if any(POLICY in part for part in argv)][:1],
                                 [["git", "-C", str(self.config.live), "ls-tree", "--name-only", self.live, "--", POLICY]],
                                 "read at the live commit, never the candidate")

    def test_a_policy_that_cannot_be_read_is_recorded_as_unread_and_does_not_stop_a_deploy_that_needs_none(self):
        # Not read is not "none beyond it"; and a policy broken at the live commit must not stop the deploy that mends it.
        self.summary.write_text(json.dumps(summary(commit=self.sha)))
        for policy, words in (("unreadable", "%s at %s could not be read" % (POLICY, self.live[:12])),
                              ({"version": 1, "required": "every step"},
                               "%s at %s names no list of required steps" % (POLICY, self.live[:12]))):
            with self.subTest(words):
                for path in self.evidence.glob("*.json"):
                    path.unlink()
                out = self.dry(self.host(policy=policy))
                measured = self.evidence_of("02-release")["record"]["measurements"]
                self.assertNotIn("steps_beyond_policy", measured)
                self.assertIn(words, measured.get("steps_beyond_policy_unread", ""))
                self.assertIn("the steps beyond %s could not be read: %s" % (POLICY, words), out)


class DeployRefusesKnownRedsOwnerAcceptance(KnownRedsFiles, DeployCase):
    """Item 482, rule 3: a known red's live_summary may name an owner's acceptance, which takes only a step and case it
    names exactly, with who accepted it and in which message."""

    def setUp(self):
        super().setUp()
        self.known_setup()
        self.acceptance = Path(self.tmp.name) / "acceptance.json"

    def again(self):
        self.tearDown()
        self.setUp()

    def accepted(self, **change):
        written = dict({"owner_acceptance": True, "cases": [{"step": RED_STEP, "case": RED_CASE}], "by": "warp",
                        "message": "m-accepted", "why": "taken on the owner's word"}, **change)
        self.acceptance.write_text(json.dumps(written))
        self.candidate_fails((RED_STEP, {}))
        self.listing((RED_STEP, RED_CASE, self.acceptance))

    def test_an_owners_acceptance_of_the_exact_case_takes_it_and_the_record_names_the_file(self):
        self.accepted()
        host = FakeHost(self.config)
        with redirect_stdout(io.StringIO()) as out, mock.patch.object(deploy, "run", host), \
                mock.patch.object(deploy, "start", side_effect=AssertionError("must not start a process")):
            try:
                deploy.deploy(self.config, SHA, self.evidence, self.summary, True, known_reds=self.known)
            except deploy.Refused as refusal:
                self.fail("the deploy refused: %s" % refusal)
        record = json.loads((self.evidence / "02-release.json").read_text())["record"]
        self.assertEqual(record["measurements"]["known_reds"], [
            {"step": RED_STEP, "case": RED_CASE, "live_summary": str(self.acceptance), "live_commit": HEAD,
             "why": "fails on the live release too", "taken_by": "owner_acceptance"}])
        self.assertIn("known red: %s %r failed; an owner's acceptance took it (%s, by warp in m-accepted)"
                      % (RED_STEP, RED_CASE, self.acceptance), out.getvalue())
        self.assertEqual([line for line in out.getvalue().splitlines() if line.startswith("known red")],
                         ["known red: %s %r failed; an owner's acceptance took it (%s, by warp in m-accepted): fails on the "
                          "live release too" % (RED_STEP, RED_CASE, self.acceptance)], "one line, the whole of it")

    def test_an_owners_acceptance_takes_only_what_it_names_exactly_with_a_by_and_a_message(self):
        for change, words in (({"cases": [{"step": RED_STEP, "case": RED_CASE.upper()}]}, "but not of %s %r" % (RED_STEP, RED_CASE)),
                              ({"cases": [{"step": RED_STEP, "case": "a reply keeps its"}]}, "but not of"),
                              ({"cases": [{"step": "native-journal", "case": RED_CASE}]}, "but not of"),
                              ({"cases": []}, "but not of"),
                              ({"cases": [{"step": RED_STEP, "case": RED_CASE, "engine": "webkit"}]}, "whose cases are not each {step, case}"),
                              ({"by": ""}, "with no by"), ({"by": " "}, "with no by"), ({"message": ""}, "with no message"),
                              ({"message": None}, "with no message"), ({"extra": 1}, "a JSON object with exactly")):
            with self.subTest(change=change):
                self.again()
                self.accepted(**change)
                refusal = self.refused(FakeHost(self.config), known_reds=self.known)
                self.assertIn("the live summary %s is an owner's acceptance" % self.acceptance, refusal)
                self.assertIn(words, refusal)
                self.assertEqual(self.records(), ["00-release-check"])

    def test_without_owner_acceptance_true_the_file_is_read_as_a_release_check_summary(self):
        self.accepted(owner_acceptance=False)
        self.assertIn("is not a release-check summary", self.refused(FakeHost(self.config), known_reds=self.known))

    def test_an_owner_acceptance_that_is_truthy_but_not_true_is_refused(self):
        # B10: only the JSON value true makes a file an owner's acceptance; "true", 1 and other truthy values are read as
        # a release-check summary, which the file is not.
        for value in ("true", 1, 1.0, ["true"], {"accepted": True}):
            with self.subTest(value=value):
                self.again()
                self.accepted(owner_acceptance=value)
                self.assertIn("the live summary %s is not a release-check summary" % self.acceptance,
                              self.refused(FakeHost(self.config), known_reds=self.known))
                self.assertEqual(self.records(), ["00-release-check"])


class KnownRedsRecorded(KnownRedsFiles, Restarting):
    def setUp(self):
        super().setUp()
        self.known_setup()

    def test_a_release_with_known_reds_is_recorded_in_the_instance_with_them(self):
        live = self.red_on_both()
        host = FakeHost(self.config)
        try:
            self.run_deploy(host, known_reds=self.known)
        except deploy.Refused as refusal:
            self.fail("the deploy refused: %s" % refusal)
        [written] = host.written
        self.assertEqual(written["measurements"]["known_reds"], [
            {"step": RED_STEP, "case": RED_CASE, "live_summary": str(live), "live_commit": HEAD, "why": "fails on the live release too",
             "taken_by": "live"}])
        self.assertEqual(self.kills, [(222, 15), (111, 15)])


class FixTopicTitles(unittest.TestCase):
    """D15: a fix topic is named for what failed, from what the deploy knows, at the release N candidate."""

    def test_each_kind_of_worse_names_what_failed(self):
        checks = [dict(UNITS_CHECK, name="Browser checks"), {"check_id": "units-2", "name": "Unit checks", "kind": "pass", "source": "release-check:units"},
                  {"check_id": "wake", "name": "Wake time", "kind": "measure", "source": "release-check:wake-replay", "better": "lower"}]
        base = {"units": {"result": "passed", "value": 10}, "units-2": {"result": "passed", "value": 900}, "wake": {"result": "measured", "value": 13}}
        found = outcome_rules.regressions(checks, base, {"units": {"result": "failed", "value": 8, "detail": {"failed": {"native-live": ["one case", "two"],
                                                                                                                      "native-rail": ["three"]}}}})["worse"]
        named = {item["check_id"]: dict(item, name=next(c["name"] for c in checks if c["check_id"] == item["check_id"])) for item in found}
        title = lambda check_id: deploy.outcome_checks.fix_title(named[check_id], 23)
        self.assertEqual(title("units"), "\u201cone case\u201d and 2 more fail at the release 23 candidate")
        self.assertEqual(title("units-2"), "Unit checks did not run at the release 23 candidate")
        self.assertEqual(title("wake"), "Wake time did not run at the release 23 candidate")
        one = dict(named["units"], cases=["one case"])
        self.assertEqual(deploy.outcome_checks.fix_title(one, 23), "\u201cone case\u201d fails at the release 23 candidate")
        slower = {"check_id": "wake", "name": "Wake time", "kind": "failed", "why": "aim-ux got worse in every run"}
        self.assertEqual(deploy.outcome_checks.fix_title(slower, 23), "Wake time got worse at the release 23 candidate")
        fewer = {"check_id": "units-2", "name": "Unit checks", "kind": "fewer", "why": "800 passing, 900 at the live release"}
        self.assertEqual(deploy.outcome_checks.fix_title(fewer, 23), "Unit checks got worse at the release 23 candidate")


UNITS_CHECK = {"check_id": "units", "group_name": "Checks", "position": 1, "name": "Unit checks", "kind": "pass", "source": "release-check:units"}


def counted(python=600, node=300, commit=SHA):
    """A passed summary whose unit steps carry their counts, and its times, as the release check writes them."""
    written = summary(commit)
    for step in written["steps"]:
        tests = {"python-units": python, "node-units": node}.get(step["name"])
        if tests is not None:
            step["counts"] = {"tests": tests, "pass": tests}
    return dict(written, started_at="2026-11-07T05:00:00-08:00", finished_at="2026-11-07T05:20:00-08:00")


PIECE = {"id": "p-release-20", "release": 20, "stage": "checks"}


def live_view(units=900, coordinator="agent-a", allowances=(), reader="agent-b", fix_topic=None, piece=PIECE):
    return {"instance_id": "i-1", "workspace_id": "building-theseus", "reader": reader, "coordinator": coordinator,
            "members": ["agent-a", "agent-b"], "checks": [UNITS_CHECK],
            "live": {"run_id": "run-19", "commit": HEAD, "tools": {"release-check": "theseus-release-check"}},
            "readings": {"units": {"result": "passed", "value": units, "detail": {}}} if units is not None else {},
            "allowances": list(allowances), "fix_topics": {"units": fix_topic}, "candidate_release": 20, "release_piece": piece}


class ChecksHost(FakeHost):
    """FakeHost that also answers whether the live code keeps outcome checks, the live CLI's checks read, and record-checks."""

    def __init__(self, config, keeps_checks=True, checks=None, record_checks=0, refuse=(), **more):
        super().__init__(config, **more)
        # keeps_checks: whether the live checkout has aim_next/checks.py, None when git cannot say. checks: the
        # CLI's (stdout, exit) for `checks OUTCOME --live SHA`, as a list answered in turn; record_checks: its exit.
        # refuse: the reporter's writes (start, post, stage-note, enter-stage) Core refuses.
        self.keeps_checks, self.checks, self.record_checks, self.recorded_runs = keeps_checks, list(checks or []), record_checks, []
        self.refuse = set(refuse)

    def __call__(self, argv, **kw):
        if argv[0] == "git" and "ls-tree" in argv and argv[-1] == "aim_next/checks.py":
            self.calls.append(argv)
            out, code = ("aim_next/checks.py\n" if self.keeps_checks else "", 0) if self.keeps_checks is not None else ("", 128)
            return subprocess.CompletedProcess(argv, code, out, "fatal: not a git repository" if code else "")
        if "checks" in argv and "--live" in argv:
            self.calls.append(argv)
            out, code = self.checks.pop(0)
            return subprocess.CompletedProcess(argv, code, out, "" if code == 0 else "Traceback: something broke")
        if "record-checks" in argv:
            self.calls.append(argv)
            self.recorded_runs.append(json.loads(Path(argv[argv.index("--run") + 1]).read_text()))
            return subprocess.CompletedProcess(argv, self.record_checks, "{}\n" if self.record_checks == 0 else "",
                                               "" if self.record_checks == 0 else "NOT_FOUND")
        verb = argv[7] if len(argv) > 7 and argv[3] == str(self.config.live / "bin/aim-next") else None
        if verb in ("start", "post", "stage-note", "enter-stage"):
            self.calls.append(argv)
            if verb in self.refuse:
                return subprocess.CompletedProcess(argv, 2, '{"error":{"code":"STAGE_GATED","message":"no"}}\n', "")
            result = {"start": {"message_id": "m-1", "topic": {"id": "t-fix"}}, "post": {"message_id": "m-2", "topic": {"id": argv[8]}},
                      "stage-note": {"piece_id": argv[8]}, "enter-stage": {"piece_id": argv[8], "stage_id": argv[9], "round": 1}}[verb]
            return subprocess.CompletedProcess(argv, 0, json.dumps({"status": "committed", "result": result}) + "\n", "")
        return super().__call__(argv, **kw)

    def wrote(self, verb):
        """The reporter's writes of one verb, each from its verb on: [verb, args...]."""
        return [argv[7:] for argv in self.calls if len(argv) > 7 and argv[3] == str(self.config.live / "bin/aim-next") and argv[7] == verb]


class OutcomeChecksConfig(DeployCase):
    def test_the_outcome_and_the_profile_that_reads_it_go_together(self):
        for given in ({"outcome": "building-theseus"}, {"reporter_profile": "agent-b.profile.json"}):
            with self.subTest(given=given):
                self.config_path.write_text(json.dumps(dict(self.raw, **given)))
                with self.assertRaises(deploy.Refused):
                    deploy.load_config(self.config_path)
        self.config_path.write_text(json.dumps(dict(self.raw, outcome="building-theseus", reporter_profile="agent-b.profile.json")))
        config = deploy.load_config(self.config_path)
        self.assertEqual((config.outcome, config.reporter_profile), ("building-theseus", self.instance / "agent-b.profile.json"))
        self.assertIsNone(self.config.outcome, "without the keys the deploy measures no outcome")


class OutcomeChecksBeforeAnythingStops(DeployCase):
    """02-checks and 02-worse (items 12, 104): the candidate's readings beside the live release's, read through the live CLI."""

    def setUp(self):
        super().setUp()
        self.config_path.write_text(json.dumps(dict(self.raw, outcome="building-theseus", reporter_profile="agent-b.profile.json")))
        self.config = deploy.load_config(self.config_path)
        self.summary.write_text(json.dumps(counted()))

    def checks_read(self, host):
        return [argv for argv in host.calls if "checks" in argv and "--live" in argv]

    def planned(self, host, **release):
        """A dry run that must go on; a refusal or a crash fails the test rather than erroring it."""
        try:
            self.deploy(host, dry=True, **release)
        except Exception as error:  # the failure under test: anything but going on
            self.fail("the deploy did not go on: %r" % error)

    def test_a_live_release_that_keeps_no_checks_is_said_and_the_deploy_goes_on(self):
        host = ChecksHost(self.config, keeps_checks=False)
        self.planned(host)
        written = json.loads((self.evidence / "02-checks.json").read_text())
        self.assertEqual(written["checks"], "the live release keeps no outcome checks yet")
        self.assertEqual((written["run"]["commit"], written["run"]["readings"]), (SHA, []), "the run is kept in the evidence only")
        self.assertEqual(self.checks_read(host), [], "nothing to read, known from the live code, not from a failed read")
        self.assertEqual(host.ran("record-checks"), [])

    def test_a_checks_read_that_fails_stops_the_deploy_rather_than_counting_as_no_checks(self):
        for host, reason in ((ChecksHost(self.config, keeps_checks=None), "whether the live code keeps outcome checks could not be read"),
                             (ChecksHost(self.config, checks=[("", 1)]), "the live outcome's checks could not be read"),
                             (ChecksHost(self.config, checks=[('{"error":{"code":"FORBIDDEN"}}\n', 2)]), "FORBIDDEN")):
            with self.subTest(reason=reason):
                self.kills = []
                self.assertIn(reason, self.refused(host))
                self.assertEqual(self.kills, [])
                self.assertEqual(host.ran("merge") + host.ran("-c"), [])

    def test_the_read_is_the_live_cli_as_the_reporter_for_this_commit_and_a_dry_run_writes_nothing(self):
        host = ChecksHost(self.config, checks=[(json.dumps(live_view()) + "\n", 0)])
        self.planned(host)
        self.assertEqual(self.checks_read(host), [[self.config.python, "-I", "-B", str(self.config.live / "bin/aim-next"), "--profile",
                                                   str(self.instance / "agent-b.profile.json"), "--json", "checks", "building-theseus", "--live", SHA]])
        self.assertEqual(json.loads((self.evidence / "02-checks.json").read_text())["worse"], [])
        self.assertEqual(host.ran("record-checks") + host.wrote("start") + host.wrote("stage-note"), [])
        self.assertNotIn("02-run", self.records())

    def test_a_reporter_that_is_the_coordinator_or_not_a_member_is_refused_before_anything_is_written(self):
        # D27: Core never delivers a message to its own author, so the coordinator telling itself wakes nobody.
        for view, reason in ((live_view(units=1000, reader="agent-a"), "the reporter, agent-a, must be a member of building-theseus and not its coordinator"),
                             (live_view(units=1000, reader="outsider"), "the reporter, outsider, must be a member")):
            with self.subTest(reason=reason):
                host = ChecksHost(self.config, checks=[(json.dumps(view) + "\n", 0)])
                self.assertIn(reason, self.refused(host))
                self.assertEqual(host.ran("record-checks") + host.wrote("start") + host.wrote("post"), [])
                self.assertEqual((self.kills, host.ran("merge")), ([], []))
        self.planned(ChecksHost(self.config, checks=[(json.dumps(live_view()) + "\n", 0)]))

    def test_a_check_made_worse_opens_its_fix_topic_records_the_run_and_holds_the_release_before_the_refusal(self):
        host = ChecksHost(self.config, checks=[(json.dumps(live_view(units=1000)) + "\n", 0)])
        reason = self.refused(host)
        self.assertTrue(reason.startswith("Unit checks got worse: "), reason)
        [start] = host.wrote("start")
        self.assertEqual(start[:-1], ["start", "building-theseus", "--title", "Unit checks got worse at the release 20 candidate",
                                      "--with", "agent-a", "--operation", "fix-units-" + SHA[:12]])
        self.assertIn("Evidence: %s" % self.summary.resolve(), start[-1])
        self.assertIn("Rename this topic to say what is wrong in your own words", start[-1])
        [argv] = host.ran("record-checks")
        self.assertEqual((argv[3], argv[argv.index("--profile") + 1], argv[argv.index("--as") + 1]),
                         (str(self.config.live / "bin/aim-next-admin"), str(self.config.server_profile), "agent-b"))
        [run] = host.recorded_runs
        self.assertEqual((run["outcome"], run["commit"], [(r["check_id"], r["value"], r.get("fix_topic_id")) for r in run["readings"]]),
                         ("building-theseus", SHA, [("units", 900, "t-fix")]))
        self.assertEqual(host.wrote("stage-note"), [["stage-note", "p-release-20", "Held: Unit checks got worse."]])
        other = ChecksHost(self.config, checks=[(json.dumps(live_view(units=1000, piece=None)) + "\n", 0)])
        self.refused(other)
        self.assertEqual(other.wrote("stage-note"), [], "with no release piece open, nothing is held")
        writes = [index for index, argv in enumerate(host.calls) if "record-checks" in argv or argv[7:8] in (["start"], ["stage-note"])]
        self.assertEqual([("record-checks" in host.calls[index]) or host.calls[index][7] for index in writes], ["start", True, "stage-note"],
                         "the topic is opened before the run is written, and the piece is held after")
        self.assertEqual((self.kills, host.ran("merge"), host.ran("-c"), host.wrote("enter-stage")), ([], [], [], []))
        self.assertNotIn("02-release", self.records(), "nothing after the verdict")
        # A dry run says the same, and writes nothing.
        dry = ChecksHost(self.config, checks=[(json.dumps(live_view(units=1000)) + "\n", 0)])
        self.assertEqual(self.refused(dry, dry=True), reason)
        self.assertEqual(dry.ran("record-checks") + dry.wrote("start") + dry.wrote("stage-note"), [])
        # What the owner allowed is not worse.
        allowed = {"check_id": "units", "kind": "fewer", "allowed_count": 900, "allowed_by": "owner", "commit_sha": None}
        self.planned(ChecksHost(self.config, checks=[(json.dumps(live_view(units=1000, allowances=[allowed])) + "\n", 0)]))

    def test_a_check_with_an_open_fix_topic_is_told_there_and_once_for_its_commit(self):
        older = live_view(units=1000, fix_topic={"topic_id": "t-open", "commit": "d" * 40})
        host = ChecksHost(self.config, checks=[(json.dumps(older) + "\n", 0)])
        self.refused(host)
        self.assertEqual(host.wrote("start"), [])
        [post] = host.wrote("post")
        self.assertEqual(post[:-1], ["post", "t-open", "--tells", "agent-a", "--subject", "check:units", "--operation", "fix-post-units-" + SHA[:12]])
        self.assertNotIn("Rename this topic", post[-1])
        self.assertEqual([r.get("fix_topic_id") for r in host.recorded_runs[0]["readings"]], ["t-open"])
        # Its reading at this commit already names the topic: the deploy was run again, and tells nobody twice.
        again = ChecksHost(self.config, checks=[(json.dumps(live_view(units=1000, fix_topic={"topic_id": "t-open", "commit": SHA})) + "\n", 0)])
        self.refused(again)
        self.assertEqual(again.wrote("start") + again.wrote("post"), [])
        self.assertEqual([r.get("fix_topic_id") for r in again.recorded_runs[0]["readings"]], ["t-open"])

    def test_a_summary_that_did_not_pass_is_recorded_as_measured_before_its_refusal(self):
        written = counted()
        for step in written["steps"]:
            if step["name"] == "native-colors":
                step.update(passed=False, exit=1)
        self.summary.write_text(json.dumps(dict(written, passed=False)))
        host = ChecksHost(self.config, checks=[(json.dumps(live_view()) + "\n", 0)])
        self.assertIn("the release check has not passed for %s: native-colors failed" % SHA, self.refused(host))
        self.assertEqual([r["check_id"] for r in host.recorded_runs[0]["readings"]], ["units"])
        self.assertEqual(host.wrote("stage-note"), [], "nothing got worse")
        # With something worse as well, the refusal names it first.
        worse = ChecksHost(self.config, checks=[(json.dumps(live_view(units=1000)) + "\n", 0)])
        self.assertTrue(self.refused(worse).startswith("Unit checks got worse"))

    def restarted(self, host):
        """A deploy that must go on to its restart (here, a start that stops the test)."""
        with self.assertRaisesRegex(AssertionError, "must not start a process"):
            self.deploy(host)

    def test_the_release_moves_on_only_from_checks_whatever_number_it_was_read_as(self):
        for piece in (dict(PIECE, stage="deploy"), dict(PIECE, stage="combine"), None):
            with self.subTest(piece=piece):
                host = ChecksHost(self.config, checks=[(json.dumps(live_view(piece=piece)) + "\n", 0)])
                self.restarted(host)
                self.assertEqual(host.wrote("enter-stage"), [])
                self.assertIsNone(json.loads((self.evidence / "03-stage.json").read_text())["entered"])
        # Core named it the release; the number it was read as (here before the deploy writes a record the instance
        # lacked) is words, not a second test of which piece it is.
        for piece in (PIECE, dict(PIECE, release=21)):
            with self.subTest(piece=piece):
                host = ChecksHost(self.config, checks=[(json.dumps(live_view(piece=piece)) + "\n", 0)])
                self.restarted(host)
                self.assertEqual(host.wrote("enter-stage"), [["enter-stage", "p-release-20", "deploy", "--left", "passed"]])

    def test_a_run_or_a_write_that_is_refused_stops_the_deploy_before_anything_stops(self):
        for host, reason in ((ChecksHost(self.config, checks=[(json.dumps(live_view(units=1000)) + "\n", 0)], record_checks=2),
                              "the candidate's run of the outcome's checks was not written"),
                             (ChecksHost(self.config, checks=[(json.dumps(live_view(units=1000)) + "\n", 0)], refuse={"start"}), "start was not written"),
                             (ChecksHost(self.config, checks=[(json.dumps(live_view()) + "\n", 0)], refuse={"enter-stage"}), "enter-stage was not written")):
            with self.subTest(reason=reason):
                self.assertIn(reason, self.refused(host))
                self.assertEqual((self.kills, host.ran("merge"), host.ran("-c")), ([], [], []))
        self.assertEqual(host.wrote("enter-stage"), [["enter-stage", "p-release-20", "deploy", "--left", "passed"]])


class OutcomeChecksWithTheRelease(Restarting):
    """02-run and 03-stage: the candidate's run is written, and the release piece enters deploy, before anything stops."""

    def setUp(self):
        super().setUp()
        self.config_path.write_text(json.dumps(dict(self.raw, outcome="building-theseus", reporter_profile="agent-b.profile.json")))
        self.config = deploy.load_config(self.config_path)
        self.summary.write_text(json.dumps(counted()))

    def test_the_run_is_written_and_the_release_piece_enters_deploy_before_anything_stops(self):
        host = ChecksHost(self.config, checks=[(json.dumps(live_view()) + "\n", 0)])
        self.completed(host)
        order = {name: [index for index, argv in enumerate(host.calls) if name in argv] for name in ("record-checks", "enter-stage", "-c", "record-release")}
        self.assertEqual([len(found) for found in order.values()], [1, 1, 1, 1])
        self.assertLess(order["record-checks"][0], order["enter-stage"][0])
        self.assertLess(order["enter-stage"][0], order["-c"][0], "the piece moves before anything stops (the backup follows the stop)")
        self.assertEqual(host.wrote("enter-stage"), [["enter-stage", "p-release-20", "deploy", "--left", "passed"]])
        self.assertEqual(json.loads((self.evidence / "03-stage.json").read_text())["entered"], "deploy")
        self.assertEqual(len([argv for argv in host.calls if "--live" in argv]), 1, "nothing is read or written once the release is live")
        self.assertNotIn("12-checks", self.records())


NATIVE_CHECK = {"check_id": "browser-checks", "group_name": "Checks", "position": 2, "name": "Browser checks", "kind": "pass",
                "source": "release-check:native"}
WAKE_CHECK = {"check_id": "wake-time", "group_name": "Measures", "position": 3, "name": "Wake time", "kind": "measure",
              "source": "release-check:wake-replay", "unit": "s", "better": "lower", "target_value": 10, "tolerance": 0.2, "repeats": 2}
CASE = "a new arrival keeps your place while you write"


class Started(Exception):
    """The deploy reached its restart: everything before the stop went on."""


class TheDeployAgainstAStore(test_agent_cli.AgentCLIBase):
    """7.2 steps 1 to 9 through the real agent CLI and admin verb into a real store (D15, D27, D31, D35): outcome w1 with
    owner "owner", coordinator agent-a and reporter agent-b; release 22 live at HEAD; release 23's piece combined with no
    commit, as Building Theseus's stages combine before the release commit exists, and moved on into checks."""

    on, add, stages = PipelineCase.on, PipelineCase.add, PipelineCase.stages

    def setUp(self):
        super().setUp()
        base = Path(self.temp.name).resolve()
        for name, pid, last in (("run-server-03", 111, None), ("run-runner-02", 222, '{"status": "stopped"}')):
            (base / name).mkdir()
            (base / name / "pid.json").write_text(json.dumps({"pid": pid}))
            if last:
                (base / name / "stdout.log").write_text('{"event": "acquired"}\n' + last + "\n")
        config = {"live": str(base / "live"), "instance": str(base), "server_profile": "agent-a.json", "port": 5192,
                  "runner_profile": "runner.json", "runner_id": "runner-01", "routes": "routes.json", "outcome": "w1", "reporter_profile": "agent-b.json"}
        (base / "deploy.json").write_text(json.dumps(config))
        self.config, self.evidence, self.summary = deploy.load_config(base / "deploy.json"), base / "evidence", base / "summary.json"
        for check in (UNITS_CHECK, NATIVE_CHECK):
            self.define(check)
        self.assertTrue(self.store.record_release({"release": 22, "commit": HEAD, "deployed_at": "2026-11-06T05:28:35-08:00", "items": [],
                                                   "source": "backfill"})["recorded"])
        self.store.record_checks({"outcome": "w1", "commit": HEAD, "started_at": "2026-11-06T04:00:00-08:00", "finished_at": "2026-11-06T04:15:00-08:00",
                                  "tools": {"release-check": release_check.TOOL}, "steps": [], "note": "", "readings": [
                                      {"check_id": "units", "result": "passed", "value": 900, "words": "900"},
                                      {"check_id": "browser-checks", "result": "passed", "value": 10 * len(release_check.NATIVE_STEPS), "words": "all"}]},
                                 "agent-a")
        self.committed(self.on(ships_in_releases=True))
        self.stages({"stage_id": "build", "name": "Build"}, {"stage_id": "wait", "name": "Wait"},
                    {"stage_id": "combine", "name": "Combine", "scope": "release"},
                    {"stage_id": "checks", "name": "Browser checks", "scope": "release", "rounds": "together", "gate": {"checks": ["units", "browser-checks"]}},
                    {"stage_id": "deploy", "name": "Deploy", "scope": "release"})
        numbers = [self.add("Item %d" % n, section="now", size="small") for n in (1, 2)]
        piece = self.committed(self.command("pipeline.piece_start", {"numbers": numbers, "topic_id": "t1", "stage_id": "build"}, {"workspace_id": "w1"}))
        self.committed(self.command("pipeline.stage_enter", {"stage_id": "wait", "left_outcome": "approved"}, {"piece_id": piece["piece_id"]}))
        self.release = self.committed(self.command("pipeline.combine", {"release_number": 23, "piece_ids": [piece["piece_id"]], "topic_id": "t1"},
                                                   {"workspace_id": "w1"}))["piece_id"]
        self.committed(self.command("pipeline.stage_enter", {"stage_id": "checks", "left_outcome": "passed"}, {"piece_id": self.release}))
        self.kills = []

    def revision(self):
        return self.store.view("owner", "outcome_checks", {"workspace_id": "w1"})["relevant_versions"]["checks"]

    def define(self, check):
        self.committed(self.command("check.define", {"check": check}, {"workspace_id": "w1"}, {"checks_revision": self.revision()}, actor="agent-a"))

    def write_summary(self, failed=(), wake=None):
        """counted(), with ten browser cases a step (one failing in each step named), and a wake replay (runs now, runs at the base) when given."""
        written = counted()
        for step in written["steps"]:
            if step["name"].startswith("native-"):
                broken = step["name"] in failed
                step.update(passed=not broken, exit=int(broken), counts={"tests": 10, "pass": 9 if broken else 10, **({"failed": [CASE]} if broken else {})})
        if wake:
            replay = lambda runs: {"name": "wake-replay", "passed": True, "series": {"aim-ux": {"runs": runs, "median": sorted(runs)[len(runs) // 2]}}}
            written["steps"].append(replay(wake[0]))
            written["base"] = {"commit": HEAD, "tools": {"release-check": release_check.TOOL}, "steps": [replay(wake[1])]}
        self.summary.write_text(json.dumps(dict(written, passed=not failed)))

    def deploy(self):
        """The deploy's answer, through the real CLI (as the reporter) and admin verb; ps, git and the release records from FakeHost."""
        fallback = ChecksHost(self.config, releases=(page(22), 0), log=log(("Items one and two", ["1", "2"])))
        live_cli, admin_cli = str(self.config.live / "bin/aim-next"), str(self.config.live / "bin/aim-next-admin")

        def run(argv, **kw):
            if len(argv) > 4 and (argv[3] == live_cli and "releases" not in argv or argv[3:5] == [admin_cli, "record-checks"]):
                out, err = io.StringIO(), io.StringIO()
                if argv[3] == live_cli:
                    code = cli.main(argv[4:], stdin=io.BytesIO(b""), stdout=out, stderr=err)
                else:
                    code = admin.main(argv[4:], stdout=out, stderr=err)
                return subprocess.CompletedProcess(argv, code, out.getvalue(), err.getvalue())
            return fallback(argv, **kw)
        with mock.patch.object(deploy, "run", run), \
                mock.patch.object(deploy.os, "kill", lambda pid, sig: self.kills.append((pid, sig))), \
                mock.patch.object(deploy, "wait_exit", return_value="exited"), \
                mock.patch.object(deploy, "start", side_effect=Started), \
                redirect_stdout(io.StringIO()):
            try:
                deploy.deploy(self.config, SHA, self.evidence, self.summary, False)
            except deploy.Refused as refusal:
                return str(refusal)
            except Started:
                return "restarting"
        self.fail("the deploy neither refused nor reached its restart")

    def candidate(self):
        return self.store.view("owner", "outcome_checks", {"workspace_id": "w1"})["loop"]["candidate"]

    def stage(self):
        return self.sql("SELECT stage_id,entered_by FROM pipeline_stage_entries WHERE piece_id=? AND left_ms IS NULL", (self.release,))[0]

    def test_a_failing_browser_case_holds_the_release_with_its_fix_topic_and_its_items_can_go_back_to_building(self):
        # Mockup 14's "A check fails": a summary that did not pass is measured, told and recorded before the refusal.
        self.write_summary(failed=("native-live",))
        topics = self.sql("SELECT count(*) FROM topics")[0][0]
        self.assertEqual(self.deploy(), "Browser checks got worse: a passing check now fails")
        [(topic, title, creator)] = self.sql("SELECT id,title,creator_id FROM topics WHERE kind='topic' AND id NOT IN ('t1')")
        self.assertEqual((title, creator), ("“%s” fails at the release 23 candidate" % CASE, "agent-b"))
        self.assertEqual(self.sql("SELECT actor_id FROM topic_participants WHERE topic_id=? ORDER BY actor_id", (topic,)), [("agent-a",), ("agent-b",)])
        candidate = self.candidate()
        self.assertEqual((candidate["label"], candidate["state"], [(item["check_id"], item["fix_topic"]["id"]) for item in candidate["worse"]]),
                         ("Release 23 candidate", "stopped", [("browser-checks", topic)]))
        self.assertEqual(self.sql("SELECT note FROM pipeline_stage_entries WHERE piece_id=? AND left_ms IS NULL", (self.release,)),
                         [("Held: Browser checks got worse.",)])
        self.assertEqual(self.kills, [])
        # Run again for the same commit, it finds the same run and topic and tells nobody twice.
        messages = self.sql("SELECT count(*) FROM messages WHERE topic_id=?", (topic,))
        self.assertEqual(self.deploy(), "Browser checks got worse: a passing check now fails")
        self.assertEqual((self.sql("SELECT count(*) FROM topics")[0][0], self.sql("SELECT count(*) FROM messages WHERE topic_id=?", (topic,))),
                         (topics + 1, messages))
        self.assertEqual(self.stage(), ("checks", "owner"), "held in checks")
        # D35: the owner takes the cause's item back to building, in the fix topic.
        back = self.committed(self.command("pipeline.piece_start", {"numbers": [1], "topic_id": topic, "stage_id": "build", "from_release": True},
                                           {"workspace_id": "w1"}))
        self.assertEqual(back["numbers"], [1])
        self.assertEqual(self.sql("SELECT number,left_reason FROM pipeline_piece_members WHERE piece_id=? ORDER BY number", (self.release,)),
                         [(1, "returned"), (2, None)])
        # The candidate lists the items still in the release; the one taken back is named under "after".
        candidate = self.candidate()
        self.assertEqual((candidate["items"], [item["number"] for item in candidate["after"]["returned"]]), ([2], [1]))

    def test_the_release_checks_report_enters_the_next_round_of_checks_and_shows_its_running_steps(self):
        # 7.1 --report through the real agent CLI (D35, D36): already in checks, the piece enters round 2, held.
        reporter = release_check.Reporter(str(Path(cli.__file__).resolve().parents[1] / "bin" / "aim-next"), str(self.profiles["agent-b"]), "w1", "d" * 40)
        self.evidence.mkdir()
        with redirect_stderr(io.StringIO()) as err:
            reporter.start(2)
            reporter.note([], {"name": "python-units", "since_ms": 1}, self.evidence)
        self.assertEqual(err.getvalue(), "")
        self.assertEqual(self.sql("SELECT stage_id,round,left_outcome FROM pipeline_stage_entries WHERE piece_id=? ORDER BY entered_ms,round",
                                  (self.release,))[-2:], [("checks", 1, "held"), ("checks", 2, None)])
        candidate = self.candidate()
        self.assertEqual((candidate["state"], candidate["commit"], [(step["name"], step["state"]) for step in candidate["steps"]][:1]),
                         ("running", "d" * 40, [("python-units", "running")]))
        landed = {"name": "python-units", "tool": "unittest", "passed": True, "counts": {"tests": 600, "pass": 600},
                  "started_at": "2026-11-07T08:00:00-08:00", "finished_at": "2026-11-07T08:01:00-08:00"}
        with redirect_stderr(io.StringIO()):
            reporter.note([landed], {"name": "node-units", "since_ms": 2}, self.evidence)
        steps = [(step["name"], step["state"]) for step in self.candidate()["steps"]]
        self.assertEqual(steps[:2], [("python-units", "landed"), ("node-units", "running")])

    def test_a_replay_measured_beside_code_that_is_not_live_is_refused_before_anything_is_told_or_stopped(self):
        self.define(WAKE_CHECK)
        topics = self.sql("SELECT count(*) FROM topics")
        for base in ("e" * 40, None):
            with self.subTest(base=base):
                self.write_summary(wake=([20.0, 21.0], [13.0, 13.4]))
                written = json.loads(self.summary.read_text())
                if base is None:
                    del written["base"]
                else:
                    written["base"]["commit"] = base
                self.summary.write_text(json.dumps(written))
                self.assertEqual(self.deploy(), "the replay was measured beside %s, but release 22 (%s) is live; run the release check again; "
                                 "nothing was stopped" % (base[:12] if base else "nothing (the summary has no base)", HEAD[:12]))
                self.assertEqual((self.kills, self.sql("SELECT count(*) FROM topics"), self.sql("SELECT count(*) FROM check_runs WHERE commit_sha=?", (SHA,))),
                                 ([], topics, [(0,)]))
        # Beside the live head, the same replay is measured (and here it got worse).
        self.write_summary(wake=([20.0, 21.0], [13.0, 13.4]))
        self.assertEqual(self.deploy(), "Wake time got worse: aim-ux got worse in every run")

    def test_a_replay_that_ran_fewer_times_than_its_check_asks_is_refused_before_anything_is_told_or_stopped(self):
        # Item 166: the release check runs the replay once unless asked; a rule of two would read it as worse and tell it.
        self.define(WAKE_CHECK)
        topics = self.sql("SELECT count(*) FROM topics")
        for now, base in (([20.0], [13.0, 13.4]), ([20.0, 21.0], [13.0])):
            with self.subTest(now=now, base=base):
                self.write_summary(wake=(now, base))
                self.assertEqual(self.deploy(), "the wake replay ran 1 time and wake-time asks for 2; run the release check with --repeats 2; "
                                 "nothing was stopped")
                self.assertEqual((self.kills, self.sql("SELECT count(*) FROM topics"), self.sql("SELECT count(*) FROM check_runs WHERE commit_sha=?", (SHA,))),
                                 ([], topics, [(0,)]))

    def test_a_worse_measure_the_owner_accepts_lets_the_same_commit_go_on_into_deploy(self):
        self.define(WAKE_CHECK)
        self.write_summary(wake=([20.0, 21.0], [13.0, 13.4]))
        self.assertEqual(self.deploy(), "Wake time got worse: aim-ux got worse in every run")
        candidate = self.candidate()
        self.assertEqual((candidate["state"], candidate["you_accept"], candidate["tools_same_as_base"]), ("stopped", True, True))
        [(title,)] = self.sql("SELECT title FROM topics WHERE kind='topic' AND id NOT IN ('t1')")
        self.assertEqual(title, "Wake time got worse at the release 23 candidate")
        # D31: only on the owner's own word.
        self.committed(self.command("check.accept_worse", {"run_id": candidate["run_id"], "why": "Worth it for the faster wakes"},
                                    {"workspace_id": "w1", "check_id": "wake-time"}, {"checks_revision": self.revision()}, actor="owner"))
        self.assertEqual(self.deploy(), "restarting")
        self.assertEqual(self.candidate()["state"], "accepted")
        self.assertEqual(self.sql("SELECT count(*) FROM check_runs WHERE commit_sha=? AND base_for_commit IS NULL", (SHA,)), [(1,)], "the same run")
        self.assertEqual(self.stage(), ("deploy", "agent-b"))
        self.assertEqual(self.sql("SELECT left_outcome,left_by FROM pipeline_stage_entries WHERE piece_id=? AND stage_id='checks'", (self.release,)),
                         [("passed", "agent-b")])
        self.assertEqual([pid for pid, _ in self.kills], [222, 111], "then everything stops, as before")

    def test_without_a_run_at_its_commit_the_release_piece_cannot_leave_checks(self):
        # The gate reads the release piece's own readings (D18); the deploy's 02-run is what writes them.
        self.rejected(self.command("pipeline.stage_enter", {"stage_id": "deploy", "left_outcome": "passed"}, {"piece_id": self.release}, actor="agent-b"),
                      "STAGE_GATED")
        self.write_summary()
        self.assertEqual(self.deploy(), "restarting")
        self.assertEqual(self.stage(), ("deploy", "agent-b"))
        self.assertEqual(json.loads((self.evidence / "03-stage.json").read_text())["entered"], "deploy")

    def recorded(self, commit, units="passed", base=None, at="2026-11-07T04:00:00-08:00"):
        """A release-check run of the outcome recorded as the deploy's 02-run records one, with its base rerun when given."""
        run = {"outcome": "w1", "commit": commit, "started_at": at, "finished_at": at, "tools": {"release-check": release_check.TOOL},
               "steps": [], "note": "", "readings": [{"check_id": "units", "result": units, "value": 900, "words": "900"},
                                                      {"check_id": "browser-checks", "result": "passed", "value": 10, "words": "all"}]}
        if base:
            run["base"] = dict(run, commit=HEAD, readings=[dict(reading, result=base) for reading in run["readings"]])
        return self.store.record_checks(run, "agent-b")

    def move_on(self, actor="agent-a"):
        return self.command("pipeline.stage_enter", {"stage_id": "deploy", "left_outcome": "passed"}, {"piece_id": self.release}, actor=actor)

    def test_the_release_piece_reads_its_own_candidates_and_nothing_else(self):
        # A base rerun that passed and a run of the live release's commit are not this release's readings.
        self.recorded(SHA, units="failed", base="passed")
        self.recorded(HEAD, at="2026-11-07T04:30:00-08:00")
        self.rejected(self.move_on(), "STAGE_GATED")
        # Whatever commit the candidate is, its passing run is what the gate reads: no commit was named at combine.
        self.recorded("d" * 40, at="2026-11-07T05:00:00-08:00")
        self.committed(self.move_on())
        self.assertEqual(self.stage(), ("deploy", "agent-a"))

    def test_a_new_commit_after_a_stop_is_checked_and_shipped_as_the_release(self):
        # D35: the first candidate stopped; its cause goes back to building; the next commit is checked and goes on.
        older = "d" * 40
        self.recorded(older, units="failed")
        self.assertEqual((self.candidate()["commit"], self.candidate()["state"]), (older, "stopped"))
        self.committed(self.command("opening.save", {"workspace_id": "w1", "topic_id": "t-fix", "title": "Fix", "participants": ["agent-a"],
                                                     "body": "Unit checks failed", "attachments": []}, {"opening_id": "o-fix"}, {"opening_revision": 0}))
        self.committed(self.command("opening.publish", {}, {"opening_id": "o-fix"}, {"opening_revision": 1}))
        self.committed(self.command("pipeline.piece_start", {"numbers": [1], "topic_id": "t-fix", "stage_id": "build", "from_release": True},
                                    {"workspace_id": "w1"}, actor="agent-a"))
        self.assertEqual(self.sql("SELECT number,left_reason FROM pipeline_piece_members WHERE piece_id=? ORDER BY number", (self.release,)),
                         [(1, "returned"), (2, None)])
        self.write_summary()
        self.assertEqual(self.deploy(), "restarting")
        self.assertEqual((self.candidate()["commit"], self.candidate()["state"]), (SHA, "passed"))
        self.assertEqual(self.stage(), ("deploy", "agent-b"))
        # What shipped is the item still in it; the one taken back has not shipped, and is not what keeps the release open.
        self.assertTrue(self.store.record_release({"release": 23, "commit": SHA, "deployed_at": "2026-11-07T09:00:00-08:00", "items": [2],
                                                   "source": "deploy"})["recorded"])
        view = self.store.view("agent-b", "outcome_live", {"workspace_id": "w1", "commit": SHA})
        self.assertEqual((view["release_piece"], view["candidate_release"]), (None, 24))

    def topic_of_w1(self, topic, workspace="w1"):
        self.committed(self.command("opening.save", {"workspace_id": workspace, "topic_id": topic, "title": "Work " + topic, "participants": ["agent-a"],
                                                     "body": "work", "attachments": []}, {"opening_id": "o-" + topic}, {"opening_revision": 0}))
        self.committed(self.command("opening.publish", {}, {"opening_id": "o-" + topic}, {"opening_revision": 1}))
        return topic

    def approved(self, numbers, topic, workspace="w1", **args):
        """A piece of these items started in topic and approved, waiting to be combined."""
        piece = self.committed(self.command("pipeline.piece_start", dict({"numbers": list(numbers), "topic_id": topic, "stage_id": "build"}, **args),
                                            {"workspace_id": workspace}, actor="agent-a"))["piece_id"]
        self.committed(self.command("pipeline.stage_enter", {"stage_id": "wait", "left_outcome": "approved"}, {"piece_id": piece}))
        return piece

    def combine(self, pieces, workspace="w1", **args):
        return self.command("pipeline.combine", dict({"piece_ids": list(pieces), "topic_id": "t1" if workspace == "w1" else "t-w2"}, **args),
                            {"workspace_id": workspace})

    def live_view(self, workspace="w1"):
        return self.store.view("agent-b", "outcome_live", {"workspace_id": workspace, "commit": SHA})

    def open_entry(self, piece):
        return self.sql("SELECT stage_id FROM pipeline_stage_entries WHERE piece_id=? AND left_ms IS NULL", (piece,))

    def test_a_stopped_release_whose_items_all_go_back_is_put_together_again_as_the_same_release(self):
        # The emptied piece used to stay the release: the rebuilt work was refused as 23, combined as 24, and the deploy
        # moved the empty piece into deploy. Every item going back ends the release, and a number is spent only by a record.
        self.recorded("d" * 40, units="failed")
        rebuilt = self.approved((1, 2), self.topic_of_w1("t-fix"), from_release=True)
        self.assertEqual(self.sql("SELECT stage_id,left_outcome,left_note FROM pipeline_stage_entries WHERE piece_id=? ORDER BY seq", (self.release,))[-1],
                         ("checks", "held", "Every item went back to building"))
        self.assertEqual((self.live_view()["candidate_release"], self.live_view()["release_piece"]), (23, None))
        self.assertIsNone(self.store.view("owner", "pipeline", {"workspace_id": "w1"})["facts"]["next_release"])
        again = self.committed(self.combine([rebuilt], release_number=23))
        self.assertEqual((again["release_number"], self.live_view()["release_piece"]["id"]), (23, again["piece_id"]))
        # The ended release's stopped run is not the new release's candidate, so nothing holds the new release.
        self.assertIsNone(self.candidate())
        self.rejected(self.command("pipeline.piece_start", {"numbers": [1], "topic_id": "t-fix", "stage_id": "build", "from_release": True},
                                   {"workspace_id": "w1"}, actor="agent-a"), "RELEASE_NOT_HELD")
        self.assertEqual(self.store.view("owner", "pipeline", {"workspace_id": "w1"})["facts"]["next_release"]["piece_id"], again["piece_id"])
        self.committed(self.command("pipeline.stage_enter", {"stage_id": "checks", "left_outcome": "passed"}, {"piece_id": again["piece_id"]}))
        self.write_summary()
        self.assertEqual(self.deploy(), "restarting")
        self.assertEqual(json.loads((self.evidence / "03-stage.json").read_text())["piece"]["id"], again["piece_id"])
        self.assertEqual((self.open_entry(again["piece_id"]), self.open_entry(self.release)), ([("deploy",)], []))
        self.assertEqual((self.candidate()["label"], self.candidate()["items"], self.candidate()["state"]), ("Release 23 candidate", [1, 2], "passed"))

    def test_a_release_whose_items_are_all_dropped_ends_and_never_held_its_number(self):
        for number in (1, 2):
            version = self.sql("SELECT version FROM pipeline_items WHERE workspace_id='w1' AND number=?", (number,))[0][0]
            self.committed(self.command("pipeline.item_move", {"section": "dropped", "underway_confirmed": True}, {"workspace_id": "w1", "number": number},
                                        {"item_revision": version}, actor="owner"))
        self.assertEqual((self.open_entry(self.release), self.live_view()["release_piece"]), ([], None))
        new = self.approved([self.add("Item 3", section="now", size="small")], self.topic_of_w1("t-new"))
        again = self.committed(self.combine([new], release_number=23))
        view = self.live_view()
        self.assertEqual((view["candidate_release"], view["release_piece"]["id"], view["release_piece"]["release"]), (23, again["piece_id"], 23))

    def test_the_next_release_is_combined_once_this_one_has_shipped(self):
        # One release at a time (3.4): work approved while release 23 is in checks waits, so the deploy, the Reporter
        # and the readings can only ever mean release 23's piece.
        waiting = self.approved([self.add("Item 3", section="now", size="small")], self.topic_of_w1("t-two"))
        refused = self.combine([waiting], release_number=24)
        self.rejected(refused, "RELEASE_OPEN")
        self.assertIn("Release 23 is still being put together (Browser checks)", refused["error"]["message"])
        self.assertEqual(self.open_entry(waiting), [("wait",)], "nothing was combined")
        self.rejected(self.combine([waiting]), "RELEASE_OPEN")
        self.write_summary()
        self.assertEqual(self.deploy(), "restarting")
        self.assertEqual((self.stage(), self.sql("SELECT DISTINCT piece_id FROM check_readings WHERE piece_id IS NOT NULL")),
                         (("deploy", "agent-b"), [(self.release,)]))
        self.assertTrue(self.store.record_release({"release": 23, "commit": SHA, "deployed_at": "2026-11-07T09:00:00-08:00", "items": [1, 2],
                                                   "source": "deploy"})["recorded"])
        self.assertEqual((self.live_view()["release_piece"], self.live_view()["candidate_release"]), (None, 24), "release 23 shipped")
        self.rejected(self.combine([waiting], release_number=23), "RELEASE_NUMBER")
        later = self.committed(self.combine([waiting]))
        self.assertEqual((later["release_number"], self.live_view()["release_piece"]["id"]), (24, later["piece_id"]))
        # Release 24 has no run of its own, so it cannot leave checks on release 23's readings.
        self.committed(self.command("pipeline.stage_enter", {"stage_id": "checks", "left_outcome": "passed"}, {"piece_id": later["piece_id"]}))
        self.rejected(self.command("pipeline.stage_enter", {"stage_id": "deploy", "left_outcome": "passed"}, {"piece_id": later["piece_id"]},
                                   actor="agent-a"), "STAGE_GATED")

    def ship_naming(self, items):
        """Release 23 (items 1 and 2) passes its checks, the deploy moves it into deploy, and its record names these items."""
        self.write_summary()
        self.assertEqual(self.deploy(), "restarting")
        self.assertTrue(self.store.record_release({"release": 23, "commit": SHA, "deployed_at": "2026-11-07T09:00:00-08:00", "items": list(items),
                                                   "source": "deploy"})["recorded"])

    def assert_release_23_shipped(self):
        """Nothing reads release 23's piece as release 24, and the shipped piece moves no more."""
        view = self.live_view()
        self.assertEqual((view["release_piece"], view["candidate_release"]), (None, 24))
        self.assertIsNone(self.store.view("owner", "pipeline", {"workspace_id": "w1"})["facts"]["next_release"])
        self.assertIsNone(self.candidate())
        self.rejected(self.command("pipeline.stage_enter", {"stage_id": "checks", "left_outcome": "passed"}, {"piece_id": self.release}), "PIECE_CLOSED")
        self.rejected(self.command("pipeline.stage_note", {"note": "Release 24 starts"}, {"piece_id": self.release}), "PIECE_CLOSED")
        self.assertEqual(self.open_entry(self.release), [], "its record closed it")
        self.assertEqual(self.entries(self.release)[-1], ("deploy", "shipped", "Shipped in release 23"))

    def combined_as_24_and_gated(self, numbers):
        """The items the record left out went back to building when it was written, so they are built again, and release 24
        is checked on its own run."""
        later = self.committed(self.combine([self.approved(numbers, self.topic_of_w1("t-again"))]))
        self.assertEqual(self.sql("SELECT number,left_reason FROM pipeline_piece_members WHERE piece_id=? ORDER BY number", (self.release,)),
                         [(number, "returned" if number in numbers else None) for number in (1, 2)])
        self.assertEqual((later["release_number"], later["numbers"], self.live_view()["release_piece"]["id"]), (24, list(numbers), later["piece_id"]))
        self.committed(self.command("pipeline.stage_enter", {"stage_id": "checks", "left_outcome": "passed"}, {"piece_id": later["piece_id"]}))
        self.rejected(self.command("pipeline.stage_enter", {"stage_id": "deploy", "left_outcome": "passed"}, {"piece_id": later["piece_id"]},
                                   actor="agent-a"), "STAGE_GATED")

    def test_a_release_whose_record_names_only_some_of_its_items_has_shipped(self):
        # A trailer that missed item 2: the deploy shipped release 23's commit, so release 23 has shipped. Its piece used to
        # stay "the release", read as release 24 with items 1 and 2, release 23's commit and readings, and could be moved
        # back into checks and on into deploy with no run of release 24.
        self.ship_naming([1])
        self.assert_release_23_shipped()
        self.combined_as_24_and_gated([2])

    def test_a_release_recorded_with_no_items_has_shipped(self):
        # deploy --no-items records release 23's commit and names no item.
        self.ship_naming([])
        self.assert_release_23_shipped()
        self.combined_as_24_and_gated([1, 2])

    def entries(self, piece):
        return self.sql("SELECT stage_id,left_outcome,left_note FROM pipeline_stage_entries WHERE piece_id=? ORDER BY seq", (piece,))

    def members(self, piece):
        return self.sql("SELECT number,left_reason FROM pipeline_piece_members WHERE piece_id=? ORDER BY number", (piece,))

    def record(self, number, commit, items, source="deploy", at="2026-11-07T09:00:00-08:00"):
        return self.store.record_release({"release": number, "commit": commit, "deployed_at": at, "items": list(items), "source": source})

    @staticmethod
    def live(number, at=None):
        """The note on a topic the pipeline holds shipped in this release (docs/pipeline.md): when it went live, in the
        page's dated form, as record() writes a release after 22 and backfill() an older one, unless at says otherwise."""
        when = datetime.datetime.fromisoformat(at or ("2026-11-07T09:00:00-08:00" if number > 22 else "2026-10-0%dT09:00:00-07:00" % number))
        return "Live in release %d since %s" % (number, when.strftime("%-I:%M %p %a %b %-d"))

    def start_of(self, numbers, topic):
        return self.command("pipeline.piece_start", {"numbers": list(numbers), "topic_id": self.topic_of_w1(topic), "stage_id": "build"},
                            {"workspace_id": "w1"}, actor="agent-a")

    def test_the_record_of_the_next_release_ships_the_release_whatever_commit_or_items_it_names(self):
        # 3.4: the release is the next release, so the record of that number ships it, from the stage it is in, even a
        # deploy of another commit naming none of its items; an item no record names has not shipped and goes back to building.
        self.recorded("d" * 40)
        self.assertTrue(self.record(23, "e" * 40, [])["recorded"])
        self.assertEqual((self.live_view()["release_piece"], self.live_view()["candidate_release"]), (None, 24))
        self.assertEqual((self.entries(self.release)[-1], self.members(self.release)),
                         (("checks", "shipped", "Shipped in release 23"), [(1, "returned"), (2, "returned")]))
        # Release 23 shares topic t1 with the piece of items 1 and 2, which went back to building, so t1 stays open.
        self.assertEqual(self.topics("t1"), [("t1", "active", "")])
        # The items it names stay in it; the rest go back.
        later = self.committed(self.combine([self.approved([1, 2], self.topic_of_w1("t-again"))], commit_sha="a" * 40))
        self.assertTrue(self.record(24, "f" * 40, [1], source="backfill")["recorded"])
        self.assertEqual((self.live_view()["release_piece"], self.entries(later["piece_id"])[-1], self.members(later["piece_id"])),
                         (None, ("combine", "shipped", "Shipped in release 24"), [(1, None), (2, "returned")]))
        # The same record written again is the record already there, and ends nothing.
        again = self.committed(self.combine([self.approved([2], self.topic_of_w1("t-four"))], commit_sha="a" * 40))
        self.assertFalse(self.record(24, "f" * 40, [1], source="backfill")["recorded"])
        self.assertEqual(self.live_view()["release_piece"], {"id": again["piece_id"], "release": 25, "stage": "combine", "round": 1})

    def test_a_record_of_an_older_release_ends_no_release_and_only_the_items_it_names_leave_it(self):
        # Release 5 recorded late (a backfill, or a deploy's kept or went-live record) names item 1: item 1 shipped in
        # release 5, so it leaves release 23, which stays open in its stage with item 2 and is still release 23.
        self.recorded("d" * 40)
        self.assertTrue(self.record(5, "e" * 40, [1], source="backfill", at="2026-10-01T09:00:00-07:00")["recorded"])
        self.assertEqual(self.live_view()["release_piece"], {"id": self.release, "release": 23, "stage": "checks", "round": 1})
        self.assertEqual((self.open_entry(self.release), self.members(self.release)), ([("checks",)], [(1, "returned"), (2, None)]))
        # Item 1's piece of work has finished, but its topic holds release 23, still under way, so it stays open.
        self.assertEqual(self.sql("SELECT state FROM topics WHERE id='t1'"), [("active",)])
        # Item 1 shipped, so the candidate does not say it went back to building.
        self.assertIsNone(self.candidate()["after"])
        self.assertEqual(self.store.view("owner", "pipeline", {"workspace_id": "w1"})["facts"]["next_release"]["items"], [2])
        # The record of release 23 then ships it with item 2.
        self.assertTrue(self.record(23, "d" * 40, [2])["recorded"])
        self.assertEqual((self.live_view()["release_piece"], self.entries(self.release)[-1], self.members(self.release)),
                         (None, ("checks", "shipped", "Shipped in release 23"), [(1, "returned"), (2, None)]))

    def test_an_item_a_record_names_is_never_built_again_whichever_record_named_it(self):
        # Within one deploy: release 23 went live without its record, so its record (items 1) is written first and ships
        # release 23, which sends item 2 back to building; the deploy's own record, release 24, then names item 2.
        self.assertTrue(self.record(23, "d" * 40, [1], source="backfill")["recorded"])
        self.assertTrue(self.record(24, SHA, [2])["recorded"])
        for number in (1, 2):
            refused = self.start_of([number], "t-again-%d" % number)
            self.rejected(refused, "ITEM_SHIPPED")
            self.assertIn("%d shipped in release %d" % (number, 22 + number), refused["error"]["message"])
        # One the records do not name is built as before.
        self.committed(self.start_of([self.add("Item 3", section="now", size="small")], "t-three"))

    def test_a_piece_whose_items_have_all_shipped_is_finished_and_its_topic_ready_for_review(self):
        # 7.2 step 11 (D1): the record of release 23 names items 1 to 4. Release 23's own topic and the piece of item 3
        # are finished, "Shipped in release 23"; the piece of items 4 and 5 still builds item 5, so it and its topic stay
        # as they are, and so does the release piece of another outcome that does not ship in releases.
        three = self.committed(self.start_of([self.add("Item 3", section="now", size="small")], "t-three"))["piece_id"]
        four, five = (self.add("Item %d" % n, section="now", size="small") for n in (4, 5))
        both = self.committed(self.start_of([four, five], "t-45"))["piece_id"]
        # A topic someone already said is done keeps their word.
        six = self.committed(self.start_of([self.add("Item 6", section="now", size="small")], "t-six"))["piece_id"]
        version = self.store.view("agent-a", "topic", {"topic_id": "t-six"})["topic"]["version"]
        self.committed(self.command("topic.update", {"state": "done", "note": "Wrapped up early"}, {"topic_id": "t-six"}, {"topic_revision": version},
                                    actor="agent-a"))
        # The outcome's conversation never ends (docs/conversation.md), even when work done there ships.
        conversation = self.store.view("owner", "conversation", {"workspace_id": "w1"})["conversation"]["topic_id"]
        seven = self.committed(self.command("pipeline.piece_start", {"numbers": [self.add("Item 7", section="now", size="small")],
                                                                     "topic_id": conversation, "stage_id": "build"}, {"workspace_id": "w1"}))["piece_id"]
        self.committed(self.command("workspace.create", {"id": "w2", "title": "Another", "purpose": "", "team": ["agent-a"]}))
        self.committed(self.command("pipeline.configure", {"enabled": True}, {"workspace_id": "w2"}, {"pipeline_revision": 0}))
        version = self.store.view("owner", "pipeline", {"workspace_id": "w2", "limit": 1})["pipeline"]["version"]
        self.committed(self.command("pipeline.stages_set", {"stages": [{"stage_id": "build", "name": "Build"}]}, {"workspace_id": "w2"},
                                    {"pipeline_revision": version}))
        theirs = [self.committed(self.command("pipeline.item_add", {"title": "Theirs %d" % n, "section": "now", "size": "small",
                                                                     "sources": [{"note": "a test"}]},
                                              {"workspace_id": "w2"}))["number"] for n in (1, 2, 3)]
        other = self.committed(self.command("pipeline.piece_start", {"numbers": theirs[2:], "topic_id": self.topic_of_w1("t-w2", workspace="w2"),
                                                                     "stage_id": "build"}, {"workspace_id": "w2"}, actor="agent-a"))["piece_id"]
        self.assertTrue(self.record(23, SHA, [1, 2, 3, 4, 6, 7])["recorded"])
        self.assertEqual((self.entries(seven)[-1], self.sql("SELECT state FROM topics WHERE id=?", (conversation,))),
                         (("build", "shipped", "Shipped in release 23"), [("active",)]))
        self.assertEqual((self.entries(three)[-1], self.entries(six)[-1], self.open_entry(both), self.open_entry(other)),
                         (("build", "shipped", "Shipped in release 23"), ("build", "shipped", "Shipped in release 23"), [("build",)], [("build",)]))
        self.assertEqual(self.sql("SELECT id,state,state_note,state_by FROM topics WHERE id IN ('t1','t-three','t-45','t-six','t-w2') ORDER BY id"),
                         [("t-45", "active", "", None), ("t-six", "done", "Wrapped up early", "agent-a"),
                          ("t-three", "review", self.live(23), None), ("t-w2", "active", "", None), ("t1", "review", self.live(23), None)])
        # Nothing reads an open piece as the release once it has shipped, and the other outcome builds its own item 1.
        self.assertEqual(self.live_view()["release_piece"], None)
        self.committed(self.command("pipeline.piece_start", {"numbers": theirs[:1], "topic_id": self.topic_of_w1("t-w2b", workspace="w2"),
                                                             "stage_id": "build"}, {"workspace_id": "w2"}, actor="agent-a"))

    def topics(self, *ids):
        return self.sql("SELECT id,state,state_note FROM topics WHERE id IN (%s) ORDER BY id" % ",".join("?" * len(ids)), ids)

    def release_24_from_its_own_topic(self, topic="t-24"):
        """Release 23 ships; then piece Q (items 3 and 4, in topic t-q) is approved and combined as release 24, in topic t-24
        (or in Q's own topic, as release 23 shares t1 with its piece)."""
        self.assertTrue(self.record(23, SHA, [1, 2])["recorded"])
        numbers = [self.add("Item %d" % n, section="now", size="small") for n in (3, 4)]
        self.assertEqual(numbers, [3, 4])
        piece = self.approved(numbers, self.topic_of_w1("t-q"))
        release = self.committed(self.combine([piece], release_number=24,
                                              topic_id=topic if topic == "t-q" else self.topic_of_w1(topic)))["piece_id"]
        return release

    def test_a_release_sharing_its_topic_with_a_piece_whose_item_went_back_leaves_that_topic_open(self):
        # 7.2 step 11 (D1): release 24 is put together in Q's own topic, t-q, and its record names only item 3. Release 24
        # has shipped, but item 4, combined from Q, went back to building, so Q's topic is not ready for review while item 4 is built there.
        release = self.release_24_from_its_own_topic("t-q")
        self.assertTrue(self.record(24, "a" * 40, [3])["recorded"])
        self.assertEqual((self.entries(release)[-1], self.members(release)), (("combine", "shipped", "Shipped in release 24"), [(3, None), (4, "returned")]))
        self.assertEqual(self.topics("t-q"), [("t-q", "active", "")])
        again = self.committed(self.command("pipeline.piece_start", {"numbers": [4], "topic_id": "t-q", "stage_id": "build"}, {"workspace_id": "w1"},
                                            actor="agent-a"))["piece_id"]
        self.assertEqual(self.topics("t-q"), [("t-q", "active", "")])
        self.assertTrue(self.record(25, "e" * 40, [4])["recorded"])
        self.assertEqual((self.entries(again)[-1], self.topics("t-q")), (("build", "shipped", "Shipped in release 25"), [("t-q", "review", self.live(25))]))

    def test_an_item_a_record_ships_from_a_piece_still_being_built_leaves_it_and_is_never_combined_again(self):
        # Item 3 is built in piece P with item 4 when the record of release 23 names it (a trailer on release 23's commit):
        # item 3 shipped in release 23, so it leaves P, which builds item 4 on, and combining P puts only item 4 in release 24.
        piece = self.committed(self.start_of([self.add("Item %d" % n, section="now", size="small") for n in (3, 4)], "t-p"))["piece_id"]
        self.ship_naming([1, 2, 3])
        self.assertEqual((self.members(piece), self.open_entry(piece), self.topics("t-p")), ([(3, "returned"), (4, None)], [("build",)], [("t-p", "active", "")]))
        self.committed(self.command("pipeline.stage_enter", {"stage_id": "wait", "left_outcome": "approved"}, {"piece_id": piece}))
        later = self.committed(self.combine([piece]))
        self.assertEqual((later["release_number"], later["numbers"]), (24, [4]))
        self.assertEqual(self.store.view("owner", "pipeline", {"workspace_id": "w1"})["facts"]["next_release"]["items"], [4])
        # Item 4 ships in release 24, and P with it.
        self.assertTrue(self.record(24, "a" * 40, [4])["recorded"])
        self.assertEqual(self.topics("t-p"), [("t-p", "review", self.live(24))])

    def test_a_piece_whose_combined_item_its_release_left_out_has_not_finished(self):
        # 7.2 step 11 (D1): items 3 and 4 left piece Q when it was combined, and release 24's record names only item 3, so
        # item 4 went back to building: Q has not shipped, and its topic stays open while item 4 is built there again.
        release = self.release_24_from_its_own_topic()
        self.assertTrue(self.record(24, "a" * 40, [3])["recorded"])
        self.assertEqual((self.entries(release)[-1], self.members(release)), (("combine", "shipped", "Shipped in release 24"), [(3, None), (4, "returned")]))
        self.assertEqual(self.topics("t-24", "t-q"), [("t-24", "review", self.live(24)), ("t-q", "active", "")])
        again = self.committed(self.command("pipeline.piece_start", {"numbers": [4], "topic_id": "t-q", "stage_id": "build"}, {"workspace_id": "w1"},
                                            actor="agent-a"))["piece_id"]
        self.assertEqual(self.topics("t-q"), [("t-q", "active", "")])
        # Once item 4 ships too, Q and the piece building item 4 are finished, and so is their topic.
        self.assertTrue(self.record(25, "e" * 40, [4])["recorded"])
        self.assertEqual((self.entries(again)[-1], self.topics("t-q")), (("build", "shipped", "Shipped in release 25"), [("t-q", "review", self.live(25))]))

    def test_a_piece_whose_combined_item_was_dropped_from_its_release_finishes_with_the_rest(self):
        # Item 4 was combined from Q into release 24 and dropped there, so it never ships: Q finishes when item 3 does.
        release = self.release_24_from_its_own_topic()
        version = self.sql("SELECT version FROM pipeline_items WHERE workspace_id='w1' AND number=4")[0][0]
        self.committed(self.command("pipeline.item_move", {"section": "dropped", "underway_confirmed": True}, {"workspace_id": "w1", "number": 4},
                                    {"item_revision": version}, actor="owner"))
        self.assertEqual(self.members(release), [(3, None), (4, "dropped")])
        self.assertTrue(self.record(24, "a" * 40, [3])["recorded"])
        self.assertEqual(self.topics("t-24", "t-q"), [("t-24", "review", self.live(24)), ("t-q", "review", self.live(24))])

    def test_older_records_that_take_every_item_out_of_the_release_end_it_unshipped(self):
        # An older record ends no release (3.4): one naming item 3 leaves release 24 open with item 4, and Q, whose item 4 is
        # still in release 24, has not shipped. One naming item 4 then empties release 24, which ends as an emptied release
        # ends: not shipped, its topic open, and the work combined next is still release 24.
        release = self.release_24_from_its_own_topic()
        self.assertTrue(self.record(5, "a" * 40, [3], source="backfill", at="2026-10-01T09:00:00-07:00")["recorded"])
        self.assertEqual((self.open_entry(release), self.members(release)), ([("combine",)], [(3, "returned"), (4, None)]))
        self.assertEqual(self.live_view()["release_piece"], {"id": release, "release": 24, "stage": "combine", "round": 1})
        self.assertEqual(self.topics("t-24", "t-q"), [("t-24", "active", ""), ("t-q", "active", "")])
        self.assertTrue(self.record(6, "e" * 40, [4], source="backfill", at="2026-10-02T09:00:00-07:00")["recorded"])
        self.assertEqual((self.entries(release)[-1], self.members(release)), (("combine", "held", "Its last items shipped in release 6"),
                                                                              [(3, "returned"), (4, "returned")]))
        self.assertEqual((self.live_view()["release_piece"], self.live_view()["candidate_release"]), (None, 24))
        self.assertEqual(self.topics("t-24", "t-q"), [("t-24", "active", ""), ("t-q", "review", self.live(6, "2026-10-02T09:00:00-07:00"))])
        new = self.approved([self.add("Item 5", section="now", size="small")], self.topic_of_w1("t-five"))
        self.assertEqual(self.committed(self.combine([new], topic_id="t-24"))["release_number"], 24)

    def test_an_older_record_naming_every_item_of_the_release_ends_it_unshipped(self):
        release = self.release_24_from_its_own_topic()
        self.assertTrue(self.record(5, "a" * 40, [3, 4], source="backfill", at="2026-10-01T09:00:00-07:00")["recorded"])
        self.assertEqual(self.entries(release)[-1], ("combine", "held", "Its last items shipped in release 5"))
        self.assertEqual((self.live_view()["release_piece"], self.live_view()["candidate_release"]), (None, 24))
        self.assertEqual(self.topics("t-24", "t-q"), [("t-24", "active", ""), ("t-q", "review", self.live(5, "2026-10-01T09:00:00-07:00"))])

    # ---- the end of a piece is one rule over the facts, whatever order they come in (3.4, D1, D25) -------------------

    def in_every_order(self, facts, final):
        """{final(): [orders]}: the named facts applied to this same store in every order, each order from a copy of the
        store as it is now. A fact refused in some order fails the check, so every order applies the same facts."""
        before = Path(self.temp.name) / "before-the-facts.sqlite3"
        with closing(sqlite3.connect(str(self.path))) as db, closing(sqlite3.connect(str(before))) as copy:
            db.backup(copy)
        states = {}
        for order in itertools.permutations(sorted(facts)):
            with closing(sqlite3.connect(str(before))) as copy, closing(sqlite3.connect(str(self.path))) as db:
                copy.backup(db)
            for name in order:
                facts[name]()
            states.setdefault(final(), []).append(" > ".join(order))
        return states

    def assert_one_end(self, facts, final, expected):
        states = self.in_every_order(facts, final)
        self.assertEqual(list(states), [expected], "\n".join("%d orders end %r, e.g. %s" % (len(orders), state, orders[0])
                                                             for state, orders in states.items()))

    def drop(self, number):
        version = self.sql("SELECT version FROM pipeline_items WHERE workspace_id='w1' AND number=?", (number,))[0][0]
        self.committed(self.command("pipeline.item_move", {"section": "dropped", "underway_confirmed": True}, {"workspace_id": "w1", "number": number},
                                    {"item_revision": version}, actor="owner"))

    def backfill(self, number, items):
        self.assertTrue(self.record(number, ("%x" % number) * 40, items, source="backfill", at="2026-10-0%dT09:00:00-07:00" % number)["recorded"])

    def end_of(self, piece, *topics):
        """A piece's end as the page reads it: its last stage entry, the items still in it, and its topics' states."""
        return (self.entries(piece)[-1], tuple(row[0] for row in self.sql("SELECT number FROM pipeline_piece_members WHERE piece_id=? "
                                                                          "AND left_ms IS NULL ORDER BY number", (piece,))), tuple(self.topics(*topics)))

    def test_a_piece_whose_last_item_is_dropped_after_its_other_item_shipped_has_shipped_whichever_came_first(self):
        # Reviewer's case A: piece P builds items 3 and 4 in t-p. Release 5, recorded late, names item 3, and item 4 is
        # dropped. Every item P held has shipped or left it and one shipped, so P has shipped and t-p is ready for review (D1), in
        # either order; the drop used to end P `dropped` by its own test and leave t-p open when it came last.
        piece = self.committed(self.start_of([self.add("Item %d" % n, section="now", size="small") for n in (3, 4)], "t-p"))["piece_id"]
        self.assert_one_end({"release 5 names 3": lambda: self.backfill(5, [3]), "drop 4": lambda: self.drop(4)},
                            lambda: self.end_of(piece, "t-p"),
                            (("build", "shipped", "Shipped in release 5"), (), (("t-p", "review", self.live(5)),)))

    def test_a_piece_whose_combined_item_is_dropped_after_its_release_shipped_finishes_whichever_came_first(self):
        # Reviewer's case B: release 24 holds items 3 and 4, combined from Q in t-q. Its record names item 3, and item 4 is
        # dropped: from the release, or after it went back to building. Either way Q's work has all shipped or left it.
        release = self.release_24_from_its_own_topic()
        self.assert_one_end({"release 24 names 3": lambda: self.assertTrue(self.record(24, "a" * 40, [3])["recorded"]),
                             "drop 4": lambda: self.drop(4)},
                            lambda: self.end_of(release, "t-24", "t-q"),
                            (("combine", "shipped", "Shipped in release 24"), (3,),
                             (("t-24", "review", self.live(24)), ("t-q", "review", self.live(24)))))

    def test_a_release_taken_apart_by_a_return_and_a_drop_ends_held_whichever_came_first(self):
        # D35 and D25 on one release: item 1 goes back to building and item 2 is dropped. The release ends held, saying
        # both, in either order (the last one used to decide it: `dropped`, or "Every item went back to building"), and
        # t1 stays open while item 1, combined from its piece there, is built again.
        self.recorded("d" * 40, units="failed")
        self.topic_of_w1("t-fix")
        back = lambda: self.committed(self.command("pipeline.piece_start", {"numbers": [1], "topic_id": "t-fix", "stage_id": "build", "from_release": True},
                                                   {"workspace_id": "w1"}))
        self.assert_one_end({"return 1": back, "drop 2": lambda: self.drop(2)}, lambda: self.end_of(self.release, "t1"),
                            (("checks", "held", "Its items went back to building or were dropped"), (), (("t1", "active", ""),)))

    def test_the_end_of_a_piece_and_its_topic_is_one_state_whatever_order_records_drops_a_return_and_a_move_come_in(self):
        # Piece P builds items 3, 4 and 6 in t-p while release 23 (items 1 and 2) is held in checks. Releases 5 and 6,
        # recorded late, name items 3 and 6; item 4 is dropped; item 1 goes back to building from the release (D35); item 1
        # is moved below item 5 in Now. In all 120 orders P has shipped, "Shipped in release 6" (the newest record naming
        # one of its items), t-p is ready for review with it, the release stays open with item 2 and t1 stays open.
        self.recorded("d" * 40, units="failed")
        numbers = [self.add("Item %d" % n, section="now", size="small") for n in (3, 4, 5, 6)]
        self.assertEqual(numbers, [3, 4, 5, 6])
        piece = self.committed(self.start_of([3, 4, 6], "t-p"))["piece_id"]
        self.topic_of_w1("t-fix")

        def move():
            version = self.sql("SELECT version FROM pipeline_items WHERE workspace_id='w1' AND number=1")[0][0]
            self.committed(self.command("pipeline.item_move", {"below": 5}, {"workspace_id": "w1", "number": 1}, {"item_revision": version}, actor="owner"))
        facts = {"release 5 names 3": lambda: self.backfill(5, [3]), "release 6 names 6": lambda: self.backfill(6, [6]), "drop 4": lambda: self.drop(4),
                 "return 1": lambda: self.committed(self.command("pipeline.piece_start", {"numbers": [1], "topic_id": "t-fix", "stage_id": "build",
                                                                                          "from_release": True}, {"workspace_id": "w1"})),
                 "move 1": move}
        self.assert_one_end(facts, lambda: (self.end_of(piece, "t-p"), self.end_of(self.release, "t1")),
                            ((("build", "shipped", "Shipped in release 6"), (), (("t-p", "review", self.live(6)),)),
                             (("checks", None, ""), (2,), (("t1", "active", ""),))))

    def test_turning_releases_on_decides_what_shipped_as_a_record_would_whichever_came_first(self):
        # Out of releases, a record names nothing of the outcome's (docs/pipeline.md, "Not yet"): piece P builds items 3 and
        # 4 while release 24's record names item 3. Turning releases back on makes that record the outcome's: item 3 leaves
        # P as a record's items leave a piece under way, and once item 4 is dropped P has shipped, whether the drop comes
        # before the switch or after it.
        self.ship_naming([1, 2])
        self.committed(self.on(ships_in_releases=False))
        numbers = [self.add("Item %d" % n, section="now", size="small") for n in (3, 4)]
        self.assertTrue(self.record(24, "a" * 40, [3], source="backfill")["recorded"])
        piece = self.committed(self.start_of(numbers, "t-p"))["piece_id"]
        self.assert_one_end({"releases on": lambda: self.committed(self.on(ships_in_releases=True)), "drop 4": lambda: self.drop(4)},
                            lambda: self.end_of(piece, "t-p"),
                            (("build", "shipped", "Shipped in release 24"), (), (("t-p", "review", self.live(24)),)))

    def test_combining_work_into_the_topic_of_a_shipped_piece_keeps_that_topic_open_whichever_came_first(self):
        # Reviewer's case: piece P builds item 3 in t-p; release 5, recorded late, names item 3; approved piece Q (item 4)
        # is combined as release 24 in t-p. Release 24 is being put together in t-p, so t-p is not ready for review, in either order;
        # the record used to close t-p "Shipped in release 5" when it came first, and the combine never opened it again.
        self.assertTrue(self.record(23, SHA, [1, 2])["recorded"])
        three, four = (self.add("Item %d" % n, section="now", size="small") for n in (3, 4))
        self.committed(self.start_of([three], "t-p"))
        waiting = self.approved([four], self.topic_of_w1("t-q"))
        self.assert_one_end({"release 5 names 3": lambda: self.backfill(5, [three]),
                             "combine Q in t-p": lambda: self.committed(self.combine([waiting], topic_id="t-p"))},
                            lambda: tuple(self.topics("t-p")), (("t-p", "active", ""),))

    def test_new_work_in_the_topic_of_a_shipped_release_keeps_that_topic_open_whichever_came_first(self):
        # Reviewer's case: release 24 holds Q's items 3 and 4 (Q in t-q); its record names both, and item 5 starts in t-q.
        # Item 5 is being built in t-q, so t-q is not ready for review, in either order; the record used to close t-q when it came
        # first, and the start never opened it again.
        self.release_24_from_its_own_topic()
        five = self.add("Item 5", section="now", size="small")
        self.assert_one_end({"release 24 names 3 and 4": lambda: self.assertTrue(self.record(24, "a" * 40, [3, 4])["recorded"]),
                             "start 5 in t-q": lambda: self.committed(self.command("pipeline.piece_start", {"numbers": [five], "topic_id": "t-q",
                                                                                                             "stage_id": "build"},
                                                                                     {"workspace_id": "w1"}, actor="agent-a"))},
                            lambda: tuple(self.topics("t-q")), (("t-q", "active", ""),))

    def test_a_record_naming_an_item_dropped_before_its_piece_was_combined_ships_it_in_that_release_whichever_came_first(self):
        # Reviewer's case: Q (items 3 and 4, in t-q) is approved; item 3 is dropped and Q is combined as release 24 in
        # t-24, in either order; then release 24's record names items 3 and 4. The record names item 3, so its drop counts
        # for nothing, and nor does the combine it kept item 3 out of: release 24 shipped holding both, Q's items were
        # both combined into it, in either order. Dropped first, release 24 used to ship holding item 4 alone.
        self.assertTrue(self.record(23, SHA, [1, 2])["recorded"])
        numbers = [self.add("Item %d" % n, section="now", size="small") for n in (3, 4)]
        waiting = self.approved(numbers, self.topic_of_w1("t-q"))
        self.topic_of_w1("t-24")
        made = {}

        def combine():
            made["release"] = self.committed(self.combine([waiting], topic_id="t-24"))["piece_id"]

        def recorded_then_the_end():
            self.assertTrue(self.record(24, "a" * 40, numbers)["recorded"])
            return self.end_of(made["release"], "t-24", "t-q"), tuple(self.members(waiting)), tuple(self.members(made["release"]))
        self.assert_one_end({"drop 3": lambda: self.drop(3), "combine Q": combine}, recorded_then_the_end,
                            ((("combine", "shipped", "Shipped in release 24"), (3, 4),
                              (("t-24", "review", self.live(24)), ("t-q", "review", self.live(24)))),
                             ((3, "combined"), (4, "combined")), ((3, None), (4, None))))

    def test_an_older_record_naming_an_item_dropped_before_its_piece_was_combined_ships_it_from_that_release_whichever_came_first(self):
        # The same, with release 5 recorded late naming item 3: it ships no release, so item 3 shipped from release 24,
        # which holds item 4 and keeps t-24 and t-q open, and Q's part in item 3 went on into release 24, in either order.
        self.assertTrue(self.record(23, SHA, [1, 2])["recorded"])
        numbers = [self.add("Item %d" % n, section="now", size="small") for n in (3, 4)]
        waiting = self.approved(numbers, self.topic_of_w1("t-q"))
        self.topic_of_w1("t-24")
        made = {}

        def combine():
            made["release"] = self.committed(self.combine([waiting], topic_id="t-24"))["piece_id"]

        def recorded_then_the_end():
            self.backfill(5, [3])
            return self.end_of(made["release"], "t-24", "t-q"), tuple(self.members(waiting)), tuple(self.members(made["release"]))
        self.assert_one_end({"drop 3": lambda: self.drop(3), "combine Q": combine}, recorded_then_the_end,
                            ((("combine", None, ""), (4,), (("t-24", "active", ""), ("t-q", "active", ""))),
                             ((3, "combined"), (4, "combined")), ((3, "returned"), (4, None))))

    def test_the_state_is_one_whatever_order_starts_combines_a_record_a_drop_and_a_move_come_in(self):
        # Release 23 has shipped. Piece P builds item 6 in t-p, and Q (items 3 and 4, in t-q) is approved. Q is combined as
        # release 24 in t-24; item 5 starts in t-5; release 5, recorded late, names item 6; item 3 is dropped (from Q, or
        # from release 24); item 5 is moved below item 6. In all 120 orders P has shipped in release 5 and t-p is ready for review with
        # it, release 24 holds item 4 and keeps t-24 open, t-q stays open while item 4 is in release 24, and t-5 is open
        # with item 5's work.
        self.assertTrue(self.record(23, SHA, [1, 2])["recorded"])
        numbers = [self.add("Item %d" % n, section="now", size="small") for n in (3, 4, 5, 6)]
        self.assertEqual(numbers, [3, 4, 5, 6])
        piece = self.committed(self.start_of([6], "t-p"))["piece_id"]
        waiting = self.approved([3, 4], self.topic_of_w1("t-q"))
        self.topic_of_w1("t-24")
        self.topic_of_w1("t-5")
        made = {}

        def combine():
            made["release"] = self.committed(self.combine([waiting], topic_id="t-24"))["piece_id"]

        def move():
            version = self.sql("SELECT version FROM pipeline_items WHERE workspace_id='w1' AND number=5")[0][0]
            self.committed(self.command("pipeline.item_move", {"below": 6}, {"workspace_id": "w1", "number": 5}, {"item_revision": version}, actor="owner"))
        facts = {"combine Q": combine, "release 5 names 6": lambda: self.backfill(5, [6]), "drop 3": lambda: self.drop(3), "move 5": move,
                 "start 5 in t-5": lambda: self.committed(self.command("pipeline.piece_start", {"numbers": [5], "topic_id": "t-5", "stage_id": "build"},
                                                                       {"workspace_id": "w1"}, actor="agent-a"))}
        self.assert_one_end(facts, lambda: (self.end_of(piece, "t-p"), self.end_of(made["release"], "t-24", "t-q"), tuple(self.topics("t-5"))),
                            ((("build", "shipped", "Shipped in release 5"), (), (("t-p", "review", self.live(5)),)),
                             (("combine", None, ""), (4,), (("t-24", "active", ""), ("t-q", "active", ""))), (("t-5", "active", ""),)))

    def test_an_item_dropped_and_put_back_before_its_piece_was_combined_is_still_that_pieces_work(self):
        # Item 3 was dropped and put back before Q built it with item 4; Q is combined as release 24 in t-24. Release 5,
        # recorded late, ships item 4, but item 3 is still in release 24, so Q's topic stays open: only a drop since Q
        # handed an item on ends Q's part in it.
        self.ship_naming([1, 2])
        three, four = (self.add("Item %d" % n, section="now", size="small") for n in (3, 4))
        self.drop(three)
        version = self.sql("SELECT version FROM pipeline_items WHERE workspace_id='w1' AND number=?", (three,))[0][0]
        self.committed(self.command("pipeline.item_move", {"section": "now"}, {"workspace_id": "w1", "number": three}, {"item_revision": version},
                                    actor="owner"))
        release = self.committed(self.combine([self.approved([three, four], self.topic_of_w1("t-q"))], topic_id=self.topic_of_w1("t-24")))["piece_id"]
        self.backfill(5, [four])
        self.assertEqual(self.end_of(release, "t-24", "t-q"), (("combine", None, ""), (three,), (("t-24", "active", ""), ("t-q", "active", ""))))

    def test_a_release_recorded_with_none_of_its_items_in_a_topic_of_its_own_closes_that_topic(self):
        # Release 24, put together in t-24 from Q's items 3 and 4, is recorded with no items: it has shipped, so t-24, where
        # nothing else is built, is ready for review; items 3 and 4 went back to building, so Q's topic is not.
        release = self.release_24_from_its_own_topic()
        self.assertTrue(self.record(24, "a" * 40, [])["recorded"])
        self.assertEqual(self.end_of(release, "t-24", "t-q"), (("combine", "shipped", "Shipped in release 24"), (),
                                                               (("t-24", "review", self.live(24)), ("t-q", "active", ""))))

    # ---- a record overtakes the acts it makes impossible: what it names shipped, as if it had come first (3.4, D25) ----

    def drop_unless_shipped(self, number):
        """A drop, or its refusal once a record names the item, since a shipped item stays where it shipped. Either way the
        record decides: the order checks below apply the record and the drop in every order, the drop refused after it."""
        version = self.sql("SELECT version FROM pipeline_items WHERE workspace_id='w1' AND number=?", (number,))[0][0]
        receipt = self.command("pipeline.item_move", {"section": "dropped", "underway_confirmed": True}, {"workspace_id": "w1", "number": number},
                               {"item_revision": version}, actor="owner")
        if receipt.get("status") == "committed":
            self.applied.add(number)
        else:
            self.rejected(receipt, "ITEM_SHIPPED")
            self.overtaken.add(number)

    def assert_one_end_overtaken(self, facts, final, expected, orders=None):
        """assert_one_end where a drop may come after the record naming its item: in some orders the drop applies and a
        record overtakes it, in the others it is refused, and every order ends the same."""
        self.applied, self.overtaken = set(), set()
        if orders is None:
            states = self.in_every_order(facts, final)
        else:
            before = Path(self.temp.name) / "before-the-facts.sqlite3"
            with closing(sqlite3.connect(str(self.path))) as db, closing(sqlite3.connect(str(before))) as copy:
                db.backup(copy)
            states = {}
            for order in orders:
                with closing(sqlite3.connect(str(before))) as copy, closing(sqlite3.connect(str(self.path))) as db:
                    copy.backup(db)
                for name in order:
                    facts[name]()
                states.setdefault(final(), []).append(" > ".join(order))
        self.assertEqual(list(states), [expected], "\n".join("%d orders end %r, e.g. %s" % (len(orders), state, orders[0])
                                                             for state, orders in states.items()))
        self.assertTrue(self.applied and self.overtaken, "some order drops before the record and some after it")

    def test_a_record_naming_an_item_dropped_from_its_piece_ships_that_piece_and_closes_its_topic(self):
        # Review case (a): the deploy takes a record's items from its commits' trailers, so a record can name an item
        # already dropped. Piece P built item 3 alone in t-p; item 3 is dropped, and release 5, recorded late, names it.
        # Item 3 shipped, so P has shipped and t-p is ready for review, as when the record came first and the drop was refused; the
        # drop used to end P `dropped` for good and leave t-p open while item 3 read as shipped, and the page showed it
        # in Dropped with the dropped glyph.
        piece = self.committed(self.start_of([self.add("Item 3", section="now", size="small")], "t-p"))["piece_id"]
        self.ship_naming([1, 2])
        self.assert_one_end_overtaken({"drop 3": lambda: self.drop_unless_shipped(3), "release 5 names 3": lambda: self.backfill(5, [3])},
                                      lambda: (self.end_of(piece, "t-p"), self.item_shipped(3)),
                                      ((("build", "shipped", "Shipped in release 5"), (), (("t-p", "review", self.live(5)),)), (5, "shipped", False)))

    def item_shipped(self, number):
        """How the page reads an item: the release it shipped in, its glyph, and whether Dropped lists it."""
        item = self.store.view("owner", "pipeline_item", {"workspace_id": "w1", "number": number})["item"]
        dropped = self.store.view("owner", "pipeline", {"workspace_id": "w1", "section": "dropped"})["items"]
        return item["shipped"] and item["shipped"].get("release"), item["glyph"], number in [row["number"] for row in dropped]

    def test_the_record_of_a_release_naming_an_item_dropped_from_it_keeps_that_item_in_it_and_closes_its_topic(self):
        # Review case (b): release 24 is combined from Q (item 4, t-q) and Q2 (item 3, t-q2) in t-24. Item 4 is dropped from
        # the release, and release 24's record names items 3 and 4: item 4 shipped in release 24, so it stays in that
        # release and Q's topic is ready for review, as when the record came first. t-q used to stay open with item 4 shipped.
        self.assertTrue(self.record(23, SHA, [1, 2])["recorded"])
        three, four = (self.add("Item %d" % n, section="now", size="small") for n in (3, 4))
        pieces = [self.approved([four], self.topic_of_w1("t-q")), self.approved([three], self.topic_of_w1("t-q2"))]
        release = self.committed(self.combine(pieces, release_number=24, topic_id=self.topic_of_w1("t-24")))["piece_id"]
        shipped = "Shipped in release 24"
        self.assert_one_end_overtaken({"drop 4": lambda: self.drop_unless_shipped(4),
                                       "release 24 names 3 and 4": lambda: self.assertTrue(self.record(24, "a" * 40, [3, 4])["recorded"])},
                                      lambda: self.end_of(release, "t-24", "t-q", "t-q2"),
                                      (("combine", "shipped", shipped), (3, 4), (("t-24", "review", self.live(24)), ("t-q", "review", self.live(24)), ("t-q2", "review", self.live(24)))))

    def test_the_record_of_a_release_emptied_by_drops_it_overtakes_ships_that_release(self):
        # Release 24 (items 3 and 4 from Q in t-q, put together in t-24) loses both items to drops, so it ended and never
        # held its number; release 24's record then names them. It ships release 24 as it would have before the drops: the
        # release is left `shipped` holding them, both topics are ready for review, and the next release is 25.
        release = self.release_24_from_its_own_topic()
        shipped = "Shipped in release 24"
        self.assert_one_end_overtaken({"drop 3": lambda: self.drop_unless_shipped(3), "drop 4": lambda: self.drop_unless_shipped(4),
                                       "release 24 names 3 and 4": lambda: self.assertTrue(self.record(24, "a" * 40, [3, 4])["recorded"])},
                                      lambda: (self.end_of(release, "t-24", "t-q"), self.live_view()["release_piece"], self.live_view()["candidate_release"]),
                                      ((("combine", "shipped", shipped), (3, 4), (("t-24", "review", self.live(24)), ("t-q", "review", self.live(24)))), None, 25))

    def test_the_record_of_the_next_release_ships_the_release_open_though_it_overtakes_a_drop_from_one_emptied_before(self):
        # Release 24's items 3 and 4 are both dropped, so it ended and passed its number on: item 5's piece is combined as
        # release 24 and deployed, and its record names item 5 and, from an older trailer, item 3. The release deployed is
        # the one it ships; the emptied release only learns where item 3 went, and Q's topic is ready for review. (Had the record come
        # before the drops, the combine would have made release 25, refused here as release 24: not the same facts.)
        release = self.release_24_from_its_own_topic()
        five = self.approved([self.add("Item 5", section="now", size="small")], self.topic_of_w1("t-five"))
        self.drop(3)
        self.drop(4)
        later = self.committed(self.combine([five], release_number=24, topic_id=self.topic_of_w1("t-25")))["piece_id"]
        self.assertTrue(self.record(24, "a" * 40, [3, 5])["recorded"])
        shipped = "Shipped in release 24"
        self.assertEqual((self.end_of(release, "t-24", "t-q"), self.end_of(later, "t-25", "t-five")),
                         ((("combine", "held", "Its items shipped in release 24 or were dropped"), (), (("t-24", "active", ""), ("t-q", "review", self.live(24)))),
                          (("combine", "shipped", shipped), (5,), (("t-25", "review", self.live(24)), ("t-five", "review", self.live(24))))))
        self.assertEqual((self.live_view()["release_piece"], self.live_view()["candidate_release"]), (None, 25))

    def test_a_release_emptied_by_drops_is_shipped_only_by_the_record_of_its_own_number(self):
        # Release 24 loses items 3 and 4 to drops; release 24 is then recorded naming neither (a hotfix, with no release
        # open), and release 25 names item 3. The emptied release would have been release 24, not 25, so release 25 does
        # not ship it: it says item 3 shipped in release 25 and item 4 was dropped, and Q's topic is ready for review.
        release = self.release_24_from_its_own_topic()
        self.drop(3)
        self.drop(4)
        self.assertTrue(self.record(24, "a" * 40, [])["recorded"])
        self.assertTrue(self.record(25, "e" * 40, [3])["recorded"])
        self.assertEqual(self.end_of(release, "t-24", "t-q"), (("combine", "held", "Its items shipped in release 25 or were dropped"), (),
                                                               (("t-24", "active", ""), ("t-q", "review", self.live(25)))))

    def test_an_older_record_naming_items_dropped_from_the_release_says_where_they_went(self):
        # Release 5, recorded late, names items 3 and 4, both dropped from release 24: they shipped in release 5, so the
        # emptied release says so and Q's topic is ready for review, as when the record came first and they left it `returned`.
        release = self.release_24_from_its_own_topic()
        self.assert_one_end_overtaken({"drop 3": lambda: self.drop_unless_shipped(3), "drop 4": lambda: self.drop_unless_shipped(4),
                                       "release 5 names 3 and 4": lambda: self.backfill(5, [3, 4])},
                                      lambda: self.end_of(release, "t-24", "t-q"),
                                      (("combine", "held", "Its last items shipped in release 5"), (),
                                       (("t-24", "active", ""), ("t-q", "review", self.live(5)))))

    def test_a_newer_record_naming_an_item_dropped_after_its_piece_shipped_is_the_release_it_shipped_in(self):
        # P builds items 3 and 4 in t-p; release 5 names item 3, item 4 is dropped, and release 7 names item 4. Every order
        # ends P "Shipped in release 7", the newest record naming one of its items, and t-p with it; the drop used to end P
        # with release 5 for good when it came before release 7.
        self.ship_naming([1, 2])
        piece = self.committed(self.start_of([self.add("Item %d" % n, section="now", size="small") for n in (3, 4)], "t-p"))["piece_id"]
        self.assert_one_end_overtaken({"release 5 names 3": lambda: self.backfill(5, [3]), "drop 4": lambda: self.drop_unless_shipped(4),
                                       "release 7 names 4": lambda: self.backfill(7, [4])},
                                      lambda: self.end_of(piece, "t-p"),
                                      (("build", "shipped", "Shipped in release 7"), (), (("t-p", "review", self.live(7)),)))

    def test_turning_releases_on_overtakes_a_drop_of_an_item_a_record_names(self):
        # Out of releases, release 24's record names item 4 of piece P (items 3 and 4, t-p), which is then dropped, as is
        # item 3. Turning releases on makes that record the outcome's, so item 4 shipped and P with it, whichever came first.
        self.ship_naming([1, 2])
        self.committed(self.on(ships_in_releases=False))
        numbers = [self.add("Item %d" % n, section="now", size="small") for n in (3, 4)]
        self.assertTrue(self.record(24, "a" * 40, [4], source="backfill")["recorded"])
        piece = self.committed(self.start_of(numbers, "t-p"))["piece_id"]
        self.assert_one_end_overtaken({"releases on": lambda: self.committed(self.on(ships_in_releases=True)),
                                       "drop 4": lambda: self.drop_unless_shipped(4), "drop 3": lambda: self.drop_unless_shipped(3)},
                                      lambda: self.end_of(piece, "t-p"),
                                      (("build", "shipped", "Shipped in release 24"), (), (("t-p", "review", self.live(24)),)))

    def test_turning_releases_back_on_never_takes_back_what_a_piece_shipped(self):
        # P's item 3 shipped in release 5, so P has shipped and t-p is ready for review. Out of releases item 3 reads as unshipped and is
        # dropped and put back; turning releases back on decides P's end again, which only a record overtaking a drop
        # changes, and never from shipped: P and t-p stay as they were.
        self.ship_naming([1, 2])
        piece = self.committed(self.start_of([self.add("Item 3", section="now", size="small")], "t-p"))["piece_id"]
        self.backfill(5, [3])
        self.committed(self.on(ships_in_releases=False))
        self.drop(3)
        version = self.sql("SELECT version FROM pipeline_items WHERE workspace_id='w1' AND number=3")[0][0]
        self.committed(self.command("pipeline.item_move", {"section": "now"}, {"workspace_id": "w1", "number": 3}, {"item_revision": version}, actor="owner"))
        self.committed(self.on(ships_in_releases=True))
        self.assertEqual(self.end_of(piece, "t-p"), (("build", "shipped", "Shipped in release 5"), (), (("t-p", "review", self.live(5)),)))

    def test_an_item_put_back_after_its_drop_is_new_work_whatever_ships_it(self):
        # Item 3 is dropped from P (t-p), put back and built again by P2 (t-p2), which release 5 ships. Only the drop the
        # record overtakes, the item's last, is undone: P stays dropped and t-p open, P2 and t-p2 have shipped.
        piece = self.committed(self.start_of([self.add("Item 3", section="now", size="small")], "t-p"))["piece_id"]
        self.ship_naming([1, 2])
        self.drop(3)
        version = self.sql("SELECT version FROM pipeline_items WHERE workspace_id='w1' AND number=3")[0][0]
        self.committed(self.command("pipeline.item_move", {"section": "now"}, {"workspace_id": "w1", "number": 3}, {"item_revision": version}, actor="owner"))
        again = self.committed(self.start_of([3], "t-p2"))["piece_id"]
        self.backfill(5, [3])
        self.assertEqual((self.end_of(piece, "t-p"), self.end_of(again, "t-p2")),
                         ((("build", "dropped", ""), (), (("t-p", "active", ""),)),
                          (("build", "shipped", "Shipped in release 5"), (), (("t-p2", "review", self.live(5)),))))

    def test_an_item_put_back_after_its_drop_from_a_release_ships_as_new_work(self):
        # Release 24 (items 3 and 4 from Q) loses both to drops; item 3 is put back and built by P2 in t-p2, and release
        # 24's record names it. The drop was taken back before the record, so nothing is overtaken: P2 has shipped, the
        # emptied release stays dropped and is not shipped as release 24, and Q's topic stays open.
        release = self.release_24_from_its_own_topic()
        self.drop(3)
        self.drop(4)
        version = self.sql("SELECT version FROM pipeline_items WHERE workspace_id='w1' AND number=3")[0][0]
        self.committed(self.command("pipeline.item_move", {"section": "now"}, {"workspace_id": "w1", "number": 3}, {"item_revision": version}, actor="owner"))
        again = self.committed(self.start_of([3], "t-p2"))["piece_id"]
        self.assertTrue(self.record(24, "a" * 40, [3])["recorded"])
        self.assertEqual((self.end_of(release, "t-24", "t-q"), self.end_of(again, "t-p2")),
                         ((("combine", "dropped", ""), (), (("t-24", "active", ""), ("t-q", "active", ""))),
                          (("build", "shipped", "Shipped in release 24"), (), (("t-p2", "review", self.live(24)),))))

    def unless_shipped(self, name, receipt):
        """An act a record makes impossible: applied before the record, refused after it (ITEM_SHIPPED)."""
        if receipt.get("status") == "committed":
            self.applied.add(name)
        else:
            self.rejected(receipt, "ITEM_SHIPPED")
            self.overtaken.add(name)

    def put_back_unless_shipped(self, number):
        version = self.sql("SELECT version FROM pipeline_items WHERE workspace_id='w1' AND number=?", (number,))[0][0]
        self.unless_shipped("put back %d" % number, self.command("pipeline.item_move", {"section": "now"}, {"workspace_id": "w1", "number": number},
                                                                  {"item_revision": version}, actor="owner"))

    def test_a_release_shipped_by_its_own_record_never_takes_back_an_item_another_record_shipped(self):
        # Review T1: release 24 holds items 3 and 4; item 4 is dropped and release 5, recorded late, names it, then release
        # 24's record names item 3. Item 4 shipped in release 5, so release 24 holds item 3 alone, whichever of the drop and
        # release 5 came first. The record of release 24 used to put item 4 back because some record named it.
        release = self.release_24_from_its_own_topic()
        facts = {"drop 4": lambda: self.drop_unless_shipped(4), "release 5 names 4": lambda: self.backfill(5, [4]),
                 "release 24 names 3": lambda: self.assertTrue(self.record(24, "a" * 40, [3])["recorded"])}
        shipped = "Shipped in release 24"
        self.assert_one_end_overtaken(facts, lambda: (self.end_of(release, "t-24", "t-q"), self.item_shipped(4)),
                                      ((("combine", "shipped", shipped), (3,), (("t-24", "review", self.live(24)), ("t-q", "review", self.live(24)))), (5, "shipped", False)),
                                      orders=[("drop 4", "release 5 names 4", "release 24 names 3"), ("release 5 names 4", "drop 4", "release 24 names 3")])

    def test_a_release_never_takes_back_an_item_a_finished_piece_keeps(self):
        # Release 23 (items 1 and 2), held by a failed check, sends both back to building in t-fix and ends. Releases are
        # turned off, the piece in t-fix finishes (a finished piece keeps its items), and releases are turned on again before
        # release 23's record names them. The finished piece keeps them, so release 23 is not shipped holding them: the
        # record is written, the emptied release says they shipped in release 23, and t1, whose work they were, is ready for review.
        self.recorded("d" * 40, units="failed")
        self.topic_of_w1("t-fix")
        piece = self.committed(self.command("pipeline.piece_start", {"numbers": [1, 2], "topic_id": "t-fix", "stage_id": "build", "from_release": True},
                                            {"workspace_id": "w1"}))["piece_id"]
        self.committed(self.on(ships_in_releases=False))
        self.committed(self.command("pipeline.piece_finish", {}, {"piece_id": piece}))
        self.committed(self.on(ships_in_releases=True))
        self.assertTrue(self.record(23, SHA, [1, 2])["recorded"])
        self.assertEqual((self.end_of(self.release, "t1"), self.members(piece)),
                         ((("checks", "held", "Its last items shipped in release 23"), (), (("t1", "review", self.live(23)),)), [(1, None), (2, None)]))

    def test_a_record_naming_none_of_an_emptied_releases_items_never_ships_it(self):
        # Review T2: release 24 (items 3 and 4) loses both to drops, release 5, recorded late, names item 3, and release 24
        # is then recorded as a hotfix naming neither. Only a record naming one of its items ships an emptied release, so it
        # stays held, saying where its items went, whichever of the drop of 3 and release 5 came first.
        release = self.release_24_from_its_own_topic()
        facts = {"drop 3": lambda: self.drop_unless_shipped(3), "drop 4": lambda: self.drop_unless_shipped(4),
                 "release 5 names 3": lambda: self.backfill(5, [3]), "release 24 names none": lambda: self.assertTrue(self.record(24, "a" * 40, [])["recorded"])}
        self.assert_one_end_overtaken(facts, lambda: (self.end_of(release, "t-24", "t-q"), self.live_view()["release_piece"], self.live_view()["candidate_release"]),
                                      ((("combine", "held", "Its items shipped in release 5 or were dropped"), (),
                                        (("t-24", "active", ""), ("t-q", "review", self.live(5)))), None, 25),
                                      orders=[("drop 3", "drop 4", "release 5 names 3", "release 24 names none"),
                                              ("release 5 names 3", "drop 3", "drop 4", "release 24 names none")])

    def test_the_record_of_a_release_whose_items_went_back_to_building_ships_that_release_holding_them(self):
        # Review T3: release 23 (items 1 and 2) is held by a failed check; both go back to building in t-fix, and release
        # 23's record (a went-live or kept record written late) names them. A return is refused after the record as a drop
        # is, so the record wins over it too: release 23 ships holding items 1 and 2, and t1 is ready for review, in either order.
        self.recorded("d" * 40, units="failed")
        self.applied, self.overtaken = set(), set()
        self.topic_of_w1("t-fix")
        facts = {"return 1 and 2": lambda: self.unless_shipped("return", self.command(
                     "pipeline.piece_start", {"numbers": [1, 2], "topic_id": "t-fix", "stage_id": "build", "from_release": True}, {"workspace_id": "w1"})),
                 "release 23 names 1 and 2": lambda: self.assertTrue(self.record(23, SHA, [1, 2])["recorded"])}
        shipped = "Shipped in release 23"
        states = self.in_every_order(facts, lambda: (self.end_of(self.release, "t1"), self.item_shipped(1), self.item_shipped(2),
                                                     self.live_view()["release_piece"], self.live_view()["candidate_release"]))
        self.assertEqual(list(states), [((("checks", "shipped", shipped), (1, 2), (("t1", "review", self.live(23)),)), (23, "shipped", False),
                                         (23, "shipped", False), None, 24)], states)
        self.assertEqual((self.applied, self.overtaken), ({"return"}, {"return"}))

    def test_an_item_dropped_and_put_back_but_never_built_again_ships_from_the_piece_it_was_dropped_from(self):
        # Review T4: piece P builds item 3 alone in t-p; item 3 is dropped (ending P) and put back in Now, as an Undo does,
        # and nothing builds it again before release 5, recorded late, names it. No piece took it since the drop, so the
        # record overtakes the drop: P has shipped and t-p is ready for review, as when the record came first.
        piece = self.committed(self.start_of([self.add("Item 3", section="now", size="small")], "t-p"))["piece_id"]
        self.ship_naming([1, 2])
        self.assert_one_end_overtaken({"drop 3": lambda: self.drop_unless_shipped(3), "put back 3": lambda: self.put_back_unless_shipped(3),
                                       "release 5 names 3": lambda: self.backfill(5, [3])},
                                      lambda: (self.end_of(piece, "t-p"), self.item_shipped(3)),
                                      ((("build", "shipped", "Shipped in release 5"), (), (("t-p", "review", self.live(5)),)), (5, "shipped", False)),
                                      orders=[("drop 3", "put back 3", "release 5 names 3"), ("release 5 names 3", "drop 3", "put back 3")])

    def test_a_topic_whose_only_piece_was_dropped_stays_open_whatever_ships_after(self):
        # A piece none of whose items shipped has not shipped: its topic is no one's to close (review follow-up M16).
        piece = self.committed(self.start_of([self.add("Item 3", section="now", size="small")], "t-d"))["piece_id"]
        self.drop(3)
        self.ship_naming([1, 2])
        self.assertEqual(self.end_of(piece, "t-d"), (("build", "dropped", ""), (), (("t-d", "active", ""),)))

    def test_a_topic_holding_the_open_release_stays_open_when_its_piece_of_work_ships(self):
        # P builds item 3 in t-p and release 24 is put together there too; release 5, recorded late, ships item 3, so P has
        # shipped, but t-p holds a release still under way (review follow-up M17).
        self.ship_naming([1, 2])
        piece = self.committed(self.start_of([self.add("Item 3", section="now", size="small")], "t-p"))["piece_id"]
        release = self.committed(self.combine([self.approved([self.add("Item 4", section="now", size="small")], self.topic_of_w1("t-q"))],
                                              topic_id="t-p"))["piece_id"]
        self.backfill(5, [3])
        self.assertEqual((self.end_of(piece, "t-p"), self.open_entry(release)),
                         ((("build", "shipped", "Shipped in release 5"), (), (("t-p", "active", ""),)), [("combine",)]))

    def test_a_persons_reopen_or_done_of_a_topic_the_pipeline_closed_stands_against_a_later_record(self):
        # P builds items 3 and 4 in t-p, Q builds 5 and 6 in t-q; 3 and 5 are dropped. Release 5, recorded late, names 4 and
        # 6, so both pieces have shipped and the pipeline holds both topics shipped, ready for review. The owner reopens t-p and says t-q is done
        # in their own words. Release 6, recorded late, names 3 and 5: the drops count for nothing, both pieces shipped in
        # release 6 and the pipeline holds both topics with it, yet each topic keeps the person's word (docs/pipeline.md,
        # "A topic the pipeline holds shipped"; review follow-up 1).
        self.ship_naming([1, 2])
        p = self.committed(self.start_of([self.add("Item %d" % n, section="now", size="small") for n in (3, 4)], "t-p"))["piece_id"]
        q = self.committed(self.start_of([self.add("Item %d" % n, section="now", size="small") for n in (5, 6)], "t-q"))["piece_id"]
        self.drop(3)
        self.drop(5)
        self.backfill(5, [4, 6])
        self.assertEqual((self.end_of(p, "t-p"), self.end_of(q, "t-q")),
                         ((("build", "shipped", "Shipped in release 5"), (), (("t-p", "review", self.live(5)),)),
                          (("build", "shipped", "Shipped in release 5"), (), (("t-q", "review", self.live(5)),))))
        for topic, args in (("t-p", {"state": "active"}), ("t-q", {"state": "done", "note": "Wrapped up"})):
            version = self.store.view("owner", "topic", {"topic_id": topic})["topic"]["version"]
            self.committed(self.command("topic.update", args, {"topic_id": topic}, {"topic_revision": version}, actor="owner"))
        self.backfill(6, [3, 5])
        self.assertEqual((self.entries(p)[-1], self.entries(q)[-1]),
                         (("build", "shipped", "Shipped in release 6"), ("build", "shipped", "Shipped in release 6")))
        self.assertEqual(dict(self.sql("SELECT topic_id,release FROM pipeline_done_topics WHERE workspace_id='w1'")), {"t1": 23, "t-p": 6, "t-q": 6})
        self.assertEqual(self.sql("SELECT id,state,state_note,state_by FROM topics WHERE id IN ('t-p','t-q') ORDER BY id"),
                         [("t-p", "active", "", "owner"), ("t-q", "done", "Wrapped up", "owner")])

    # ---- what is stored is the state derived from the facts, after every write (docs/pipeline.md, "The derived state") --

    # Each kind of write, and how often a random sequence tries it. Outcome w2 keeps a pipeline too, so every record is a
    # fact of both, and it takes the records as its own while it ships in releases, which only one outcome does at a time
    # (RELEASES_TAKEN); "ask" and "release move" enter stages that wait on the owner's word, and the acts that leave
    # them, or would end a piece there, are made by the owner or the coordinator at random.
    WRITES = {"record": 2, "drop": 3, "put back": 3, "move": 2, "undo": 2, "add": 3, "start": 4, "approve": 5, "ask": 6, "combine": 6,
              "release move": 4, "checks": 2, "return": 5, "finish": 6, "releases on": 1, "releases off": 2, "topic done": 1,
              "topic reopened": 2, "topic message": 1, "w2 start": 2, "w2 drop": 1, "w2 finish": 2, "w2 releases on": 3, "w2 releases off": 1}
    # Who acts where the owner's word may be needed.
    ACTORS = ("owner", "agent-a")

    def assert_stored_is_derived(self, said, workspace="w1"):
        """The member rows, each piece's end, the topics the pipeline holds and their states, read from the store, are
        pipeline_state.derive() of the outcome's facts computed again from nothing; and every stage that waits on the
        owner's word was left by the owner or by a record."""
        with closing(sqlite3.connect(str(self.path))) as db:
            db.row_factory = sqlite3.Row
            state = pipeline_state.derive(pipeline_state.read_facts(db, workspace))
            members = sorted(tuple(row) for row in db.execute("SELECT piece_id,number,left_ms,left_reason FROM pipeline_piece_members WHERE workspace_id=?",
                                                              (workspace,)))
            self.assertEqual(members, sorted(state.members), said)
            for piece, end in state.ends.items():
                last = db.execute("SELECT left_outcome,left_note,left_ms,left_by FROM pipeline_stage_entries WHERE piece_id=? ORDER BY seq DESC LIMIT 1",
                                  (piece,)).fetchone()
                self.assertEqual(tuple(last) if end is not None else last["left_ms"], end, "%s: the end of %s" % (said, piece))
            self.assertEqual(dict(db.execute("SELECT topic_id,release FROM pipeline_done_topics WHERE workspace_id=?", (workspace,)).fetchall()),
                             state.done_topics, said)
            # Read from the log itself, not derive(): a release ships only by a record written after it was combined.
            combined = {json.loads(row[1])["release"]: row[0] for row in db.execute("SELECT seq,payload FROM pipeline_facts WHERE workspace_id=? AND kind='combine'",
                                                                                    (workspace,))}
            recorded = {json.loads(row[1])["release"]: row[0] for row in db.execute("SELECT seq,payload FROM pipeline_facts WHERE workspace_id=? AND kind='record'",
                                                                                    (workspace,))}
            for piece, note in db.execute("SELECT p.id,e.left_note FROM pipeline_pieces p JOIN pipeline_stage_entries e ON e.piece_id=p.id "
                                          "WHERE p.workspace_id=? AND p.kind='release' AND e.left_outcome='shipped'", (workspace,)):
                number = int(note.rsplit(" ", 1)[1])
                if piece in combined:
                    self.assertLess(combined[piece], recorded[number], "%s: %s shipped in release %d, recorded before it was combined" % (said, piece, number))
            # A stage that waits on the owner's word was left by the owner, or by nobody at a record or at turning releases on (D24).
            for stage, piece, left_ms, left_by in db.execute(
                    "SELECT e.stage_id,e.piece_id,e.left_ms,e.left_by FROM pipeline_stage_entries e JOIN pipeline_stages s ON s.workspace_id=e.workspace_id "
                    "AND s.stage_id=e.stage_id WHERE e.workspace_id=? AND e.left_ms IS NOT NULL AND json_extract(s.gate,'$.owner')", (workspace,)):
                by_a_record = left_by is None and db.execute("SELECT 1 FROM pipeline_facts WHERE workspace_id=? AND ms=? AND (kind='record' OR "
                                                             "(kind='ships' AND json_extract(payload,'$.on')))", (workspace, left_ms)).fetchone()
                self.assertTrue(left_by == "owner" or by_a_record, "%s: %s left %s by %s" % (said, piece, stage, left_by))
            for topic in db.execute("SELECT t.* FROM topics t WHERE t.id IN (SELECT topic_id FROM pipeline_pieces WHERE workspace_id=?)", (workspace,)):
                nobodys = topic["state_by"] is None and topic["state_message_id"] is None
                # The pipeline never says a topic is Done: only a person does.
                self.assertFalse(nobodys and topic["state"] == "done", "%s: %s is Done by nobody" % (said, topic["id"]))
                if topic["id"] in state.done_topics and nobodys:
                    # Nobody has said anything since the pipeline did: it is Ready for review, live since the release that
                    # shipped the last of it.
                    self.assertEqual((topic["state"], topic["state_note"]), ("review", self.live(state.done_topics[topic["id"]], db.execute(
                        "SELECT deployed_at FROM releases WHERE number=?", (state.done_topics[topic["id"]],)).fetchone()[0])), said)
                elif topic["id"] not in state.done_topics:
                    self.assertFalse(nobodys and topic["state"] == "review", "%s: %s is held for review by nobody with work left in it" % (said, topic["id"]))

    def words(self):
        """Every topic's (state, note, who said it, the message that reopened it), and the topics the pipeline holds."""
        return ({row[0]: tuple(row[1:]) for row in self.sql("SELECT id,state,state_note,state_by,state_message_id FROM topics")},
                {row[0] for row in self.sql("SELECT topic_id FROM pipeline_done_topics")})

    def assert_a_persons_word_stands(self, said, before, after):
        """A person's word on a topic (docs/pipeline.md, "A topic the pipeline holds shipped"): what they said is what the topic
        says, and nothing but another person's word changes it afterwards. A Done stands against every write; a reopen
        stands while the pipeline goes on holding the topic (a record naming more of its work, review follow-up 1), lets
        it go, or never held it, and gives way only when the pipeline starts to hold the topic again."""
        (was, held_before), (now, held_after) = before, after
        for topic, expected in self.said.items():
            self.assertEqual(now[topic], expected, "%s: %s does not say what was said" % (said, topic))
        for topic, words in was.items():
            if topic in self.said or (words[2] is None and words[3] is None):
                continue
            if words[0] == "done" or topic in held_before or topic not in held_after:
                self.assertEqual(now[topic], words, "%s: %s lost a person's word" % (said, topic))

    def write_at_random(self, rng, kind, topics, w2_topics):
        """One write of this kind with arguments chosen by rng; whether it was taken (a refusal is a write not taken).
        What a person said of a topic in it, and so what the topic must say afterwards, goes in self.said."""
        self.said = {}
        items = self.sql("SELECT number,section,version FROM pipeline_items WHERE workspace_id='w1' ORDER BY number")
        theirs = self.sql("SELECT number,section,version FROM pipeline_items WHERE workspace_id='w2' ORDER BY number")
        named = {n for (row,) in self.sql("SELECT items FROM releases") for n in json.loads(row)}
        pieces = self.sql("SELECT p.id,p.kind,e.stage_id,p.topic_id FROM pipeline_pieces p JOIN pipeline_stage_entries e ON e.piece_id=p.id AND e.left_ms IS NULL "
                          "WHERE p.workspace_id=? ORDER BY p.rowid", ("w2" if kind.startswith("w2 ") else "w1",))

        def taken(receipt):
            return receipt.get("status") == "committed"

        def move(number, version, args, workspace="w1", actor=None):
            return taken(self.command("pipeline.item_move", dict(args, underway_confirmed=True), {"workspace_id": workspace, "number": number},
                                      {"item_revision": version}, actor=actor or rng.choice(self.ACTORS)))

        def message(topic, ask):
            """agent-a writes in topic (an ask to the owner when ask); a done topic is reopened by it, said by agent-a."""
            before = self.sql("SELECT state,state_note,state_by,state_message_id FROM topics WHERE id=?", (topic,))[0]
            args = {"body": "@owner would you look?", "addressees": ["owner"], "intent": "ask"} if ask else {"body": "Still on it"}
            receipt = self.command("message.post", args, {"topic_id": topic}, actor="agent-a")
            if not taken(receipt):
                return None
            sent = receipt["result"]["message_id"]
            self.said[topic] = ("active", "Reopened by a new message", "agent-a", sent) if before[0] == "done" else tuple(before)
            return sent

        def enter(piece, stage, left_outcome, actor):
            args = {"stage_id": stage, "left_outcome": left_outcome}
            if stage in ("sign", "approve"):
                ask = message(piece[3], True)
                if ask is None:
                    return False
                args["ask_message_id"] = ask
            return taken(self.command("pipeline.stage_enter", args, {"piece_id": piece[0]}, actor=actor))
        if kind == "record":
            written = [row[0] for row in self.sql("SELECT number FROM releases")]
            draw = rng.random()
            # The next number, a gap past it, or an older one recorded late.
            number = (max(written) + 1 if draw < 0.5 else max(written) + rng.randint(2, 3) if draw < 0.65 else
                      rng.choice([n for n in range(1, 22) if n not in written] or [max(written) + 1]))
            free = sorted({row[0] for row in items + theirs if row[0] not in named})
            chosen = rng.sample(free, min(len(free), rng.randint(0, 3)))
            # Often one dropped item too: a record that overtakes a drop is where a closed piece and its topic change.
            dropped = [row[0] for row in items if row[1] == "dropped" and row[0] not in named and row[0] not in chosen]
            if dropped and rng.random() < 0.5:
                chosen.append(rng.choice(dropped))
            try:
                return self.record(number, "%040x" % rng.getrandbits(160), chosen, source="backfill")["recorded"]
            except Refusal:
                return False
        if kind in ("drop", "put back", "w2 drop"):
            rows = [row for row in (theirs if kind == "w2 drop" else items) if (row[1] == "dropped") == (kind == "put back")]
            # A drop mostly takes an item from a piece, which a later record can overtake.
            building = {row[0] for row in self.sql("SELECT number FROM pipeline_piece_members WHERE workspace_id=? AND left_ms IS NULL",
                                                   ("w2" if kind == "w2 drop" else "w1",))}
            rows = [row for row in rows if row[0] in building] if kind != "put back" and building & {row[0] for row in rows} and rng.random() < 0.7 else rows
            if not rows:
                return False
            row = rng.choice(rows)
            return move(row[0], row[2], {"section": "dropped"} if kind != "put back" else {"section": rng.choice(("now", "now", "next"))},
                        workspace="w2" if kind == "w2 drop" else "w1", actor="owner" if kind == "w2 drop" else None)
        if kind == "move":
            row, other = rng.choice(items), rng.choice(items)[0]
            return move(row[0], row[2], rng.choice(({"section": rng.choice(("now", "next", "later"))}, {"below": other}, {"above": other})))
        if kind == "undo":
            # The newest placement of an item, when it was a move.
            moved = self.sql("SELECT e.number,e.seq,i.version FROM pipeline_events e JOIN pipeline_items i ON i.workspace_id=e.workspace_id AND i.number=e.number "
                             "WHERE e.workspace_id='w1' AND e.kind='moved' AND e.seq=(SELECT max(seq) FROM pipeline_events WHERE workspace_id='w1' "
                             "AND number=e.number AND kind IN ('added','moved'))")
            if not moved:
                return False
            number, seq, version = rng.choice(moved)
            return move(number, version, {"undo_of": seq})
        if kind == "add":
            return bool(self.add("Item added later", section=rng.choice(("now", "now", "next")), size="small", actor=rng.choice(self.ACTORS)))
        if kind in ("start", "return", "w2 start"):
            if kind == "return":
                release = next((piece for piece in pieces if piece[1] == "release"), None)
                ready = [row[0] for row in self.sql("SELECT number FROM pipeline_piece_members WHERE piece_id=? AND left_ms IS NULL", (release[0],))] if release else []
                args = {"from_release": True}
            else:
                # Mostly items no piece holds, so work keeps flowing towards the owner's word; now and then any item in Now.
                held = {row[0] for row in self.sql("SELECT number FROM pipeline_piece_members WHERE workspace_id=? AND left_ms IS NULL",
                                                   ("w2" if kind == "w2 start" else "w1",))} if rng.random() < 0.8 else set()
                ready = [row[0] for row in (theirs if kind == "w2 start" else items) if row[1] == "now" and row[0] not in held]
                args = {}
            if not ready:
                return False
            return taken(self.command("pipeline.piece_start", dict({"numbers": rng.sample(ready, min(len(ready), rng.choice((1, 2, 2, 3)))),
                                                                    "topic_id": rng.choice(w2_topics if kind == "w2 start" else topics), "stage_id": "build"}, **args),
                                      {"workspace_id": "w2" if kind == "w2 start" else "w1"}, actor=rng.choice(self.ACTORS)))
        if kind == "approve":
            building = [piece for piece in pieces if piece[2] == "build"]
            return bool(building) and enter(rng.choice(building), "wait", "approved", "agent-a")
        if kind == "ask":
            # Into the last stage a piece goes through, which waits on the owner's word: only the owner leaves it.
            waiting = [piece for piece in pieces if piece[2] == "wait"]
            return bool(waiting) and enter(rng.choice(waiting), "sign", "approved", "agent-a")
        if kind == "combine":
            signed = [piece[0] for piece in pieces if piece[2] == "sign"]
            return bool(signed) and taken(self.command("pipeline.combine", {"piece_ids": rng.sample(signed, rng.randint(1, len(signed))),
                                                                            "topic_id": rng.choice(topics)}, {"workspace_id": "w1"},
                                                       actor=rng.choice(("owner", "owner", "owner", "agent-a"))))
        if kind == "release move":
            release = next((piece for piece in pieces if piece[1] == "release"), None)
            following = {"combine": "checks", "checks": "approve", "approve": "deploy"}
            if release is None or release[2] not in following:
                return False
            return enter(release, following[release[2]], "passed", "agent-a" if following[release[2]] == "approve" else rng.choice(self.ACTORS))
        if kind == "checks":
            try:
                return bool(self.recorded(rng.choice((SHA, "d" * 40)), units=rng.choice(("failed", "passed")),
                                          at="2026-11-07T05:%02d:%02d-08:00" % (rng.randrange(60), rng.randrange(60))))
            except Refusal:
                return False
        if kind in ("finish", "w2 finish"):
            open_work = [piece[0] for piece in pieces if piece[1] == "piece"]
            return bool(open_work) and taken(self.command("pipeline.piece_finish", {}, {"piece_id": rng.choice(open_work)},
                                                          actor="owner" if kind == "w2 finish" else rng.choice(self.ACTORS)))
        if kind in ("releases on", "releases off"):
            return taken(self.on(ships_in_releases=kind == "releases on"))
        if kind in ("w2 releases on", "w2 releases off"):
            version = self.store.view("owner", "pipeline", {"workspace_id": "w2", "limit": 1})["pipeline"]["version"]
            return taken(self.command("pipeline.configure", {"enabled": True, "ships_in_releases": kind == "w2 releases on"}, {"workspace_id": "w2"},
                                      {"pipeline_revision": version}))
        # Mostly a topic the pipeline holds shipped, where a person's word meets the pipeline's.
        held = [row[0] for row in self.sql("SELECT topic_id FROM pipeline_done_topics ORDER BY topic_id")]
        topic = rng.choice(held if held and rng.random() < 0.7 else topics + w2_topics)
        if kind == "topic message":
            return message(topic, False) is not None
        version = self.store.view("owner", "topic", {"topic_id": topic})["topic"]["version"]
        args = {"state": "done", "note": "Wrapped up"} if kind == "topic done" else {"state": "active"}
        if not taken(self.command("topic.update", args, target={"topic_id": topic}, expected={"topic_revision": version})):
            return False
        self.said[topic] = ("done", "Wrapped up", "owner", None) if kind == "topic done" else ("active", "", "owner", None)
        return True

    def test_after_every_write_what_is_stored_is_the_state_derived_from_the_facts(self):
        # A seeded property check: 60 sequences of 60 writes, each write of any kind with arguments chosen at random, from
        # release 23 (items 1 and 2) in checks with items 3 to 8 in Now, under stages where a piece's last stage and the
        # release's approval wait on the owner's word, beside outcome w2, which keeps a pipeline of its own items 1 to 8
        # in Now and ships in releases whenever w1 does not. After every write, taken or refused, the store holds exactly
        # the state derived again from each outcome's facts, and every topic keeps the word a person last said of it.
        self.with_stages_on_the_owners_word()
        conversation = self.store.view("owner", "conversation", {"workspace_id": "w1"})["conversation"]["topic_id"]
        topics = ["t1", conversation] + [self.topic_of_w1(topic) for topic in ("t-a", "t-b", "t-c", "t-d", "t-e", "t-f")]
        for n in range(3, 9):
            # agent-a placed the odd ones, so it can move and drop them; the owner placed the others.
            self.add("Item %d" % n, section="now", size="small", actor="agent-a" if n % 2 else "owner")
        self.committed(self.command("workspace.create", {"id": "w2", "title": "Another", "purpose": "", "team": ["agent-a"]}))
        self.committed(self.command("pipeline.configure", {"enabled": True}, {"workspace_id": "w2"}, {"pipeline_revision": 0}))
        version = self.store.view("owner", "pipeline", {"workspace_id": "w2", "limit": 1})["pipeline"]["version"]
        self.committed(self.command("pipeline.stages_set", {"stages": [{"stage_id": "build", "name": "Build"}]}, {"workspace_id": "w2"},
                                    {"pipeline_revision": version}))
        theirs = [self.committed(self.command("pipeline.item_add", {"title": "Theirs %d" % n, "section": "now", "size": "small", "sources": [{"note": "a test"}]},
                                              {"workspace_id": "w2"}))["number"] for n in range(1, 9)]
        self.assertEqual(theirs, list(range(1, 9)), "w2 numbers its items as w1 does, so a record names both outcomes' items")
        w2_topics = [self.topic_of_w1(topic, workspace="w2") for topic in ("t-w2a", "t-w2b")]
        start = Path(self.temp.name) / "before-the-writes.sqlite3"
        with closing(sqlite3.connect(str(self.path))) as db, closing(sqlite3.connect(str(start))) as copy:
            db.backup(copy)
        taken = dict.fromkeys(self.WRITES, 0)
        kinds = [kind for kind, weight in self.WRITES.items() for _ in range(weight)]
        for seed in range(60):
            with closing(sqlite3.connect(str(start))) as copy, closing(sqlite3.connect(str(self.path))) as db:
                copy.backup(db)
            rng = random.Random(seed)
            done = []
            for step in range(60):
                kind = rng.choice(kinds)
                before = self.words()
                if self.write_at_random(rng, kind, topics, w2_topics):
                    taken[kind] += 1
                    done.append(kind)
                said = "seed %d, write %d (%s), after %s" % (seed, step, kind, " > ".join(done))
                for workspace in ("w1", "w2"):
                    self.assert_stored_is_derived(said, workspace)
                self.assert_a_persons_word_stands(said, before, self.words())
        # Combining needs a piece through three stages and the owner's word while no release is open, so it is the rarest.
        self.assertEqual([kind for kind, count in taken.items() if count < (10 if kind == "combine" else 20)], [],
                         "every kind of write was taken: %r" % taken)

    def test_release_pieces_release_23_numbered_ship_by_the_next_release_one_at_a_time(self):
        # Release 23's combine stored a number, and could combine release 24 while 23 was open. Upgraded, the earlier is the
        # release; the record of 23 ships it and release 24's piece becomes the release, which the record of 24 ships.
        waiting = self.approved([self.add("Item 3", section="now", size="small")], self.topic_of_w1("t-24"))
        ms = self.sql("SELECT max(entered_ms) FROM pipeline_stage_entries")[0][0] + 1
        with sqlite3.connect(str(self.path)) as db:
            db.execute("UPDATE pipeline_pieces SET release_number=23 WHERE id=?", (self.release,))
            db.execute("UPDATE pipeline_stage_entries SET left_ms=?,left_outcome='combined',left_by='owner' WHERE piece_id=? AND left_ms IS NULL",
                       (ms, waiting))
            db.execute("UPDATE pipeline_piece_members SET left_ms=?,left_reason='combined' WHERE piece_id=?", (ms, waiting))
            db.execute("INSERT INTO pipeline_pieces VALUES ('p-release-24','w1','release','t-24',24,NULL,1)")
            db.execute("INSERT INTO pipeline_piece_members VALUES ('p-release-24','w1',3,NULL,NULL)")
            db.execute("INSERT INTO pipeline_stage_entries(piece_id,workspace_id,stage_id,round,entered_ms,entered_by) "
                       "VALUES ('p-release-24','w1','combine',1,?,'owner')", (ms,))
            # The combine as the upgrade to version 30 writes it into the outcome's facts.
            db.execute("INSERT INTO pipeline_facts(workspace_id,kind,ms,by,payload) VALUES ('w1','combine',?,'owner',?)",
                       (ms, json.dumps({"release": "p-release-24", "pieces": [waiting], "numbers": [3]})))
        self.assertEqual(self.live_view()["release_piece"], {"id": self.release, "release": 23, "stage": "checks", "round": 1})
        self.assertTrue(self.record(23, "d" * 40, [1, 2])["recorded"])
        self.assertEqual((self.entries(self.release)[-1], self.open_entry("p-release-24")),
                         (("checks", "shipped", "Shipped in release 23"), [("combine",)]))
        self.assertEqual(self.live_view()["release_piece"], {"id": "p-release-24", "release": 24, "stage": "combine", "round": 1})
        self.assertTrue(self.record(24, "e" * 40, [])["recorded"])
        self.assertEqual((self.entries("p-release-24")[-1], self.members("p-release-24"), self.live_view()["release_piece"]),
                         (("combine", "shipped", "Shipped in release 24"), [(3, "returned")], None))

    def test_a_release_combined_at_a_commit_already_recorded_is_still_being_put_together(self):
        # A record made before the release existed never ends it: release 23's own commit, given again at combine.
        self.ship_naming([1, 2])
        later = self.committed(self.combine([self.approved([self.add("Item 3", section="now", size="small")], self.topic_of_w1("t-c"))],
                                            commit_sha=SHA))
        self.assertEqual(self.live_view()["release_piece"], {"id": later["piece_id"], "release": 24, "stage": "combine", "round": 1})
        self.assertEqual(self.store.view("owner", "pipeline", {"workspace_id": "w1"})["facts"]["next_release"]["items"], [3])

    def combined_after_a_record_is_still_being_put_together(self, pieces, number, numbers):
        """The release combined after a record of the next number, written while none was open, is release number,
        holding these items, still in combine; its own record then ships it."""
        later = self.committed(self.combine(pieces))
        self.assertEqual((later["release_number"], self.live_view()["release_piece"]),
                         (number, {"id": later["piece_id"], "release": number, "stage": "combine", "round": 1}))
        self.assertEqual((self.open_entry(later["piece_id"]), self.members(later["piece_id"])), ([("combine",)], [(n, None) for n in numbers]))
        self.assertEqual(self.store.view("owner", "pipeline", {"workspace_id": "w1"})["facts"]["next_release"]["items"], list(numbers))
        self.assertTrue(self.record(number, "f" * 40, numbers)["recorded"])
        self.assertEqual((self.entries(later["piece_id"])[-1], self.live_view()["release_piece"]),
                         (("combine", "shipped", "Shipped in release %d" % number), None))

    def test_a_record_written_while_no_release_is_open_ships_no_release_combined_after_it(self):
        # A hotfix, or a deploy's record of the commit already live, written after release 23 shipped and before anything
        # was combined: the release combined next is release 25 and is being put together, never shipped by that record.
        self.ship_naming([1, 2])
        self.assertTrue(self.record(24, "1234" * 10, [], source="deploy")["recorded"])
        self.combined_after_a_record_is_still_being_put_together(
            [self.approved([self.add("Item 3", section="now", size="small")], self.topic_of_w1("t-q"))], 25, [3])

    def test_a_record_written_after_the_release_emptied_ships_no_release_combined_after_it(self):
        # Release 23's items all went back to building; release 23 is recorded naming none of them, so the emptied release
        # finds nothing to ship, and the rebuilt work combined after it is release 24, being put together.
        self.recorded("d" * 40, units="failed")
        rebuilt = self.approved((1, 2), self.topic_of_w1("t-fix"), from_release=True)
        self.assertEqual(self.live_view()["release_piece"], None)
        self.assertTrue(self.record(23, "e" * 40, [], source="deploy")["recorded"])
        self.combined_after_a_record_is_still_being_put_together([rebuilt], 24, [1, 2])

    def with_stages_on_the_owners_word(self):
        """Stage sign (a piece's) and stage approve (a release's) wait on the owner's word (D24)."""
        self.stages({"stage_id": "build", "name": "Build"}, {"stage_id": "wait", "name": "Wait"},
                    {"stage_id": "sign", "name": "Sign off", "gate": {"owner": True}},
                    {"stage_id": "combine", "name": "Combine", "scope": "release"},
                    {"stage_id": "checks", "name": "Browser checks", "scope": "release", "rounds": "together", "gate": {"checks": ["units", "browser-checks"]}},
                    {"stage_id": "approve", "name": "Approve", "scope": "release", "gate": {"owner": True}},
                    {"stage_id": "deploy", "name": "Deploy", "scope": "release"})

    def on_the_owners_word(self, piece, topic, stage, **args):
        """agent-a asks the owner in the piece's topic and moves it into stage, which only the owner leaves."""
        ask = self.committed(self.command("message.post", {"body": "@owner would you look?", "addressees": ["owner"], "intent": "ask"},
                                          {"topic_id": topic}, actor="agent-a"))["message_id"]
        self.committed(self.command("pipeline.stage_enter", dict({"stage_id": stage, "ask_message_id": ask}, **args), {"piece_id": piece}, actor="agent-a"))
        self.rejected(self.command("pipeline.stage_enter", {"stage_id": "deploy" if stage == "approve" else "wait", "left_outcome": "passed"},
                                   {"piece_id": piece}, actor="agent-a"), "STAGE_GATED")

    def last_left(self, piece):
        return self.sql("SELECT stage_id,left_outcome,left_note,left_by FROM pipeline_stage_entries WHERE piece_id=? ORDER BY seq DESC LIMIT 1", (piece,))[0]

    def test_a_record_ships_what_it_names_from_a_stage_on_the_owners_word(self):
        # What a record names has shipped wherever it is (docs/pipeline.md): the record leaves a stage only the owner
        # leaves, by nobody, while every act of a member is still refused there.
        self.with_stages_on_the_owners_word()
        piece = self.committed(self.start_of([self.add("Item 3", section="now", size="small")], "t-sign"))["piece_id"]
        self.on_the_owners_word(piece, "t-sign", "sign", left_outcome="approved")
        self.assertTrue(self.record(5, "e" * 40, [3], source="backfill", at="2026-10-01T09:00:00-07:00")["recorded"])
        self.assertEqual(self.last_left(piece), ("sign", "shipped", "Shipped in release 5", None))
        # Release 23 waits for the owner's approval; the deploy's record of release 23 ships it from there.
        self.recorded("d" * 40)
        self.on_the_owners_word(self.release, "t1", "approve", left_outcome="passed")
        self.assertTrue(self.record(23, "d" * 40, [1, 2])["recorded"])
        self.assertEqual(self.last_left(self.release), ("approve", "shipped", "Shipped in release 23", None))
        self.assertEqual((self.live_view()["release_piece"], self.live_view()["candidate_release"]), (None, 24))

    def test_turning_releases_on_ships_what_the_records_name_from_a_stage_on_the_owners_word(self):
        # Records written while releases were off become the outcome's when the owner turns them on, and what they name
        # leaves a stage that waits on the owner by nobody: it is the record's act (docs/pipeline.md, "The end of a piece").
        self.with_stages_on_the_owners_word()
        self.ship_naming([1, 2])
        self.committed(self.on(ships_in_releases=False))
        piece = self.committed(self.start_of([self.add("Item 3", section="now", size="small")], "t-sign"))["piece_id"]
        self.on_the_owners_word(piece, "t-sign", "sign", left_outcome="approved")
        self.assertTrue(self.record(24, "e" * 40, [3], source="backfill")["recorded"])
        self.assertEqual(self.open_entry(piece), [("sign",)], "out of releases, the record is not the outcome's yet")
        self.committed(self.on(ships_in_releases=True))
        self.assertEqual(self.last_left(piece), ("sign", "shipped", "Shipped in release 24", None))

    def test_an_item_that_shipped_stays_in_its_release_and_is_never_built_again(self):
        # Release 23 ships items 1 and 2. Item 1 is still in Now (shipped is never stored on the item).
        self.ship_naming([1, 2])
        three = self.add("Item 3", section="now", size="small")
        self.rejected(self.command("pipeline.piece_start", {"numbers": [1, three], "topic_id": self.topic_of_w1("t-b"), "stage_id": "build"},
                                   {"workspace_id": "w1"}, actor="agent-a"), "ITEM_SHIPPED")
        self.assertEqual(self.members(self.release), [(1, None), (2, None)], "what shipped in release 23 stays in it")
        shipped = self.store.view("owner", "pipeline", {"workspace_id": "w1", "section": "shipped"})["groups"]
        self.assertEqual([(group["label"], [item["number"] for item in group["items"]]) for group in shipped][:1], [("Release 23", [1, 2])])
        # Item 3 alone is release 24, and while it is open nothing else is combined.
        later = self.committed(self.combine([self.approved([three], "t-b")]))
        self.assertEqual((later["release_number"], self.live_view()["release_piece"]["id"]), (24, later["piece_id"]))
        self.rejected(self.combine([self.approved([self.add("Item 4", section="now", size="small")], self.topic_of_w1("t-b2"))]), "RELEASE_OPEN")

    def test_a_release_being_put_together_keeps_its_pipeline_on_and_shipping_in_releases(self):
        # Whether an item is in the release is a fact of the release, so the settings it depends on cannot change under it.
        refused = self.on(ships_in_releases=False)
        self.rejected(refused, "RELEASE_OPEN")
        self.assertIn("Release 23 is still being put together", refused["error"]["message"])
        self.rejected(self.on(actor="agent-a", enabled=False), "RELEASE_OPEN")
        # Dropping an item leaves it (D25), and a member cannot start a piece for an item still in it.
        version = self.sql("SELECT version FROM pipeline_items WHERE workspace_id='w1' AND number=2")[0][0]
        self.committed(self.command("pipeline.item_move", {"section": "dropped", "underway_confirmed": True}, {"workspace_id": "w1", "number": 2},
                                    {"item_revision": version}, actor="owner"))
        self.assertEqual(self.members(self.release), [(1, None), (2, "dropped")])
        self.rejected(self.command("pipeline.piece_start", {"numbers": [1], "topic_id": self.topic_of_w1("t-t"), "stage_id": "build"},
                                   {"workspace_id": "w1"}, actor="agent-b"), "ITEM_IN_PIECE")
        # Once it has shipped, the owner stops shipping in releases, and what it shipped stays as it was.
        self.recorded("d" * 40)
        self.assertTrue(self.store.record_release({"release": 23, "commit": "d" * 40, "deployed_at": "2026-11-07T09:00:00-08:00", "items": [1],
                                                   "source": "deploy"})["recorded"])
        self.committed(self.on(ships_in_releases=False))
        # Out of releases, item 1 no longer reads as shipped and can be dropped, but it stays in the release it shipped in.
        version = self.sql("SELECT version FROM pipeline_items WHERE workspace_id='w1' AND number=1")[0][0]
        self.committed(self.command("pipeline.item_move", {"section": "dropped"}, {"workspace_id": "w1", "number": 1}, {"item_revision": version},
                                    actor="owner"))
        self.committed(self.on(ships_in_releases=True))
        self.assertEqual((self.members(self.release), self.live_view()["release_piece"]), ([(1, None), (2, "dropped")], None))

    def test_the_gate_reads_the_release_at_its_own_commit(self):
        # D18: a check the newest candidate did not read is not passed by an older candidate's reading at another commit.
        self.recorded("d" * 40)
        self.store.record_checks({"outcome": "w1", "commit": "e" * 40, "started_at": "2026-11-07T05:00:00-08:00",
                                  "finished_at": "2026-11-07T05:00:00-08:00", "tools": {"release-check": release_check.TOOL}, "steps": [], "note": "",
                                  "readings": [{"check_id": "units", "result": "passed", "value": 900, "words": "900"}]}, "agent-b")
        self.rejected(self.move_on(), "STAGE_GATED")
        self.recorded("f" * 40, at="2026-11-07T06:00:00-08:00")
        self.committed(self.move_on())

    def test_a_worse_check_holds_the_release_and_only_its_items_go_back(self):
        waiting = self.approved([self.add("Item 3", section="now", size="small")], self.topic_of_w1("t-two"))
        self.write_summary(failed=("native-live",))
        self.assertEqual(self.deploy(), "Browser checks got worse: a passing check now fails")
        self.assertEqual(self.sql("SELECT note FROM pipeline_stage_entries WHERE left_ms IS NULL AND piece_id=?", (self.release,)),
                         [("Held: Browser checks got worse.",)])
        self.assertEqual(self.candidate()["label"], "Release 23 candidate")
        # D35: an item that is not in the release does not go back from it.
        self.rejected(self.command("pipeline.piece_start", {"numbers": [3], "topic_id": "t-two", "stage_id": "build", "from_release": True},
                                   {"workspace_id": "w1"}), "RELEASE_NOT_HELD")
        self.assertEqual(self.open_entry(waiting), [("wait",)])
        [(topic,)] = self.sql("SELECT id FROM topics WHERE kind='topic' AND id NOT IN ('t1','t-two')")
        self.committed(self.command("pipeline.piece_start", {"numbers": [1], "topic_id": topic, "stage_id": "build", "from_release": True},
                                    {"workspace_id": "w1"}))

    def test_another_outcome_that_ships_in_releases_puts_its_own_release_together(self):
        # Releases are an outcome's: once w1's release 23 has shipped and w1 stops shipping in releases, w2 combines its own
        # release 24, and neither outcome reads the other's. (Release records name bare item numbers, so w2's items 1 and 2
        # read as the ones release 23 shipped: docs/pipeline.md, "Not yet"; w2 builds its item 3.)
        self.ship_naming([1, 2])
        self.committed(self.on(ships_in_releases=False))
        self.committed(self.command("workspace.create", {"id": "w2", "title": "Another", "purpose": "", "team": ["agent-a", "agent-b"]}))
        self.committed(self.command("pipeline.configure", {"enabled": True, "ships_in_releases": True}, {"workspace_id": "w2"}, {"pipeline_revision": 0}))
        version = self.store.view("owner", "pipeline", {"workspace_id": "w2", "limit": 1})["pipeline"]["version"]
        self.committed(self.command("pipeline.stages_set", {"stages": [{"stage_id": "build", "name": "Build"}, {"stage_id": "wait", "name": "Wait"},
                                                                       {"stage_id": "combine", "name": "Combine", "scope": "release"}]},
                                    {"workspace_id": "w2"}, {"pipeline_revision": version}))
        numbers = [self.committed(self.command("pipeline.item_add", {"title": "Theirs %d" % n, "section": "now", "size": "small",
                                                                     "sources": [{"note": "a test"}]}, {"workspace_id": "w2"}))["number"] for n in range(3)]
        self.assertEqual(numbers, [1, 2, 3])
        self.topic_of_w1("t-w2", workspace="w2")
        theirs = self.committed(self.combine([self.approved(numbers[2:], "t-w2", workspace="w2")], workspace="w2", release_number=24))
        head = self.store.view("owner", "pipeline", {"workspace_id": "w2"})["facts"]["next_release"]
        self.assertEqual((head["piece_id"], head["release"], head["items"]), (theirs["piece_id"], 24, [3]))
        self.assertEqual(self.live_view("w2")["release_piece"], {"id": theirs["piece_id"], "release": 24, "stage": "combine", "round": 1})
        self.assertEqual((self.live_view()["release_piece"], self.members(self.release)), (None, [(1, None), (2, None)]), "w1 reads none")
        self.recorded("d" * 40)
        self.assertEqual(self.sql("SELECT count(*) FROM check_readings g JOIN check_runs u ON u.id=g.run_id WHERE u.commit_sha=? "
                                  "AND g.piece_id IS NOT NULL", ("d" * 40,)), [(0,)], "w1's run names no release")

    def test_a_trial_or_a_base_rerun_never_reads_for_a_release_piece(self):
        # D34: code with a suspect reverted is not the code being shipped, so its readings never gate the release.
        older = "d" * 40
        self.recorded(older, units="failed")
        readings = [{"check_id": "units", "result": "passed", "value": 900, "words": "900", "piece_id": self.release},
                    {"check_id": "browser-checks", "result": "passed", "value": 10, "words": "all", "piece_id": self.release}]
        trial = {"commit_sha": "e" * 40, "started_at": "2026-11-07T06:00:00-08:00", "finished_at": "2026-11-07T06:00:00-08:00", "tools": {},
                 "steps": [], "trial_of": older, "without": {"items": [1], "reverted": ["f" * 40]}}
        refused = self.command("check.report", {"run": trial, "readings": readings}, {"workspace_id": "w1"}, actor="agent-b")
        self.rejected(refused, "INVALID_ARGUMENT")
        self.assertIn("belong to no piece", refused["error"]["message"])
        base = dict(trial, commit_sha=HEAD, readings=readings)
        del base["trial_of"], base["without"]
        unpieced = [{key: value for key, value in reading.items() if key != "piece_id"} for reading in readings]
        refused = self.command("check.report", {"run": dict(trial, base=base), "readings": unpieced}, {"workspace_id": "w1"}, actor="agent-b")
        self.rejected(refused, "INVALID_ARGUMENT")
        self.assertIn("belong to no piece", refused["error"]["message"])
        with self.assertRaisesRegex(Exception, "belong to no piece"):
            self.store.record_checks({"outcome": "w1", "commit": "c" * 40, "started_at": "2026-11-07T06:30:00-08:00", "finished_at": "2026-11-07T06:30:00-08:00",
                                      "tools": {}, "steps": [], "note": "", "readings": [], "base": dict(base, commit=HEAD)}, "agent-b")
        self.rejected(self.move_on(), "STAGE_GATED")
        self.assertEqual(self.store.view("owner", "pipeline", {"workspace_id": "w1"})["release_piece"]["commit"], older)
        # The same trial without piece ids is recorded, as a trial.
        self.committed(self.command("check.report", {"run": dict(trial, base=dict(base, readings=unpieced)), "readings": unpieced},
                                    {"workspace_id": "w1"}, actor="agent-b"))
        self.rejected(self.move_on(), "STAGE_GATED")
        # Nor does a member's report of a reported check, nor a release check run that names a piece itself: here, one of
        # the live release's commit, which record_checks would never give the release's id.
        self.define({"check_id": "said", "group_name": "Checks", "position": 3, "name": "Said", "kind": "pass", "source": "reported"})
        report = {"commit_sha": "c" * 40, "started_at": "2026-11-07T07:00:00-08:00", "finished_at": "2026-11-07T07:00:00-08:00", "tools": {}, "steps": []}
        refused = self.command("check.report", {"run": report, "readings": [{"check_id": "said", "result": "passed", "words": "said", "piece_id": self.release}]},
                               {"workspace_id": "w1"}, actor="agent-b")
        self.rejected(refused, "INVALID_ARGUMENT")
        self.assertIn("belong to no release", refused["error"]["message"])
        with self.assertRaisesRegex(Exception, "leave piece_id out"):
            self.store.record_checks({"outcome": "w1", "commit": HEAD, "started_at": "2026-11-07T07:30:00-08:00", "finished_at": "2026-11-07T07:30:00-08:00",
                                      "tools": {}, "steps": [], "note": "", "readings": [dict(reading, piece_id=self.release) for reading in unpieced]}, "agent-b")
        self.rejected(self.move_on(), "STAGE_GATED")
        self.assertEqual(self.store.view("owner", "pipeline", {"workspace_id": "w1"})["release_piece"]["commit"], older)


if __name__ == "__main__":
    unittest.main()
