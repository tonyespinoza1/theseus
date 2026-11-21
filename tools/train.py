from __future__ import annotations

"""Items 481 and 482: how the deploy knows a merged train was checked, in place of a local release check.

tools/deploy.py --train FILE takes the record of a train that merged into the release branch. Everything it says
is read again from git, and from GitHub through `gh api` (read-only), before anything is stopped; the first fact
that does not hold, or cannot be read, refuses. Whether the release branch holds the merge is GitHub's to say
(on_branch): a remote-tracking ref in the live checkout is only what its last fetch saw. merge_commit must be pull request pr's own merge commit, as git's
parents and GitHub's pulls/N say (pull_merge). docs/tools.md ("A merged train") is the contract.

FILE: {"pr": N, "merge_commit": SHA, "merged_tree": TREE, "kind": "ordinary" | "judged-first",
       "items": [{"item": N, "approved_head": SHA, "review": "message or round"}],
       "hosted_run": RUN_ID | null,
       "judgment": {"producer_runs": [RUN_ID], "verdict_checks": [CHECK_RUN_ID], "audit": "message id"} | null}

An ordinary train is judged on GitHub after its merge: its record says the hosted judgment is pending, and
tools/releases.py attach-judgment attaches it once it lands. A train that changes the deploy tool, the controller
or the service's data migrations is judged first: its judged run, of a parent of the merge commit with the same
tree, comes with the train, and each conclusion is recorded as it is, beside the audit that read it.
"""

import json
import hashlib
import io
import tempfile
import zipfile
from collections import Counter
from pathlib import Path, PurePosixPath
import re
import subprocess
from typing import Protocol

from tools import base_move, ci_known_reds as ci, release_check, ci_input_reads
from aim_next.judgment import DEPLOY_TOOL, CONTROLLER, MIGRATIONS, SHA, guarded

KEYS = ("pr", "merge_commit", "items", "merged_tree", "kind", "hosted_run", "judgment")
ITEM_FIELDS = ("item", "approved_head", "review")
JUDGMENT_FIELDS = ("producer_runs", "verdict_checks", "audit")
KINDS = ("ordinary", "judged-first")
# GitHub's REST API paths for one workflow run, one check run and one pull request, and its compare of a commit with a
# branch's tip (repos/R/compare/BASE...HEAD).
RUN_PATH, CHECK_PATH, PULL_PATH = "repos/%s/actions/runs/%d", "repos/%s/check-runs/%d", "repos/%s/pulls/%d"
COMPARE_PATH = "repos/%s/compare/%s...%s"
# GitHub's compare statuses that say the branch's tip is the commit or follows it.
CONTAINED = ("identical", "ahead")
# What the record keeps of each run and check GitHub lists.
LISTED = ("id", "name", "head_sha", "status", "conclusion")


class Refused(Exception):
    """The train does not let the deploy go on; the message names the first fact that does not hold."""


class Unreadable(Exception):
    """GitHub could not be read: not the same as an answer."""


class GitHub(Protocol):
    def get(self, path: str) -> object:
        """The JSON GitHub's REST API answers for path, or Unreadable."""


class GhCli:
    """GitHub read through the gh CLI, GET only."""

    def __init__(self, gh="gh"):
        self.gh = gh

    def get(self, path):
        try:
            answer = subprocess.run([self.gh, "api", "-X", "GET", path], capture_output=True, text=True)
        except OSError as error:
            raise Unreadable("gh could not be run: %s" % error)
        if answer.returncode != 0:
            raise Unreadable("gh api %s exited %d: %s" % (path, answer.returncode, answer.stderr.strip()[:300]))
        try:
            return json.loads(answer.stdout)
        except ValueError as error:
            raise Unreadable("gh api %s answered something other than JSON: %s" % (path, error))


def _sha(value):
    return isinstance(value, str) and SHA.fullmatch(value) is not None


def _number(value):
    return type(value) is int and value > 0


def _words(value):
    return isinstance(value, str) and bool(value.strip())


def load(path):
    """The train file, or Refused naming what is wrong with it."""
    if not Path(path).is_absolute():
        raise Refused("--train is an absolute path, not %s" % path)
    try:
        train = base_move.loads(Path(path).read_text())
    except (OSError, ValueError, RecursionError, base_move.Refused) as error:
        raise Refused("no readable train at %s: %s" % (path, error))
    if not isinstance(train, dict) or set(train) not in (set(KEYS), set(KEYS) | {"base_move"}, set(KEYS) | {"scoped"}):
        raise Refused("the train is a JSON object with exactly %s, optionally base_move or scoped (mutually exclusive)" % ", ".join(KEYS))
    if not _number(train["pr"]):
        raise Refused("pr is the pull request's number, not %r" % (train["pr"],))
    for key in ("merge_commit", "merged_tree"):
        if not _sha(train[key]):
            raise Refused("%s is a full 40-character object name, not %r" % (key, train[key]))
    if train["kind"] not in KINDS:
        raise Refused("kind is %s, not %r" % (" or ".join(KINDS), train["kind"]))
    items = train["items"]
    if not isinstance(items, list) or not items:
        raise Refused("items lists at least one item")
    for item in items:
        if (not isinstance(item, dict) or set(item) != set(ITEM_FIELDS) or not _number(item["item"])
                or not _sha(item["approved_head"]) or not _words(item["review"])):
            raise Refused("each item is {item, approved_head, review}: a number, a full commit and the review that "
                          "approved it, not %r" % (item,))
    if train["hosted_run"] is not None and not _number(train["hosted_run"]):
        raise Refused("hosted_run is the merged head's run id, or null until GitHub lists it, not %r" % (train["hosted_run"],))
    judgment = train["judgment"]
    if judgment is not None:
        if not isinstance(judgment, dict) or set(judgment) != set(JUDGMENT_FIELDS):
            raise Refused("the judgment is null or a JSON object with exactly %s" % ", ".join(JUDGMENT_FIELDS))
        ids(judgment["producer_runs"], "producer_runs")
        ids(judgment["verdict_checks"], "verdict_checks")
        if not _words(judgment["audit"]):
            raise Refused("the judgment names its audit, the message that read it")
    if "base_move" in train:
        if train["kind"] != "judged-first":
            raise Refused("base_move is only for a judged-first train")
        try:
            base_move.fields(train["base_move"], "path sha256")
        except base_move.Refused as error:
            raise Refused(str(error))
    if 'scoped' in train:
        if train['kind'] != 'judged-first' or judgment is None:
            raise Refused('scoped evidence requires a judged-first judgment')
        try:
            fields(train['scoped'], 'path sha256')
        except ScopedRefused as error:
            raise Refused(str(error))
    return train


def ids(values, what):
    """values as a list of distinct GitHub ids, at least one, or Refused."""
    if not isinstance(values, list) or not values or not all(_number(value) for value in values) or len(set(values)) != len(values):
        raise Refused("%s lists at least one GitHub id, each once, not %r" % (what, values))
    return values


def _ask(git, what, *args):
    """git's answer, or Refused when it could not be read."""
    answer = git(*args)
    if answer.returncode != 0:
        raise Refused("%s could not be read (git %s exited %d): %s" % (what, " ".join(args), answer.returncode,
                                                                       answer.stderr.strip()[:300]))
    return answer.stdout


def _contains(git, older, newer, what):
    """Whether older is newer or one of its ancestors; Refused when git cannot say."""
    answer = git("merge-base", "--is-ancestor", older, newer)
    if answer.returncode not in (0, 1):
        raise Refused("whether %s could not be read (exit %d): %s" % (what, answer.returncode, answer.stderr.strip()[:300]))
    return answer.returncode == 0


def tree_of(git, commit):
    return _ask(git, "the tree of %s" % commit[:12], "rev-parse", "--verify", commit + "^{tree}").strip()




def listed(github, repository, kind, number):
    """What GitHub lists for one workflow run ("run") or check run ("check"), or Refused when it cannot be read."""
    path = (RUN_PATH if kind == "run" else CHECK_PATH) % (repository, number)
    try:
        found = github.get(path)
    except Unreadable as error:
        raise Refused("the %s %d could not be read: %s" % ("run" if kind == "run" else "check", number, error))
    if not isinstance(found, dict) or found.get("id") != number or not _sha(found.get("head_sha")):
        raise Refused("GitHub's answer for %s names no head commit" % path)
    return dict({key: found.get(key) for key in LISTED}, kind=kind)


def on_branch(github, repository, merge, release_branch):
    """Refused unless GitHub's release_branch, as GitHub holds it now, contains merge: its compare of merge with the
    branch's tip says the tip is merge or follows it, with merge as their merge base. A ref a clone last fetched is not
    read: a branch force-pushed since then would still seem to hold what it dropped."""
    path = COMPARE_PATH % (repository, merge, release_branch)
    try:
        compared = github.get(path)
    except Unreadable as error:
        raise Refused("whether %s is on %s could not be read from GitHub: %s" % (merge, release_branch, error))
    if not isinstance(compared, dict):
        raise Refused("GitHub's answer for %s is not a comparison" % path)
    status, base = compared.get("status"), (compared.get("merge_base_commit") or {}).get("sha")
    if status not in CONTAINED or compared.get("behind_by") != 0 or base != merge:
        raise Refused("the merge commit %s is not on %s at GitHub (compared as %r, %r behind, merge base %r)"
                      % (merge, release_branch, status, compared.get("behind_by"), base))


def parents_of(git, commit):
    return _ask(git, "the parents of %s" % commit[:12], "rev-list", "--parents", "-n", "1", commit).split()[1:]


def pull_merge(git, github, repository, number, merge, release_branch):
    """Refused unless merge is pull request number's own merge commit: a merge (two parents) whose second parent is
    the pull request's head, which GitHub's pulls/N names as merged into release_branch with merge as its
    merge_commit_sha. A commit that only says so in its message, or the branch's tip copied in, is not.
    Return the verified PR answer for the explicit base-move provenance chain."""
    parents = parents_of(git, merge)
    if len(parents) != 2:
        raise Refused("%s has %d parent%s, so it is not PR #%d's merge commit, which merges the pull request's head into "
                      "%s" % (merge, len(parents), "" if len(parents) == 1 else "s", number, release_branch))
    path = PULL_PATH % (repository, number)
    try:
        pull = github.get(path)
    except Unreadable as error:
        raise Refused("PR #%d could not be read: %s" % (number, error))
    if not isinstance(pull, dict) or pull.get("number") != number:
        raise Refused("GitHub's answer for %s is not PR #%d" % (path, number))
    if pull.get("merged") is not True or pull.get("merge_commit_sha") != merge:
        raise Refused("GitHub names %r as PR #%d's merge commit (merged %r), not %s"
                      % (pull.get("merge_commit_sha"), number, pull.get("merged"), merge))
    base, head = (pull.get("base") or {}).get("ref"), (pull.get("head") or {}).get("sha")
    if base != release_branch:
        raise Refused("PR #%d was merged into %r, not %s" % (number, base, release_branch))
    if head != parents[1]:
        raise Refused("PR #%d's head is %r, not %s, the merge commit's second parent" % (number, head, parents[1]))
    return pull


def judged(github, repository, judgment):
    """Each run and check of a judgment as GitHub lists it, every one completed, its conclusion as it is."""
    runs = [listed(github, repository, "run", number) for number in judgment["producer_runs"]]
    checks = [listed(github, repository, "check", number) for number in judgment["verdict_checks"]]
    for found in runs + checks:
        if found["status"] != "completed":
            raise Refused("the %s %d (%s) is %s, not completed; a judgment is attached once it has landed"
                          % (found["kind"], found["id"], found["name"], found["status"]))
    return runs, checks


def on_commit(github, repository, judgment, commit, whose):
    """The judgment of one commit: each run and check completed and naming that commit as its head, or Refused."""
    ids(judgment.get("producer_runs"), "producer_runs")
    ids(judgment.get("verdict_checks"), "verdict_checks")
    if not _words(judgment.get("audit")):
        raise Refused("the judgment names its audit, the message that read it")
    runs, checks = judged(github, repository, judgment)
    for found in runs + checks:
        if found["head_sha"] != commit:
            raise Refused("the %s %d (%s) names %s as its head, not %s, %s" % (
                found["kind"], found["id"], found["name"], found["head_sha"], commit, whose))
    return {"head": commit, "producer_runs": runs, "verdict_checks": checks, "audit": judgment["audit"]}


def judge(path, sha, live, release_branch, repository, git, github):
    """What the deploy records of a merged train, or Refused naming the first fact that does not hold.

    sha: the commit the deploy was asked for; live: the live checkout's HEAD; git(*args): git in the live checkout;
    github: a GitHub. Returns the train as the release record keeps it (docs/releases.md, `train`)."""
    train = load(path)
    merge = train["merge_commit"]
    if sha != merge:
        raise Refused("the deploy was asked for %s, but the train's merge commit is %s; the deploy ships the merge commit"
                      % (sha, merge))
    on_branch(github, repository, merge, release_branch)
    for item in train["items"]:
        if not _contains(git, item["approved_head"], merge, "item %d's approved head is in %s" % (item["item"], merge[:12])):
            raise Refused("item %d's approved head %s is not in the merge commit %s" % (item["item"], item["approved_head"], merge))
    tree = tree_of(git, merge)
    if tree != train["merged_tree"]:
        raise Refused("the merge commit's tree is %s, not the merged_tree %s" % (tree, train["merged_tree"]))
    merged_pull = pull_merge(git, github, repository, train["pr"], merge, release_branch)
    hosted = None
    if train["hosted_run"] is not None:
        hosted = listed(github, repository, "run", train["hosted_run"])
        if hosted["head_sha"] != merge:
            raise Refused("the hosted run %d names %s as its head, not the merge commit %s"
                          % (hosted["id"], hosted["head_sha"], merge))
    changed = [name for name in _ask(git, "the paths changed from the live commit %s" % live[:12], "diff", "--name-only",
                                     "-z", live, merge).split("\0") if name]
    kept = guarded(changed)
    if train["kind"] == "ordinary":
        if kept:
            raise Refused("an ordinary train cannot change %s: the deploy tool, the controller and the data migrations keep "
                          "their judged run before the merge; make the train judged-first" % ", ".join(kept[:5]))
        judgment = "pending" if train["judgment"] is None else on_commit(
            github, repository, train["judgment"], merge, "the merge commit")
    else:
        if train["judgment"] is None:
            raise Refused("a judged-first train comes with its judgment: the runs and checks of its judged run, and the audit")
        if "base_move" in train:
            # The exception has a concrete evidence contract; the original guard
            # below remains the only path when no explicit record exists.
            runs, checks = judged(github, repository, train["judgment"])
            heads = {found["head_sha"] for found in runs + checks}
            if len(heads) != 1:
                raise Refused("the judgment's runs and checks do not name one commit")
            judgment = {"head": next(iter(heads)), "producer_runs": runs, "verdict_checks": checks,
                        "audit": train["judgment"]["audit"]}
            try:
                moved = base_move.validate(train["base_move"], judgment, git, github, repository, live, merge, train["items"],
                                           merged_pull, train["pr"], release_branch)
            except (base_move.Refused, Unreadable) as error:
                raise Refused(str(error))
        else:
            judgment = before_merge(git, github, repository, train["judgment"], merge, tree)
            if 'scoped' in train:
                try:
                    scoped = validate_scoped(train['scoped'], judgment, git, github, repository,
                                             live, merge, train['items'], train['pr'])
                except (ScopedRefused, Unreadable) as error:
                    raise Refused(str(error))
    result = {"file": str(path), "pr": train["pr"], "kind": train["kind"], "merge_commit": merge, "merged_tree": tree,
            "release_branch": release_branch, "live": live, "items": train["items"], "hosted_run": hosted,
            "guarded_changes": kept, "judgment": judgment}
    if "base_move" in train:
        result["base_move"] = moved
    if "scoped" in train:
        result["scoped"] = scoped
    return result


def before_merge(git, github, repository, judgment, merge, tree):
    """A judged-first train's judgment: every run and check names one head, a parent of the merge commit whose tree
    is the merge commit's, and each conclusion is kept as it is."""
    runs, checks = judged(github, repository, judgment)
    heads = sorted({found["head_sha"] for found in runs + checks})
    if len(heads) != 1:
        raise Refused("the judgment's runs and checks name %s as their heads, not one commit" % ", ".join(heads))
    head = heads[0]
    parents = parents_of(git, merge)
    if head not in parents:
        raise Refused("the judgment's runs and checks name %s as their head, which is not a parent of the merge commit %s"
                      % (head, merge))
    judged_tree = tree_of(git, head)
    if judged_tree != tree:
        raise Refused("the judged parent %s has the tree %s, not the merge commit's %s" % (head, judged_tree, tree))
    return {"head": head, "producer_runs": runs, "verdict_checks": checks, "audit": judgment["audit"]}


def said(train):
    """One line for the deploy's output."""
    judgment = train["judgment"]
    if judgment == "pending":
        words = "the hosted judgment is pending (PR #%d); attach it with tools/releases.py attach-judgment" % train["pr"]
    else:
        conclusions = ", ".join("%s %d %s" % (found["kind"], found["id"], found["conclusion"])
                                for found in judgment["producer_runs"] + judgment["verdict_checks"])
        words = "judged at %s: %s (audit %s)" % (judgment["head"][:12], conclusions, judgment["audit"])
    if "base_move" in train:
        words += "; reviewed base move to %s (review %s)" % (train["base_move"]["assembly_head"][:12], train["base_move"]["review"]["sha256"])
    return "train: PR #%d (%s), merge commit %s on %s at GitHub; %s" % (
        train["pr"], train["kind"], train["merge_commit"][:12], train["release_branch"], words)


class ScopedRefused(Exception):
    pass


def require(ok, why):
    if not ok:
        raise ScopedRefused('scoped: ' + why)


def pairs(rows):
    result = {}
    for key, value in rows:
        require(key not in result, 'duplicate field ' + key)
        result[key] = value
    return result


def loads(data):
    return json.loads(data, object_pairs_hook=pairs,
                      parse_constant=lambda value: require(False, 'invalid JSON constant ' + value))


def fields(value, names):
    require(isinstance(value, dict) and set(value) == set(names.split()), 'invalid fields: expected ' + names)
    return value


def sha(value, size=40):
    require(isinstance(value, str) and re.fullmatch('[0-9a-f]{%d}' % size, value), 'invalid object digest')
    return value


def words(value):
    require(isinstance(value, str) and bool(value.strip()), 'missing review attribution')


def number(value):
    require(type(value) is int and value > 0, 'invalid positive number')


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def artifact(ref):
    fields(ref, 'path sha256')
    sha(ref['sha256'], 64)
    require(isinstance(ref['path'], str) and Path(ref['path']).is_absolute(), 'artifact path must be absolute')
    try:
        data = Path(ref['path']).read_bytes()
    except OSError as error:
        raise ScopedRefused('scoped: unreadable artifact: %s' % error)
    require(digest(data) == ref['sha256'], 'artifact digest differs: ' + ref['path'])
    return data


def document(ref):
    return loads(artifact(ref))


def ask(git, *args):
    result = git(*args)
    require(result.returncode == 0, 'git facts unreadable: ' + ' '.join(args))
    return result.stdout


def ancestor(git, older, newer):
    require(git('merge-base', '--is-ancestor', older, newer).returncode == 0, 'unrelated heads or bases')


def entries(git, head):
    rows = ask(git, 'ls-tree', '-r', '-z', '--full-tree', head).split('\0')
    return {row.split('\t', 1)[1]: row.split('\t', 1)[0] for row in rows if row}


def tree(git, head):
    return ask(git, 'rev-parse', '--verify', head + '^{tree}').strip()


def parents(git, head):
    return ask(git, 'rev-list', '--parents', '-n', '1', head).split()[1:]

def safe_path(path):
    require(isinstance(path, str) and bool(path) and not PurePosixPath(path).is_absolute()
            and all(part not in ('', '.', '..', '.git') for part in path.split('/')),
            'unsafe source artifact path')


# The existing App identity, read independently from GitHub; never supplied by
# an untrusted manifest as an override.
VERDICT_APP = {'id': 5763648, 'slug': 'theseus-verdict'}


def base_fact(value):
    fields(value, 'ref sha'); sha(value['sha'])
    ref = value['ref']
    # Short branch syntax, matching git check-ref-format --branch restrictions.
    require(isinstance(ref, str) and bool(ref) and not ref.startswith(('-', 'refs/'))
            and ref != '@' and not any(c in ref for c in ('..', '@{', '//'))
            and not re.search(r'[\x00-\x20\x7f~^:?*\[\\]', ref)
            and not ref.endswith('.') and all(part and not part.startswith('.')
                and not part.endswith('.lock') for part in ref.split('/')), 'invalid base ref')
    return value

def controller_archive(ref, github, repository, controller, name):
    fields(ref, 'id archive'); number(ref['id'])
    raw = artifact(ref['archive'])
    meta = github.get('repos/%s/actions/artifacts/%d' % (repository, ref['id']))
    require(meta.get('id') == ref['id'] and meta.get('name') == name
            and meta.get('digest') == 'sha256:' + digest(raw)
            and meta.get('workflow_run', {}).get('id') == controller['id']
            and meta['workflow_run'].get('head_sha') == controller['head_sha'], 'controller artifact provenance differs')
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        names = archive.namelist()
        require(len(names) == len(set(names)), 'duplicate controller artifact member')
        return {name: archive.read(name) for name in names}


def controller_binding(source, judgment, github, repository, head):
    data = fields(source['controller'], 'run attempt current_base verdict_artifact published_artifact')
    current = base_fact(data['current_base'])
    original = base_fact(source.get('base_at_run'))
    require(source['tested_merge']['first_parent'] == original['sha']
            and source['tested_merge']['second_parent'] == head, 'controller original merge binding differs')
    number(data['run']); number(data['attempt'])
    require(data['run'] != source['run'], 'controller is not a separate run')
    run = github.get('repos/%s/actions/runs/%d' % (repository, data['run']))
    require(run.get('id') == data['run'] and run.get('run_attempt') == data['attempt']
            and run.get('name') == 'Verdict' and run.get('path') == '.github/workflows/verdict.yml'
            and run.get('event') == 'workflow_run' and run.get('status') == 'completed'
            and run.get('conclusion') in ('success', 'failure'), 'controller run identity differs')
    sha(run['head_sha'])
    suffix = '%d-%d-c%d' % (source['run'], source['attempt'], data['attempt'])
    verdict = controller_archive(data['verdict_artifact'], github, repository, run, 'verdict-' + suffix)
    published = controller_archive(data['published_artifact'], github, repository, run, 'published-' + suffix)
    binding = loads(verdict['producer.json']); check = loads(verdict['check-run.json'])
    pub = loads(published['published.json'])
    producer, controller = binding['producer'], binding['controller']
    ci.same_run({'id': producer.get('run_id'), 'attempt': producer.get('run_attempt')},
                {'id': source['run'], 'attempt': source['attempt']})
    require(producer.get('head_sha') == head and producer.get('head_repository') == repository
            and producer.get('event') == 'pull_request' and producer.get('workflow_name') == 'Checks'
            and producer.get('workflow_path') == '.github/workflows/checks.yml'
            and producer.get('conclusion') == judgment['producer_runs'][0]['conclusion'], 'controller producer binding differs')
    pr = producer['pull_request']
    require(pr.get('head_sha') == head and pr.get('base_sha') == current['sha']
            and pr.get('base_ref') == current['ref'], 'controller current PR binding differs')
    number(pr['number'])
    url = 'https://github.com/%s/actions/runs/%d' % (repository, data['run'])
    ci.same_run({'id': controller.get('run_id'), 'attempt': controller.get('run_attempt')},
                {'id': data['run'], 'attempt': data['attempt']})
    require(controller.get('repository') == repository and controller.get('url') == url
            and controller.get('sha') == controller.get('workflow_sha') == run['head_sha']
            and controller.get('ref') == 'refs/heads/' + run['head_branch']
            and controller.get('workflow_ref') == repository + '/.github/workflows/verdict.yml@' + controller['ref'],
            'protected controller identity differs')
    require(len(judgment['verdict_checks']) == 1, 'need exact published Verdict check')
    listed = judgment['verdict_checks'][0]
    raw = github.get('repos/%s/check-runs/%d' % (repository, listed['id']))
    prs = raw.get('pull_requests')
    require(isinstance(prs, list) and len(prs) == 1 and prs[0].get('number') == pr['number']
            and prs[0].get('head', {}).get('sha') == head
            and prs[0].get('base', {}).get('ref') == current['ref']
            and prs[0].get('base', {}).get('sha') == current['sha'], 'archived check current PR binding differs')
    external = '%d-%d' % (source['run'], source['attempt'])
    # The early, unretargeted receipt publishes an empty base, not a guessed
    # original ref. Both PR records above still bind its complete base facts.
    retargeted = current != original
    name = 'Verdict for ' + current['ref'] if retargeted else 'Verdict'
    published_base = current['ref'] if retargeted else ''
    require(pub.get('check_run_id') == listed['id'] == raw.get('id')
            and pub.get('head_sha') == head and pub.get('name') == name and pub.get('base') == published_base
            and pub.get('external_id') == external, 'published Verdict binding differs')
    for key, expected in (('controller_run', data['run']), ('controller_attempt', data['attempt']),
                          ('producer_run', source['run']), ('producer_attempt', source['attempt'])):
        require(str(pub.get(key)) == str(expected), 'published attempt binding differs')
    require(raw.get('app', {}).get('id') == VERDICT_APP['id']
            and raw.get('app', {}).get('slug') == VERDICT_APP['slug'], 'untrusted Verdict App')
    expected = {'name': name, 'head_sha': head, 'status': 'completed',
                'conclusion': listed['conclusion'], 'details_url': url, 'external_id': external}
    require(listed['conclusion'] in ('success', 'failure') and all(
        raw.get(k) == check.get(k) == v for k, v in expected.items()), 'source check binding differs')
    output = check['output']; api_output = raw.get('output') or {}
    summary = output['summary']
    appended = '\nPublished by controller run %d attempt %d.' % (data['run'], data['attempt'])
    require(api_output.get('title') == output.get('title')
            and api_output.get('summary') in (summary, summary + appended), 'Verdict output differs')
    refusal = verdict['refusals'].decode()
    if refusal.strip():
        require(output.get('title') == 'refused' and check['conclusion'] == 'failure'
                and summary.startswith('Refused:\n' + ''.join('- ' + line + '\n' for line in refusal.splitlines())),
                'Verdict refusal ground differs')
    return {'run': data['run'], 'attempt': data['attempt'], 'head': run['head_sha'],
            'check': listed['id'], 'current_base': current, 'output': output, 'refusals': refusal,
            'producer': {'head_ref': producer['head_branch'], 'pr': pr['number']}}



def archive_members(row, run, github, repository):
    fields(row, 'id name archive'); number(row['id'])
    raw = artifact(row['archive'])
    meta = github.get('repos/%s/actions/artifacts/%d' % (repository, row['id']))
    require(meta.get('id') == row['id'] and meta.get('name') == row['name']
            and meta.get('digest') == 'sha256:' + digest(raw)
            and meta.get('workflow_run', {}).get('id') == run['id']
            and meta['workflow_run'].get('head_sha') == run['head_sha'], 'artifact API provenance differs')
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        result = {}
        for name in archive.namelist():
            if name.endswith('/'):
                continue
            safe_path(name)
            require(name not in result, 'duplicate artifact member')
            result[name] = archive.read(name)
        return result


def raw_run(data, judgment, git, github, repository, head, scope):
    source = fields(data, 'run attempt tested_merge base_at_run workflow_head archives inventory audit controller')
    number(source['run']); number(source['attempt']); sha(source['workflow_head'])
    require(source['attempt'] == 1, 'producer retries require a fresh run')
    merge = fields(source['tested_merge'], 'sha tree first_parent second_parent')
    for value in merge.values():
        sha(value)
    base_fact(source['base_at_run'])
    require(source['base_at_run']['sha'] == scope['live'] and merge['first_parent'] == scope['live']
            and merge['second_parent'] == head and parents(git, merge['sha']) == [scope['live'], head]
            and tree(git, merge['sha']) == scope['tree'], 'tested merge/source/base differs')
    require([r['id'] for r in judgment['producer_runs']] == [source['run']], 'producer run differs')
    run = github.get('repos/%s/actions/runs/%d' % (repository, source['run']))
    require(run.get('id') == source['run'] and run.get('run_attempt') == source['attempt']
            and run.get('head_sha') == head and run.get('event') == 'pull_request'
            and run.get('name') == 'Checks' and run.get('path') == '.github/workflows/checks.yml'
            and run.get('status') == 'completed' and run.get('conclusion') == 'success'
            and judgment['producer_runs'][0]['conclusion'] == 'success', 'producer API identity or green differs')
    number(run.get('workflow_id'))
    registered = github.get('repos/%s/actions/workflows/%d' % (repository, run['workflow_id']))
    require(registered.get('id') == run['workflow_id'] and registered.get('path') == '.github/workflows/checks.yml',
            'unregistered producer workflow')
    require(source['workflow_head'] in (head, merge['sha']), 'unbound PR workflow head')
    controller = controller_binding(source, judgment, github, repository, head)
    require(controller['current_base'] == source['base_at_run'] and not controller['refusals'].strip()
            and judgment['verdict_checks'][0]['conclusion'] == 'success', 'controller refusal or stale base')
    prs = run.get('pull_requests')
    require(isinstance(prs, list) and len([p for p in prs if p.get('number') == controller['producer']['pr']
            and p.get('head', {}).get('sha') == head and p.get('base', {}).get('sha') == source['base_at_run']['sha']
            and p.get('base', {}).get('ref') == source['base_at_run']['ref']]) == 1, 'producer API PR/base/head differs')
    inventory = document(source['inventory'])
    require(set(inventory) == set(scope['selected']), 'raw step inventory differs from computed coverage')
    for name, rows in inventory.items():
        if name == 'wake-replay':
            continue
        require(isinstance(rows, list) and bool(rows), 'empty raw case inventory')
        for row in rows:
            fields(row, 'name phase engine expectedFailure')
            require(type(row['expectedFailure']) is bool and isinstance(row['name'], str) and bool(row['name']), 'invalid raw case identity')
        require(len({ci.key(row) for row in rows}) == len(rows), 'duplicate raw case inventory')
    audit = fields(document(source['audit']), 'version reviewer record source_head scope_sha256 source_sha256 inventory')
    require(audit['version'] == 2 and audit['record'] == judgment['audit'] and audit['source_head'] == head
            and audit['scope_sha256'] == digest(canonical(scope)) and audit['inventory'] == source['inventory']
            and audit['source_sha256'] == digest(canonical({k: v for k, v in source.items() if k != 'audit'})),
            'independent audit/source binding differs')
    words(audit['reviewer']); words(audit['record'])
    archives = source['archives']
    require(isinstance(archives, list) and bool(archives) and len({r['id'] for r in archives}) == len(archives),
            'missing or duplicate archives')
    members = {}
    for row in archives:
        require(row['name'] not in members, 'duplicate archive name')
        members[row['name']] = archive_members(row, run, github, repository)
    require('release-check' in members, 'missing aggregate archive')
    combined = loads(members['release-check']['summary.json'])
    provenance = loads(members['release-check']['provenance.json'])
    expected_run = {'id': source['run'], 'attempt': source['attempt'], 'repository': repository,
                    'workflow': 'Checks', 'event': 'pull_request', 'workflow_sha': source['workflow_head'],
                    'workflow_ref': repository + '/.github/workflows/checks.yml@refs/pull/%d/merge' % controller['producer']['pr']}
    ci.same_run(provenance['run'], expected_run)
    require(provenance.get('merge') == merge and provenance.get('base') == source['base_at_run']
            and provenance.get('base_at_run') == source['base_at_run']
            and provenance.get('head') == {'ref': controller['producer']['head_ref'], 'sha': head}
            and provenance.get('checked') == {'commit': merge['sha'], 'tree_unchanged': True}, 'aggregate provenance differs')
    require(combined.get('tool') == release_check.TOOL and combined.get('commit') == merge['sha']
            and combined.get('tree_unchanged') is True and combined.get('tree_after') == {'head': merge['sha'], 'changes': ''}
            and not combined.get('only') and not combined.get('skipped') and combined.get('passed') is True
            and not combined.get('failed') and not combined.get('missing') and not combined.get('missing_jobs')
            and not combined.get('cancelled_shards'), 'dirty, partial or red aggregate')
    # Planning used the tested merge; compare its exact tree with the source,
    # retaining its separate commit identity in the record.
    expected_scope = dict(scope, source=merge['sha'])
    require(combined.get('scoped') == expected_scope, 'aggregate coverage differs')
    require(Counter(combined.get('required', [])) == Counter(scope['required'])
            and Counter(provenance.get('required', [])) == Counter(scope['required']), 'aggregate required inventory differs')
    selected = scope['selected']; plan = ci.plan_steps(provenance['plan'])
    # Python partitions are permitted only with the existing complete partition
    # combiner; the raw inventory below still requires every case exactly once.
    require(set(s for job in plan for s in job) == set(selected), 'shard plan coverage differs')
    by_name = {s['name']: s for s in combined['steps']}
    require(Counter(s['name'] for s in combined['steps']) == Counter(selected), 'aggregate step coverage differs')
    entries = provenance['shards']
    names = [s['artifact'] for s in entries]
    require(Counter(names) == Counter('check-%d' % i for i in range(len(plan)))
            and set(members) == set(names) | {'release-check'}
            and Counter(combined.get('combined_from', [])) == Counter(n + '/summary.json' for n in names),
            'archive/shard inventory differs')
    ran, unit_rows, raw_steps = [], {s: [] for s in ci.UNITS}, []
    docs, receipts = [], []
    source_bytes = ci_input_reads.source_files(ask(git, 'rev-parse', '--show-toplevel').strip(), merge['sha'])
    with tempfile.TemporaryDirectory(prefix='scoped-raw-') as temporary:
        for entry in entries:
            name = entry['artifact']; content = members[name]; out = Path(temporary) / name
            for path, raw in content.items():
                target = out / path; target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes(raw)
                if path.endswith('.json'):
                    loads(raw)
            receipt, raw = loads(content['run.json']), loads(content['summary.json'])
            docs.append(raw); receipts.append(receipt)
            ci.same_run(receipt.get('run'), expected_run)
            require(entry.get('receipt') == receipt and not entry.get('refused')
                    and receipt.get('merge') == merge and receipt.get('base') == source['base_at_run']
                    and receipt.get('base_at_run') == source['base_at_run'] and receipt.get('head') == provenance['head']
                    and receipt.get('summary_sha256') == digest(content['summary.json']), 'shard identity/digest differs')
            names_here = [s['name'] for s in raw['steps']]
            assigned = receipt['shard']['steps']; assigned = assigned.split(',') if isinstance(assigned, str) else assigned
            require(names_here == assigned == plan[int(name.split('-')[1])]
                    and raw.get('only') == names_here and not raw.get('skipped') and not raw.get('scoped')
                    and raw.get('tool') == release_check.TOOL and raw.get('commit') == merge['sha']
                    and raw.get('tree_unchanged') is True and raw.get('tree_after') == combined['tree_after']
                    and Counter(raw.get('required', [])) == Counter(combined['required']) and raw.get('passed') is True
                    and entry.get('commit') == merge['sha'] and entry.get('tree_unchanged') is True
                    and entry.get('steps') == names_here and entry.get('tools') == raw.get('tools'), 'dirty or partial shard')
            tools = raw['tools']
            require(tools.get('release_check') == ask(git, 'rev-parse', merge['sha'] + ':tools/release_check.py').strip(), 'tool source differs')
            for path in release_check.harnesses():
                require(tools.get('harnesses', {}).get(path) == ask(git, 'rev-parse', merge['sha'] + ':' + path).strip(), 'harness source differs')
            for step in raw['steps']:
                step_name = step['name']; ran.append(step_name)
                require(step.get('passed') is True and type(step.get('exit')) is int and step['exit'] == 0
                        and not step.get('quarantine'), 'raw red or waived step')
                if step_name in ci.NATIVE:
                    require(release_check.ran_whole(step), 'partial native step')
                    rows = ci.cases(step, out, inventory[step_name])
                    report = loads(content['native/' + ci.NATIVE[step_name].name + '/' + ci.NATIVE[step_name].results])
                    require(not report.get('teardown') and not report.get('fixtureErrors')
                            and not report.get('instrumentError') and all(not r.get('instrumentError') for r in rows), 'native teardown/infrastructure failure')
                    require(not report.get('left') and not report.get('pageErrors')
                            and all(m.get('caught') is True for m in report.get('mutations', [])), 'native cleanup or mutation failure')
                    for hashes in (report.get('hashes', {}), report.get('sourceHashes', {})):
                        require(isinstance(hashes, dict), 'invalid native input hashes')
                        for path, value in hashes.items():
                            safe_path(path)
                            require(path in source_bytes and value == digest(source_bytes[path]), 'native input hash differs')
                    require(step == by_name[step_name], 'aggregate native step differs')
                elif step_name in ci.UNITS:
                    text = (out / (step_name + '.log')).read_text()
                    rows = ci.unit_records(text, node=step_name == 'node-units')
                    scoped_unit_evidence(step, out, [r for r, _ in rows],
                                         provenance['plan'][int(name.split('-')[1])], raw, receipt)
                    unit_rows[step_name].extend(r for r, _ in rows)
                elif step_name == 'wake-replay':
                    traffic = loads(ask(git, 'show', merge['sha'] + ':' + release_check.REPLAY_FILE))
                    require(ci.unique(inventory[step_name].get('routed'), 'reviewed replay actors') ==
                            ci.unique(traffic.get('routed'), 'source replay actors'), 'replay inventory differs')
                    ci.replay_evidence(step, out, traffic)
                    require(step == by_name[step_name], 'aggregate replay differs')
                raw_steps.append({'name': step_name, 'archive': name, 'summary_sha256': digest(content['summary.json']),
                                  'passed': step['passed'], 'exit': step['exit'], 'counts': step.get('counts')})
        folded, _, _, missing_jobs, _ = combine_scoped_units(docs, provenance['plan'], receipts)
        require(not missing_jobs and {s['name']: s for s in folded} == by_name, 'raw/combined results or partitions differ')
        partitions = [job for job in provenance['plan'] if job.get('python_shard')]
        if partitions:
            actual_files = ci_input_reads.python_files(ci_input_reads.source_files(
                root=ask(git, 'rev-parse', '--show-toplevel').strip(), head=merge['sha']))
            planned_files = [p for job in partitions for p in job['files']]
            require(Counter(planned_files) == Counter(actual_files), 'Python partition source inventory differs')
        require(set(ran) == set(selected) and all(ran.count(s) == 1 for s in selected if s != 'python-units'), 'actual coverage differs')
        for name, rows in unit_rows.items():
            require(Counter(ci.key(r) for r in rows) == Counter(ci.key(r) for r in inventory[name])
                    and len({ci.key(r) for r in rows}) == len(rows), 'unit case inventory differs')
    return {'controller': controller, 'producer': {k: run[k] for k in ('id', 'head_sha', 'run_attempt', 'conclusion')},
            'workflow_head': source['workflow_head'], 'tested_merge': merge, 'steps': raw_steps,
            'inventory': source['inventory'], 'audit': source['audit']}


def validate_scoped(ref, judgment, git, github, repository, live, merge, items, pr):
    try:
        data = fields(document(ref), 'version live_base source_head scope source')
        require(data['version'] == 2 and data['live_base'] == live, 'stale live base or unknown scope version')
        head = data['source_head']; sha(head)
        require(judgment['head'] == head and parents(git, merge) == [live, head]
                and tree(git, merge) == tree(git, head), 'release source/tree/parents differs')
        root = ask(git, 'rev-parse', '--show-toplevel').strip()
        scope = ci_input_reads.pr_record(root, live, head)
        require(scope == data['scope'] and set(scope['required']) == set(release_check.REQUIRED), 'computed input coverage differs')
        for item in items:
            ancestor(git, item['approved_head'], head)
        result = raw_run(data['source'], judgment, git, github, repository, head, scope)
        require(result['controller']['producer']['pr'] == pr, 'authenticated PR differs from train')
        return {'version': 2, 'evidence': ref, 'scope': scope, 'source': result}
    except (ci.Refused, release_check.Refused, ci_input_reads.Refused, OSError, ValueError, KeyError, IndexError, TypeError,
            AttributeError, RecursionError, SyntaxError, zipfile.BadZipFile) as error:
        raise ScopedRefused('scoped evidence invalid or incomplete: %s' % error)


def combine_scoped_units(docs, plan, receipts):
    """Fold Python partitions into one required step; name absent planned jobs.

    The plan is an obligation even when a cancelled job wrote no summary. File
    coverage is checked before adding counts, so half a suite is never Python.
    """
    required = sorted(set(name for job in plan for name in job['steps'].split(',')) |
                      {name for doc in docs for name in doc.get('required', [])})
    steps = [step for doc in docs for step in doc.get('steps', [])]
    partitions = [job for job in plan if job.get('python_shard')]
    jobs, missing_jobs, parts = [], [], []
    for job in plan:
        name = job.get('name', job['steps'])
        matches = [(doc, receipt) for doc, receipt in zip(docs, receipts)
                   if (receipt.get('shard') or {}).get('name') == name]
        # Older receipts/tests name only the single step, without a job name.
        if not matches and 'name' not in job:
            matches = [(doc, receipt) for doc, receipt in zip(docs, receipts)
                       if [step['name'] for step in doc.get('steps', [])] == job['steps'].split(',')]
        present = len(matches) == 1
        if present:
            doc, _ = matches[0]
            present = sorted(s['name'] for s in doc.get('steps', [])) == sorted(job['steps'].split(','))
            if job.get('python_shard'):
                index, total = map(int, job['python_shard'].split('/'))
                actual = doc.get('python_shard') or {}
                present = (present and actual.get('index') == index and actual.get('total') == total
                           and sorted(actual.get('files', [])) == sorted(job['files']))
                if present:
                    parts.append(dict(doc['steps'][0], partition=actual))
        jobs.append({'name': name, 'steps': job['steps'].split(','), 'missing': not present,
                     **({'python_shard': job['python_shard']} if job.get('python_shard') else {})})
        if not present:
            missing_jobs.append(name)
    if partitions:
        python_results = sum(step['name'] == 'python-units' for step in steps)
        steps = [step for step in steps if step['name'] != 'python-units']
        expected = [job['python_shard'] for job in partitions]
        files = [path for job in partitions for path in job['files']]
        total = len(partitions)
        coverage = (set(expected) == {'%d/%d' % (i, total) for i in range(1, total + 1)}
                    and len(files) == len(set(files))
                    and len(parts) == python_results)
        if len(parts) == total and coverage:
            counts = {'tests': sum(p['counts']['tests'] for p in parts),
                      'status': 'OK' if all(p['counts'].get('status') == 'OK' for p in parts) else 'FAILED'}
            for key in ('skipped', 'failures', 'errors', 'expected failures', 'unexpected successes'):
                if any(key in p['counts'] for p in parts):
                    counts[key] = sum(p['counts'].get(key, 0) for p in parts)
            steps.append({'name': 'python-units', 'tool': 'unittest', 'counts': counts,
                          'exit': max(p.get('exit', 1) for p in parts),
                          'passed': counts['tests'] > 0 and all(
                              p.get('passed') is True and p['counts'].get('tests', -1) >= 0 for p in parts),
                          'shards': parts})
    names = [step['name'] for step in steps]
    if len(names) != len(set(names)):
        raise ScopedRefused('a step ran in two shards: %s' % sorted(n for n in set(names) if names.count(n) > 1))
    missing = sorted(set(required) - set(names))
    return steps, required, jobs, missing_jobs, missing


def scoped_unit_evidence(step, out, expected, job, raw, receipt):
    """Scoped-only original partition compatibility; shared readers stay exact."""
    partitioned = 'python_shard' in job
    if not partitioned:
        require('python_shard' not in raw and 'python_shard' not in step,
                'unplanned Python partition')
        return ci.unit_evidence(step, out, expected)
    require(job['steps'] == 'python-units' and len(raw['steps']) == 1,
            'Python partition must be a Python-only job')
    part = fields(step.get('python_shard'), 'index total files')
    require(type(part['index']) is int and type(part['total']) is int
            and 1 <= part['index'] <= part['total']
            and job['python_shard'] == '%d/%d' % (part['index'], part['total'])
            and raw.get('python_shard') == part and job.get('files') == part['files'],
            'Python partition source metadata differs')
    require(receipt.get('shard') == {'name': job.get('name'), 'steps': ['python-units']}
            and isinstance(job.get('name'), str) and bool(job['name']), 'Python partition receipt differs')
    checkout = raw.get('checkout')
    require(isinstance(checkout, str) and Path(checkout).is_absolute()
            and step.get('tool') == 'unittest' and isinstance(step.get('argv'), list)
            and len(step['argv']) == 6 and isinstance(step['argv'][0], str) and bool(step['argv'][0])
            and step['argv'][1:] == [checkout + '/tools/unit_shards.py', '--root', checkout,
                                    '--shard', job['python_shard']], 'Python partition command differs')
    text = (out / 'python-units.log').read_text()
    metadata = [loads(line) for line in text.splitlines() if line.startswith('{"unit_shards":')]
    require(len(metadata) == 1 and type(metadata[0].get('unit_shards')) is int
            and metadata[0]['unit_shards'] == 1 and metadata[0].get('python_shard') == part
            and type(metadata[0].get('tests')) is int and metadata[0]['tests'] == step.get('counts', {}).get('tests')
            and metadata[0].get('timing') == step.get('timing'), 'Python partition log metadata differs')
    require(len(re.findall(r'^Ran (\d+) tests? in ', text, re.M)) == 1
            and len(re.findall(r'^(OK|FAILED)(?: \((.*)\))?$', text, re.M)) == 1, 'Python partition log result differs')
    paths = part['files']
    require(isinstance(paths, list) and all(isinstance(path, str) for path in paths)
            and len(paths) == len(set(paths)), 'Python partition files differ')
    for path in paths:
        safe_path(path)
    if metadata[0]['tests'] == 0:
        require(re.findall(r'^Ran (\d+) tests? in ', text, re.M) == ['0']
                and re.findall(r'^(OK|FAILED)(?: \((.*)\))?$', text, re.M) == [('OK', '')]
                and step.get('counts') == {'tests': 0, 'status': 'OK'}
                and type(step.get('exit')) is int and step['exit'] == 0 and step.get('passed') is True
                and not expected, 'empty Python partition differs')
        return
    return ci.unit_evidence(step, out, expected)
