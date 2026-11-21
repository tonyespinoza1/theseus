from __future__ import annotations

"""One path policy for the train guard and Core's actual-diff freeze record."""
import re
import subprocess
from pathlib import Path

# What an ordinary train may not change: the deploy tool and the modules it
# loads, the files GitHub's classifier pins to its event base, the controller,
# and every schema step. tests/test_tools_train.py derives the dependencies and
# pinned files from their real owners, so a newly loaded module must be guarded.
DEPLOY_TOOL = ("tools/deploy.py", "tools/release_check.py", "tools/unit_shards.py", "tools/known_reds.py", "tools/releases.py", "tools/train.py",
               "tools/checks.py", "tools/ci_known_reds.py", "tools/ci_input_reads.py", "tools/ci-input-reads.json",
               "tools/quarantine.py", "tools/steps_for.py", "tools/base_move.py", "tools/own_case_receipt.py", "tools/continuation_envelope.py",
               "tools/runner_runtime.py", "tools/gauntlet_admission.py", "aim_next/judgment.py")
CONTROLLER = ".github/"
MIGRATIONS = ("aim_next/migrations.py",)
SHA = re.compile(r"[0-9a-f]{40}")


def guarded(paths):
    return [path for path in paths if path in DEPLOY_TOOL or path in MIGRATIONS or path.startswith(CONTROLLER)]


def source_diff(repo, base, head, *, run=None, check_ancestry=True):
    """Read actual source outside Core's transaction. Deletions/renames keep both paths."""
    if not Path(repo).is_absolute() or any(not isinstance(s, str) or SHA.fullmatch(s) is None for s in (base, head)):
        raise ValueError("Name an absolute repository and full base/head commits")
    def git(*args):
        argv = ['git', '-C', str(repo)] + list(args)
        result = run(argv) if run is not None else subprocess.run(argv, capture_output=True)
        if result.returncode:
            error = result.stderr.decode('utf-8', 'replace') if isinstance(result.stderr, bytes) else result.stderr or ''
            raise ValueError("The frozen source could not be read: " + error[:300])
        return result.stdout
    if check_ancestry:
        git('merge-base', '--is-ancestor', base, head)
    # No external diff/filter, and no rename folding that could hide a guarded deletion.
    names = git('diff', '--no-ext-diff', '--no-renames', '--name-only', '-z', base, head)
    try:
        if isinstance(names, bytes):
            names = names.decode('utf-8')
        if not isinstance(names, str):
            raise ValueError("The changed source paths could not be read")
        return [name for name in names.split('\0') if name]
    except UnicodeDecodeError:
        raise ValueError("Changed paths must be UTF-8")
