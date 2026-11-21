from __future__ import annotations

"""Read-only admission at each caller's effect boundary; not a reservation."""
import json
from pathlib import Path

from tools import train, releases
from aim_next.judgment import SHA, source_diff


class Refused(Exception):
    pass


def require(holds, words):
    if not holds:
        raise Refused(words)


def _json_call(run, argv, request=None):
    result = run(argv, **({'input': json.dumps(request)} if request is not None else {}))
    require(result.returncode == 0, 'The admission pipeline could not be read; no judged work is authorized')
    try:
        value = json.loads(result.stdout)
    except (ValueError, TypeError):
        raise Refused('The admission pipeline returned no complete JSON answer')
    require(isinstance(value, dict) and not value.get('error'), 'The admission pipeline refused its read')
    return value


def approved(config, run):
    outcome = getattr(config, 'pipeline_outcome', None)
    instance = getattr(config, 'pipeline_instance', None)
    broker = getattr(config, 'pipeline_broker', None)
    profile = getattr(config, 'pipeline_profile', None)
    require(outcome and instance and bool(broker) != bool(profile),
            'Judged admission needs pipeline_outcome, pipeline_instance and exactly one pipeline_broker or pipeline_profile')
    selected = broker or profile
    require(Path(selected).is_absolute(), 'The admission broker/profile path must be absolute')
    argv = [config.python, '-I', '-B', str(config.live / 'bin/aim-next'),
            '--broker' if broker else '--profile', str(selected)]
    binding = _json_call(run, argv + ['binding']).get('binding')
    require(isinstance(binding, dict) and binding.get('instance_id') == instance,
            'The admission binding belongs to another instance')
    cursor, seen, pieces, expected, waiting = 0, set(), {}, None, None
    unseen = object()
    open_release = unseen
    for _ in range(1000):
        request = {'v': 1, 'binding': binding, 'view': {'name': 'pipeline',
            'args': {'workspace_id': outcome, 'section': 'now', 'cursor': cursor, 'limit': 100}}}
        page = _json_call(run, argv + ['view', '-'], request)
        require(page.get('instance_id') == instance and page.get('workspace_id') == outcome,
                'The admission pipeline belongs to another instance or outcome')
        require(isinstance(page.get('pipeline'), dict) and page['pipeline'].get('enabled') is True and
                page['pipeline'].get('ships_in_releases') is True, 'Admission needs the enabled release pipeline')
        version = (page.get('relevant_versions') or {}).get('pipeline')
        require(type(version) is int and version > 0 and (expected is None or version == expected),
                'The admission pipeline changed while it was being read; read it afresh')
        expected = version
        facts = page.get('facts') or {}
        numbers = facts.get('approved_waiting')
        require(isinstance(numbers, list) and all(type(n) is int and n > 0 for n in numbers),
                'The admission pipeline omitted its approved/unshipped facts')
        require(waiting is None or set(numbers) == waiting, 'The admission pipeline has inconsistent approval facts')
        waiting = set(numbers)
        require('next_release' in facts, 'The admission pipeline omitted its open-release facts')
        next_release = facts['next_release']
        require(next_release is None or isinstance(next_release, dict) and
                type(next_release.get('release')) is int and next_release['release'] > 0 and
                isinstance(next_release.get('piece_id'), str) and bool(next_release['piece_id']) and
                isinstance(next_release.get('items'), list) and bool(next_release['items']) and
                all(type(n) is int and n > 0 for n in next_release['items']) and
                len(set(next_release['items'])) == len(next_release['items']) and
                isinstance(next_release.get('stage'), dict) and bool(next_release['stage'].get('stage_id')),
                'The admission pipeline has malformed open-release facts')
        require(open_release is unseen or open_release == next_release,
                'The admission pipeline changed its open release while paging')
        open_release = next_release
        require(isinstance(page.get('pieces'), list) and isinstance(page.get('items'), list) and 'next_cursor' in page,
                'The admission pipeline answer is incomplete')
        for piece in page['pieces']:
            require(isinstance(piece, dict) and isinstance(piece.get('id'), str) and isinstance(piece.get('items'), list) and
                    all(type(n) is int and n > 0 for n in piece['items']), 'The admission pipeline has a malformed piece')
            value = {'id': piece['id'], 'items': piece['items'], 'freeze': piece.get('freeze'),
                     'version': piece.get('version'), 'stage': (piece.get('stage') or {}).get('stage_id')}
            require(piece['id'] not in pieces or pieces[piece['id']] == value,
                    'The admission pipeline changed a piece while paging')
            pieces[piece['id']] = value
        if open_release is not None:
            release = page.get('release_piece')
            require(isinstance(release, dict) and release.get('kind') == 'release' and
                    release.get('id') == open_release['piece_id'] and release.get('items') == open_release['items'] and
                    release.get('release_number') == open_release['release'] and
                    release.get('stage') == open_release['stage'] and type(release.get('version')) is int,
                    'The admission pipeline omitted or changed its open-release owner')
            value = {'id': release['id'], 'items': release['items'], 'freeze': release.get('freeze'),
                     'version': release['version'], 'stage': release['stage']['stage_id'], 'release': open_release['release']}
            require(value['id'] not in pieces or pieces[value['id']] == value,
                    'The admission pipeline changed a release while paging')
            pieces[value['id']] = value
        else:
            require(page.get('release_piece') is None, 'The admission pipeline has an unaccounted open release')
        next_cursor = page['next_cursor']
        if next_cursor is None:
            pending = waiting | (set(open_release['items']) if open_release else set())
            require(pending <= {n for p in pieces.values() for n in p['items']},
                    'The admission pipeline omitted an approved piece')
            return list(pieces.values()), pending, expected
        require(type(next_cursor) is int and next_cursor > cursor and next_cursor not in seen,
                'The admission pipeline did not complete its pages')
        seen.add(cursor); cursor = next_cursor
    raise Refused('The admission pipeline exceeded its bounded read; no judged work is authorized')


def admit(config, candidate, run, *, started_landing=False):
    require(isinstance(candidate, str) and SHA.fullmatch(candidate), 'Admission names the full candidate commit')
    def git(*args):
        answer = run(['git', '-C', str(config.live)] + list(args))
        require(answer.returncode == 0, 'The admission candidate is not a fast-forward of live' if args[0] == 'merge-base'
                else 'The admission candidate/base diff could not be read')
        return answer.stdout
    base = git('rev-parse', 'HEAD').strip()
    require(SHA.fullmatch(base), 'The admission release base is not a full commit')
    try:
        # Ordinary heads still do not consult the pipeline or start a gauntlet.
        # The binder checks ancestry here; admission keeps its judged check below.
        paths = source_diff(config.live, base, candidate, run=run, check_ancestry=False)
    except ValueError as error:
        raise Refused('The admission candidate/base diff could not be read: ' + str(error))
    guarded = train.guarded(paths)
    if not guarded:
        return {'judged': False, 'base_commit': base, 'head_commit': candidate, 'blocked': []}
    git('merge-base', '--is-ancestor', base, candidate)
    items = set()
    for line in git('log', '--format=%(trailers:key=Pipeline,valueonly,separator=%x1d)', base + '..' + candidate).splitlines():
        if line.strip():
            try:
                items.update(releases.trailer_items(line.strip()))
            except releases.Malformed as error:
                raise Refused(str(error))
    require(len(items) <= 1, 'A judged item pays its own gauntlet; land it alone, without bundling other items')
    # take_train authenticated a merge already on the release branch: finish
    # that landing before later ordinary approvals, retaining the guards above.
    if started_landing:
        return {'judged': True, 'base_commit': base, 'head_commit': candidate, 'guarded_paths': guarded,
                'items': sorted(items), 'blocked': []}
    pieces, waiting, version = approved(config, run)
    blocked = []
    for piece in pieces:
        pending = sorted(set(piece['items']) if piece.get('release') else (set(piece['items']) & waiting) - items)
        if not pending:
            continue
        freeze = piece['freeze']
        if freeze is not None:
            require(isinstance(freeze, dict), 'An approved piece has malformed freeze classification')
            source_version = freeze.get('source_piece_version') if piece.get('release') else piece['version']
            if piece.get('release'):
                require(isinstance(freeze.get('source_piece_id'), str) and bool(freeze['source_piece_id']) and
                        freeze['source_piece_id'] != piece['id'],
                        'An open release has no source freeze owner')
            require(type(freeze.get('judged')) is bool and
                    all(isinstance(freeze.get(k), str) and SHA.fullmatch(freeze[k])
                        for k in ('base_commit', 'head_commit')) and
                    isinstance(freeze.get('files'), list) and all(isinstance(p, str) and p for p in freeze['files']) and
                    type(freeze.get('piece_revision')) is int and type(source_version) is int and
                    0 < freeze['piece_revision'] <= source_version and
                    isinstance(freeze.get('by'), str) and bool(freeze['by']) and
                    type(freeze.get('at_ms')) is int and freeze['at_ms'] > 0 and
                    freeze['judged'] == bool(train.guarded(freeze['files'])),
                    'An approved piece has malformed or inconsistent freeze classification')
        if freeze is not None and freeze['judged']:
            continue
        blocked.append({'piece_id': piece['id'], 'items': pending, 'unclassified': freeze is None})
    if blocked:
        names = '; '.join('%s (items %s%s)' % (p['piece_id'], ', '.join(map(str, p['items'])),
                         ', classification missing' if p['unclassified'] else '') for p in blocked)
        raise Refused('Approved, unshipped work is waiting: %s. Land ordinary heads first (classify unknown freezes), '
                      'then carry and rerun the judged head on live; nothing else waits on its gauntlet.' % names)
    return {'judged': True, 'base_commit': base, 'head_commit': candidate, 'guarded_paths': guarded,
            'items': sorted(items), 'pipeline_revision': version, 'blocked': []}
