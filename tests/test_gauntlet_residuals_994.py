from __future__ import annotations

"""Actual scratch Core records and Git/CLI admission; never live board data."""
import unittest
from unittest import mock
import subprocess
from pathlib import Path
from types import SimpleNamespace

from test_gauntlet_987 import Freeze987
from test_gauntlet_admission_987 import Admission987
from tools import deploy
from aim_next.profile import write_profile


class Core994(Freeze987):
    def pipeline(self):
        value = self.store.view('owner', 'pipeline', {'workspace_id': 'w1'})
        self.assertNotIn('error', value, value)
        return value

    def snapshot(self):
        return {table: self.sql('SELECT * FROM ' + table) for table in
                ('pipeline_pieces', 'pipeline_stage_entries', 'pipeline_piece_members', 'pipeline_facts')}

    def test_994_unchanged_source_reclassifies_and_appends_while_approved(self):
        piece = self.piece([1])
        before = self.committed(self.freeze(piece, ['web/new-policy.mjs']))['freeze']
        self.wait(piece)
        version = self.version(piece)
        entries = self.sql('SELECT * FROM pipeline_stage_entries')
        with mock.patch('aim_next.piece_freeze.guarded', side_effect=lambda paths: paths):
            changed = self.committed(self.freeze(piece, ['web/new-policy.mjs']))
            self.assertTrue(changed['changed'], 'the same source can have a newly derived classification')
            self.assertTrue(changed['freeze']['judged'])
            self.assertEqual(changed['piece_revision'], version + 1)
            unchanged = self.committed(self.freeze(piece, ['web/new-policy.mjs']))
            self.assertFalse(unchanged['changed'])
            self.assertEqual(unchanged['freeze'], changed['freeze'])
            self.rejected(self.freeze(piece, ['web/changed-source.mjs']), 'PIECE_APPROVED')
        self.assertEqual(self.sql('SELECT judged FROM pipeline_piece_freezes ORDER BY seq'), [(0,), (1,)])
        self.assertEqual(self.sql('SELECT * FROM pipeline_stage_entries'), entries)
        self.assertFalse(before['judged'], 'the previous append-only record stays ordinary')

    def test_994_reclassification_in_build_still_obeys_permissions_and_revision(self):
        piece = self.piece([1])
        self.committed(self.freeze(piece, ['web/a']))
        version = self.version(piece)
        with mock.patch('aim_next.piece_freeze.guarded', side_effect=lambda paths: paths):
            self.rejected(self.freeze(piece, ['web/a'], actor='agent-b'), 'FORBIDDEN')
            self.rejected(self.freeze(piece, ['web/a'], expected=version - 1), 'REVISION_CONFLICT')
            self.assertEqual(self.version(piece), version)
            result = self.committed(self.freeze(piece, ['web/a']))
        self.assertTrue(result['freeze']['judged'])
        self.assertEqual(self.version(piece), version + 1)

    def test_994_multi_piece_bundle_requires_each_freeze_before_any_release_effect(self):
        first = self.piece([1])
        second = self.piece([2], 'other-994')
        self.committed(self.freeze(first, ['web/a']))
        self.wait(first); self.wait(second)
        self.build_topic('release-987')
        before = self.snapshot()
        self.rejected(self.combine([first, second]), 'FREEZE_REQUIRED')
        self.assertEqual(self.snapshot(), before, 'no release, member, stage or fact leaked from refusal')

    def test_994_multi_item_single_piece_also_requires_freeze(self):
        piece = self.piece([1, 2])
        self.wait(piece)
        self.build_topic('release-987')
        before = self.snapshot()
        self.rejected(self.combine([piece, piece]), 'FREEZE_REQUIRED')
        self.assertEqual(self.snapshot(), before)

    def test_994_lone_unclassified_item_keeps_its_existing_combine_path(self):
        piece = self.piece([1])
        self.wait(piece)
        release = self.committed(self.combine([piece, piece]))['piece_id']
        self.assertEqual(self.pipeline()['release_piece']['id'], release)
        self.assertIsNone(self.pipeline()['release_piece']['freeze'])

    def test_994_release_keeps_the_source_freeze_and_its_origin(self):
        piece = self.piece([1])
        expected = self.committed(self.freeze(piece))['freeze']
        self.wait(piece)
        release = self.committed(self.combine([piece]))['piece_id']
        value = self.pipeline()['release_piece']
        self.assertEqual(value['id'], release)
        self.assertIsNotNone(value['freeze'], 'the Judged mark survives combination')
        self.assertEqual({key: value['freeze'][key] for key in expected}, expected)
        self.assertEqual(value['freeze']['source_piece_id'], piece)
        self.assertEqual(value['freeze']['source_piece_version'], self.version(piece))

    def test_994_release_uses_latest_freeze_of_its_own_combined_members(self):
        first = self.piece([1])
        self.committed(self.freeze(first, ['web/first']))
        second = self.piece([2], 'second-994')
        expected = self.committed(self.freeze(second, ['web/second']))['freeze']
        unrelated = self.piece([3], 'unrelated-994')
        self.committed(self.freeze(unrelated))
        self.wait(first); self.wait(second)
        self.committed(self.combine([first, second]))
        value = self.pipeline()['release_piece']['freeze']
        self.assertIsNotNone(value)
        self.assertEqual({key: value[key] for key in expected}, expected)
        self.assertFalse(value['judged'], 'an unrelated newer judged freeze is not this release')
        self.assertEqual(value['source_piece_id'], second)

    def test_994_real_core_combined_release_blocks_real_git_admission(self):
        piece = self.piece([2])
        self.committed(self.freeze(piece, ['web/ordinary']))
        self.wait(piece)
        release = self.committed(self.combine([piece]))['piece_id']
        self.assertEqual(self.pipeline()['facts']['approved_waiting'], [])
        repo = Path(self.temp.name).resolve() / 'admission-source'
        repo.mkdir()
        def git(*args):
            return subprocess.check_output(['git', '-C', str(repo)] + list(args),
                    text=True, stderr=subprocess.DEVNULL).strip()
        git('init', '-q'); git('config', 'user.email', 'scratch994@example.invalid')
        git('config', 'user.name', 'Scratch 994')
        (repo / 'first').write_text('base')
        git('add', '.'); git('commit', '-qm', 'base'); base = git('rev-parse', 'HEAD')
        (repo / 'tools').mkdir(); (repo / 'tools/deploy.py').write_text('# guarded scratch source')
        git('add', '.'); git('commit', '-qm', 'Candidate\n\nPipeline: 994'); head = git('rev-parse', 'HEAD')
        git('checkout', '--detach', base)
        # Run the actual CLI/Store on the fixture's own identity and records.
        # Only its executable location is adapted to the disposable Git repo.
        (repo / 'bin').mkdir()
        entry = Path(__file__).resolve().parents[1] / 'bin/aim-next'
        (repo / 'bin/aim-next').write_text('import runpy\nrunpy.run_path(' + repr(str(entry)) + ', run_name="__main__")\n')
        profile = Path(self.temp.name).resolve() / 'admission-reader.json'
        write_profile(profile, str(self.path.resolve()), 'test-instance', 'owner', binding=self.binding)
        config = SimpleNamespace(live=repo, python='/usr/bin/python3', pipeline_broker=None,
                pipeline_profile=profile, pipeline_instance='test-instance', pipeline_outcome='w1')
        with self.assertRaisesRegex(deploy.Refused, release):
            deploy.admit(config, head)


class Admission994(Admission987):
    def release_page(self, judged=False, unclassified=False):
        value = self.page()
        value['facts']['next_release'] = {'release': 7, 'piece_id': 'release-42',
                'items': [42], 'stage': {'stage_id': 'checks'}}
        release = self.piece(42, judged)
        release.update(id='release-42', kind='release', version=1,
                       release_number=7, stage={'stage_id': 'checks'})
        if unclassified:
            release['freeze'] = None
        else:
            release['freeze'].update(source_piece_id='p-42', source_piece_version=4)
        value['release_piece'] = release
        return value

    def test_994_combined_ordinary_release_is_still_unlanded(self):
        self.pages({0: self.release_page()})
        with self.assertRaisesRegex(deploy.Refused, 'release-42.*42|42.*release-42'):
            deploy.admit(self.config, self.head)

    def test_994_combined_unclassified_release_cannot_be_skipped(self):
        self.pages({0: self.release_page(unclassified=True)})
        with self.assertRaisesRegex(deploy.Refused, 'classif'):
            deploy.admit(self.config, self.head)

    def test_994_judged_release_uses_source_version_not_the_release_piece_version(self):
        self.pages({0: self.release_page(judged=True)})
        self.assertTrue(deploy.admit(self.config, self.head)['judged'])

    def test_994_open_release_facts_must_be_complete_and_match_the_read_owner(self):
        for change in ('missing-owner', 'wrong-owner', 'wrong-items', 'missing-fact'):
            value = self.release_page(judged=True)
            if change == 'missing-owner':
                del value['release_piece']
            elif change == 'wrong-owner':
                value['release_piece']['id'] = 'other'
            elif change == 'wrong-items':
                value['release_piece']['items'] = [999]
            else:
                del value['facts']['next_release']
            self.pages({0: value})
            with self.subTest(change=change), self.assertRaises(deploy.Refused):
                deploy.admit(self.config, self.head)

    def test_994_malformed_release_freeze_is_a_refusal_not_a_traceback(self):
        for freeze in ([], 'bad', False):
            value = self.release_page()
            value['release_piece']['freeze'] = freeze
            self.pages({0: value})
            with self.subTest(freeze=freeze), self.assertRaises(deploy.Refused):
                deploy.admit(self.config, self.head)

    def test_994_open_release_is_not_excluded_by_candidate_item_identity(self):
        value = self.release_page()
        value['facts']['next_release']['items'] = [987]
        value['release_piece']['items'] = [987]
        self.pages({0: value})
        with self.assertRaisesRegex(deploy.Refused, 'release-42'):
            deploy.admit(self.config, self.head)

    def test_994_open_release_cannot_change_between_pages(self):
        first = self.release_page(judged=True); first['next_cursor'] = 20
        second = self.release_page(judged=True)
        second['facts']['next_release']['items'] = [99]
        second['release_piece']['items'] = [99]
        self.pages({0: first, 20: second})
        with self.assertRaisesRegex(deploy.Refused, 'changed.*release'):
            deploy.admit(self.config, self.head)


def load_tests(loader, tests, pattern):
    return unittest.TestSuite(case(name) for case in (Core994, Admission994)
            for name in loader.getTestCaseNames(case) if name.startswith('test_994_'))
