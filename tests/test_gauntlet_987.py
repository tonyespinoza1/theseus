from __future__ import annotations

"""Real Core records, isolated scratch store; no live pipeline writes."""
import unittest
import sqlite3

import test_pipeline_steering as fixture

BASE = 'a' * 40
HEAD = 'b' * 40
STAGES = ({'stage_id': 'build', 'name': 'Build', 'rounds': 'together'},
          {'stage_id': 'review', 'name': 'Review', 'rounds': 'split'},
          {'stage_id': 'wait', 'name': 'Wait'},
          {'stage_id': 'checks', 'name': 'Checks', 'scope': 'release'})


class Freeze987(fixture.PipelineCase):
    def setUp(self):
        super().setUp()
        self.committed(self.on(ships_in_releases=True))
        self.stages(*STAGES)
        for n in range(3):
            self.add('Synthetic work ' + str(n), section='now', size='small')

    def piece(self, numbers, topic='piece-987'):
        self.build_topic(topic)
        return self.committed(self.command('pipeline.piece_start',
            {'numbers': numbers, 'topic_id': topic, 'stage_id': 'build'},
            {'workspace_id': 'w1'}, actor='owner'))['piece_id']

    def version(self, piece):
        return self.sql('SELECT version FROM pipeline_pieces WHERE id=?', (piece,))[0][0]

    def freeze(self, piece, files=None, actor='owner', expected=None, **more):
        return self.command('pipeline.freeze', dict(base_commit=BASE, head_commit=HEAD,
            files=['tools/deploy.py'] if files is None else files, **more), {'piece_id': piece},
            {'piece_revision': self.version(piece) if expected is None else expected}, actor=actor)

    def shown(self, piece):
        return self.store.view('owner', 'pipeline_freeze', {'piece_id': piece})

    def wait(self, piece):
        self.committed(self.command('pipeline.stage_enter', {'stage_id': 'wait', 'left_outcome': 'approved'},
            {'piece_id': piece}, {'piece_revision': self.version(piece)}, actor='owner'))

    def combine(self, pieces):
        if not self.sql("SELECT 1 FROM topics WHERE id='release-987'"):
            self.build_topic('release-987')
        return self.command('pipeline.combine', {'piece_ids': pieces, 'topic_id': 'release-987',
            'commit_sha': 'c' * 40}, {'workspace_id': 'w1'}, actor='owner')

    def test_987_actual_freeze_is_shared_by_views_and_keeps_source_and_provenance(self):
        piece = self.piece([1])
        self.assertIsNone(self.shown(piece).get('freeze'))
        result = self.committed(self.freeze(piece))
        value = result['freeze']
        self.assertTrue(value['judged'])
        self.assertEqual((value['base_commit'], value['head_commit'], value['files'], value['by']),
                         (BASE, HEAD, ['tools/deploy.py'], 'owner'))
        self.assertEqual(self.shown(piece)['freeze'], value)
        view = self.store.view('agent-b', 'pipeline', {'workspace_id': 'w1'})
        self.assertEqual(next(p for p in view['pieces'] if p['id'] == piece)['freeze'], value)
        self.assertEqual(self.sql('SELECT count(*) FROM pipeline_piece_freezes'), [(1,)])

    def test_987_ordinary_has_explicit_false_and_unchanged_freeze_is_a_noop(self):
        piece = self.piece([1])
        first = self.committed(self.freeze(piece, ['web/synthetic.mjs']))
        self.assertFalse(first['freeze']['judged'])
        version = self.version(piece)
        second = self.committed(self.freeze(piece, ['web/synthetic.mjs']))
        self.assertFalse(second['changed'])
        self.assertEqual(self.version(piece), version)
        self.assertEqual(first['freeze'], second['freeze'])

    def test_987_stale_foreign_and_malformed_inputs_leave_freeze_unchanged(self):
        piece = self.piece([1])
        before = self.version(piece)
        self.committed(self.freeze(piece))
        self.rejected(self.freeze(piece, expected=before), 'REVISION_CONFLICT')
        self.rejected(self.freeze(piece, actor='agent-b'), 'FORBIDDEN')
        for files in [['../tools/deploy.py'], ['/tools/deploy.py'], ['web/a', 'web/a'], ['web\\a']]:
            self.rejected(self.freeze(piece, files), 'INVALID_ARGUMENT')
        self.rejected(self.freeze(piece, judged=False), 'INVALID_ARGUMENT')
        self.assertEqual(self.sql('SELECT count(*) FROM pipeline_piece_freezes'), [(1,)])

    def test_987_changed_freeze_after_approval_refuses_and_retry_keeps_evidence(self):
        piece = self.piece([1])
        self.committed(self.freeze(piece))
        self.wait(piece)
        self.rejected(self.freeze(piece, ['web/synthetic.mjs']), 'PIECE_APPROVED')
        self.assertTrue(self.shown(piece)['freeze']['judged'])

    def test_987_known_judged_with_other_piece_refuses_before_any_combine_fact(self):
        first = self.piece([1])
        second = self.piece([2], 'other-987')
        self.committed(self.freeze(first))
        self.committed(self.freeze(second, ['web/synthetic.mjs']))
        self.wait(first); self.wait(second)
        before = self.sql('SELECT count(*) FROM pipeline_facts')
        self.rejected(self.combine([first, second]), 'JUDGED_BUNDLE')
        self.assertEqual(self.sql('SELECT count(*) FROM pipeline_facts'), before)
        self.assertEqual(self.sql('SELECT count(*) FROM pipeline_pieces WHERE kind=\'release\''), [(0,)])

    def test_987_judged_piece_with_two_items_refuses(self):
        piece = self.piece([1, 2])
        self.committed(self.freeze(piece))
        self.wait(piece)
        self.rejected(self.combine([piece]), 'JUDGED_BUNDLE')

    def test_987_lone_judged_item_combines(self):
        piece = self.piece([1])
        self.committed(self.freeze(piece))
        self.wait(piece)
        self.committed(self.combine([piece]))
        self.assertEqual(self.sql("SELECT count(*) FROM pipeline_pieces WHERE kind='release'"), [(1,)])

    def test_987_ordinary_only_frozen_bundle_combines(self):
        a = self.piece([1]); b = self.piece([2], 'second')
        for piece in (a, b):
            self.committed(self.freeze(piece, ['web/a']))
            self.wait(piece)
        self.committed(self.combine([a, b]))

    def test_987_judged_order_and_two_judged_pieces_both_refuse_atomically(self):
        a = self.piece([1]); b = self.piece([2], 'second')
        for piece in (a, b):
            self.committed(self.freeze(piece)); self.wait(piece)
        before = self.sql('SELECT * FROM pipeline_facts')
        for pieces in ([a,b], [b,a]):
            self.rejected(self.combine(pieces), 'JUDGED_BUNDLE')
            self.assertEqual(self.sql('SELECT * FROM pipeline_facts'), before)
        self.assertEqual(self.sql("SELECT count(*) FROM pipeline_pieces WHERE kind='release'"), [(0,)])

    def test_987_retry_refreeze_history_and_foreign_authority(self):
        piece = self.piece([1])
        expected = {'piece_revision': self.version(piece)}
        args = dict(base_commit=BASE, head_commit=HEAD, files=['web/a'])
        first = self.command('pipeline.freeze', args, {'piece_id':piece}, expected, op='freeze-once')
        self.committed(first)
        self.assertEqual(self.command('pipeline.freeze', args, {'piece_id':piece}, expected, op='freeze-once'), first)
        self.committed(self.freeze(piece))
        self.assertTrue(self.shown(piece)['freeze']['judged'])
        self.assertEqual(self.sql('SELECT count(*) FROM pipeline_piece_freezes'), [(2,)])
        for statement in ('UPDATE pipeline_piece_freezes SET judged=0', 'DELETE FROM pipeline_piece_freezes'):
            with self.assertRaises(sqlite3.IntegrityError):self.sql(statement)
        self.assertEqual(self.store.view('outsider','pipeline_freeze',{'piece_id':piece})['error']['code'], 'FORBIDDEN')
        self.rejected(self.store.command('owner',dict(v=1,instance_id='test-instance',operation_id='foreign-freeze',
            kind='pipeline.freeze',target={'piece_id':piece},args=args,expected={
            'piece_revision':self.version(piece),'authority':{'authority_id':'foreign','authority_epoch':'foreign'}})), 'AUTHORITY_CHANGED')

    def test_987_schema_upgrade_downgrade_preserves_rows_and_holds_frozen_proof(self):
        from aim_next.migrations import migrate, MigrationError
        from aim_next import Store
        from aim_next.store import SCHEMA_VERSION
        before = self.sql('SELECT id, title FROM workspaces')
        self.assertEqual(migrate(self.path,'test-instance',self.binding,'60')['schema_version'],'60')
        self.assertEqual(migrate(self.path,'test-instance',self.binding)['schema_version'],SCHEMA_VERSION)
        self.store=Store(self.path,'test-instance',expected_binding=self.binding)
        self.assertEqual(self.sql('SELECT id, title FROM workspaces'),before)
        piece=self.piece([1]);self.committed(self.freeze(piece))
        with self.assertRaises(MigrationError):migrate(self.path,'test-instance',self.binding,'60')
        self.assertTrue(self.shown(piece)['freeze']['judged'])

    def test_987_unclassified_lone_legacy_item_keeps_existing_combine_behavior(self):
        first = self.piece([1])
        self.wait(first)
        self.committed(self.combine([first]))


def load_tests(loader, tests, pattern):
    return unittest.TestSuite(Freeze987(name) for name in loader.getTestCaseNames(Freeze987)
                              if name.startswith('test_987_'))
