from __future__ import annotations

"""Actual Git and executable read-only CLI stubs; no real dispatch or account."""
import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest

from tools import deploy


class Admission987(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='gauntlet987-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.repo = self.root / 'repo'; self.repo.mkdir()
        self.git('init', '-q')
        self.git('config', 'user.email', 'scratch987@example.invalid')
        self.git('config', 'user.name', 'Scratch 987')
        (self.repo / 'first').write_text('base')
        self.base = self.commit('base')
        (self.repo / 'tools').mkdir()
        (self.repo / 'tools/deploy.py').write_text('# synthetic guarded change')
        self.head = self.commit('guarded\n\nPipeline: 987')
        (self.repo / 'ordinary').write_text('ordinary')
        self.ordinary = self.commit('ordinary\n\nPipeline: 988')
        self.git('checkout', '--detach', self.base)
        self.bin = self.repo / 'bin'; self.bin.mkdir()
        (self.bin / 'aim-next').write_text('''from __future__ import annotations
import json,sys
from pathlib import Path
root=Path(__file__).resolve().parents[2]
with (root/'reads').open('a') as f:f.write(json.dumps(sys.argv[1:])+'\\n')
if sys.argv[-1]=='binding':
 print(json.dumps({'v':1,'binding':{'instance_id':'scratch987','actor_id':'scratch987'}}))
else:
 request=json.load(sys.stdin);args=request['view']['args'];pages=json.loads((root/'pages').read_text())
 with (root/'cursors').open('a') as f:f.write(str(args.get('cursor',0))+'\\n')
 print(json.dumps(pages[str(args.get('cursor',0))]))
''')
        self.config = SimpleNamespace(live=self.repo, python='/usr/bin/python3', pipeline_broker=self.root/'core.sock',
            pipeline_profile=None, pipeline_outcome='work987', pipeline_instance='scratch987')
        self.pages({0:self.page()})

    def git(self, *args):
        return subprocess.check_output(['git', '-C', str(self.repo)] + list(args), text=True, stderr=subprocess.DEVNULL).strip()

    def commit(self, words):
        self.git('add', '.'); self.git('commit', '-qm', words)
        return self.git('rev-parse', 'HEAD')

    def page(self, pieces=None, approved=None, cursor=None, **more):
        return dict(instance_id='scratch987', workspace_id='work987', relevant_versions={'pipeline':1},
            pipeline={'enabled':True, 'ships_in_releases':True}, pieces=pieces or [], items=[],
            facts={'approved_waiting':approved or [], 'next_release':None}, next_cursor=cursor, **more)

    def piece(self, number, judged=False):
        return {'id':'p-'+str(number), 'items':[number], 'version':4, 'freeze':{'judged':judged,'base_commit':'a'*40,'head_commit':'b'*40,
                'files':['tools/deploy.py'] if judged else ['web/a'], 'piece_revision':2,'by':'scratch-builder','at_ms':1},
                'stage':{'stage_id':'wait'}, 'ahead':[]}

    def pages(self, values):
        (self.root/'pages').write_text(json.dumps(values))

    def test_987_blocker_after_first_page_names_piece_and_prevents_all_effects(self):
        self.pages({0:self.page(approved=[42],cursor=20),20:self.page([self.piece(42)], [42])})
        effects=[]
        with self.assertRaisesRegex(deploy.Refused, '42.*p-42|p-42.*42'):
            deploy.admit(self.config,self.head)
            effects.extend(['dispatch','merge','combine','stop'])
        self.assertEqual(effects,[])
        self.assertEqual((self.root/'cursors').read_text().splitlines(), ['0','20'])

    def test_987_ordinary_candidate_does_not_read_or_wait_on_pipeline(self):
        # Its base is the guarded source; only the ordinary delta is under consideration.
        self.git('checkout','--detach',self.head)
        (self.root/'pages').write_text('unavailable')
        self.assertFalse(deploy.admit(self.config,self.ordinary)['judged'])
        self.assertFalse((self.root/'reads').exists())

    def test_987_judged_only_and_unapproved_rows_are_not_ordinary_blockers(self):
        self.pages({0:self.page([self.piece(44,True),self.piece(45)], [44])})
        self.assertTrue(deploy.admit(self.config,self.head)['judged'])

    def test_987_unclassified_approved_row_refuses_explicitly(self):
        piece=self.piece(42);piece['freeze']=None
        self.pages({0:self.page([piece],[42])})
        with self.assertRaisesRegex(deploy.Refused,'classif'):
            deploy.admit(self.config,self.head)

    def test_987_foreign_unavailable_incomplete_and_cyclic_reads_do_not_authorize(self):
        for pages in [{0:{'error':{'code':'UNAVAILABLE'}}}, {0:{'instance_id':'foreign'}},
                      {0:self.page(cursor=0)}, {0:self.page(approved=[99])}]:
            with self.subTest(pages=pages):
                self.pages(pages)
                with self.assertRaises(deploy.Refused):deploy.admit(self.config,self.head)

    def test_987_missing_or_foreign_config_never_authorizes_judged_work(self):
        for field,value in [('pipeline_outcome',None),('pipeline_instance','foreign'),
                            ('pipeline_broker',None),('pipeline_profile',self.root/'second.json')]:
            before=getattr(self.config,field);setattr(self.config,field,value)
            try:
                with self.subTest(field=field),self.assertRaises(deploy.Refused):deploy.admit(self.config,self.head)
            finally:setattr(self.config,field,before)

    def test_987_malformed_and_future_freeze_never_skip_an_approved_piece(self):
        for field,value in [('head_commit','bad'),('piece_revision',99),('files',['web/a']),('by',None)]:
            piece=self.piece(42,True);piece['freeze'][field]=value
            self.pages({0:self.page([piece],[42])})
            with self.subTest(field=field),self.assertRaises(deploy.Refused):deploy.admit(self.config,self.head)

    def test_987_direct_deploy_floor_refuses_before_host_effects(self):
        self.pages({0:self.page([self.piece(42)],[42])})
        evidence=self.root/'not-created'
        from unittest import mock
        from test_tools_deploy import summary
        file=self.root/'summary.json';file.write_text(json.dumps(summary()))
        with mock.patch.object(deploy.release_check,'judge_summary',return_value=([],[])), \
                mock.patch.object(deploy,'runner_policy',side_effect=AssertionError('host effect reached')):
            with self.assertRaisesRegex(deploy.Refused,'p-42'):
                deploy.deploy(self.config,self.head,evidence,file,False)
        self.assertFalse((evidence/'00-plan.json').exists())

    def test_987_admission_cli_refuses_before_executable_effects_and_allows_ordinary(self):
        config=self.root/'deploy.json'
        config.write_text(json.dumps(dict(live=str(self.repo),instance=str(self.root),
            server_profile='server.json',runner_profile='runner.json',routes='routes.json',
            runner_id='scratch-runner',port=12345,pipeline_outcome='work987',
            pipeline_instance='scratch987',pipeline_broker=str(self.root/'core.sock'))))
        tool=Path(deploy.__file__).resolve()
        self.pages({0:self.page([self.piece(42)],[42])})
        # A human's exact pre-dispatch CLI; following executable is never reached on refusal.
        effect=self.root/'effects'
        program=self.root/'launcher.py'
        program.write_text("import subprocess,sys\nfrom pathlib import Path\n"
            "r=subprocess.run(sys.argv[1:])\n"
            "if r.returncode:sys.exit(r.returncode)\n"
            "Path("+repr(str(effect))+").write_text('dispatch,merge,combine,stop')\n")
        def launch(head):
            return subprocess.run(['/usr/bin/python3','-I',str(program),'/usr/bin/python3','-I',
                str(tool),'admit',head,'--config',str(config)],capture_output=True,text=True)
        refused=launch(self.head)
        self.assertNotEqual(refused.returncode,0,refused.stdout+refused.stderr)
        self.assertIn('p-42',refused.stderr);self.assertFalse(effect.exists())
        self.git('checkout','--detach',self.head)
        allowed=launch(self.ordinary)
        self.assertEqual(allowed.returncode,0,allowed.stdout+allowed.stderr)
        self.assertEqual(effect.read_text(),'dispatch,merge,combine,stop')

    def test_987_second_admission_reads_new_blocker_at_effect_boundary(self):
        deploy.admit(self.config,self.head)
        self.pages({0:self.page([self.piece(42)],[42])})
        effects=[]
        with self.assertRaises(deploy.Refused):
            deploy.admit(self.config,self.head)
            effects.append('merge')
        self.assertEqual(effects,[])

    def test_987_judged_head_naming_two_items_is_refused_without_a_broker_read(self):
        self.git('checkout','--detach',self.head)
        (self.repo/'more').write_text('second')
        head=self.commit('mixed\n\nPipeline: 987, 999')
        self.git('checkout','--detach',self.base)
        with self.assertRaisesRegex(deploy.Refused,'alone|bundl'):
            deploy.admit(self.config,head)
        self.assertFalse((self.root/'reads').exists())


if __name__ == '__main__':unittest.main()
