from __future__ import annotations

"""Exercise the actual deploy's second admission, with isolated host effects."""
import json
import subprocess
import unittest
from unittest import mock

from tools import deploy
import test_tools_deploy as fixture
import test_tools_train as trains


class DeployBoundary987(fixture.DeployCase):
    def test_987_blocker_arriving_after_runner_policy_stops_actual_deploy(self):
        self.config=self.config._replace(pipeline_outcome='w-scratch',pipeline_instance='scratch',
            pipeline_broker=self.instance/'scratch.sock')
        ordinary={'id':'p-42','items':[42],'freeze':None,'version':1,'stage':{'stage_id':'wait'}}
        state={'blocked':False,'reads':0}
        fake=fixture.FakeHost(self.config,changed=['tools/deploy.py'])
        def host(argv,**kw):
            if argv[0]=='git' and '--format=%(trailers:key=Pipeline,valueonly,separator=%x1d)' in argv:
                return subprocess.CompletedProcess(argv,0,'987\n','')
            if argv[-1:] == ['binding']:
                return subprocess.CompletedProcess(argv,0,json.dumps({'binding':{'instance_id':'scratch'}}),'')
            if argv[-2:] == ['view','-']:
                state['reads']+=1
                page={'instance_id':'scratch','workspace_id':'w-scratch','relevant_versions':{'pipeline':1},
                    'pipeline':{'enabled':True,'ships_in_releases':True},'items':[],
                    'pieces':[ordinary] if state['blocked'] else [],
                    'facts':{'approved_waiting':[42] if state['blocked'] else [],'next_release':None},'next_cursor':None}
                return subprocess.CompletedProcess(argv,0,json.dumps(page),'')
            return fake(argv,**kw)
        def policy(*args):
            state['blocked']=True
            return {'kind':'restart','reason':'synthetic boundary'}
        with mock.patch.object(deploy,'runner_policy',side_effect=policy):
            with self.assertRaisesRegex(deploy.Refused,'p-42'):self.deploy(host,dry=True)
        self.assertEqual(state['reads'],2)
        self.assertEqual(self.kills,[])
        self.assertFalse(any(a[0]=='git' and 'merge' in a for a in fake.calls))
        self.assertEqual(fake.written,[])
        self.assertFalse((self.evidence/'02-checks.json').exists())



class MergedTrain987(trains.TrainCase):
    def setUp(self):
        super().setUp()
        self.head=self.branch('guarded-987',self.L,[('tools/deploy.py','# synthetic judged source\n')])
        self.git(self.live,'fetch','-q',str(self.work),self.head)
        self.judgment={'producer_runs':[101],'verdict_checks':[201],'audit':'m-scratch-review'}
        self.waiting=False
        self.pipeline_reads=0

    def waiting_host(self):
        fake=self.host()
        def run(argv,**kwargs):
            if '--broker' in argv and argv[-2:]==['view','-']:
                self.pipeline_reads+=1
                ordinary={'id':'p-later-ordinary','items':[42],'version':2,'stage':{'stage_id':'wait'},
                    'freeze':{'judged':False,'base_commit':self.L,'head_commit':self.head,'files':['web/a'],
                        'piece_revision':2,'by':'scratch-builder','at_ms':1}}
                page={'instance_id':'i-1','workspace_id':'scratch-pipeline','relevant_versions':{'pipeline':1},
                    'pipeline':{'enabled':True,'ships_in_releases':True},'pieces':[ordinary] if self.waiting else [],
                    'items':[],'facts':{'approved_waiting':[42] if self.waiting else [],'next_release':None},'next_cursor':None}
                return subprocess.CompletedProcess(argv,0,json.dumps(page),'')
            return fake(argv,**kwargs)
        return run

    def merged_train(self,heads=None):
        self.merged=self.merge(self.head)
        self.assertEqual(self.git(self.origin,'rev-parse','refs/heads/'+trains.BRANCH),self.merged)
        self.github=self.hub(runs=[trains.listed(101,self.head)],
            checks=[trains.listed(201,self.head,conclusion='failure',name='Verdict')])
        return self.train(self.merged,heads or [self.head],kind='judged-first',judgment=self.judgment)

    def test_987_authenticated_merged_train_does_not_wait_on_later_ordinary_approval(self):
        host=self.waiting_host()
        with mock.patch.object(deploy,'run',host):
            self.assertTrue(deploy.admit(self.config,self.head)['judged'])
        train=self.merged_train()
        self.waiting=True  # Approval arrives AFTER the real merge/push to the release branch.
        with mock.patch.object(deploy,'run',host):
            with self.assertRaisesRegex(deploy.Refused,'p-later-ordinary'):
                deploy.admit(self.config,self.merged)  # Pre-merge/default callers still enforce waiting.
        reads=self.pipeline_reads
        observed=[];original=deploy.admit
        def capture(*args,**kwargs):
            answer=original(*args,**kwargs);observed.append((kwargs,answer));return answer
        with mock.patch.object(deploy,'admit',side_effect=capture):
            self.trained(host,train,self.merged,self.github)
        self.assertEqual(self.pipeline_reads,reads,'already-started landing does not recheck later approvals')
        self.assertEqual(len(observed),2,'both deploy floors still classify the authenticated merge')
        for kwargs,answer in observed:
            self.assertTrue(kwargs['started_landing'])
            self.assertTrue(answer['judged'])
            self.assertEqual(answer['items'],[481])
            self.assertIn('tools/deploy.py',answer['guarded_paths'])
        recorded=self.evidence_of('02-release')['record']['measurements']['train']['judgment']
        self.assertEqual(recorded['verdict_checks'][0]['conclusion'],'failure')
        self.assertTrue(any('/compare/' in path for path in self.github.asked))
        self.assertTrue(any('/pulls/' in path for path in self.github.asked))
        self.assertEqual(self.git(self.live,'rev-parse','HEAD'),self.L,'dry-run leaves live unchanged')

    def test_987_authenticated_merged_train_still_refuses_multiple_items(self):
        self.write('extra','synthetic second item')
        self.head=self.commit('Mixed\n\nPipeline: 481, 482')
        train=self.merged_train(heads=[self.head,self.head])
        host=self.waiting_host();self.waiting=True
        with self.assertRaisesRegex(deploy.Refused,'alone|bundl'):
            self.deployed(host,train,self.merged,self.github)
        self.assertTrue(self.evidence_of('00-train')['judgment'],'train authentication completed before bundle refusal')
        self.assertFalse((self.evidence/'00-plan.json').exists())
        self.assertEqual(self.pipeline_reads,0,'bundle refusal precedes any waiting read')


def load_tests(loader,tests,pattern):
    return unittest.TestSuite([DeployBoundary987('test_987_blocker_arriving_after_runner_policy_stops_actual_deploy')] +
        [MergedTrain987(name) for name in loader.getTestCaseNames(MergedTrain987) if name.startswith('test_987_')])
