import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from cross_runner import INSERTED, run_cross
from runner import EXPECTED

CLIENTS=[{'id':name,'command':[name],'hashes':{name:'a'*64}} for name in ['typescript','python','go']]

class CrossRunnerTests(unittest.TestCase):
    def execute(self, bad_writer=False, bad_reader=False, setup_failure=False):
        actions=[]; stored={}
        def fake(command,request,timeout):
            scope=request['schema_scope'];action=request['action'];actions.append((command[0],action,scope))
            if action=='setup':
                stored[scope]=EXPECTED
                if setup_failure: raise ValueError('lost setup acknowledgment')
            elif action=='insert': stored[scope]=EXPECTED+([['corrupt']] if bad_writer else [INSERTED])
            elif action=='observe':
                if command[0] in ['typescript','python','go'] and bad_reader: rows=[['fabricated']]
                else: rows=stored[scope]
                return {'rows':rows,'artifact_hashes':request.get('artifact_hashes')}
            return {'artifact_hashes':request.get('artifact_hashes')}
        with patch.dict(os.environ,{'NEUTRON_TEST_DATABASE_URL':'private'}),patch('cross_runner.invoke',side_effect=fake):
            result=run_cross(CLIENTS,1)
        return result,actions
    def test_all_nine_pairs_have_isolated_fixtures_and_cleanup(self):
        result,actions=self.execute()
        self.assertEqual(result['executed'],9)
        scopes={scope for _,_,scope in actions};self.assertEqual(len(scopes),9)
        for scope in scopes:
            self.assertEqual([action for _,action,s in actions if s==scope],['setup','observe','insert','observe','observe','cleanup'])
    def test_writer_native_mismatch_fails(self):
        with self.assertRaisesRegex(ValueError,'writer differs'): self.execute(bad_writer=True)
    def test_reader_native_mismatch_fails(self):
        with self.assertRaisesRegex(ValueError,'reader artifacts'): self.execute(bad_reader=True)
    def test_no_database_and_nonfinite_timeout_fail(self):
        with patch.dict(os.environ,{},clear=True),self.assertRaises(ValueError): run_cross(CLIENTS,1)
        for value in [float('nan'),float('inf'),0]:
            with self.assertRaises(ValueError): run_cross(CLIENTS,value)

if __name__=='__main__': unittest.main()
