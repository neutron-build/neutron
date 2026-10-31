import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from validate_contracts import validate
ROOT=Path(__file__).resolve().parents[3]/'contracts/data'

class FixtureTests(unittest.TestCase):
    def test_repository_fixtures_validate(self):
        self.assertEqual(validate(ROOT)['runtime_compliance'],'unverified')
    def test_lost_update_state_refuses(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); shutil.copytree(ROOT/'values',root/'values'); shutil.copytree(ROOT/'execution',root/'execution')
            path=root/'values/v1/write-states.json'; data=json.loads(path.read_text()); data['cases'][1]['inputs'].pop(); path.write_text(json.dumps(data))
            with self.assertRaises(ValueError): validate(root)
    def test_future_version_refuses(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); shutil.copytree(ROOT/'values',root/'values'); shutil.copytree(ROOT/'execution',root/'execution')
            path=root/'execution/v1/profiles.json'; data=json.loads(path.read_text()); data['version']=2; path.write_text(json.dumps(data))
            with self.assertRaises(ValueError): validate(root)
