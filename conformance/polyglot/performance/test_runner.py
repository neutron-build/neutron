"""Pure calibration regressions; no database or timing campaigns."""
import importlib.util
import json
from pathlib import Path
import unittest

spec=importlib.util.spec_from_file_location('performance_runner',Path(__file__).with_name('runner.py'))
runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)

class CalibrationTests(unittest.TestCase):
    def setUp(self): self.profile=json.loads(runner.PROFILE.read_text())
    def samples(self,values): return [{'elapsed_ns':int(x*1000000)} for x in values]
    def test_stable_aa_accepts(self):
        self.assertTrue(runner.calibration(self.samples([100,101,100,101,100,101,100,101]),self.profile)['accepted'])
    def test_consistently_slower_b_fails_despite_low_variance(self):
        result=runner.calibration(self.samples([100,106]*4),self.profile)
        self.assertIn('median-drift',result['reasons'])
    def test_one_excursion_refuses_cherry_picked_median(self):
        result=runner.calibration(self.samples([100,100,100,100,100,100,100,130]),self.profile)
        self.assertIn('pair-drift',result['reasons'])
    def test_short_stable_samples_do_not_qualify(self):
        self.assertIn('sample-too-short',runner.calibration(self.samples([5]*8),self.profile)['reasons'])
    def test_slow_whole_pairs_are_detected(self):
        self.assertIn('variance',runner.calibration(self.samples([100,100,200,200,100,100,200,200]),self.profile)['reasons'])

if __name__=='__main__': unittest.main()
