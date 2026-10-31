import copy
import json
from pathlib import Path
import unittest
from check_schema import classifications, ROOT

class ClassificationTest(unittest.TestCase):
    def setUp(self):
        self.manifest = json.loads((ROOT / "golden/manifest.json").read_text())
    def test_actual_total_partition(self):
        groups = classifications(self.manifest, ROOT / "golden")
        self.assertEqual(sum(map(len, groups.values())), len(self.manifest["invalid"]))
    def test_missing_duplicate_omitted_and_bad_path_fail(self):
        for defect in ("missing", "duplicate", "omitted", "path", "unknown"):
            with self.subTest(defect=defect):
                m = copy.deepcopy(self.manifest)
                if defect == "missing": m["invalid"][0].pop("schemaExpectation")
                if defect == "duplicate": m["invalid"].append(m["invalid"][0])
                if defect == "omitted": m["invalid"].pop()
                if defect == "path": m["invalid"][0]["document"] = "../outside.json"
                if defect == "unknown": m["invalid"][0]["schemaExpectation"] = "skip"
                with self.assertRaises(ValueError): classifications(m, ROOT / "golden")

if __name__ == "__main__": unittest.main()
