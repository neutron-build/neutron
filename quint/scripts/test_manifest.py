import copy
import json
import unittest
from manifest import inventory, ROOT

class InventoryTest(unittest.TestCase):
    def test_actual_inventory(self):
        self.assertGreater(len(inventory()), 0)
    def test_incomplete_duplicate_counts_and_runnable_omission_fail(self):
        actual = json.loads((ROOT / "manifest.json").read_text())
        for defect in ("file", "duplicate", "module", "count", "invariant"):
            with self.subTest(defect=defect):
                m = copy.deepcopy(actual)
                if defect == "file": m["files"].pop()
                if defect == "duplicate": m["files"].append(m["files"][0])
                if defect == "module": m["files"][0]["modules"].pop()
                if defect == "count": m["files"][0]["modules"][0]["expectedTests"] += 1
                if defect == "invariant": next(e for e in m["files"] if "invariant" in e).pop("invariant")
                with self.assertRaises(ValueError): inventory(m)

if __name__ == "__main__": unittest.main()
