"""Shared helpers for preparing the added native harness files.

Only authored harness files are added to a reconstructed tree. They are never
mixed into the frozen upstream product files, whose bytes are re-verified.
"""
import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SHARED_TEMPLATE = ROOT / "shared" / "neutron_corpus_scenario_test.go.tmpl"
SCENARIO = ROOT / "shared" / "neutron_corpus_scenario.json"
PACKAGE_PLACEHOLDER = "package __PACKAGE__"

# Authored harness files placed in the original go-admin API test package. The
# destination names are fixed so the runbook and the comparison can refer to them.
ORIGINAL_TEST = ROOT / "original-native" / "neutron_corpus_native_test.go"
ORIGINAL_DESTINATION = "app/admin/apis"

CONVERTED_GO_FILES = ("service.go", "api.go", "neutron_corpus_converted_test.go")
CONVERTED_MODULE_TEMPLATE = ROOT / "converted" / "go.mod.tmpl"


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def render_shared(package: str) -> bytes:
    """Render the stdlib-only scenario helper for one Go package name."""
    text = SHARED_TEMPLATE.read_text()
    if text.count(PACKAGE_PLACEHOLDER) != 1:
        raise ValueError("shared scenario template must contain exactly one package placeholder")
    return text.replace(PACKAGE_PLACEHOLDER, "package " + package).encode()
