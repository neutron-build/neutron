#!/usr/bin/env python3
"""Exercise the actual POSIX installer with fake HTTP and platform commands.

Downloads are real archive/checksum bytes; every refused case preserves an
existing installation. No network, privileged writes or native cross-builds.
"""
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
INSTALLER = ROOT / "scripts/install.sh"


class InstallerTests(unittest.TestCase):
    def run_case(self, system="Linux", machine="x86_64", problem=None,
                 hash_tool="sha256sum", pinned=True, wrapper=False):
        with tempfile.TemporaryDirectory(prefix="neutron-installer-") as tmp:
            tmp = Path(tmp)
            commands = tmp / "bin"
            commands.mkdir()
            # Isolated PATH makes missing hashing tools a genuine condition.
            for name in ["awk", "grep", "head", "sed", "mktemp", "rm", "tar",
                         "mkdir", "cp", "chmod", "mv", "dirname", "sh", "gzip"]:
                os.symlink(shutil.which(name), commands / name)
            if hash_tool:
                os.symlink(shutil.which(hash_tool), commands / hash_tool)
            (commands / "uname").write_text(
                f"#!{sys.executable}\nimport os,sys\n"
                "print(os.environ['TEST_SYSTEM' if sys.argv[1]=='-s' else 'TEST_MACHINE'])\n"
            )
            (commands / "curl").write_text(
                f"#!{sys.executable}\nimport os,sys,pathlib\n"
                "a=sys.argv[1:]; u=a[-1]; root=pathlib.Path(os.environ['TEST_FIXTURE'])\n"
                "with (root/'requests').open('a') as f: f.write(u+'\\n')\n"
                "if os.environ.get('TEST_DOWNLOAD_FAIL')=='1': sys.exit(22)\n"
                "if '/releases' in u and '/download/' not in u: "
                "print((root/'releases.json').read_text()); sys.exit(0)\n"
                "name=u.rsplit('/',1)[-1]; p=root/name\n"
                "if not p.exists(): sys.exit(22)\n"
                "pathlib.Path(a[a.index('-o')+1]).write_bytes(p.read_bytes())\n"
            )
            for name in ["uname", "curl"]:
                (commands / name).chmod(0o755)
            osname = {"Darwin": "darwin", "Linux": "linux"}.get(system, "linux")
            arch = "arm64" if machine in ["aarch64", "arm64"] else "amd64"
            archive = f"neutron_0.9.1_{osname}_{arch}.tar.gz"
            payload = b"#!/bin/sh\necho neutron cli 0.9.1\n"
            with tarfile.open(tmp / archive, "w:gz") as tar:
                item = tarfile.TarInfo("wrong" if problem == "wrong-entry" else "neutron")
                item.mode = 0o755
                if problem == "symlink":
                    item.type = tarfile.SYMTYPE
                    item.linkname = "/etc/passwd"
                    tar.addfile(item)
                else:
                    item.size = len(payload)
                    tar.addfile(item, io.BytesIO(payload))
            checksum = hashlib.sha256((tmp / archive).read_bytes()).hexdigest()
            if problem == "mismatch":
                checksum = "0" * 64
            if problem == "malformed":
                checksum = "x" * 64
            lines = f"{checksum}  {archive}\n"
            if problem == "missing-entry":
                lines = f"{checksum}  {archive}.other\n"
            if problem == "duplicate":
                lines *= 2
            if problem == "substring":
                lines += f"{'0' * 64}  {archive}.other\n"
            (tmp / "checksums.txt").write_text(lines)
            (tmp / "releases.json").write_text(json.dumps([
                {"tag_name": "nucleus/v2.0.0"}, {"tag_name": "cli/v0.9.1"}
            ]))
            dest = tmp / "install with spaces"
            dest.mkdir()
            (dest / "neutron").write_text("old binary")
            env = dict(os.environ, PATH=str(commands), TEST_SYSTEM=system,
                       TEST_MACHINE=machine, TEST_FIXTURE=str(tmp),
                       NEUTRON_INSTALL_DIR=str(dest))
            env.pop("NEUTRON_VERSION", None)
            if pinned:
                env["NEUTRON_VERSION"] = "0.9.1"
            if problem == "invalid-version":
                env["NEUTRON_VERSION"] = "../bad"
            if problem == "download":
                env["TEST_DOWNLOAD_FAIL"] = "1"
            script = ROOT / "cli/scripts/install.sh" if wrapper else INSTALLER
            result = subprocess.run(["/bin/sh", str(script)], env=env,
                                    capture_output=True, text=True)
            successful = problem in [None, "substring"] and system in ["Linux", "Darwin"] and machine in ["arm64", "aarch64", "amd64", "x86_64"] and hash_tool
            if successful:
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual((dest / "neutron").read_bytes(), payload)
                self.assertTrue((dest / "neutron").stat().st_mode & 0o111)
                self.assertIn("neutron version", result.stdout)
                requests = (tmp / "requests").read_text().splitlines()
                self.assertIn(f"https://github.com/neutron-build/neutron/releases/download/cli/v0.9.1/{archive}", requests)
            else:
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertEqual((dest / "neutron").read_text(), "old binary")
            return result

    def test_four_release_platforms(self):
        for system, arch in [("Darwin", "arm64"), ("Darwin", "x86_64"),
                             ("Linux", "aarch64"), ("Linux", "amd64")]:
            with self.subTest(system=system, arch=arch):
                self.run_case(system, arch)

    def test_shasum_fallback(self):
        self.run_case(hash_tool="shasum")

    def test_latest_cli_ignores_other_product_tags(self):
        self.run_case(pinned=False)

    def test_repository_entry_point(self):
        self.run_case(wrapper=True)

    def test_exact_checksum_filename(self):
        self.run_case(problem="substring")

    def test_checksum_and_archive_refusals_preserve_install(self):
        for problem in ["mismatch", "malformed", "missing-entry", "duplicate",
                        "wrong-entry", "symlink", "invalid-version", "download"]:
            with self.subTest(problem=problem):
                self.run_case(problem=problem)

    def test_missing_hash_tool_fails_closed(self):
        result = self.run_case(hash_tool=None)
        self.assertIn("SHA-256 verification requires", result.stderr)

    def test_unsupported_platforms(self):
        for system in ["MINGW64_NT", "MSYS_NT", "CYGWIN_NT", "FreeBSD"]:
            with self.subTest(system=system):
                self.run_case(system=system)
        self.run_case(machine="riscv64")

    def test_hosted_artifact_is_canonical(self):
        subprocess.run(["sh", str(ROOT / "cli/installer-host/prepare.sh")], check=True)
        self.assertEqual(INSTALLER.read_bytes(), (ROOT / "cli/installer-host/dist/install.sh").read_bytes())


if __name__ == "__main__":
    unittest.main(verbosity=2)
