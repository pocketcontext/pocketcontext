#!/usr/bin/env python3
"""Exercise the POSIX installer offline; optionally install a real native binary."""

import argparse
import hashlib
import io
import os
from pathlib import Path
import platform
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "scripts" / "install.sh.in"
VERSION = "0.9.1"
NATIVE_BINARY = None


class InstallerFixture:
    def __init__(self, os_name="Linux", machine="x86_64", checksum=None):
        self.checksum = checksum or ("sha256sum" if shutil.which("sha256sum") else "shasum")
        self.temporary = tempfile.TemporaryDirectory(prefix="pocketcontext-installer-test-")
        self.root = Path(self.temporary.name)
        self.home = self.root / "home with spaces $(false)"
        self.home.mkdir()
        self.downloads = self.root / "downloads"
        self.downloads.mkdir()
        self.temporary_files = self.root / "temporary files"
        self.temporary_files.mkdir()
        self.tools = self.root / "tools"
        self.tools.mkdir()
        self.destination = self.home / ".local" / "bin" / "pocketcontext"
        self.log = self.root / "requests.log"
        self.script = TEMPLATE.read_text().replace("@VERSION@", VERSION).encode()
        self.environment = {
            "HOME": str(self.home),
            "TMPDIR": str(self.temporary_files),
            "PATH": str(self.tools),
            "LC_ALL": "C",
            "PC_TEST_FIXTURE": str(self.downloads),
            "PC_TEST_LOG": str(self.log),
            "PC_TEST_VERSION": VERSION,
            "PC_TEST_OS": os_name,
            "PC_TEST_MACHINE": machine,
            "PC_TEST_MACOS": "15.0",
        }
        for tool in ("tar", "gzip", "mktemp", "mkdir", "chmod", "mv", "rm", self.checksum):
            path = shutil.which(tool)
            if not path:
                self.close()
                raise RuntimeError(f"installer tests require {tool}")
            (self.tools / tool).symlink_to(path)
        self.wrapper("uname", """
import os, sys
print(os.environ['PC_TEST_OS' if sys.argv[1] == '-s' else 'PC_TEST_MACHINE'])
""")
        self.wrapper("sw_vers", """
import os
print(os.environ['PC_TEST_MACOS'])
""")
        self.wrapper("curl", """
import os, pathlib, sys
arguments = sys.argv[1:]
output = pathlib.Path(arguments[arguments.index('--output') + 1])
url = arguments[-1]
prefix = 'https://github.com/pocketcontext/pocketcontext/releases/download/v' + os.environ['PC_TEST_VERSION'] + '/'
if not url.startswith(prefix) or '/' in url[len(prefix):]:
    raise SystemExit('unexpected download URL: ' + url)
name = url[len(prefix):]
with open(os.environ['PC_TEST_LOG'], 'a') as log:
    log.write(url + '\\n')
if name == os.environ.get('PC_TEST_DOWNLOAD_FAIL'):
    output.write_bytes(b'interrupted download')
    raise SystemExit(22)
source = pathlib.Path(os.environ['PC_TEST_FIXTURE']) / name
if not source.is_file():
    raise SystemExit('missing local release fixture: ' + name)
output.write_bytes(source.read_bytes())
""")
        self.payload = f"#!/bin/sh\nprintf '%s\\n' 'pocketcontext {VERSION} (PocketBase v0.40.4; commit synthetic)'\n".encode()
        self.archive_name = self.archive_for(os_name, machine)
        self.archive([(tarfile.REGTYPE, "pocketcontext", self.payload)])

    @staticmethod
    def archive_for(os_name, machine):
        os_part = "darwin" if os_name == "Darwin" else "linux"
        arch = "arm64" if machine in ("aarch64", "arm64") else "amd64"
        return f"pocketcontext_{VERSION}_{os_part}_{arch}.tar.gz"

    def wrapper(self, name, code):
        path = self.tools / name
        if path.exists() or path.is_symlink():
            path.unlink()
        path.write_text(f"#!{sys.executable}\n" + code)
        path.chmod(0o755)

    def archive(self, members):
        archive = self.downloads / self.archive_name
        with tarfile.open(archive, "w:gz") as output:
            for kind, name, data in [*members,
                (tarfile.REGTYPE, "LICENSE", b"Synthetic license fixture\n"),
                (tarfile.REGTYPE, "THIRD_PARTY_NOTICES.txt", b"Synthetic notice fixture\n"),
            ]:
                entry = tarfile.TarInfo(name)
                entry.type = kind
                entry.mode = 0o7777  # Installer must discard archive permissions.
                if kind in (tarfile.SYMTYPE, tarfile.LNKTYPE):
                    entry.linkname = "outside"
                    output.addfile(entry)
                elif kind == tarfile.REGTYPE:
                    entry.size = len(data)
                    output.addfile(entry, io.BytesIO(data))
                else:
                    output.addfile(entry)
        self.checksums()

    def checksums(self, text=None):
        digest = hashlib.sha256((self.downloads / self.archive_name).read_bytes()).hexdigest()
        (self.downloads / "checksums.txt").write_text(
            text if text is not None else f"{digest}  {self.archive_name}\n"
        )

    def existing(self):
        self.destination.parent.mkdir(parents=True, exist_ok=True)
        self.destination.write_bytes(b"previous executable\n")
        self.destination.chmod(0o751)

    def run(self, script=None):
        return subprocess.run(
            ["/bin/sh"], input=self.script if script is None else script,
            cwd=self.root, env=self.environment, capture_output=True, timeout=20,
        )

    def close(self):
        self.temporary.cleanup()


class InstallerTests(unittest.TestCase):
    def fixture(self, **kwargs):
        fixture = InstallerFixture(**kwargs)
        self.addCleanup(fixture.close)
        return fixture

    def assert_clean(self, fixture):
        self.assertEqual(list(fixture.temporary_files.iterdir()), [])
        self.assertEqual(list(fixture.destination.parent.glob(".pocketcontext.*")), [])

    def assert_failure_preserves(self, fixture, message):
        result = fixture.run()
        self.assertNotEqual(result.returncode, 0, result.stdout.decode())
        self.assertIn(message.encode(), result.stderr)
        self.assertEqual(fixture.destination.read_bytes(), b"previous executable\n")
        self.assertEqual(stat.S_IMODE(fixture.destination.stat().st_mode), 0o751)
        self.assert_clean(fixture)

    def test_supported_platforms_and_version_bound_downloads(self):
        for os_name, machine in (
            ("Linux", "x86_64"), ("Linux", "amd64"),
            ("Linux", "aarch64"), ("Linux", "arm64"), ("Darwin", "arm64"),
        ):
            with self.subTest(os=os_name, machine=machine):
                fixture = self.fixture(os_name=os_name, machine=machine)
                result = fixture.run()
                self.assertEqual(result.returncode, 0, result.stderr.decode())
                self.assertEqual(fixture.destination.read_bytes(), fixture.payload)
                self.assertEqual(stat.S_IMODE(fixture.destination.stat().st_mode), 0o755)
                self.assertIn(b'export PATH="$HOME/.local/bin:$PATH"', result.stdout)
                self.assertEqual(fixture.log.read_text().splitlines(), [
                    f"https://github.com/pocketcontext/pocketcontext/releases/download/v{VERSION}/{name}"
                    for name in ("checksums.txt", fixture.archive_name)
                ])
                self.assert_clean(fixture)

    def test_existing_binary_is_replaced(self):
        fixture = self.fixture()
        fixture.existing()
        fixture.environment["PATH"] += os.pathsep + str(fixture.destination.parent)
        result = fixture.run()
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(fixture.destination.read_bytes(), fixture.payload)
        self.assertNotIn(b"Add it to PATH", result.stdout)
        self.assert_clean(fixture)

    def test_shasum_fallback(self):
        fixture = self.fixture(checksum="shasum")
        result = fixture.run()
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(fixture.destination.read_bytes(), fixture.payload)
        self.assert_clean(fixture)

    def test_unsupported_platforms_fail_before_download(self):
        for os_name, machine in (("Darwin", "x86_64"), ("Linux", "armv7l"), ("FreeBSD", "amd64")):
            with self.subTest(os=os_name, machine=machine):
                fixture = self.fixture(os_name=os_name, machine=machine)
                fixture.existing()
                self.assert_failure_preserves(fixture, "unsupported platform")
                self.assertFalse(fixture.log.exists())

    def test_missing_required_tools(self):
        for tool, message in (("curl", "required command is missing: curl"), ("checksum", "sha256sum or shasum is required")):
            with self.subTest(tool=tool):
                fixture = self.fixture()
                fixture.existing()
                (fixture.tools / (fixture.checksum if tool == "checksum" else tool)).unlink()
                self.assert_failure_preserves(fixture, message)

    def test_macos_minimum_version(self):
        for version, message in (("14.7", "macOS 15 or newer"), ("unknown", "could not detect macOS version")):
            with self.subTest(version=version):
                fixture = self.fixture(os_name="Darwin", machine="arm64")
                fixture.existing()
                fixture.environment["PC_TEST_MACOS"] = version
                self.assert_failure_preserves(fixture, message)
                self.assertFalse(fixture.log.exists())

    def test_interrupted_downloads_preserve_existing_binary(self):
        for checksum in (True, False):
            with self.subTest(checksum=checksum):
                fixture = self.fixture()
                fixture.existing()
                fixture.environment["PC_TEST_DOWNLOAD_FAIL"] = "checksums.txt" if checksum else fixture.archive_name
                self.assert_failure_preserves(fixture, "download failed")

    def test_checksum_mismatch(self):
        fixture = self.fixture()
        fixture.existing()
        fixture.checksums("0" * 64 + "  " + fixture.archive_name + "\n")
        self.assert_failure_preserves(fixture, "checksum mismatch")

    def test_invalid_checksum_entries(self):
        for kind in ("missing", "duplicate", "short", "nonhex", "trailing"):
            with self.subTest(kind=kind):
                fixture = self.fixture()
                fixture.existing()
                text = (fixture.downloads / "checksums.txt").read_text()
                invalid = {
                    "missing": text.replace(fixture.archive_name, "another.tar.gz"),
                    "duplicate": text + text,
                    "short": "abc  " + fixture.archive_name + "\n",
                    "nonhex": "z" * 64 + "  " + fixture.archive_name + "\n",
                    "trailing": text.rstrip() + " extra\n",
                }[kind]
                fixture.checksums(invalid)
                self.assert_failure_preserves(fixture, "checksum")

    def test_checksum_manifest_without_final_newline(self):
        fixture = self.fixture()
        fixture.checksums((fixture.downloads / "checksums.txt").read_text().rstrip("\n"))
        result = fixture.run()
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assert_clean(fixture)

    def test_unsafe_or_empty_archives(self):
        for kind in ("extra", "duplicate", "traversal", "symlink", "hardlink", "directory", "empty"):
            with self.subTest(kind=kind):
                fixture = self.fixture()
                fixture.existing()
                regular = (tarfile.REGTYPE, "pocketcontext", fixture.payload)
                members = {
                    "extra": [regular, (tarfile.REGTYPE, "extra", b"extra")],
                    "duplicate": [regular, regular],
                    "traversal": [(tarfile.REGTYPE, "../pocketcontext", fixture.payload)],
                    "symlink": [(tarfile.SYMTYPE, "pocketcontext", b"")],
                    "hardlink": [(tarfile.LNKTYPE, "pocketcontext", b"")],
                    "directory": [(tarfile.DIRTYPE, "pocketcontext", b"")],
                    "empty": [(tarfile.REGTYPE, "pocketcontext", b"")],
                }[kind]
                fixture.archive(members)
                self.assert_failure_preserves(fixture, "archive")
                self.assertFalse((fixture.root / "pocketcontext").exists())

    def test_corrupt_archive_with_matching_checksum(self):
        fixture = self.fixture()
        fixture.existing()
        (fixture.downloads / fixture.archive_name).write_bytes(b"not a gzip archive")
        fixture.checksums()
        self.assert_failure_preserves(fixture, "invalid release archive")

    def test_staging_failures_preserve_existing_binary(self):
        for tool in ("tar", "chmod", "mv"):
            with self.subTest(tool=tool):
                fixture = self.fixture()
                fixture.existing()
                if tool == "tar":
                    real_tar = shutil.which("tar")
                    fixture.wrapper("tar", f"""
import subprocess, sys
if sys.argv[1] == '-xOzf':
    sys.stdout.write('incomplete extraction')
    raise SystemExit(1)
raise SystemExit(subprocess.call([{real_tar!r}] + sys.argv[1:]))
""")
                else:
                    fixture.wrapper(tool, "raise SystemExit(1)\n")
                self.assert_failure_preserves(fixture, "pocketcontext:")

    def test_symlink_and_directory_destinations_are_rejected(self):
        for kind in ("symlink", "dangling", "directory"):
            with self.subTest(kind=kind):
                fixture = self.fixture()
                fixture.destination.parent.mkdir(parents=True)
                outside = fixture.root / "outside"
                outside.write_bytes(b"do not replace")
                if kind == "directory":
                    fixture.destination.mkdir()
                else:
                    fixture.destination.symlink_to(outside if kind == "symlink" else fixture.root / "absent")
                result = fixture.run()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(b"destination must be a regular file or absent", result.stderr)
                self.assertEqual(outside.read_bytes(), b"do not replace")
                self.assertFalse(fixture.log.exists())
                self.assert_clean(fixture)

    def test_invalid_home_and_unrendered_template(self):
        fixture = self.fixture()
        for value in ("", "relative/path"):
            fixture.environment["HOME"] = value
            result = fixture.run()
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b"HOME must be an absolute directory path", result.stderr)
        fixture.environment["HOME"] = str(fixture.home)
        result = fixture.run(TEMPLATE.read_bytes())
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b"invalid release version", result.stderr)
        self.assertFalse(fixture.log.exists())

    def test_truncated_pipe_never_starts_partial_function(self):
        fixture = self.fixture()
        fixture.existing()
        cuts = [
            fixture.script.index(b"    set -f") + 6,
            fixture.script.index(b"    pc_expected="),
            fixture.script.index(b"    mv -f"),
            fixture.script.rindex(b'\npocketcontext_install "$@"'),
        ]
        for cut in cuts:
            with self.subTest(cut=cut):
                fixture.run(fixture.script[:cut])
                self.assertFalse(fixture.log.exists())
                self.assertEqual(fixture.destination.read_bytes(), b"previous executable\n")
                self.assert_clean(fixture)

    @unittest.skipUnless(NATIVE_BINARY, "pass --binary to install and execute a native release artifact")
    def test_real_native_install_and_version(self):
        fixture = self.fixture(os_name=platform.system(), machine=platform.machine())
        if platform.system() == "Darwin":
            fixture.environment["PC_TEST_MACOS"] = platform.mac_ver()[0]
        fixture.archive([(tarfile.REGTYPE, "pocketcontext", NATIVE_BINARY.read_bytes())])
        result = fixture.run()
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(fixture.destination.read_bytes(), NATIVE_BINARY.read_bytes())
        version = subprocess.run(
            [str(fixture.destination), "--version"], cwd=fixture.root,
            env=fixture.environment, capture_output=True, text=True, timeout=20,
        )
        self.assertEqual(version.returncode, 0, version.stderr)
        self.assertRegex(version.stdout.strip(), rf"^pocketcontext {VERSION.replace('.', '[.]')} \(PocketBase v[0-9.]+; commit [0-9a-f]{{40}}\)$")
        self.assertFalse((fixture.root / "pb_data").exists())
        self.assertFalse((fixture.destination.parent / "pb_data").exists())
        self.assert_clean(fixture)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, help="also install and execute this native release binary")
    parser.add_argument("--version", default=VERSION, help="release version for rendering and version assertion")
    options, remaining = parser.parse_known_args()
    VERSION = options.version
    if options.binary:
        NATIVE_BINARY = options.binary.resolve(strict=True)
        InstallerTests.test_real_native_install_and_version.__unittest_skip__ = False
    unittest.main(argv=[sys.argv[0], *remaining], verbosity=2)
