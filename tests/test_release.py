"""Check release integrity failures and publication retry boundaries offline."""

import argparse
import importlib.util
import io
import json
import os
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location("release", Path(__file__).resolve().parents[1] / "scripts/release.py")
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)
REVISION = "a" * 40
VERSION = "0.9.1"


def archive(path, entries=None):
    entries = entries or [("pocketcontext", 0o755), ("LICENSE", 0o644), ("THIRD_PARTY_NOTICES.txt", 0o644)]
    with tarfile.open(path, "w:gz") as bundle:
        for name, mode in entries:
            item = tarfile.TarInfo(name)
            item.mode, item.size = mode, 7
            bundle.addfile(item, io.BytesIO(b"fixture"))


class ReleaseArtifacts(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.artifacts = self.root / "artifacts"
        self.artifacts.mkdir()
        for platform in release.PLATFORMS:
            path = self.artifacts / release.archive_name(VERSION, platform)
            archive(path)
            (self.artifacts / f"{platform}.json").write_text(json.dumps({
                "version": VERSION, "commit": REVISION, "platform": platform,
                "archive": path.name, "sha256": release.sha256(path)}))
        self.args = argparse.Namespace(version=VERSION, commit=REVISION,
                                       artifacts=str(self.artifacts), output=str(self.root / "release"))

    def test_complete_assembly_binds_installer_and_checksums(self):
        release.assemble(self.args)
        output = Path(self.args.output)
        self.assertEqual(len(release.validate_assets(output, VERSION)), 5)
        self.assertIn(VERSION, (output / "install.sh").read_text())
        self.assertNotIn("@VERSION@", (output / "install.sh").read_text())

    def test_reject_mixed_commits(self):
        path = self.artifacts / "linux_arm64.json"
        manifest = json.loads(path.read_text())
        manifest["commit"] = "b" * 40
        path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError, "Inconsistent release manifest"):
            release.assemble(self.args)

    def test_reject_corrupt_downloaded_artifact(self):
        path = self.artifacts / release.archive_name(VERSION, "linux_arm64")
        path.write_bytes(path.read_bytes() + b"corruption")
        with self.assertRaisesRegex(ValueError, "Artifact checksum mismatch"):
            release.assemble(self.args)

    def test_reject_missing_platform(self):
        (self.artifacts / "darwin_arm64.json").unlink()
        with self.assertRaisesRegex(ValueError, "exactly one manifest"):
            release.assemble(self.args)

    def test_reject_archive_paths_and_links(self):
        path = self.root / "bad.tar.gz"
        archive(path, [("../pocketcontext", 0o755)])
        with self.assertRaisesRegex(ValueError, "archive layout"):
            release.check_archive(path)
        with tarfile.open(path, "w:gz") as bundle:
            item = tarfile.TarInfo("pocketcontext")
            item.type, item.linkname = tarfile.SYMTYPE, "/tmp/untrusted"
            bundle.addfile(item)
        with self.assertRaisesRegex(ValueError, "archive layout"):
            release.check_archive(path)

    def test_reject_checksum_duplicate_and_unexpected_asset(self):
        release.assemble(self.args)
        output = Path(self.args.output)
        sums = output / "checksums.txt"
        sums.write_text(sums.read_text() + sums.read_text().splitlines()[0] + "\n")
        with self.assertRaisesRegex(ValueError, "Duplicate checksum"):
            release.validate_assets(output, VERSION)
        (output / "unexpected").touch()
        with self.assertRaisesRegex(ValueError, "exactly the five"):
            release.validate_assets(output, VERSION)


class PublicationBoundaries(unittest.TestCase):
    def setUp(self):
        self.args = argparse.Namespace(version=VERSION, commit=REVISION, assets="unused")
        environment = {"GITHUB_EVENT_NAME": "push", "GITHUB_REF": "refs/tags/v" + VERSION,
                       "GITHUB_REPOSITORY": release.REPOSITORY}
        self.patches = [mock.patch.dict(os.environ, environment),
                        mock.patch.object(release, "remote_commit", return_value=REVISION),
                        mock.patch.object(release, "validate_assets", return_value={"install.sh": "digest"})]
        for patch in self.patches:
            patch.start()
            self.addCleanup(patch.stop)
        self.existing = {"id": 1, "body": f"<!-- pocketcontext-release:{REVISION} -->", "draft": False,
                         "immutable": True, "assets": []}

    def test_manual_runs_cannot_publish(self):
        with mock.patch.dict(os.environ, {"GITHUB_EVENT_NAME": "workflow_dispatch"}), mock.patch.object(release, "api") as api:
            with self.assertRaisesRegex(ValueError, "matching tag-push"):
                release.publish(self.args)
            api.assert_not_called()

    def test_published_retry_only_verifies_without_mutations(self):
        with mock.patch.object(release, "api", return_value=self.existing) as api, mock.patch.object(release, "verify_remote") as verify:
            release.publish(self.args)
            api.assert_called_once_with("releases/tags/v0.9.1", missing_ok=True)
            verify.assert_called_once()

    def test_published_mismatch_never_replaces_assets(self):
        with mock.patch.object(release, "api", return_value=self.existing) as api, mock.patch.object(release, "verify_remote", side_effect=ValueError("mismatch")):
            with self.assertRaisesRegex(ValueError, "mismatch"):
                release.publish(self.args)
            self.assertEqual(api.call_count, 1)

    def test_published_mutable_release_is_not_repaired(self):
        self.existing["immutable"] = False
        with mock.patch.object(release, "api", return_value=self.existing) as api:
            with self.assertRaisesRegex(ValueError, "not immutable"):
                release.publish(self.args)
            self.assertEqual(api.call_count, 1)

    def test_unowned_draft_is_not_modified(self):
        self.existing.update(draft=True, body="A manually authored draft")
        with mock.patch.object(release, "api", return_value=self.existing) as api:
            with self.assertRaisesRegex(ValueError, "not created for this validated commit"):
                release.publish(self.args)
            self.assertEqual(api.call_count, 1)

    def test_uploads_are_verified_before_publication_and_older_release_is_not_latest(self):
        self.existing.update(draft=True, assets=[{"id": 2, "name": "install.sh"}])
        events = []

        def api(path, method="GET", body=None, missing_ok=False):
            events.append((method, path))
            if path == "releases/latest":
                return {"tag_name": "v0.10.0"}
            if method == "PATCH":
                self.assertIn(("verify", "assets"), events)
                self.assertEqual(body["make_latest"], "false")
                return {"immutable": True}
            return self.existing

        with mock.patch.object(release, "api", side_effect=api), mock.patch.object(release, "run") as run, mock.patch.object(release, "verify_remote", side_effect=lambda *_: events.append(("verify", "assets"))):
            release.publish(self.args)
            self.assertIn(("DELETE", "releases/assets/2"), events)
            run.assert_called_once()
            self.assertNotIn("--clobber", run.call_args.args[0])
            self.assertEqual(events[-1], ("PATCH", "releases/1"))


if __name__ == "__main__":
    unittest.main()
