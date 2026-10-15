#!/usr/bin/env python3
"""Build, assemble and publish the three supported PocketContext release assets."""

import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import re
import shutil
import struct
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = "pocketcontext/pocketcontext"
PLATFORMS = ("linux_amd64", "linux_arm64", "darwin_arm64")
TAGS = "sqlite_math_functions,sqlite_percentile,sqlite_fts5"
GO_IMAGE = "golang:1.27.1-alpine@sha256:8a5910f31396cd4d89662f56c68b3ae31d374308270a1c3bd96672ee5ed43414"
VERSION_RE = re.compile(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\Z")
COMMIT_RE = re.compile(r"[0-9a-f]{40}\Z")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def run(args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def output(args, **kwargs):
    return run(args, text=True, capture_output=True, **kwargs).stdout.strip()


def version(value):
    require(VERSION_RE.fullmatch(value), "Version must be X.Y.Z without a leading v or prerelease suffix")
    return value


def commit(value):
    require(COMMIT_RE.fullmatch(value), "Commit must be a full lowercase Git SHA")
    return value


def archive_name(release_version, target):
    return f"pocketcontext_{release_version}_{target}.tar.gz"


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def json_objects(raw):
    decoder = json.JSONDecoder()
    while raw.strip():
        raw = raw.lstrip()
        item, end = decoder.raw_decode(raw)
        yield item
        raw = raw[end:]


def legal_files(directory):
    return sorted(path for path in directory.rglob("*") if path.is_file()
                  and re.fullmatch(r"(?:LICENSE|LICENCE|COPYING|COPYRIGHT|NOTICE)(?:[._-].*)?", path.name, re.I))


def third_party_notices(target):
    """Preserve license and notice files from the actual linked Go modules."""
    packages = json_objects(output(["go", "list", "-deps", "-json", "-tags", TAGS, "./cmd/pocketcontext"], cwd=ROOT))
    modules = {}
    for package in packages:
        module = package.get("Module")
        if module and not module.get("Main"):
            require("Replace" not in module, "Release dependencies must not use local module replacements")
            modules[module["Path"]] = module
    sections = ["PocketContext third-party license and copyright notices\n"]
    for name, module in sorted(modules.items()):
        directory = Path(module["Dir"])
        files = legal_files(directory)
        require(bool(files), f"No license file found for linked module {name}")
        for path in files:
            sections.append(f"\n--- {name} {module['Version']} / {path.relative_to(directory)} ---\n\n" + path.read_text(encoding="utf-8"))
    goroot = Path(output(["go", "env", "GOROOT"]))
    for path in [goroot / "LICENSE", goroot / "PATENTS", *legal_files(goroot / "src/vendor")]:
        if path.is_file():
            sections.append(f"\n--- Go 1.27.1 / {path.relative_to(goroot)} ---\n\n" + path.read_text(encoding="utf-8"))
    if target.startswith("linux_"):
        for path in sorted((ROOT / "scripts/licenses").glob("*.txt")):
            sections.append(f"\n--- Linux static runtime / {path.name} ---\n\n" + path.read_text(encoding="utf-8"))
    return "\n".join(sections).encode("utf-8")


def metadata(args):
    event = os.environ.get("GITHUB_EVENT_NAME", "")
    ref = os.environ.get("GITHUB_REF", "")
    require(event in ("push", "workflow_dispatch"), "Only tag pushes and manual build validation are supported")
    if event == "push":
        require(ref.startswith("refs/tags/v"), "Publication requires a vX.Y.Z tag push")
        release_version = version(ref.removeprefix("refs/tags/v"))
    else:
        require(ref == "refs/heads/main", "Manual validation must run from main")
        release_version = version(args.version)
    head = commit(output(["git", "rev-parse", "HEAD"], cwd=ROOT))
    run(["git", "merge-base", "--is-ancestor", head, "origin/main"], cwd=ROOT)
    if event == "push":
        require(output(["git", "rev-parse", f"{ref}^{{commit}}"], cwd=ROOT) == head,
                "Tag does not identify the checked-out commit")
    values = {"version": release_version, "commit": head, "publish": str(event == "push").lower()}
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as stream:
            for key, value in values.items():
                stream.write(f"{key}={value}\n")
    print(json.dumps(values, sort_keys=True))


def verify_binary(binary, target):
    if target.startswith("linux_"):
        with binary.open("rb") as stream:
            header = stream.read(64)
            require(header[:6] == b"\x7fELF\x02\x01", "Expected a little-endian ELF64 binary")
            machine = struct.unpack_from("<H", header, 18)[0]
            require(machine == {"linux_amd64": 62, "linux_arm64": 183}[target], "ELF architecture mismatch")
            offset = struct.unpack_from("<Q", header, 32)[0]
            size, count = struct.unpack_from("<HH", header, 54)
            require(size >= 56 and count > 0, "Invalid ELF program headers")
            for index in range(count):
                stream.seek(offset + index * size)
                kind = struct.unpack("<I", stream.read(4))[0]
                require(kind not in (2, 3), "Linux release must be static: dynamic/interpreter program header found")
    else:
        require(output(["lipo", "-archs", str(binary)]) == "arm64", "Expected a native arm64 Mach-O")
        libraries = output(["otool", "-L", str(binary)]).splitlines()[1:]
        require(bool(libraries), "Missing Mach-O dependency information")
        for line in libraries:
            require(line.strip().startswith(("/usr/lib/", "/System/Library/")), "Unexpected non-system macOS dependency")
        commands = output(["otool", "-l", str(binary)])
        require(re.search(r"cmd LC_BUILD_VERSION\s+cmdsize \d+\s+platform (?:1|macos)\s+minos 15\.0(?:\s|\.)", commands),
                "macOS deployment target must be 15.0")
        run(["codesign", "--verify", "--strict", str(binary)])
        signature = run(["codesign", "-dvv", str(binary)], text=True, capture_output=True)
        require("Signature=adhoc" in signature.stderr, "Expected an ad hoc macOS code signature")


def build(args):
    release_version, revision = version(args.version), commit(args.commit)
    require(output(["git", "rev-parse", "HEAD"], cwd=ROOT) == revision, "Build commit does not match checkout")
    require(not output(["git", "status", "--porcelain", "--untracked-files=normal"], cwd=ROOT),
            "Release builds require a clean checkout so the embedded commit identifies all compiled source")
    target = args.platform
    native = f"{platform.system().lower()}_{ {'x86_64': 'amd64', 'aarch64': 'arm64', 'arm64': 'arm64'}.get(platform.machine(), platform.machine()) }"
    require(native == target, f"Release builds must be native ({native} != {target})")
    destination = Path(args.output).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    binary_dir = destination / target
    binary_dir.mkdir(exist_ok=True)
    binary = binary_dir / "pocketcontext"
    flags = f"-s -w -X main.version={release_version} -X main.commit={revision}"
    if target.startswith("linux_"):
        flags += " -linkmode external -extldflags '-static'"
        # The image is an immutable multiarchitecture index. apk provides the
        # native musl compiler; CGO remains enabled for the SQLite authorizer.
        script = '''apk add --no-cache build-base
test "$(go env GOVERSION)" = go1.27.1
CGO_ENABLED=1 go build -trimpath -buildvcs=false -tags "$1" -ldflags "$2" -o /out/pocketcontext ./cmd/pocketcontext
chown "$3:$4" /out/pocketcontext
'''
        run(["docker", "run", "--rm", "--mount", f"type=bind,src={ROOT},dst=/src,readonly",
             "--mount", f"type=bind,src={binary_dir},dst=/out", "--workdir", "/src",
             GO_IMAGE, "sh", "-ec", script, "build", TAGS,
             flags, str(os.getuid()), str(os.getgid())])
    else:
        environment = os.environ.copy()
        environment.update(CGO_ENABLED="1", MACOSX_DEPLOYMENT_TARGET="15.0",
                           CGO_CFLAGS="-mmacosx-version-min=15.0", CGO_LDFLAGS="-mmacosx-version-min=15.0")
        require(output(["go", "env", "GOVERSION"]) == "go1.27.1", "Expected Go 1.27.1")
        run(["go", "build", "-trimpath", "-buildvcs=false", "-tags", TAGS,
             "-ldflags", flags, "-o", str(binary), "./cmd/pocketcontext"], cwd=ROOT, env=environment)
        run(["codesign", "--force", "--sign", "-", str(binary)])
    verify_binary(binary, target)
    notices = third_party_notices(target)
    stamp = int(output(["git", "show", "-s", "--format=%ct", revision], cwd=ROOT))
    archive = destination / archive_name(release_version, target)
    with archive.open("wb") as raw, gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=stamp) as compressed:
        with tarfile.open(fileobj=compressed, mode="w") as bundle:
            entry = tarfile.TarInfo("pocketcontext")
            entry.size, entry.mode, entry.mtime = binary.stat().st_size, 0o755, stamp
            with binary.open("rb") as stream:
                bundle.addfile(entry, stream)
            for name, content in (("LICENSE", (ROOT / "LICENSE").read_bytes()), ("THIRD_PARTY_NOTICES.txt", notices)):
                entry = tarfile.TarInfo(name)
                entry.size, entry.mode, entry.mtime = len(content), 0o644, stamp
                bundle.addfile(entry, io.BytesIO(content))
    manifest = {"version": release_version, "commit": revision, "platform": target,
                "archive": archive.name, "sha256": sha256(archive)}
    (destination / f"{target}.json").write_text(json.dumps(manifest, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(manifest, sort_keys=True))


def check_archive(archive):
    with tarfile.open(archive, "r:gz") as bundle:
        entries = bundle.getmembers()
        expected = {"pocketcontext": 0o755, "LICENSE": 0o644, "THIRD_PARTY_NOTICES.txt": 0o644}
        require(len(entries) == len(expected) and {entry.name for entry in entries} == set(expected)
                and all(entry.isreg() and entry.mode == expected[entry.name] and entry.size > 0 for entry in entries),
                f"Unexpected archive layout: {archive.name}")


def assemble(args):
    release_version, revision = version(args.version), commit(args.commit)
    artifacts, destination = Path(args.artifacts), Path(args.output)
    require(not destination.exists() or not list(destination.iterdir()), "Assembly output must be empty")
    destination.mkdir(parents=True, exist_ok=True)
    for target in PLATFORMS:
        manifests = list(artifacts.rglob(f"{target}.json"))
        require(len(manifests) == 1, f"Expected exactly one manifest for {target}")
        manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
        name = archive_name(release_version, target)
        require(manifest.get("version") == release_version and manifest.get("commit") == revision
                and manifest.get("platform") == target and manifest.get("archive") == name,
                f"Inconsistent release manifest for {target}")
        archive = manifests[0].parent / name
        require(sha256(archive) == manifest.get("sha256"), f"Artifact checksum mismatch for {target}")
        check_archive(archive)
        shutil.copyfile(archive, destination / name)
    template = (ROOT / "scripts/install.sh.in").read_text(encoding="utf-8")
    require("@VERSION@" in template, "Installer version placeholder is missing")
    installer = template.replace("@VERSION@", release_version)
    require("@VERSION@" not in installer, "Installer template was not fully rendered")
    (destination / "install.sh").write_text(installer, encoding="utf-8")
    (destination / "install.sh").chmod(0o755)
    files = sorted(destination.iterdir())
    (destination / "checksums.txt").write_text("".join(f"{sha256(path)}  {path.name}\n" for path in files), encoding="utf-8")
    validate_assets(destination, release_version)
    print(f"Assembled {len(files) + 1} assets for v{release_version}")


def validate_assets(directory, release_version):
    names = {archive_name(release_version, target) for target in PLATFORMS} | {"install.sh", "checksums.txt"}
    require({path.name for path in directory.iterdir()} == names, "Release must contain exactly the five expected assets")
    checksums = {}
    for line in (directory / "checksums.txt").read_text(encoding="utf-8").splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  ([A-Za-z0-9_.-]+)", line)
        require(match is not None, "Malformed checksum line")
        digest, name = match.groups()
        require(name not in checksums, "Duplicate checksum entry")
        checksums[name] = digest
    require(set(checksums) == names - {"checksums.txt"}, "Checksum manifest has missing or unexpected assets")
    for name, digest in checksums.items():
        require(sha256(directory / name) == digest, f"Checksum mismatch: {name}")
    for target in PLATFORMS:
        check_archive(directory / archive_name(release_version, target))
    return {name: sha256(directory / name) for name in names}


def api(path, method="GET", body=None, missing_ok=False):
    command = ["gh", "api", f"repos/{REPOSITORY}/{path}", "--method", method,
               "-H", "X-GitHub-Api-Version: 2026-03-10", "-H", "Cache-Control: no-cache"]
    if body is not None:
        command += ["--input", "-"]
    result = subprocess.run(command, input=None if body is None else json.dumps(body), text=True, capture_output=True)
    if result.returncode:
        if missing_ok and "(HTTP 404)" in result.stderr:
            return None
        # Do not echo raw CLI output or environment on API failures.
        raise RuntimeError(f"GitHub {method} {path} failed (exit {result.returncode})")
    return json.loads(result.stdout) if result.stdout.strip() else None


def remote_commit(tag):
    item = api(f"git/ref/tags/{tag}")["object"]
    for _ in range(8):
        if item["type"] == "commit":
            return item["sha"]
        require(item["type"] == "tag", "Release tag does not resolve to a commit")
        item = api(f"git/tags/{item['sha']}")["object"]
    raise ValueError("Excessive annotated tag nesting")


def verify_remote(release, tag, expected):
    assets = release.get("assets", [])
    require(len(assets) == len(expected) and {a["name"] for a in assets} == set(expected),
            "Remote release assets are incomplete or unexpected")
    require(all(asset["state"] == "uploaded" for asset in assets), "Remote upload is incomplete")
    with tempfile.TemporaryDirectory(prefix="pocketcontext-release-verify-") as temporary:
        run(["gh", "release", "download", tag, "--repo", REPOSITORY, "--dir", temporary], capture_output=True)
        require({p.name for p in Path(temporary).iterdir()} == set(expected), "Downloaded assets differ from expected assets")
        for name, digest in expected.items():
            require(sha256(Path(temporary) / name) == digest, f"Uploaded asset checksum mismatch: {name}")


def publish(args):
    release_version, revision = version(args.version), commit(args.commit)
    tag = f"v{release_version}"
    require(os.environ.get("GITHUB_EVENT_NAME") == "push" and os.environ.get("GITHUB_REF") == f"refs/tags/{tag}",
            "Publishing is only permitted for the matching tag-push workflow")
    require(os.environ.get("GITHUB_REPOSITORY") == REPOSITORY, "Unexpected publishing repository")
    require(remote_commit(tag) == revision, "Remote tag changed since validation")
    directory = Path(args.assets)
    expected = validate_assets(directory, release_version)
    marker = f"<!-- pocketcontext-release:{revision} -->"
    release = api(f"releases/tags/{tag}", missing_ok=True)
    if release is not None:
        require(marker in (release.get("body") or ""), "Existing release was not created for this validated commit")
        if not release["draft"]:
            require(release.get("immutable") is True, "Published release is not immutable; refusing to modify it")
            verify_remote(release, tag, expected)
            print(f"Published {tag} already matches all validated assets; no changes made")
            return
    else:
        body = (f"PocketContext {release_version}\n\n"
                "Linux amd64 and arm64: static musl binaries. macOS arm64: macOS 15 or later, ad hoc signed.\n\n"
                "Install: `curl -fsSL https://pocketcontext.com/pocketcontext/install.sh | sh`\n\n"
                "See [installation and release documentation](https://github.com/pocketcontext/pocketcontext/blob/"
                f"{tag}/docs/releases.md).\n\n{marker}")
        release = api("releases", "POST", {"tag_name": tag, "target_commitish": revision,
                      "name": f"PocketContext {release_version}", "body": body, "draft": True, "prerelease": False})
    require({asset["name"] for asset in release.get("assets", [])} <= set(expected), "Draft contains unexpected assets")
    # Retry only repairs this unpublished, commit-marked draft. Published assets
    # never pass this branch and are never overwritten or deleted.
    for asset in release.get("assets", []):
        require(api(f"releases/{release['id']}")["draft"], "Release was published while preparing uploads")
        api(f"releases/assets/{asset['id']}", "DELETE")
    run(["gh", "release", "upload", tag, "--repo", REPOSITORY,
         *[str(directory / name) for name in sorted(expected)]], capture_output=True)
    release = api(f"releases/{release['id']}")
    require(release["draft"], "Release was published before upload verification")
    verify_remote(release, tag, expected)
    require(remote_commit(tag) == revision, "Remote tag changed before publication")
    latest = api("releases/latest", missing_ok=True)
    make_latest = True
    if latest:
        latest_version = latest["tag_name"].removeprefix("v")
        require(VERSION_RE.fullmatch(latest_version), "Latest release has an unrecognized version; refusing automatic promotion")
        make_latest = tuple(map(int, release_version.split("."))) > tuple(map(int, latest_version.split(".")))
    published = api(f"releases/{release['id']}", "PATCH", {"draft": False, "prerelease": False,
                    "make_latest": str(make_latest).lower()})
    require(published.get("immutable") is True, "Release was published without immutability; check repository settings immediately")
    print(f"Published immutable {tag}; latest={str(make_latest).lower()}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("metadata")
    prepare.add_argument("--version", default="")
    prepare.set_defaults(function=metadata)
    for name, handler in (("build", build), ("assemble", assemble), ("publish", publish)):
        command = commands.add_parser(name)
        command.add_argument("--version", required=True)
        command.add_argument("--commit", required=True)
        if name == "build":
            command.add_argument("--platform", choices=PLATFORMS, required=True)
            command.add_argument("--output", default="dist")
        elif name == "assemble":
            command.add_argument("--artifacts", required=True)
            command.add_argument("--output", required=True)
        else:
            command.add_argument("--assets", required=True)
        command.set_defaults(function=handler)
    args = parser.parse_args()
    try:
        args.function(args)
    except (ValueError, RuntimeError, OSError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"Release failed: {error}\n")


if __name__ == "__main__":
    main()
