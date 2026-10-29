# Binary releases

PocketContext releases install only the application-independent server. Each
application still supplies its own configuration, migrations, hooks and data.
See the [README installation instructions](../README.md#install).

## Supported artifacts

| Archive suffix | Build and runtime contract |
| --- | --- |
| `linux_amd64` | Native amd64, CGO enabled, statically linked musl executable |
| `linux_arm64` | Native arm64, CGO enabled, statically linked musl executable |
| `darwin_arm64` | Native Apple silicon, macOS 15 or later, system libraries only |

Linux releases are built with the immutable multiarchitecture Go 1.27.1 Alpine
image recorded in `scripts/release.py`. The release gate executes each Linux
binary on Ubuntu 24.04 and in pinned Alpine 3.23 and Debian 12 containers. These
are tested distribution environments, not a promise of support for every Linux
kernel or distribution. Containers share the runner's kernel. Normal application
networking still needs the host's resolver configuration and CA certificates.

macOS builds use `MACOSX_DEPLOYMENT_TARGET=15.0`, explicit compiler/linker minimum
version flags and an ad hoc signature. The workflow verifies the architecture,
minimum OS, system-library dependencies and signature, then executes the binary
on macOS 15. Ad hoc signing is not Apple Developer ID signing or notarization.

Every platform preserves `sqlite_math_functions,sqlite_percentile,sqlite_fts5`
and CGO. Linux release inspection rejects dynamic ELF dependencies and an ELF
interpreter. No additional SQLite or Go resolver build tags are enabled.

The five public assets for `v0.9.3` are:

- `pocketcontext_0.9.3_linux_amd64.tar.gz`
- `pocketcontext_0.9.3_linux_arm64.tar.gz`
- `pocketcontext_0.9.3_darwin_arm64.tar.gz`
- `checksums.txt`
- `install.sh`

Each archive contains exactly three regular root files: `pocketcontext`,
`LICENSE` and `THIRD_PARTY_NOTICES.txt`. The notices include linked Go module
licenses, Go notices and, for Linux, the static runtime notices. Review
`scripts/licenses/` when changing the compiler or builder image.

The SHA-256 manifest covers the three archives and installer, using lowercase
hexadecimal digests and two spaces before each filename. Checksums detect
corruption; they are not an independent signature. The published installer embeds
its own version and downloads all assets from that same release tag. The website
route redirects to GitHub's latest `install.sh`; it never maintains a second copy.

## Validation before a tag

The `Release` workflow supports manual build validation from `main`. A manual
run never creates a tag, draft or public release:

```sh
gh workflow run release.yml --ref main -f version=0.9.3
gh run list --workflow release.yml --limit 5
gh run watch RUN_ID --exit-status
gh run view RUN_ID --json headSha,conclusion
```

Record the successful run's commit before creating a tag. Native jobs run
`make test` with the race detector and `make build`. They then build the actual
release executable and run:

- CLI version checks from an empty directory, with no application-data creation.
- An isolated default-`users` application with REST writes, HTTP health, filtered
  SQL, JSON/math/percentiles, FTS5 search and authorization checks.
- Installer tests covering failure preservation and real native installation.
- The same HTTP smoke suite in the Linux distribution containers listed above.
- Linux client hostname resolution and verified HTTPS against a synthetic
  `localhost` certificate. No public endpoint or host trust-store change is used.

The TLS fixture is Linux-specific because macOS uses its system trust store.
These checks do not exercise an external DNS server, public CA endpoint or macOS
outbound TLS. Ordinary server tests provide the broader behavioral coverage.

The assembly job checks all three artifact manifests identify the same version
and commit, verifies archive checksums and layouts, then renders `install.sh` and
the final checksum manifest. A successful manual run retains its complete
`release-0.9.3` workflow artifact for 14 days.

Offline tooling checks also run in the ordinary push/pull-request test workflow:

```sh
python3 -B -m unittest discover -s tests -p test_release.py -v
python3 tests/installer.py
```

For a native local build, use Python 3.11 or later, Go 1.27.1 and a clean checkout.
Linux additionally needs Docker; macOS needs the command line developer tools.
The commit must be the full SHA of the checkout:

```sh
python3 scripts/release.py build --version 0.9.3 --commit FULL_COMMIT_SHA --platform linux_amd64 --output dist
python3 tests/release_smoke.py --binary dist/linux_amd64/pocketcontext --version 0.9.3 --commit FULL_COMMIT_SHA
python3 tests/installer.py --binary dist/linux_amd64/pocketcontext --version 0.9.3
```

Use `linux_arm64` or `darwin_arm64` on those native hosts. Cross-compilation is
deliberately unsupported by this helper. Each build writes an archive, manifest
and unpacked executable beneath ignored `dist/`.

## Publish a release

Publishing is an explicit maintainer action. Merge release changes to `main`,
pass the manual matrix on the intended commit, and verify repository release
immutability before pushing the first tag:

```sh
gh api repos/pocketcontext/pocketcontext/immutable-releases \
  -H 'X-GitHub-Api-Version: 2026-03-10' -H 'Cache-Control: no-cache' \
  --jq '{enabled,enforced_by_owner}'
```

`enabled` must be `true`. Enable release immutability in repository settings if
needed, then verify it again. This preflight requires a maintainer's
administration-read permission. The workflow uses only `contents: write` in its
publishing job and cannot independently inspect that setting. Its postpublication
immutability assertion detects an unexpected setting change; it does not prevent
publication if someone disabled immutability after the maintainer's preflight.

Create the tag at the exact successfully tested commit and push only that tag:

```sh
git tag -a v0.9.3 FULL_COMMIT_SHA -m 'PocketContext 0.9.3'
git push origin v0.9.3
```

The workflow rejects malformed versions and commits outside `main` ancestry.
It rebuilds and validates every platform before a serialized publishing job
creates a draft, uploads all five assets, downloads them again and verifies their
checksums. Only then does it publish the draft and check it is immutable. The
release is regular, not a prerelease; `0.9.3` is valid as a regular release. A
release becomes latest only if its version is greater than the existing latest
version. Parallel builds cannot move latest backward by finishing out of order.

Do not disable immutability or reuse a published version to repair a bad binary.
Publish a new version. After successful publication, verify anonymous asset
downloads and the website redirect before sharing the installation command.

## Failures and retries

Build/test failures create no public release. An upload failure may leave a draft
marked with its source commit. Rerunning the failed publishing job reuses the
retained assembled artifacts, repairs only that matching draft, verifies uploads
and publishes it. A manually authored draft or one for a different commit is not
modified. Unexpected assets are rejected.

If a release is already public, a retry only verifies immutable status, the
source marker and every asset's bytes. It never replaces assets, deletes a
published release or moves a tag. The same validated assets produce a successful
no-op. Different bytes cause a refusal.

Rebuilding an old commit after a runner or Alpine package change can produce
different bytes despite the pinned Go image. This pipeline checks provenance and
asset integrity; it does not claim fully reproducible compiler environments.
Prefer rerunning only the failed publishing job while its original workflow
artifacts remain available. If those assets have expired, inspect the published
release or cut a new version rather than replacing an existing public asset.

Draft/public state reads use explicit API-version and no-cache headers. The
publisher verifies the remote tag immediately before publication. Native builds
require a clean checkout so embedded commit metadata describes compiled source.
