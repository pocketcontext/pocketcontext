# Static runtime notices

These texts accompany the Linux binaries, which statically link musl and may
include GCC runtime support. They were retrieved unchanged from:

- musl 1.2.6: <https://git.musl-libc.org/cgit/musl/plain/COPYRIGHT?h=v1.2.6>
- GCC 15.2.0 runtime exception: <https://github.com/gcc-mirror/gcc/blob/releases/gcc-15.2.0/COPYING.RUNTIME>
- GCC 15.2.0 GPL text: <https://github.com/gcc-mirror/gcc/blob/releases/gcc-15.2.0/COPYING3>

Review these notices when changing the pinned Alpine builder or compiler.
`scripts/release.py` also collects license and notice files from linked Go modules
and the Go toolchain. Every archive contains the project `LICENSE` and the
generated `THIRD_PARTY_NOTICES.txt` beside the executable.
