# This fork is licensed AGPL-3.0-only

SPDX identifier: **`AGPL-3.0-only`**.
Licence text: <https://www.gnu.org/licenses/agpl-3.0.html>.

This repository is a fork of [Eclipse zenoh](https://github.com/eclipse-zenoh/zenoh),
branched at tag `1.10.2` (commit `cc839802`).

**Upstream zenoh is dual-licensed `EPL-2.0 OR Apache-2.0`.** This fork is taken under the
**Apache License 2.0** option, which permits redistribution of the work and of derivative
works under different terms, provided the conditions in its section 4 are met. Accordingly:

* **This fork, and every modification made in it, is licensed `AGPL-3.0-only`.**
* **Upstream's licence text and notice are retained unmodified** as [`LICENSE`](LICENSE) and
  [`NOTICE.md`](NOTICE.md). They state the terms the original work is available under; they
  are not this fork's terms.
* **Every change is described in this repository's commit history**, which is what Apache-2.0
  section 4(b) asks for.
* Upstream copyright notices are left in place in every file.

Nothing here relicenses upstream zenoh. Code taken FROM this fork is AGPL-3.0-only; code
taken from upstream remains available from upstream under its own dual licence.

## What this fork changes

Each item is one commit, and its message carries the reasoning and the measurements.

| change | why |
|---|---|
| this notice, and `license = "AGPL-3.0-only"` in the workspace manifest | the whole fork is AGPL-3.0-only; every member crate inherits the field with `license = { workspace = true }` |
| `api/session.rs`: never reuse an expression id a live declaration still holds | upstream issue [#2860](https://github.com/eclipse-zenoh/zenoh/issues/2860) -- the `u16` id counter wraps and re-issues ids that live declarations still hold, which misroutes one publication per collision and then silently drops every later one on the affected declaration |
| `rust-toolchain.toml`: 1.99.1 | upstream pins 1.97.1; the workspace MSRV is unchanged at 1.75.0 |
| `zenoh/tests/expr_id_rollover.rs`, `zenoh/tests/keyexpr_desync_*` | regression gates for the above, so a later rebase cannot lose the fix quietly. They assert on what ARRIVED rather than on zenoh's log text, so rewording a diagnostic does not break them |

## Building and testing this fork

```bash
CARGO_TARGET_DIR=/path/outside/the/synced/tree \
  cargo test -F test -F plugins,unstable,internal \
  --exclude zenoh-examples --exclude zenoh-plugin-example --exclude zenoh-uring \
  --workspace --no-fail-fast
```

Three things about that command are deliberate.

**`CARGO_TARGET_DIR`.** This fork is checked out inside the repository that carries it, so
that repository's `.cargo/config.toml` applies here: without an override, cargo writes this
fork's build output into the parent project's own target directory, and that directory is the
one its production binaries are built in. Set it somewhere else, and outside any replicated
tree -- a debug build of this workspace is tens of gigabytes.

**The feature flags.** `cargo build -p zenoh` with DEFAULT features leaves five items unused
(`DynamicRuntime`, `WeakDynamicRuntime`, `TimestampInstrumentationBuilderTrait`, and two of
their methods), so it warns. That is upstream's, not ours -- it is feature-dependent dead
code, present at tag 1.10.2 -- but the carrying project sets `build.warnings = "deny"`, which
turns those warnings into a failed build. With the flags above the items are used and the
build is clean.

**`--exclude zenoh-uring`.** Its `reader` stress test does not terminate: measured at roughly
1500% CPU for 8 m 54 s with 3 of its 18 tests finished, and killed by hand. Nothing in this
fork's changes reaches that crate.

**The suite passes.** Measured 2026-11-13 on rustc 1.99.1: plain `cargo test` is 902 passed /
0 failed / 48 ignored, and the full-feature command above is 151 binaries, 999 passed, 0
failed, 73 ignored. Vanilla 1.10.2 fails ~104 of its own tests here; three of those were real
defects and are fixed, and the rest exercise features this fork's consumer does not use and
are gated behind `upstream_only_tests` -- see the table below. So a red run now means
something, which is the whole point of carrying the fork.

Run everything, including the gated targets, with:

```bash
CARGO_TARGET_DIR=/path/outside/the/synced/tree \
  cargo test -F test -F plugins,unstable,internal -F upstream_only_tests \
  --exclude zenoh-examples --exclude zenoh-plugin-example \
  --workspace --no-fail-fast
```

That is expected to fail: it includes upstream's `regions` scenarios, whose failures are
timeouts whose count moves run to run, and `zenoh-uring`'s `reader`, which does not
terminate.
