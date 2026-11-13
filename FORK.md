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
