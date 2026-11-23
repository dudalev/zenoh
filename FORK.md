# This fork is licensed AGPL-3.0-only

SPDX identifier: **`AGPL-3.0-only`**.
Licence text: <https://www.gnu.org/licenses/agpl-3.0.html>.

This repository is a fork of [Eclipse zenoh](https://github.com/eclipse-zenoh/zenoh),
branched at tag `1.10.2` (commit `cc839802`).

**Upstream zenoh is dual-licensed `EPL-2.0 OR Apache-2.0`.** This fork is taken under the
**Apache License 2.0** option, which permits redistribution of the work and of derivative
works under different terms, provided the conditions in its section 4 are met. Accordingly:

* **This fork, and every modification made in it, is licensed `AGPL-3.0-only`.** The
  workspace manifest carries `license = "AGPL-3.0-only"`, and `cargo metadata --no-deps`
  reports 42 workspace members: 39 inherit that field, and three declare no licence at all
  (`zenoh-test`, `zenoh-backend-example`, `zenoh-plugin-example`), as they do upstream.
  Re-derive those counts rather than trusting them here.
* **Upstream's licence text and notice are retained unmodified** as [`LICENSE`](LICENSE) and
  [`NOTICE.md`](NOTICE.md). They state the terms the original work is available under; they
  are not this fork's terms.
* **Every change is described in this repository's commit history**, which is what Apache-2.0
  section 4(b) asks for.
* Upstream copyright notices are left in place in every file.

Nothing here relicenses upstream zenoh. Code taken FROM this fork is AGPL-3.0-only; code
taken from upstream remains available from upstream under its own dual licence.

## What this fork changes

**The authoritative list is `git log cc839802..HEAD`**, and each commit message carries the
reasoning and the measurements. This section gives the THEMES only, deliberately: a table of
commits in a file nobody updates goes stale, and an earlier version of this one listed four
rows while the fork carried fourteen commits -- including rows for changes that did not exist
yet at the commit that introduced the table.

* **The defect this fork exists for.** `api/session.rs` never reuses an expression id a live
  declaration still holds -- upstream issue
  [#2860](https://github.com/eclipse-zenoh/zenoh/issues/2860). The `u16` id counter wraps and
  re-issues ids that live declarations still hold, which misroutes one publication per
  collision and then silently drops every later one on the affected declaration.
* **Three further routing defects found while building the gate for it**, each root-caused
  rather than guarded: a resource left in a router hat's set without its context, the runtime
  stripping the resource tree while hats still held `Arc<Resource>`, and a closing face
  leaving its context on resources that outlive it.
* **Shared memory: a receive path that reports what it drops, and stops when it closes.** A
  receiver that cannot map a buffer reports the loss at ERROR, rate-limited per peer, where
  upstream discarded it at DEBUG. In `init` mode the transport's own pool is built at
  session open, and peers exchange pool ids at establishment, so a receiver mounts a
  sender's pool before any message needs it and releases the mount when the link closes.
  Nothing `mlock`s a mapping, so locked memory does not limit shared memory at all. A read
  of zenoh-shm's statics after process exit has finalized them fails instead of panicking.
  A transport manager that has begun to close admits no new unicast transport and delivers
  nothing, so a closing session maps no shared-memory buffer; so does a process that has begun
  to exit with its session open, and the exit waits for the maps already in flight. And zenohd
  builds with shared memory by default.
* **A query no longer waits on a peer that has closed.** A closing face takes itself out of
  every cached route and only then finalizes its pending queries. Upstream left a closed peer
  in the routes of a peer that had not yet had its answer to the initial interest, so the next
  query waited out its timeout there.
* **One upstream fix carried**, [#2862](https://github.com/eclipse-zenoh/zenoh/pull/2862),
  under two criteria: it has a test that fails here, and it is reachable by what we run. That
  PR is OPEN and unreviewed upstream -- it is not accepted code.
* **The regression gates**, `zenoh/tests/expr_id_rollover.rs` and
  `zenoh/tests/keyexpr_desync_*`. They assert on what ARRIVED, not on zenoh's log text, so
  rewording a diagnostic cannot blind them.
* **The consumer's features are zenoh defaults.** `shared-memory` and `unstable` join
  `transport_tcp` in `zenoh`'s `default`, so zenoh's default build, on its own or as a
  dependency, has the code the consumer runs. The consumer itself still names them, with
  `default-features = false`.
* **Test-suite scope.** Upstream targets exercising features this fork's consumer does not use
  are gated behind `upstream_only_tests`, and a few single tests are `#[ignore]`d with their
  reason. Nothing is deleted.
* **`rust-toolchain.toml`: 1.100** (upstream pins 1.97.1). The fork's own MSRV declaration is
  unchanged at `rust-version = "1.75.0"`, which is upstream's claim and what the non-member
  `ci/zenoh-1-75` crate exists to test; nothing builds this fork on anything but the pinned
  toolchain.

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

**The feature flags.** They are not required any more, and the reason is worth keeping. A
DEFAULT-feature `cargo build -p zenoh` used to emit five warnings, over six items
(`DynamicRuntime`, `WeakDynamicRuntime`, `TimestampInstrumentationBuilderTrait`, and three
methods) that are feature-dependent dead code present at tag 1.10.2. The carrying project
sets `build.warnings = "deny"`, so those warnings failed the build outright. Each is now
either gated to exactly its users' condition or allowed with a comment naming which feature
uses it, so every configuration builds clean -- including
`--no-default-features --features transport_tcp,shared-memory`, which did NOT build until a
dead import was gated: every verification run before that had `unstable` on. The consumer
itself pins `zenoh` with `transport_tcp`, `shared-memory` and `unstable` (its root
`Cargo.toml`), so build that set as well as the one without `unstable`.

**`--exclude zenoh-uring`.** Its `reader` stress test does not terminate: measured at roughly
1500% CPU for 8 m 54 s with 3 of its 18 tests finished, and killed by hand. No CODE of ours
reaches that crate -- but this fork does edit its manifest, to gate that target, so "nothing
in this fork's changes reaches it" (as this said) is not true.

**What the suite does, with the number of runs beside it.** Measured 2026-11-14 on
`rustc 1.100.0 (0ef1bc386)`:

| command | runs | result |
|---|---|---|
| `cargo test` (plain) | 1, after gating the class below | **green**, 782 passed / 0 failed / 102 ignored |
| `cargo test` (plain), before that gating | 6, two campaigns | 4 green at 880 / 0 / 54; 2 red |
| the full-feature command above | 1 | green, 152 binaries, 977 passed / 0 failed / 79 ignored |
| `--no-default-features -F transport_tcp,shared-memory` (the consumer's set then; it has since added `unstable`) | 1 | builds clean |

Each red run failed exactly ONE test, and a DIFFERENT one each time:

* `transport_unicast_tls_only_mutual_success` -- a 60 s `Elapsed` under suite load. TLS is not
  a transport the consumer uses.
* `test_liveliness_regression_3` -- `liveliness.rs:3889`, `assertion failed:
  sub.try_recv().unwrap().is_none()`, a timing-dependent ABSENCE assertion. `liveliness` has
  zero uses in the consumer.

Neither was related to anything this fork changes: they were the third and fourth members of
one class, a flaky upstream test for a feature we do not ship. **That class is now gated**, by
operator decision, as a class rather than one test at a time -- which had been tried twice and
produced two more members each time. 52 tests in
`io/zenoh-transport/tests/unicast_transport.rs` and the whole `liveliness` target, every one
selected by parsing its own `#[cfg]` against the consumer's feature set rather than by reading
test names. 7 of that file's 70 tests remain: the TCP path, zid matching, link close, and one
that needs no transport at all.

The arithmetic of that change reconciles, which is how it was checked: ignored 54 + 52 - 4
(liveliness's own ignored tests, which leave the totals entirely with their target) = 102, and
passed 880 - 52 - 46 = 782.

**So the plain suite is not reliably green, and saying so is the point.** Four green in six
is the measurement; a single run is not. One run cannot distinguish a green suite from a lucky
one -- which is the same argument this fork uses to gate `regions`, applied to the suite that
remains. Read a red plain run by checking WHICH test failed before concluding anything: if it
is a non-TCP transport or another feature the consumer does not build, it is this class and
not a regression.

Vanilla 1.10.2 fails ~104 of its own tests here. Of those, 103 are now gated or `#[ignore]`d
with their reason and exactly ONE was made to pass -- and both of its faults were that test's
own environment assumptions. The three real routing defects this fork fixes were found by
other means and are fixed in their own commits; they are not part of that 104.

**The shared-memory targets need no locked memory.** `zenoh/tests/shm.rs` reports 10 tests,
where before the feature reached the crate at all it reported `running 0 tests`. Until
c19c210a1 most of them failed or hung on an ordinary host, downstream of one line --
`Unable to create POSIX shm segment: OS error 12` (ENOMEM) -- because every shared-memory
mapping was `mlock`ed and the default transport pool is 16 MiB, more than a typical
`RLIMIT_MEMLOCK`. Nothing locks a mapping now, so that limit does not apply: neither raising it
nor shrinking `transport/shared_memory/transport_optimization/pool_size` is needed to run them,
and an ENOMEM from a segment says nothing about the limit.

**The multicast tests join their group on loopback** (`#iface=127.0.0.1`): left to choose, the
udp link takes the first non-loopback multicast interface, and on a host whose firewall drops
multicast there they cannot pass at all (BER-2458). To run them over a real interface, name it,
or its address, in `ZENOH_TEST_MULTICAST_IFACE`. It applies to `zenoh/tests/shm.rs` and
`io/zenoh-transport/tests/shm_rx_after_close_multicast.rs`.

Run everything, including the gated targets, with:

```bash
CARGO_TARGET_DIR=/path/outside/the/synced/tree \
  cargo test -F test -F plugins,unstable,internal -F upstream_only_tests \
  --exclude zenoh-examples --exclude zenoh-plugin-example \
  --workspace --no-fail-fast
```

That is expected to HANG, not to fail, and the difference matters if you run it: it drops
`--exclude zenoh-uring`, so it includes that crate's `reader` target, which does not
terminate and has to be killed by hand. It also includes upstream's `regions` scenarios,
whose failures are timeouts whose count moves run to run.
