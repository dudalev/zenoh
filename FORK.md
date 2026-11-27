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
  of zenoh-shm's statics after process exit has finalized them fails instead of panicking,
  so an allocation then fails, and a send that would have copied its payload into shared
  memory sends it inline as it is.
  A transport manager that has begun to close runs no accept handshake, dials nothing, admits
  no new unicast transport and delivers nothing, so a closing session maps no shared-memory
  buffer. It refuses a transport with no Close frame, so the healthy peer whose dial it
  refuses logs no ERROR, and it counts what it did not deliver and logs the count at DEBUG
  when it has closed.
  A process that has begun to exit with a shared-memory session open does the same, on unix
  only: an `atexit` hook marks exit as begun before zenoh-shm's statics are finalized, and
  waits for the maps already in flight, for at most 1 s. A handshake already running when
  exit begins, accept or open, is refused where it reaches the transport's own static, the
  shared-memory handoff reactor, whether exit has finalized it or it was never initialized:
  with no Close frame, and nothing logged above DEBUG. Not covered: a map that takes longer
  than 1 s can still meet a finalized static; and in `lazy` mode, zenoh's default and not
  the consumer's, the first use of one of zenoh-shm's statics after static_init has
  finalized the others panics (`commons/zenoh-shm/src/statics.rs`). And zenohd builds with
  shared memory by default.
* **A query no longer waits on a peer that has closed.** A closing face takes itself out of
  every cached route and only then finalizes its pending queries. Upstream left a closed peer
  in the routes of a peer that had not yet had its answer to the initial interest, so the next
  query waited out its timeout there.
* **A dropped message is reported to the sender and to the receiver** (BER-2554), through
  `zenoh::drop_observer`, under `unstable`.
  * **Sender.** A push a face's transport does not deliver reaches the sending process's push
    drop observer, with the face's ZenohId, the key expression, the priority and a reason:
    * `Dropped` for a discard: congestion, a `BlockFirst` push that waited out its turn or
      then found no batch, a push that does not fit the link's batches even as fragments, and
      a push a failed link refuses while its transport keeps another link to the peer;
    * `TransportClosed` for a transport that had closed or lost its last link, which is not a
      drop;
    * `LinkClosedQueued { batches }` for batches a reader that is alive lost: the link closed
      at this end while its peer was not reading (a write at close found no room before the
      keep-alive timeout, or was cut short by the close), or its write failed while the
      transport kept another link to the peer;
    * `LinkResetQueued { batches }` for batches a link held when its connection was closed or
      reset at the peer's end with no other link to it, which is what a peer whose process
      ends gives. A peer whose zenoh closes the link while its process lives gives it too.

    A link whose write fails refuses every later push at once. The TX task that drains a link
    never waits for a push's lock: it takes the batch a push is filling only if no push holds
    it, so a push waiting for a batch cannot hold up the process's single TX thread, and is
    released when the link's tasks end.
  * **Receiver.** A sender that drops a message leaves one frame SN unused at its next frame
    on that priority and reliability, so the receiver sees a gap of 2, and the receiving
    process's rx loss observer reports it with the peer's ZenohId. It also reports the
    fragments a unicast receive path discards. The wire protocol is unchanged: a receiver that
    does not report gaps accepts one, as upstream's does.
  * **Upstream SN slips that would read as gaps, fixed:** the first frame after a transport
    opens read as a gap of 2 when the peer's initial SN was 0; every fragmented message took
    an SN after its last fragment, so the frame after it read as a gap; a fragmented message
    abandoned part way left the SN of its stop fragment unused (a stop fragment has no
    payload, so it never encoded, and the pipeline no longer builds one); and the "can not be
    fragmented" path rewound the SN below the fragments it had sent, and reported the message
    sent.
  * **Queue.** A link's queue takes up to 64 batches per priority, still allocated lazily.
    Only a stage's own batches enter its rings, so a ring the length of the cap cannot
    overflow. A size outside 1..=64 set on its own, which the config's validator does not
    see, fails the transport manager's build, so the session does not open.
  * **Two links in one transport.** Two peers that open to each other at once can end up with
    two links in one transport, even at `max_links` 1, since upstream counts only inbound
    links against it. A link that fails beside another reports what it held as
    `LinkClosedQueued`, and the receiver sees the gap on the other link.
  * **Observers** are installed once per process and must neither block nor panic; a panic is
    caught after the panic hook has reported it, so an observer that must stop the process
    exits or aborts.
  * **Not covered.** The sender's observer sees pushes only, not requests or replies. A push
    an interceptor refuses reaches neither observer. The multicast receive path reports
    nothing, and a multicast face's push drops carry zenoh's placeholder ZenohId. A drop the
    sender never follows with a frame on that channel never reaches the receiver, nor does a
    message lost with a failed link, since the new transport starts its SNs afresh. Not
    counted in `LinkClosedQueued` or `LinkResetQueued`: batches already written to the
    socket, and, in the instant the link fails or closes, a push under way and the batch a
    push is filling. A sender without these fixes, upstream's included, leaves a gap after
    every fragmented message it sends. Upstream defect, not fixed: on a transport without QoS
    the congested bit is set on `Priority::DEFAULT` and cleared on bit 0, so after its first
    congestion drop every later droppable push on it is dropped.
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
* **No test reaches a zenoh the host runs** (BER-2469). A test that scouts does so in a
  scouting group of its own, with scouting on, and a test whose subject needs a router runs
  the zenohd built from this fork. See "Tests and the zenoh on the host" below.
* **No test binds a socket in the source tree** (BER-2480). Upstream's unix-socket and
  unixpipe tests bound theirs at paths relative to the package, so one that ended early left
  its socket in the tree. See "Tests and the files they bind" below.
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

**Running one target.** Name its package, the target, and the features the target's own
`#![cfg]` asks for:

```bash
CARGO_TARGET_DIR=/path/outside/the/synced/tree \
  cargo test -p zenoh-transport --features shared-memory,transport_tcp --test shm_rx_after_close
```

zenoh-transport's defaults do not include `transport_tcp`, and a target whose `#![cfg]` asks
for a feature that is off compiles to `running 0 tests` and reports `ok`: read the count.
zenoh-transport's tests do not build zenoh (BER-2470), so its features need not match zenoh's.
A target of zenoh or of zenohd runs the same way, for instance `cargo test -p zenoh --test qos`
or `cargo test -p zenohd --test attachments`.

**What the suite does, with the number of runs beside it.** Measured 2026-11-14 on
`rustc 1.100.0 (0ef1bc386)`:

| command | runs | result |
|---|---|---|
| `cargo test` (plain) | 1, after gating the class below | **green**, 782 passed / 0 failed / 102 ignored |
| `cargo test` (plain), before that gating | 6, two campaigns | 4 green at 880 / 0 / 54; 2 red |
| the full-feature command above | 1 | green, 152 binaries, 977 passed / 0 failed / 79 ignored |
| `--no-default-features -F transport_tcp,shared-memory` (the consumer's set then; it has since added `unstable`) | 1 | builds clean |

At tag `hecate-1.10.2-7`, measured 2026-11-24 on the same toolchain, with other builds running
on the host. The tag was cut at `1669dc033` and moves to the code of the later rows, which
adds BER-2479, BER-2480, BER-2486 and BER-2487:

| command | at | runs | result |
|---|---|---|---|
| the full-feature command above | `1669dc033` | 2 | green both times, 118 test binaries and 37 doc-test sets, 960 passed / 0 failed / 126 ignored |
| `cargo build -p zenoh --no-default-features -F transport_tcp,shared-memory`, and with `,unstable` (the consumer's set) | `1669dc033` | 1 each | builds clean |
| the full-feature command above | the later code | 1 | green, 123 test binaries and 37 doc-test sets, 973 passed / 0 failed / 126 ignored |
| the consumer's two builds, as above | the later code | 1 each | builds clean |
| `cargo doc -p zenoh-test`, and with `--features sessions` | the later code | 1 each | builds clean |

`tests::link_weights` picks its routers' ports free and binds them only as each router opens,
so another test of the binary can take one in between. It then opens its routers again on new
ports and prints "opening the routers again on new ports", which is not a failure.

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

**What the exit hold costs** (BER-2474). Exit waits for the maps in flight through a hold,
two SeqCst read-modify-writes on one counter that every receive thread shares. Only a message
that carries a shared-memory buffer takes it, where the buffer is mapped; a message with none
pays nothing for it. A payload below `message_size_threshold` (3072 bytes by default) travels
with none, unless the sender put it in shared memory itself. Measured 2026-11-24 with the
review's micro-benchmark of a copy of `ExitGate`
(`rustc -O`, rustc 1.100.0), nanoseconds per message, the better of two runs of best of 3,
every thread running the same loop:

| receive threads | loop alone | a message at -7, before | no buffer, now | a buffer, now |
|---|---|---|---|---|
| 1 | 0.40 | 2.50 | 0.41 | 2.55 |
| 2 | 0.41 | 20.87 | 0.41 | 17.73 |
| 8 | 0.43 | 97.29 | 0.45 | 84.88 |

The loop has nothing else in it, so the contended figures are an upper bound for real traffic.

**The multicast tests join their group on loopback** (`#iface=127.0.0.1`): left to choose, the
udp link takes the first non-loopback multicast interface, and on a host whose firewall drops
multicast there they cannot pass at all (BER-2458). To run them over a real interface, name it,
or its address, in `ZENOH_TEST_MULTICAST_IFACE`. It applies to `zenoh/tests/shm.rs`,
`io/zenoh-transport/tests/shm_rx_after_close_multicast.rs`, `TestSessions::open_pairs_multicast`
(`zenoh/tests/session.rs`'s `zenoh_session_multicast`), `zenoh/tests/adminspace.rs`'s
`test_adminspace_read` and `zenoh/tests/routing.rs`'s `three_node_combination_multicast`, and to
the interface every `ScoutingGroup` scouts on. The zenoh-transport targets `multicast_transport`
and `multicast_compression` still join theirs on the default interface; they are gated behind
`upstream_only_tests`.

**Tests and the zenoh on the host** (BER-2469). A session opened from `Config::default()`
scouts the default group, 224.0.0.224:7446, and finds whatever zenoh the host runs there, so
its test's result would depend on the host. So:

* A test that scouts does so in a `zenoh_test::ScoutingGroup`: 224.0.0.224 on a port the group
  holds for its lifetime, joined on loopback or `ZENOH_TEST_MULTICAST_IFACE`, with scouting
  on. Switching scouting off to keep clear of the host's router would test a configuration
  the consumer does not run.
* A test whose subject needs a router lives in `zenohd/tests/` and runs the zenohd built from
  this fork, `env!("CARGO_BIN_EXE_zenohd")`, in its group through `zenoh_test::Zenohd`. Cargo
  builds that binary for the same run, and only a test of the zenohd package is given it. Each
  session waits for its router link (`Zenohd::wait_linked`), and two sessions that find each
  other wait for theirs (`wait_for_peers`), before the test body: a fixed sleep is how
  BER-2457 was found.
* A doc example that opens a session or scouts builds its `config` from a `ScoutingGroup` in a
  hidden line, so its rendered text changes by one token at most. Count such examples from
  rustdoc's own list, which `cargo test --doc` prints: a scan that pairs doc fences in order
  misreads the blocks upstream leaves unclosed.
* `zenohd/tests/scouting_group.rs` checks the property itself: a session finds the zenohd in
  its own group, and a session in another group finds nothing.

**Tests and the files they bind** (BER-2480). An endpoint whose address is a path,
`unixsock-stream` or `unixpipe`, binds where the path says, and a relative path binds in the
test's working directory, which is its package's directory. So a test takes such an endpoint
from a `zenoh_test::TestDir`, as `dir.endpoint("unixsock-stream", "a.sock")`: a directory of
its own in the system's temporary directory, removed when it drops, also when the test fails
or panics; `commons/zenoh-test/tests/test_dir.rs` tests that. Two checks keep it so:

* `io/zenoh-transport/tests/endpoints.rs` asserts that its package's directory holds no
  socket or named pipe while its own are bound;
* `commons/zenoh-test/tests/unix_endpoints.rs` scans every `.rs` file under a `tests`
  directory for such an endpoint a test writes itself. A scan, because most of these tests
  are ignored or need a feature that is off, and a test that does not run checks nothing.

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

A run with `--include-ignored` and `transport_unixpipe` on HANGS as well, on three tests
upstream ignores without giving a reason (`fa0bc23f7`): `transport_whitelist_unixpipe` in
`io/zenoh-transport/tests/transport_whitelist.rs`, `transport_unixpipe_concurrent` in
`unicast_concurrent.rs` and `transport_unixpipe_simultaneous` in `unicast_simultaneous.rs`.
Each hangs after its body has finished. They hang the same way with their files as they
were before BER-2480, so a hang there is not a regression (BER-2487).
