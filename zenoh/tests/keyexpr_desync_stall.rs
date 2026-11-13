//
// LOCAL PATCH, not part of upstream zenoh. Regression test carried with the patch to
// `api/session.rs` (eclipse-zenoh/zenoh issue 2860).
//

//! A subscriber whose callback blocks past the transport lease must not misroute a sample.
//!
//! These arms were the ORIGINAL hypothesis for the key-expression desync seen in production:
//! that a transport teardown racing a reconnect leaves the two ends disagreeing about what an
//! expression id means. That hypothesis was wrong -- the cause was #2860, the `u16` id counter
//! wrapping, which needs no perturbation at all and is pinned by `expr_id_rollover.rs`.
//!
//! This is kept as a GATE, not as a measurement: a future change that does make transport churn
//! misroute a sample cannot then land quietly. It asserts an invariant -- traffic flowed, and
//! nothing arrived on a key other than the one it was put on -- and deliberately never asserts a
//! count of zenoh's own log lines. See `keyexpr_desync_support` for why the oracle is arrival.
//!
//! One arm per file because cargo runs test BINARIES one at a time but the functions inside one
//! binary in parallel, and three of these pumping traffic at once starve each other.

// The lint set of the project that carries this fork (its root Cargo.toml,
// `[workspace.lints]`), applied here as crate attributes rather than through the workspace.
// Every file under tests/ is its own crate root, so this binds EXACTLY the code we wrote.
// Opting the upstream crates in would mean 2,451 findings in code we do not own -- about
// 2,000 of them missing doc comments on zenoh's public API -- and every one of those edits
// would conflict on the next rebase onto a newer upstream tag.
#![deny(warnings)]
#![deny(missing_docs)]
#![deny(clippy::all)]
#![deny(clippy::too_many_lines)]
#![deny(clippy::equatable_if_let)]
#![deny(clippy::cognitive_complexity)]
#![deny(clippy::unnested_or_patterns)]

mod keyexpr_desync_support;

use std::{sync::Arc, time::Duration};

use keyexpr_desync_support::{
    checking_subscribers, open_client_with_lease, probe_delivery, spawn_pump, Observed,
};
use zenoh_core::ztimeout;
use zenoh_test::TestSessions;

const TIMEOUT: Duration = Duration::from_secs(60);

/// How long each arm perturbs for. Short enough to belong in the default suite; the arms
/// are gates on an invariant, so their value does not come from a long run.
const ARM: Duration = Duration::from_secs(21);

/// Let the pump get going before the first perturbation, so data is genuinely in flight.
const SETTLE: Duration = Duration::from_secs(1);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_stalled_subscriber_does_not_misroute() {
    zenoh::init_log_from_env_or("error");
    let mut ctx = TestSessions::new();
    let (observer, publisher) = ztimeout!(ctx.open_pairs_client());

    let observed = Observed::new();
    let _watching = checking_subscribers(&observer, true, None, &observed).await;
    let publisher = Arc::new(publisher);
    let pump = spawn_pump(Arc::clone(&publisher));
    tokio::time::sleep(SETTLE).await;

    let delivered_before = observed.delivered();
    let deadline = std::time::Instant::now() + ARM;
    let mut cycles = 0u32;
    while std::time::Instant::now() < deadline {
        cycles += 1;
        // The lease is set SHORT here on purpose. The default is 10 s, and this arm used to
        // block for 6 s against it while claiming to block "longer than the transport
        // lease" -- so the lease never expired, the far side never closed the link as
        // unresponsive, and the arm measured nothing it claimed to. With a 2 s lease a 4 s
        // block genuinely expires it, and the cycle is faster besides.
        let session = open_client_with_lease(&mut ctx, 2_000).await;
        let subs =
            checking_subscribers(&session, true, Some(Duration::from_secs(4)), &observed).await;
        tokio::time::sleep(Duration::from_secs(6)).await;
        drop(subs);
        let _ = session.close().await;
    }

    pump.abort();
    assert!(cycles > 0, "the arm ran no cycles, so it perturbed nothing");
    observed.assert_traffic_flowed(&format!("stall arm, {cycles} cycle(s)"));
    observed.assert_delivered_since(delivered_before, &format!("stall arm, {cycles} cycle(s)"));
    observed.assert_no_misroute(&format!("stall arm, {cycles} cycle(s)"));

    // THE PROBE PHASE. Everything above establishes that traffic flowed while the
    // perturbation ran; this establishes that it still flows AFTERWARDS, exactly. The
    // production defect's lasting symptom was a silent discard of every later publication,
    // which a misroute count and a delivered count both read as healthy.
    probe_delivery(&publisher, &observed, 50, &format!("stall arm, {cycles} cycle(s)")).await;
    ctx.close().await;
}
