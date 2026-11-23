//
// LOCAL PATCH, not part of upstream zenoh. Regression test carried with the patch to
// `api/session.rs` (eclipse-zenoh/zenoh issue 2860).
//

//! A subscriber session that closes and reopens must not misroute a sample.
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
    checking_subscribers, open_client, open_router_with_two_clients, probe_delivery, spawn_pump,
    Observed,
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
async fn test_subscriber_reconnect_does_not_misroute() {
    zenoh::init_log_from_env_or("error");
    let mut ctx = TestSessions::new();
    // THE ONE ARM THAT RUNS AGAINST A ROUTER, which is the topology we deploy: two clients
    // on a zenohd. The other arms keep the default peer listener, so both hats are covered.
    // `_router` is bound, not dropped, because dropping it closes the hub.
    let (_router, observer, publisher) = ztimeout!(open_router_with_two_clients(&mut ctx));

    let observed = Observed::new();
    let _watching = checking_subscribers(&observer, true, None, &observed).await;
    let publisher = Arc::new(publisher);
    let pump = spawn_pump(Arc::clone(&publisher));
    tokio::time::sleep(SETTLE).await;

    // Sampled before the window: the assertions below ask whether delivery happened DURING
    // the perturbation, which a cumulative counter cannot answer on its own.
    let delivered_before = observed.delivered();
    let deadline = std::time::Instant::now() + ARM;
    let mut cycles = 0u32;
    while std::time::Instant::now() < deadline {
        cycles += 1;
        let session = open_client(&mut ctx).await;
        let subs = checking_subscribers(&session, true, None, &observed).await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        drop(subs);
        // Close and reopen with no pause, so the next link lands while the far side is
        // still tearing the old transport down.
        let _ = session.close().await;
    }

    pump.abort();
    assert!(cycles > 0, "the arm ran no cycles, so it perturbed nothing");
    observed.assert_traffic_flowed(&format!("reconnect arm, {cycles} cycle(s)"));
    observed.assert_delivered_since(
        delivered_before,
        &format!("reconnect arm, {cycles} cycle(s)"),
    );
    observed.assert_no_misroute(&format!("reconnect arm, {cycles} cycle(s)"));

    // THE PROBE PHASE. Everything above establishes that traffic flowed while the
    // perturbation ran; this establishes that it still flows AFTERWARDS, exactly. The
    // production defect's lasting symptom was a silent discard of every later publication,
    // which a misroute count and a delivered count both read as healthy.
    probe_delivery(
        &publisher,
        &observed,
        50,
        &format!("reconnect arm, {cycles} cycle(s)"),
    )
    .await;
    ctx.close().await;
}
