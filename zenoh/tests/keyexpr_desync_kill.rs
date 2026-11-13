//
// LOCAL PATCH, not part of upstream zenoh. Regression test carried with the patch to
// `api/session.rs` (eclipse-zenoh/zenoh issue 2860).
//

//! A subscriber PROCESS killed with SIGKILL must not misroute a sample for its neighbours.
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

use std::{process::Stdio, sync::Arc, time::Duration};

use keyexpr_desync_support::{
    checking_subscribers, listener_locator, probe_delivery, spawn_pump, Observed,
    VICTIM_READY_KEY,
};
use zenoh_core::ztimeout;
use zenoh_test::TestSessions;

const TIMEOUT: Duration = Duration::from_secs(60);

/// Env var carrying the locator the victim child connects to. Its absence means the test
/// binary was not re-executed as a victim, and the victim test then does nothing.
const VICTIM_LOCATOR: &str = "ZENOH_DESYNC_VICTIM_LOCATOR";

/// How long each arm perturbs for. Short enough to belong in the default suite; the arms
/// are gates on an invariant, so their value does not come from a long run.
const ARM: Duration = Duration::from_secs(21);

/// Let the pump get going before the first perturbation, so data is genuinely in flight.
const SETTLE: Duration = Duration::from_secs(1);

/// A victim subscriber running in its own process, killed when this value is dropped.
///
/// `tokio::process` is not available here (zenoh's tokio dev-dependency does not enable the
/// `process` feature), and `std::process::Child` has no `kill_on_drop`, so the guard is
/// explicit: an assertion failure or a panic anywhere in the arm still reaps the child
/// instead of leaving a subscriber connected to a listener nobody owns any more.
struct Victim(std::process::Child);

impl Victim {
    fn spawn(locator: &str) -> Self {
        // Re-exec this test binary against the one ignored victim test, the way `atexit.rs`
        // runs a test in its own process.
        let child = std::process::Command::new(std::env::current_exe().expect("current_exe"))
            .args([
                "victim_subscriber_until_killed",
                "--exact",
                "--include-ignored",
            ])
            .env(VICTIM_LOCATOR, locator)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn victim");
        Self(child)
    }

    /// Fail if the child has ALREADY exited.
    ///
    /// `kill_now` on a dead child is silent, so without this a cycle that killed nothing
    /// cannot be told from one that killed a live subscriber. The readiness check before the
    /// loop proves a victim CAN connect; this proves this cycle's victim was still there.
    fn assert_still_running(&mut self, cycle: u32) {
        if let Some(status) = self.0.try_wait().expect("try_wait on victim") {
            panic!(
                "cycle {cycle}: the victim had already exited ({status}) before it could be \
                 killed, so this cycle perturbed nothing"
            );
        }
    }

    /// SIGKILL the child and reap it. Safe to call more than once.
    fn kill_now(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Drop for Victim {
    fn drop(&mut self) {
        self.kill_now();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_killed_subscriber_process_does_not_misroute() {
    zenoh::init_log_from_env_or("error");
    let mut ctx = TestSessions::new();
    let (observer, publisher) = ztimeout!(ctx.open_pairs_client());
    let locator = listener_locator(&ctx);

    // The observer's subscriptions are declared ONCE, here, and watched across every kill.
    // They are what a long-running service holds while a peer dies beside it.
    let observed = Observed::new();
    let _watching = checking_subscribers(&observer, true, None, &observed).await;
    let publisher = Arc::new(publisher);
    let pump = spawn_pump(Arc::clone(&publisher));
    tokio::time::sleep(SETTLE).await;

    // NON-VACUITY: a victim must be shown to connect and subscribe before any kill counts.
    // Without this the arm passes when every child failed to start -- it would kill nothing,
    // observe nothing go wrong, and report success. The victim publishes on a key the
    // observer already matches, so this also proves the router routes between them.
    {
        let mut probe = Victim::spawn(&locator);
        let probe_deadline = std::time::Instant::now() + Duration::from_secs(20);
        while observed.ready() == 0 && std::time::Instant::now() < probe_deadline {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        probe.kill_now();
        drop(probe);
        assert!(
            observed.ready() > 0,
            "no victim ever announced itself within 20 s, so this run kills nothing that was \
             connected and proves nothing about an abruptly dying subscriber"
        );
    }

    let delivered_before = observed.delivered();
    let deadline = std::time::Instant::now() + ARM;
    let mut cycles = 0u32;
    while std::time::Instant::now() < deadline {
        cycles += 1;
        // Re-exec this test binary as a victim subscriber, the way `atexit.rs` runs a test
        // in its own process. A separate process is the point: it can be SIGKILLed, so the
        // transport is left to time out with no Close frame, as after a kill -9.
        let mut victim = Victim::spawn(&locator);
        // Long enough to connect and declare, short enough to churn.
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        victim.assert_still_running(cycles);
        // SIGKILL: no Close frame, so the transport is left to time out.
        victim.kill_now();
        // Respawn at once, so the next link races the far side's teardown of the old one.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    pump.abort();
    assert!(
        cycles > 0,
        "the arm ran no cycles, so no victim was ever killed"
    );
    observed.assert_traffic_flowed(&format!("kill arm, {cycles} cycle(s)"));
    observed.assert_delivered_since(delivered_before, &format!("kill arm, {cycles} cycle(s)"));
    observed.assert_no_misroute(&format!("kill arm, {cycles} cycle(s)"));

    // THE PROBE PHASE. Everything above establishes that traffic flowed while the
    // perturbation ran; this establishes that it still flows AFTERWARDS, exactly. The
    // production defect's lasting symptom was a silent discard of every later publication,
    // which a misroute count and a delivered count both read as healthy.
    probe_delivery(&publisher, &observed, 50, &format!("kill arm, {cycles} cycle(s)")).await;
    ctx.close().await;
}

/// The victim: connect, subscribe, and wait to be killed.
///
/// Ignored, because it is not a test -- it is the child half of
/// `test_killed_subscriber_process_does_not_misroute`, which re-executes this binary to
/// reach it. With no locator in the environment it returns at once, so running the suite
/// with `--include-ignored` cannot hang here.
#[ignore = "child process of the kill arm; run only via re-exec, never on its own"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn victim_subscriber_until_killed() {
    let Ok(locator) = std::env::var(VICTIM_LOCATOR) else {
        return;
    };
    zenoh::init_log_from_env_or("error");

    let mut config = zenoh::Config::default();
    config.insert_json5("mode", "\"client\"").unwrap();
    config
        .insert_json5("connect/endpoints", &format!("[\"{locator}\"]"))
        .unwrap();
    config
        .insert_json5("scouting/multicast/enabled", "false")
        .unwrap();
    let session = zenoh::open(config).await.expect("victim session");

    let observed = Observed::new();
    let _subs = checking_subscribers(&session, true, None, &observed).await;

    // Announce, repeatedly, that this victim is connected and subscribed. The parent counts
    // these to know a child really linked; publishing rather than sleeping silently is what
    // makes the parent's non-vacuity check possible at all. The payload is the key, like
    // every other publication here, so the misroute check is unaffected.
    //
    // The loop also waits to be SIGKILLed; the bound only stops a victim whose parent died
    // from outliving it for long.
    let until = std::time::Instant::now() + Duration::from_secs(120);
    while std::time::Instant::now() < until {
        let _ = session.put(VICTIM_READY_KEY, VICTIM_READY_KEY).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
