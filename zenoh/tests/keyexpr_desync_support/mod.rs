//
// LOCAL PATCH, not part of upstream zenoh. Shared support for the key-expression
// desync regression tests (eclipse-zenoh/zenoh issue 2860). Carried with the patch
// to `api/session.rs`.
//

//! Shared pieces for the `keyexpr_desync_*` tests.
//!
//! # The oracle is what ARRIVED, not what zenoh logged
//!
//! Every sample's payload IS the key it was published to, so a sample that arrives
//! on a key its payload disagrees with is a misroute -- a string comparison the
//! subscriber makes for itself. It cannot be confused by ordering or by how many
//! samples are in flight, and, unlike a count of zenoh's own `unknown expr_id` log
//! lines, it does not break when zenoh rewords a message. These tests exist to
//! catch a REGRESSION of the id-reuse fix on some future rebase, so the assertion
//! has to outlive zenoh's internal strings.
//!
//! # Every test here needs a non-vacuity control
//!
//! "No misroutes" and "no traffic at all" look identical from the outside, and a
//! session that never connected reports the quieter of the two. So every test both
//! asserts a defect is absent AND asserts delivery happened -- see
//! [`Observed::assert_traffic_flowed`]. A test that only checked for the defect
//! would pass on a broken harness.

// Each test binary compiles this whole module and uses a subset of it; the unused
// remainder is not dead code in the project, only in that binary.
#![allow(dead_code)]

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use zenoh::{pubsub::Subscriber, qos::CongestionControl, Session};
use zenoh_test::TestSessions;

/// The keys the publisher writes to.
///
/// Several, because a desync is per expression id: one key may land on an id that
/// still happens to resolve, and a single key could therefore read clean while the
/// session is broken.
pub const TRACKED_KEYS: &[&str] = &["desync/alpha/one", "desync/beta/two", "desync/gamma/three"];

/// The key the kill arm's victim publishes to say it has connected and subscribed.
///
/// Under `desync/alpha/`, so the observer's existing wildcard matches it, and the payload is
/// the key like every other publication so the misroute check is unaffected.
pub const VICTIM_READY_KEY: &str = "desync/alpha/victim_ready";

/// The key the PROBE phase publishes to, after the perturbation has stopped.
///
/// Separate from [`TRACKED_KEYS`] so the probe count is exact: the pump writes the tracked
/// keys continuously and is aborted before probing, and only `desync/alpha/**` matches this,
/// so exactly one of the observer's subscribers sees each probe.
pub const PROBE_KEY: &str = "desync/alpha/probe";

/// Wildcard forms of [`TRACKED_KEYS`], one per key, matching what a service
/// actually subscribes with.
pub const TRACKED_WILDCARDS: &[&str] = &["desync/alpha/**", "desync/beta/**", "desync/gamma/**"];

/// What the far side saw.
#[derive(Default)]
pub struct Observed {
    /// Samples that arrived at all, so "no misroutes" is distinguishable from
    /// "no traffic".
    delivered: AtomicU64,
    /// Samples that arrived on a key their own payload disagrees with.
    misrouted: AtomicU64,
    /// The first few misroutes, in full, so a failure says what went where
    /// instead of only how many.
    detail: Mutex<Vec<String>>,
    /// Arrivals on [`VICTIM_READY_KEY`], i.e. proof that a victim process linked.
    ready: AtomicU64,
    /// Arrivals on [`PROBE_KEY`], counted so the probe phase can assert an EXACT number.
    probes: AtomicU64,
}

impl Observed {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn delivered(&self) -> u64 {
        self.delivered.load(Ordering::SeqCst)
    }

    pub fn misrouted(&self) -> u64 {
        self.misrouted.load(Ordering::SeqCst)
    }

    /// The non-vacuity control. Call it in EVERY test, beside the assertion about
    /// the defect: without it a session that never connected reports the same
    /// zeroes as a session that worked perfectly.
    pub fn assert_traffic_flowed(&self, context: &str) {
        assert!(
            self.delivered() > 0,
            "{context}: nothing was delivered at all, so this run proves nothing about \
             misrouting -- the sessions probably never linked up"
        );
    }

    /// The defect assertion: no sample arrived on a key other than the one it was
    /// published to.
    pub fn assert_no_misroute(&self, context: &str) {
        let n = self.misrouted();
        if n == 0 {
            return;
        }
        let detail = self.detail.lock().unwrap().join("\n  ");
        panic!(
            "{context}: {n} sample(s) arrived on a key other than the one they were put on, \
             after {} delivered. This is the id-reuse defect of #2860 or a regression of its \
             fix in api/session.rs.\n  {detail}",
            self.delivered()
        );
    }

    pub fn ready(&self) -> u64 {
        self.ready.load(Ordering::SeqCst)
    }

    /// The control for a window: delivery must have happened DURING it.
    ///
    /// [`Observed::assert_traffic_flowed`] reads a cumulative counter, which the pump
    /// satisfies before the first perturbation -- so on its own it cannot tell "traffic kept
    /// flowing" from "traffic stopped the instant the window opened". Pass a count sampled
    /// before the window.
    pub fn assert_delivered_since(&self, before: u64, context: &str) {
        let now = self.delivered();
        assert!(
            now > before,
            "{context}: nothing arrived during the window ({before} delivered before, {now} \
             after), so a clean misroute count says nothing -- delivery had already stopped"
        );
    }

    pub fn probes(&self) -> u64 {
        self.probes.load(Ordering::SeqCst)
    }

    fn record(&self, arrived_on: &str, payload: &str) {
        self.delivered.fetch_add(1, Ordering::SeqCst);
        if arrived_on == VICTIM_READY_KEY {
            self.ready.fetch_add(1, Ordering::SeqCst);
        }
        if arrived_on == PROBE_KEY {
            self.probes.fetch_add(1, Ordering::SeqCst);
        }
        if arrived_on != payload {
            let n = self.misrouted.fetch_add(1, Ordering::SeqCst) + 1;
            if n <= 8 {
                self.detail
                    .lock()
                    .unwrap()
                    .push(format!("arrived on {arrived_on} but was put on {payload}"));
            }
        }
    }
}

/// Declare one checking subscriber per target key.
///
/// `stall` blocks the FIRST subscriber's callback for that long, which is how a
/// transport is made to look unresponsive: zenoh closes it once the lease expires.
/// Only the first stalls -- stalling all of them collapses throughput, and a
/// misroute cannot be seen on a subscription that never delivers.
pub async fn checking_subscribers(
    session: &Session,
    wildcard: bool,
    stall: Option<Duration>,
    observed: &Arc<Observed>,
) -> Vec<Subscriber<()>> {
    let targets: &[&str] = if wildcard {
        TRACKED_WILDCARDS
    } else {
        TRACKED_KEYS
    };
    let mut subs = Vec::with_capacity(targets.len());
    for (i, key) in targets.iter().enumerate() {
        let observed = Arc::clone(observed);
        let stall_for = if i == 0 { stall } else { None };
        let sub = session
            .declare_subscriber(*key)
            .callback(move |sample| {
                // Under a wildcard the declared expression is not the key a sample
                // arrives on, so the comparison is ARRIVAL key against payload --
                // which stays exact, because the publisher writes the key it put to.
                let arrived_on = sample.key_expr().as_str().to_owned();
                let payload = String::from_utf8_lossy(&sample.payload().to_bytes()).into_owned();
                observed.record(&arrived_on, &payload);
                if let Some(d) = stall_for {
                    std::thread::sleep(d);
                }
            })
            .await
            .unwrap_or_else(|e| panic!("declare_subscriber {key}: {e}"));
        subs.push(sub);
    }
    subs
}

/// Publish on every tracked key, forever, with the payload set to the key.
///
/// No pause between rounds: a perturbation can only be observed if data is
/// genuinely in flight when it happens.
pub fn spawn_pump(session: Arc<Session>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            for key in TRACKED_KEYS {
                // Drop, not Block: zenoh's default and what every caller in our
                // services uses. Block would make a stalled subscriber back the
                // publisher up instead of exercising the transport.
                let _ = session
                    .put(*key, key.as_bytes())
                    .congestion_control(CongestionControl::Drop)
                    .await;
            }
            tokio::task::yield_now().await;
        }
    })
}

/// Send exactly `n` probes, then require exactly `n` of them to arrive.
///
/// This is the check the three churn arms were missing, and it is the one that matches the
/// production symptom. The incident behind this harness did not misroute for long: it went
/// on to DISCARD every later publication silently (BER-1887, BER-1889). A clean misroute
/// count and a non-zero delivered count are both consistent with that, so neither sees it.
/// An exact count after the perturbation does.
///
/// Probes are published with `CongestionControl::Block`, unlike the pump: the pump is
/// aborted before this runs, so the link is idle, and Block means a missing probe is a real
/// loss rather than congestion control doing its job.
pub async fn probe_delivery(session: &Session, observed: &Arc<Observed>, n: u64, context: &str) {
    let before = observed.probes();
    for _ in 0..n {
        session
            .put(PROBE_KEY, PROBE_KEY.as_bytes())
            .congestion_control(CongestionControl::Block)
            .await
            .unwrap_or_else(|e| panic!("{context}: probe put failed: {e}"));
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while observed.probes() - before < n && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let got = observed.probes() - before;
    assert_eq!(
        got, n,
        "{context}: {got} of {n} probes arrived after the perturbation stopped. Fewer means \
         publications are being discarded silently, which is what the production incident \
         actually did once its ids desynced -- a clean misroute count does not see it. More \
         means a sample was duplicated."
    );
}

/// Open a ROUTER plus two CLIENTS on it, which is the topology we actually run.
///
/// `TestSessions`'s own listener never sets a mode, and zenoh's default is `peer`
/// (`zenoh-config`'s defaults), so every other gate here runs client-to-PEER. A peer and a
/// router are different hats -- `net/routing/gateway.rs` picks `hat::peer::Hat` against
/// `hat::router::Hat` -- and the router hat is the one our bus uses and the one the
/// incident's routing defects were found in. Without this, none of these tests touches it.
///
/// `listen/endpoints` is inherited from `get_listener_config`, i.e. set EXPLICITLY to
/// `tcp/127.0.0.1:0`. That is not incidental: a router that is left to default listens on
/// `tcp/[::]:7447`, the production router port, on every interface.
pub async fn open_router_with_two_clients(ctx: &mut TestSessions) -> (Session, Session, Session) {
    let mut config = ctx.get_listener_config("tcp/127.0.0.1:0", 1);
    config
        .insert_json5("mode", "\"router\"")
        .expect("set router mode");
    let router = ctx.open_listener_with_cfg(config).await;
    let observer = open_client(ctx).await;
    let publisher = open_client(ctx).await;
    (router, observer, publisher)
}

/// Open another CLIENT session on the same listener as `ctx`.
/// Open a CLIENT session whose transport lease is `lease_ms`.
///
/// The default lease is 10 s (`zenoh-config`'s defaults), which is longer than any stall a
/// test can afford to sit through -- so a stall test using the default never actually expires
/// a lease, and silently measures nothing. Shortening it is what makes that arm real.
pub async fn open_client_with_lease(ctx: &mut TestSessions, lease_ms: u64) -> Session {
    let mut config = ctx.get_connector_config();
    config
        .insert_json5("mode", "\"client\"")
        .expect("set client mode");
    config
        .insert_json5("transport/link/tx/lease", &lease_ms.to_string())
        .expect("set lease");
    ctx.open_connector_with_cfg(config).await
}

pub async fn open_client(ctx: &mut TestSessions) -> Session {
    let mut config = ctx.get_connector_config();
    // json5 rather than a typed setter, so this module needs no types from the protocol
    // crate and keeps compiling if those move.
    config
        .insert_json5("mode", "\"client\"")
        .expect("set client mode");
    ctx.open_connector_with_cfg(config).await
}

/// The first locator of `ctx`'s listener, as a string a child process can connect to.
pub fn listener_locator(ctx: &TestSessions) -> String {
    ctx.locators()
        .first()
        .expect("TestSessions has no locator: open_listener was never called")
        .to_string()
}
