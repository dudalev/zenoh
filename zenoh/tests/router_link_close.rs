//
// LOCAL PATCH, not part of upstream zenoh. Regression test for the router-hat panic on a
// closing ROUTER link. Carried with the guard in net/routing/hat/router/mod.rs --
// resource.rs is only where the panic SURFACES (`context_mut()` -> `ctx.as_mut().unwrap()`),
// and it carries no part of the fix.
//

//! A ROUTER link closing must not take the surviving router's routing tables with it.
//!
//! `Hat::res_hat_mut` reaches `Resource::context_mut()`, which is `self.ctx.as_mut().unwrap()`.
//! zenoh's own comment beside it says a matched resource is "expected" to have a context and
//! that the invariant is "worth checking in debug mode, until we are confident it always
//! holds". It does not always hold: when a router link closes, `unregister_router_entities`
//! walks the hat's `router_subs` / `router_qabls` / `router_tokens` sets and calls
//! `res_hat_mut` on each, and a resource whose context has already gone panics there.
//!
//! The panic is raised while the caller holds `tables.ctrl_lock`, so it does not merely kill
//! one task: it POISONS the router's control lock, and every later `zlock!` on it fails. That
//! is the damage this test is written around.
//!
//! # WHAT THIS TEST DOES NOT DO: it is not a gate for that panic
//!
//! Measured 2026-11-13, on the tree BEFORE the context guard was added: this test passes,
//! with zero panics and zero `PoisonError`, both when the peer router is closed gracefully
//! with `Session::close()` AND when its process is SIGKILLed. So a router link going away --
//! by either route -- does not by itself produce a context-less resource.
//!
//! The only known trigger remains zenoh's own `tests/regions` scenarios, which exercise the
//! `Region` / `Bound` machinery (`FaceState::region`, `remote_bound`, and the `is_north()`
//! branch in `Resource::decl_key`) that this two-router topology does not. The guard's
//! verification is therefore the count of non-test panics in one `--nocapture` run of that
//! binary: 27 at `resource.rs:491` before, and what it is after.
//!
//! This test is kept anyway, and it is not vacuous: it pins the SURROUNDING invariant, that
//! an abruptly dying router link leaves the survivor routing between its own clients. If a
//! future change makes a router link loss poison the tables, this fails where the `regions`
//! panic count would only tell you afterwards. The peer runs in its own process, re-executed
//! from this binary the way `atexit.rs` does, so the link can be killed with no Close frame.
//!
//! # The oracle is that the survivor still routes
//!
//! A panic on a background task does not by itself fail a test, and asserting on panic TEXT
//! would be the brittle thing this suite avoids. So the assertion is behavioural: after the
//! second router goes away, a client of the first must still be able to publish to another
//! client of the first. A poisoned `ctrl_lock` stops that, and so does a corrupted table.
//!
//! The control, asserted BEFORE the close, is that the same publication works at all --
//! otherwise a test that never routed anything would "pass" by delivering nothing twice.

// The lint set of the project that carries this fork (its root Cargo.toml,
// `[workspace.lints]`), applied here as crate attributes rather than through the workspace.
// Every file under tests/ is its own crate root, so this binds EXACTLY the code we wrote.
#![deny(warnings)]
#![deny(missing_docs)]
#![deny(clippy::all)]
#![deny(clippy::too_many_lines)]
#![deny(clippy::equatable_if_let)]
#![deny(clippy::cognitive_complexity)]
#![deny(clippy::unnested_or_patterns)]

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use std::process::Stdio;

use zenoh_core::ztimeout;
use zenoh_test::get_free_tcp_port;

const TIMEOUT: Duration = Duration::from_secs(60);

/// Samples put on the probe key by each round.
const PROBES: usize = 10;

/// Env var carrying the port the peer router connects to. Its absence means this binary was
/// not re-executed as the peer, and the peer test then does nothing.
const PEER_PORT: &str = "ZENOH_ROUTER_CLOSE_PEER_PORT";

const PROBE_KEY: &str = "test/router_close/probe";
const SUB_ALL: &str = "test/router_close/**";
/// Declared by a THIRD CLIENT OF ROUTER1 -- not, as this said until it was checked, by a
/// client of the second router. `remote_client` connects to `port`, and `port` is router1's
/// own listener, so there is no path by which it reaches router2.
///
/// It is kept because it is a subscription that outlives the peer link and so must still
/// resolve after it closes. What actually puts entries in router1's hat keyed by the PEER's
/// zid -- the set `unregister_router_entities` walks -- is the peer router's own two
/// declarations, made inside the child process.
const REMOTE_SUB: &str = "test/router_close/remote/**";

/// A router-mode config listening on `port`.
fn listening_router(port: u16) -> zenoh::Config {
    let mut c = zenoh::Config::default();
    c.insert_json5("mode", "\"router\"").unwrap();
    c.insert_json5("listen/endpoints", &format!("[\"tcp/127.0.0.1:{port}\"]"))
        .unwrap();
    c.insert_json5("scouting/multicast/enabled", "false")
        .unwrap();
    c
}

/// A config in `mode` that connects to `port` and listens on nothing.
///
/// `listen/endpoints` is set to the EMPTY list deliberately, and that is not redundant: the
/// default for `router` mode is `tcp/[::]:7447` (`zenoh-config`'s defaults), which is the
/// port a production zenohd binds. Leaving it at the default made this test open that port on
/// whatever machine ran `cargo test`, and if the port was already held the peer below died
/// silently and the test passed with no router link at all -- a vacuous pass of the exact
/// thing it exists to check. The link assertion further down is the other half of that fix.
fn connecting(mode: &str, port: u16) -> zenoh::Config {
    let mut c = zenoh::Config::default();
    c.insert_json5("mode", &format!("\"{mode}\"")).unwrap();
    c.insert_json5("listen/endpoints", "[]").unwrap();
    c.insert_json5("connect/endpoints", &format!("[\"tcp/127.0.0.1:{port}\"]"))
        .unwrap();
    c.insert_json5("scouting/multicast/enabled", "false")
        .unwrap();
    c
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_router_link_close_leaves_the_surviving_router_routing() {
    zenoh::init_log_from_env_or("error");
    let port = get_free_tcp_port();

    // The survivor, and the peer router in its own process so the link can be killed.
    let router1 = ztimeout!(zenoh::open(listening_router(port))).unwrap();

    // Baseline BEFORE the peer exists, so the check below is growth rather than a count --
    // whether a router reports its own zid here does not matter.
    let routers_before = router1.info().routers_zid().await.count();
    let mut peer = Peer::spawn(port);

    // Clients of router1: one subscribing, one publishing.
    let sub_client = ztimeout!(zenoh::open(connecting("client", port))).unwrap();
    let pub_client = ztimeout!(zenoh::open(connecting("client", port))).unwrap();

    let delivered = Arc::new(AtomicUsize::new(0));
    let _probe_sub = {
        let delivered = delivered.clone();
        ztimeout!(sub_client.declare_subscriber(SUB_ALL).callback(move |_| {
            delivered.fetch_add(1, Ordering::SeqCst);
        }))
        .unwrap()
    };

    // A third client of ROUTER1 (see REMOTE_SUB): its subscription outlives the peer link, so
    // it must still resolve after that link closes. It is NOT reached through router2 -- the
    // peer-keyed entries come from the peer router's own declarations in the child.
    let remote_client = ztimeout!(zenoh::open(connecting("client", port))).unwrap();
    let _remote_sub = ztimeout!(remote_client
        .declare_subscriber(REMOTE_SUB)
        .callback(|_| {}))
    .unwrap();
    // Long enough for the peer router to connect and declare, so the survivor's hat really
    // holds entries keyed by the peer's zid before the peer is killed.
    tokio::time::sleep(Duration::from_secs(4)).await;

    // NON-VACUITY: the peer router must actually have linked. Without this the test passes
    // when the child never started -- which is what a busy port used to cause, silently,
    // because the child's output goes to /dev/null. Everything below this point is about a
    // router link going away, so a run where none was ever established proves nothing.
    let mut linked = routers_before;
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while std::time::Instant::now() < deadline {
        linked = router1.info().routers_zid().await.count();
        if linked > routers_before {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        linked > routers_before,
        "the peer router never linked to the survivor ({routers_before} known routers before, \
         {linked} after 20 s), so this run cannot say anything about a router link closing"
    );

    // CONTROL: the path works before anything closes. Without this, "still delivers 0 after
    // the close" would read as a pass.
    for _ in 0..PROBES {
        ztimeout!(pub_client.put(PROBE_KEY, PROBE_KEY)).unwrap();
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        delivered.load(Ordering::SeqCst),
        PROBES,
        "the probe must be delivered before the router link closes"
    );

    // Kill the peer ROUTER's process. No Close frame: the survivor finds the link gone and
    // runs `unregister_router_entities` over its hat's router-keyed sets.
    peer.kill_now();
    drop(peer);
    tokio::time::sleep(Duration::from_secs(6)).await;

    // THE ASSERTION: router1 still routes between its own clients. A `ctrl_lock` poisoned by
    // a panic inside the unregister walk stops this.
    for _ in 0..PROBES {
        let _ = ztimeout!(pub_client.put(PROBE_KEY, PROBE_KEY));
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        delivered.load(Ordering::SeqCst),
        2 * PROBES,
        "the surviving router stopped routing between its own clients after the other router's \
         link closed: {} of {} probes arrived",
        delivered.load(Ordering::SeqCst),
        2 * PROBES
    );

    ztimeout!(sub_client.close()).unwrap();
    ztimeout!(pub_client.close()).unwrap();
    ztimeout!(remote_client.close()).unwrap();
    ztimeout!(router1.close()).unwrap();
}

/// The peer router, in its own process, killed when this value is dropped.
struct Peer(std::process::Child);

impl Peer {
    fn spawn(port: u16) -> Self {
        let child = std::process::Command::new(std::env::current_exe().expect("current_exe"))
            .args(["peer_router_until_killed", "--exact", "--include-ignored"])
            .env(PEER_PORT, port.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn peer router");
        Self(child)
    }

    /// SIGKILL the peer and reap it. Safe to call more than once.
    fn kill_now(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.kill_now();
    }
}

/// The peer router: connect as a ROUTER, declare, and wait to be killed.
///
/// Ignored, because it is not a test -- it is the second half of the test above, which
/// re-executes this binary to reach it. With no port in the environment it returns at once,
/// so running the suite with `--include-ignored` cannot hang here.
#[ignore = "peer process of the router-close test; run only via re-exec, never on its own"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_router_until_killed() {
    let Ok(port) = std::env::var(PEER_PORT) else {
        return;
    };
    let port: u16 = port.parse().expect("peer port");
    zenoh::init_log_from_env_or("error");

    let session = ztimeout!(zenoh::open(connecting("router", port))).unwrap();
    // Subscriptions the survivor records against THIS router's zid.
    let _a = ztimeout!(session.declare_subscriber(SUB_ALL).callback(|_| {})).unwrap();
    let _b = ztimeout!(session.declare_subscriber(REMOTE_SUB).callback(|_| {})).unwrap();

    // Wait to be SIGKILLed; the bound only stops a peer outliving a parent that died.
    tokio::time::sleep(Duration::from_secs(120)).await;
}
