//
// Copyright (c) 2026 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
//! A put dropped for congestion reaches the drop observers of both peers, and a put that
//! reaches its subscriber reaches neither (BER-2554).
//!
//! Two peers, no router, over TCP on loopback with explicit endpoints. A held subscriber's
//! callback holds its receive thread until the test releases it, so the subscriber reads
//! nothing, the publisher's link backs up, and the publisher's droppable puts are dropped. The
//! publisher's push drop observer names the subscriber and the key. Once the callback is
//! released, a blocking put after the drops arrives behind the gap the drops left, and the
//! subscriber's rx loss observer reports it.
//!
//! The observers are installed once per process, so each test reads the events of its own
//! sessions only.
#![cfg(all(feature = "unstable", feature = "transport_tcp"))]
// The lint set of the project that carries this fork, as in `expr_id_rollover.rs`.
#![deny(warnings)]
#![deny(missing_docs)]
#![deny(clippy::all)]
#![deny(clippy::too_many_lines)]
#![deny(clippy::equatable_if_let)]
#![deny(clippy::cognitive_complexity)]
#![deny(clippy::unnested_or_patterns)]

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Condvar, Mutex, Once,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use zenoh::{
    drop_observer::{self, PushDropReason, RxLoss, RxLossKind},
    pubsub::Publisher,
    qos::{CongestionControl, Priority, Reliability},
    session::ZenohId,
    Session, Wait,
};
use zenoh_core::ztimeout;
use zenoh_link::EndPoint;
use zenoh_test::{get_free_tcp_port, get_tcp_locator, TestSessions};

const TIMEOUT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(10);
const PAYLOAD_SIZE: usize = 1024;
/// Larger than a loopback TCP link's batch of 49,152 bytes, so that a put of it is fragmented.
const FRAGMENTED_SIZE: usize = 200_000;
const MARKER: &[u8] = b"marker";
/// Puts the subscriber that keeps up is sent, one at a time.
const IN_STEP: usize = 200;

/// A push drop as the observer saw it, with its key owned.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PushDropRecord {
    zid: ZenohId,
    key_expr: Option<String>,
    priority: Option<Priority>,
    reason: PushDropReason,
}

static PUSH_DROPS: Mutex<Vec<PushDropRecord>> = Mutex::new(Vec::new());
static RX_LOSSES: Mutex<Vec<RxLoss>> = Mutex::new(Vec::new());
/// The faces for which the push drop observer panics once it has recorded a drop.
static PANICKING: Mutex<Vec<ZenohId>> = Mutex::new(Vec::new());

fn install_observers() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        drop_observer::set_push_drop_observer(|dropped| {
            PUSH_DROPS.lock().unwrap().push(PushDropRecord {
                zid: dropped.zid,
                key_expr: dropped.key_expr.map(str::to_owned),
                priority: dropped.priority,
                reason: dropped.reason,
            });
            let panics = PANICKING.lock().unwrap().contains(&dropped.zid);
            if panics {
                panic!("the observer panics for this face");
            }
        })
        .unwrap();
        drop_observer::set_rx_loss_observer(|loss| RX_LOSSES.lock().unwrap().push(*loss)).unwrap();
    });
}

/// The pushes dropped on the way to `zid`.
fn push_drops_to(zid: ZenohId) -> Vec<PushDropRecord> {
    PUSH_DROPS
        .lock()
        .unwrap()
        .iter()
        .filter(|dropped| dropped.zid == zid)
        .cloned()
        .collect()
}

/// The losses detected in what `zid` sent.
fn rx_losses_from(zid: ZenohId) -> Vec<RxLoss> {
    RX_LOSSES
        .lock()
        .unwrap()
        .iter()
        .filter(|loss| loss.zid == zid)
        .copied()
        .collect()
}

/// Holds whoever waits on it until it is released, for at most [`TIMEOUT`].
#[derive(Default)]
struct Gate {
    released: Mutex<bool>,
    changed: Condvar,
}

impl Gate {
    fn wait(&self) {
        let released = self.released.lock().unwrap();
        drop(
            self.changed
                .wait_timeout_while(released, TIMEOUT, |released| !*released)
                .unwrap(),
        );
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

async fn wait_until(what: &str, condition: impl Fn() -> bool) {
    let start = Instant::now();
    while !condition() {
        assert!(start.elapsed() < TIMEOUT, "timed out waiting until {what}");
        tokio::time::sleep(POLL).await;
    }
}

async fn wait_matching(publisher: &Publisher<'_>) {
    let start = Instant::now();
    while !ztimeout!(publisher.matching_status()).unwrap().matching() {
        assert!(
            start.elapsed() < TIMEOUT,
            "the publisher found no subscriber"
        );
        tokio::time::sleep(POLL).await;
    }
}

/// A publishing and a subscribing peer, each config adjusted by `adjust`.
async fn open_pair(
    sessions: &mut TestSessions,
    adjust: impl Fn(&mut zenoh_config::Config),
) -> (Session, Session) {
    let mut listener = sessions.get_listener_config("tcp/127.0.0.1:0", 1);
    adjust(&mut listener);
    let publishing = sessions.open_listener_with_cfg(listener).await;
    let mut connector = sessions.get_connector_config();
    adjust(&mut connector);
    let subscribing = sessions.open_connector_with_cfg(connector).await;
    (publishing, subscribing)
}

fn without_shm(config: &mut zenoh_config::Config) {
    config
        .insert_json5("transport/shared_memory/enabled", "false")
        .unwrap();
}

/// A subscriber whose callback holds its receive thread at the first sample until `gate` is
/// released, and that records whether the marker arrived.
async fn held_subscriber(
    session: &Session,
    key: &'static str,
    gate: &Arc<Gate>,
    markers: &Arc<AtomicUsize>,
) -> zenoh::pubsub::Subscriber<()> {
    ztimeout!(session.declare_subscriber(key).callback({
        let gate = gate.clone();
        let markers = markers.clone();
        move |sample| {
            if sample.payload().to_bytes().as_ref() == MARKER {
                markers.fetch_add(1, Ordering::SeqCst);
            }
            gate.wait();
        }
    }))
    .unwrap()
}

/// Puts `size` bytes until the push drop observer reports a put dropped on the way to
/// `subscriber` that satisfies `reported`.
async fn put_until(
    publisher: &Publisher<'_>,
    subscriber: ZenohId,
    size: usize,
    reported: impl Fn(&PushDropRecord) -> bool,
) {
    let start = Instant::now();
    let mut puts = 0;
    while !push_drops_to(subscriber).iter().any(&reported) {
        assert!(start.elapsed() < TIMEOUT, "nothing reported in {puts} puts");
        ztimeout!(publisher.put(vec![0_u8; size])).unwrap();
        puts += 1;
    }
}

/// Releases the held subscriber and sends two markers on the same channel with blocking puts,
/// waiting for each. The second starts a frame of its own: the first can share the frame of a
/// put that took its SN before the drops, as a `BlockFirst` put does.
async fn release_and_mark(publishing: &Session, key: &str, gate: &Gate, markers: &AtomicUsize) {
    gate.release();
    for sent in 1..=2 {
        ztimeout!(publishing
            .put(key, MARKER)
            .congestion_control(CongestionControl::Block)
            .reliability(Reliability::Reliable))
        .unwrap();
        wait_until("the marker arrives", || {
            markers.load(Ordering::SeqCst) == sent
        })
        .await;
    }
}

/// Asserts that `publishing` reported only keyed congestion drops on the way to
/// `subscribing`, and that `subscribing` saw only gaps of 2 in what `publishing` sent.
fn assert_drops_and_gaps(publishing: &Session, subscribing: &Session, key: &str) {
    let drops = push_drops_to(subscribing.zid());
    assert!(!drops.is_empty());
    for dropped in &drops {
        assert_eq!(
            dropped,
            &PushDropRecord {
                zid: subscribing.zid(),
                key_expr: Some(key.to_owned()),
                priority: Some(Priority::Data),
                reason: PushDropReason::Dropped,
            }
        );
    }
    let losses = rx_losses_from(publishing.zid());
    assert!(!losses.is_empty(), "the subscriber saw no gap");
    for loss in &losses {
        assert_eq!(loss.kind, RxLossKind::SnGap { gap: 2 });
        assert_eq!(loss.priority, Some(Priority::Data));
        assert_eq!(loss.reliability, Reliability::Reliable);
    }
    assert!(push_drops_to(publishing.zid()).is_empty());
    assert!(rx_losses_from(subscribing.zid()).is_empty());
}

/// Puts dropped for a held subscriber reach both observers: the publisher's names the
/// subscriber and the key, the subscriber's reports the gap once it reads again.
async fn held_subscriber_drops(
    key: &'static str,
    size: usize,
    adjust: fn(&mut zenoh_config::Config),
) {
    zenoh::init_log_from_env_or("error");
    install_observers();
    let mut sessions = TestSessions::new();
    let (publishing, subscribing) = open_pair(&mut sessions, adjust).await;

    let gate = Arc::new(Gate::default());
    let markers = Arc::new(AtomicUsize::new(0));
    let _subscriber = held_subscriber(&subscribing, key, &gate, &markers).await;
    let publisher = ztimeout!(publishing
        .declare_publisher(key)
        .congestion_control(CongestionControl::Drop)
        .reliability(Reliability::Reliable))
    .unwrap();
    wait_matching(&publisher).await;

    put_until(&publisher, subscribing.zid(), size, |_| true).await;
    assert!(rx_losses_from(publishing.zid()).is_empty());
    release_and_mark(&publishing, key, &gate, &markers).await;
    assert_drops_and_gaps(&publishing, &subscribing, key);

    sessions.close().await;
}

/// Small puts dropped for a held subscriber reach both observers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_put_reaches_both_observers() {
    held_subscriber_drops("test/drop_observer/held", PAYLOAD_SIZE, |_| {}).await;
}

/// A panic in the push drop observer stays in the observer: the put it was called from returns
/// as before (BER-2554).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_panicking_push_drop_observer_does_not_unwind_into_put() {
    zenoh::init_log_from_env_or("error");
    install_observers();
    const KEY: &str = "test/drop_observer/panicking";
    let mut sessions = TestSessions::new();
    let (publishing, subscribing) = open_pair(&mut sessions, |_| {}).await;
    PANICKING.lock().unwrap().push(subscribing.zid());

    let gate = Arc::new(Gate::default());
    let markers = Arc::new(AtomicUsize::new(0));
    let _subscriber = held_subscriber(&subscribing, KEY, &gate, &markers).await;
    let publisher = ztimeout!(publishing
        .declare_publisher(KEY)
        .congestion_control(CongestionControl::Drop)
        .reliability(Reliability::Reliable))
    .unwrap();
    wait_matching(&publisher).await;

    put_until(&publisher, subscribing.zid(), PAYLOAD_SIZE, |_| true).await;
    for _ in 0..10 {
        ztimeout!(publisher.put(vec![0_u8; PAYLOAD_SIZE])).unwrap();
    }
    release_and_mark(&publishing, KEY, &gate, &markers).await;

    sessions.close().await;
}

/// Fragmented puts dropped for a held subscriber reach both observers, and leave gaps of 2
/// only, whether they were dropped before or part way through their fragments.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_fragmented_put_reaches_both_observers() {
    held_subscriber_drops(
        "test/drop_observer/held_fragmented",
        FRAGMENTED_SIZE,
        without_shm,
    )
    .await;
}

/// Fragmented puts that all arrive leave no gap: the sender takes one SN per fragment and no
/// more.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fragmented_put_leaves_no_gap() {
    zenoh::init_log_from_env_or("error");
    install_observers();
    const KEY: &str = "test/drop_observer/fragmented";
    let mut sessions = TestSessions::new();
    let (publishing, subscribing) = open_pair(&mut sessions, without_shm).await;

    let received = Arc::new(AtomicUsize::new(0));
    let _subscriber = ztimeout!(subscribing.declare_subscriber(KEY).callback({
        let received = received.clone();
        move |_| {
            received.fetch_add(1, Ordering::SeqCst);
        }
    }))
    .unwrap();
    let publisher = ztimeout!(publishing
        .declare_publisher(KEY)
        .congestion_control(CongestionControl::Block)
        .reliability(Reliability::Reliable))
    .unwrap();
    wait_matching(&publisher).await;

    let sizes = [FRAGMENTED_SIZE, PAYLOAD_SIZE].repeat(3);
    for (sent, size) in sizes.iter().enumerate() {
        ztimeout!(publisher.put(vec![0_u8; *size])).unwrap();
        wait_until("the put arrives", || {
            received.load(Ordering::SeqCst) == sent + 1
        })
        .await;
    }

    assert_eq!(rx_losses_from(publishing.zid()), []);
    assert_eq!(push_drops_to(subscribing.zid()), []);

    sessions.close().await;
}

/// A `BlockFirst` put that waits out its turn behind a held subscriber reaches both observers:
/// the transport drops it before the pipeline, and still leaves the gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_block_first_put_reaches_both_observers() {
    zenoh::init_log_from_env_or("error");
    install_observers();
    const KEY: &str = "test/drop_observer/block_first";
    let mut sessions = TestSessions::new();
    let (publishing, subscribing) = open_pair(&mut sessions, |_| {}).await;

    let gate = Arc::new(Gate::default());
    let markers = Arc::new(AtomicUsize::new(0));
    let _subscriber = held_subscriber(&subscribing, KEY, &gate, &markers).await;
    let publisher = ztimeout!(publishing
        .declare_publisher(KEY)
        .congestion_control(CongestionControl::BlockFirst)
        .reliability(Reliability::Reliable))
    .unwrap();
    wait_matching(&publisher).await;

    put_until(&publisher, subscribing.zid(), PAYLOAD_SIZE, |_| true).await;
    release_and_mark(&publishing, KEY, &gate, &markers).await;
    assert_drops_and_gaps(&publishing, &subscribing, KEY);

    sessions.close().await;
}

/// A `BlockFirst` put that the transport accepted, and that then finds no batch before
/// `wait_before_close`, is reported dropped, without its key, which the transport does not
/// resolve.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_first_put_that_finds_no_batch_is_reported() {
    zenoh::init_log_from_env_or("error");
    install_observers();
    const KEY: &str = "test/drop_observer/block_first_closed";
    let mut sessions = TestSessions::new();
    let (publishing, subscribing) = open_pair(&mut sessions, |config| {
        config
            .insert_json5(
                "transport/link/tx/queue/congestion_control/block/wait_before_close",
                "300000",
            )
            .unwrap();
    })
    .await;

    let gate = Arc::new(Gate::default());
    let markers = Arc::new(AtomicUsize::new(0));
    let _subscriber = held_subscriber(&subscribing, KEY, &gate, &markers).await;
    let publisher = ztimeout!(publishing
        .declare_publisher(KEY)
        .congestion_control(CongestionControl::BlockFirst)
        .reliability(Reliability::Reliable))
    .unwrap();
    wait_matching(&publisher).await;

    put_until(&publisher, subscribing.zid(), PAYLOAD_SIZE, |dropped| {
        dropped.key_expr.is_none()
    })
    .await;
    gate.release();
    assert!(push_drops_to(subscribing.zid()).contains(&PushDropRecord {
        zid: subscribing.zid(),
        key_expr: None,
        priority: Some(Priority::Data),
        reason: PushDropReason::Dropped,
    }));

    sessions.close().await;
}

/// Puts to a subscriber that closes while they stream are reported as for a closed transport,
/// never as dropped: the transport loses its last link only as it closes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_subscriber_that_closes_while_streamed_to_reports_no_drop() {
    zenoh::init_log_from_env_or("error");
    install_observers();
    const KEY: &str = "test/drop_observer/closing";
    let mut sessions = TestSessions::new();
    let (publishing, subscribing) = open_pair(&mut sessions, |_| {}).await;
    let subscriber_zid = subscribing.zid();

    let _subscriber = ztimeout!(subscribing.declare_subscriber(KEY).callback(|_| {})).unwrap();
    let publisher = ztimeout!(publishing
        .declare_publisher(KEY)
        .congestion_control(CongestionControl::Drop)
        .reliability(Reliability::Reliable))
    .unwrap();
    wait_matching(&publisher).await;

    // About 20,000 small puts a second, which a subscriber keeps up with, before, while and
    // after it closes.
    let streaming = std::thread::spawn(move || {
        let begin = Instant::now();
        while begin.elapsed() < Duration::from_millis(1_500) {
            publisher.put(vec![0_u8; 256]).wait().unwrap();
            std::thread::sleep(Duration::from_micros(50));
        }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    ztimeout!(subscribing.close()).unwrap();
    tokio::task::spawn_blocking(move || streaming.join().unwrap())
        .await
        .unwrap();

    let reasons: Vec<PushDropReason> = push_drops_to(subscriber_zid)
        .into_iter()
        .map(|dropped| dropped.reason)
        .collect();
    assert!(
        !reasons.contains(&PushDropReason::Dropped),
        "a closing subscriber reported as a drop: {reasons:?}"
    );

    sessions.close().await;
}

/// A TCP proxy that forwards between one accepted connection and `target`, stops reading
/// while `stalled` is set, and drops both sockets once killed, so that a peer with data unread
/// in them sees its connection reset.
struct Proxy {
    endpoint: EndPoint,
    stalled: Arc<AtomicBool>,
    killed: Arc<AtomicBool>,
    pumps: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl Proxy {
    fn start(target: &EndPoint) -> Self {
        let port = get_free_tcp_port();
        let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
        let target = target.address().as_str().to_owned();
        let stalled = Arc::new(AtomicBool::new(false));
        let killed = Arc::new(AtomicBool::new(false));
        let pumps = Arc::new(Mutex::new(vec![]));
        std::thread::spawn({
            let (stalled, killed, pumps) = (stalled.clone(), killed.clone(), pumps.clone());
            move || {
                let (downstream, _) = listener.accept().unwrap();
                let upstream = TcpStream::connect(target).unwrap();
                for (from, to) in [
                    (
                        downstream.try_clone().unwrap(),
                        upstream.try_clone().unwrap(),
                    ),
                    (upstream, downstream),
                ] {
                    let (stalled, killed) = (stalled.clone(), killed.clone());
                    pumps.lock().unwrap().push(std::thread::spawn(move || {
                        pump(from, to, &stalled, &killed)
                    }));
                }
            }
        });
        Proxy {
            endpoint: format!("tcp/127.0.0.1:{port}").parse().unwrap(),
            stalled,
            killed,
            pumps,
        }
    }

    fn kill(&self) {
        self.killed.store(true, Ordering::SeqCst);
        for pump in self.pumps.lock().unwrap().drain(..) {
            pump.join().unwrap();
        }
    }
}

fn pump(mut from: TcpStream, mut to: TcpStream, stalled: &AtomicBool, killed: &AtomicBool) {
    from.set_read_timeout(Some(POLL)).unwrap();
    let mut buf = [0_u8; 16_384];
    while !killed.load(Ordering::SeqCst) {
        if stalled.load(Ordering::SeqCst) {
            std::thread::sleep(POLL);
            continue;
        }
        match from.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                if to.write_all(&buf[..n]).is_err() {
                    break;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {}
            Err(_) => break,
        }
    }
    // Dropping the sockets with data unread in them resets the connection.
}

/// A link that fails with batches queued reports them lost, by priority, to the publisher's
/// push drop observer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_link_reports_its_queued_batches() {
    zenoh::init_log_from_env_or("error");
    install_observers();
    const KEY: &str = "test/drop_observer/failed_link";
    let mut sessions = TestSessions::new();
    let subscribing = sessions.open_listener().await;
    let proxy = Proxy::start(&get_tcp_locator(&subscribing).await);
    let publishing = sessions
        .open_connector_with_cfg(
            sessions.get_connector_config_with_endpoint(vec![proxy.endpoint.clone()]),
        )
        .await;

    let _subscriber = ztimeout!(subscribing.declare_subscriber(KEY).callback(|_| {})).unwrap();
    let publisher = ztimeout!(publishing
        .declare_publisher(KEY)
        .congestion_control(CongestionControl::Drop)
        .reliability(Reliability::Reliable))
    .unwrap();
    wait_matching(&publisher).await;

    // The proxy stops reading, so the publisher's queue fills until a put is dropped.
    proxy.stalled.store(true, Ordering::SeqCst);
    put_until(&publisher, subscribing.zid(), PAYLOAD_SIZE, |_| true).await;
    proxy.kill();
    wait_until("the link's queued batches are reported", || {
        push_drops_to(subscribing.zid()).iter().any(|dropped| {
            matches!(dropped.reason, PushDropReason::LinkClosedQueued { batches } if batches > 0)
                && dropped.key_expr.is_none()
                && dropped.priority == Some(Priority::Data)
        })
    })
    .await;

    sessions.close().await;
}

/// Puts that a subscriber keeps up with all arrive, and reach neither observer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_subscriber_that_keeps_up_reaches_neither_observer() {
    zenoh::init_log_from_env_or("error");
    install_observers();
    const KEY: &str = "test/drop_observer/fast";
    let mut sessions = TestSessions::new();
    let (publishing, subscribing) = sessions.open_pairs().await;

    let received = Arc::new(AtomicUsize::new(0));
    let _subscriber = ztimeout!(subscribing.declare_subscriber(KEY).callback({
        let received = received.clone();
        move |_| {
            received.fetch_add(1, Ordering::SeqCst);
        }
    }))
    .unwrap();
    let publisher = ztimeout!(publishing
        .declare_publisher(KEY)
        .congestion_control(CongestionControl::Drop)
        .reliability(Reliability::Reliable))
    .unwrap();
    wait_matching(&publisher).await;

    // One put in flight at a time, so that no queue can fill.
    for sent in 1..=IN_STEP {
        ztimeout!(publisher.put(vec![0_u8; PAYLOAD_SIZE])).unwrap();
        wait_until("the put arrives", || {
            received.load(Ordering::SeqCst) == sent
        })
        .await;
    }

    assert_eq!(received.load(Ordering::SeqCst), IN_STEP);
    for zid in [publishing.zid(), subscribing.zid()] {
        assert_eq!(push_drops_to(zid), []);
        assert_eq!(rx_losses_from(zid), []);
    }

    sessions.close().await;
}

/// A queue size outside the allowed range fails the session's open, also when set on its own,
/// which the config's validator does not see.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_queue_size_out_of_range_fails_open() {
    zenoh::init_log_from_env_or("error");
    const DATA: &str = "transport/link/tx/queue/size/data";
    let sessions = TestSessions::new();

    let mut config = sessions.get_listener_config("tcp/127.0.0.1:0", 1);
    config.insert_json5(DATA, "65").unwrap();
    assert!(ztimeout!(zenoh::open(config)).is_err());

    let mut config = sessions.get_listener_config("tcp/127.0.0.1:0", 1);
    config.insert_json5(DATA, "64").unwrap();
    let session = ztimeout!(zenoh::open(config)).unwrap();
    ztimeout!(session.close()).unwrap();
}
