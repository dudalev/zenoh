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
    drop_observer::{self, LocalClose, PushDropReason, RxLoss, RxLossKind},
    pubsub::Publisher,
    qos::{CongestionControl, Priority, Reliability},
    session::ZenohId,
    Session, Wait,
};
use zenoh_core::ztimeout;
use zenoh_link::EndPoint;
use zenoh_test::{get_free_tcp_port, get_tcp_locator, wait_for_peers, TestSessions};

const TIMEOUT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(10);
const PAYLOAD_SIZE: usize = 1024;
/// The socket buffers of a link that a test stops, smaller than one batch, so that a write of a
/// full batch to a stopped proxy cannot complete.
const SOCKET_BUFFER: usize = 8192;
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

/// Small socket buffers for the session's links: see [`SOCKET_BUFFER`].
fn small_socket_buffers(config: &mut zenoh_config::Config) {
    config
        .insert_json5(
            "transport/link/tcp",
            &format!("{{so_sndbuf: {SOCKET_BUFFER}, so_rcvbuf: {SOCKET_BUFFER}}}"),
        )
        .unwrap();
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
        // A small receive buffer, inherited by the accepted socket, so that a stopped proxy
        // holds little of what a peer sends it.
        socket2::SockRef::from(&listener)
            .set_recv_buffer_size(SOCKET_BUFFER)
            .unwrap();
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

/// Puts far more than the small sockets between a publisher and a stopped proxy can hold, so
/// that the link's write blocks and its queue fills, however busy the process's one TX thread
/// is with other links.
async fn overfill(publisher: &Publisher<'_>) {
    for _ in 0..4_000 {
        ztimeout!(publisher.put(vec![0_u8; 8 * PAYLOAD_SIZE])).unwrap();
    }
}

/// A publishing peer that reaches the subscribing peer through a proxy, each config adjusted by
/// `adjust`.
async fn open_pair_through_proxy(
    sessions: &mut TestSessions,
    adjust: impl Fn(&mut zenoh_config::Config),
) -> (Session, Session, Proxy) {
    let mut listener = sessions.get_listener_config("tcp/127.0.0.1:0", 1);
    adjust(&mut listener);
    let subscribing = sessions.open_listener_with_cfg(listener).await;
    let proxy = Proxy::start(&get_tcp_locator(&subscribing).await);
    let mut connector = sessions.get_connector_config_with_endpoint(vec![proxy.endpoint.clone()]);
    adjust(&mut connector);
    let publishing = sessions.open_connector_with_cfg(connector).await;
    (publishing, subscribing, proxy)
}

/// Waits until the publisher reports batches it queued for `subscriber`, of the `Data`
/// priority and with no key, lost for the reason `lost` matches.
async fn wait_queued_loss(subscriber: ZenohId, lost: fn(&PushDropReason) -> bool) {
    wait_until("the link's queued batches are reported", || {
        push_drops_to(subscriber).iter().any(|dropped| {
            lost(&dropped.reason)
                && dropped.key_expr.is_none()
                && dropped.priority == Some(Priority::Data)
        })
    })
    .await;
}

/// A link whose connection is reset at the subscriber's end, as a subscriber whose process ends
/// resets it, reports the batches it queued as lost to a reader that is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_link_reset_at_its_peer_reports_its_queued_batches() {
    zenoh::init_log_from_env_or("error");
    install_observers();
    const KEY: &str = "test/drop_observer/reset_link";
    let mut sessions = TestSessions::new();
    let (publishing, subscribing, proxy) =
        open_pair_through_proxy(&mut sessions, small_socket_buffers).await;

    let _subscriber = ztimeout!(subscribing.declare_subscriber(KEY).callback(|_| {})).unwrap();
    let publisher = ztimeout!(publishing
        .declare_publisher(KEY)
        .congestion_control(CongestionControl::Drop)
        .reliability(Reliability::Reliable))
    .unwrap();
    wait_matching(&publisher).await;

    // The proxy stops reading, so the publisher's queue fills.
    proxy.stalled.store(true, Ordering::SeqCst);
    overfill(&publisher).await;
    proxy.kill();
    wait_queued_loss(
        subscribing.zid(),
        |reason| matches!(reason, PushDropReason::LinkResetQueued { batches } if *batches > 0),
    )
    .await;
    assert!(!push_drops_to(subscribing.zid())
        .iter()
        .any(|dropped| matches!(dropped.reason, PushDropReason::LinkClosedQueued { .. })));

    sessions.close().await;
}

/// A link that its publisher closes while the subscriber's connection is open but not read
/// reports the batches it queued as lost to a reader that is alive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_link_closed_while_its_peer_is_not_reading_reports_its_queued_batches() {
    zenoh::init_log_from_env_or("error");
    install_observers();
    const KEY: &str = "test/drop_observer/unread_link";
    let mut sessions = TestSessions::new();
    // A short lease, so that the publisher closes the link soon after the proxy stops
    // forwarding, and a short keep-alive period, which bounds a write at close.
    let (publishing, subscribing, proxy) = open_pair_through_proxy(&mut sessions, |config| {
        config
            .insert_json5("transport/link/tx/lease", "1000")
            .unwrap();
        small_socket_buffers(config);
    })
    .await;

    let _subscriber = ztimeout!(subscribing.declare_subscriber(KEY).callback(|_| {})).unwrap();
    let publisher = ztimeout!(publishing
        .declare_publisher(KEY)
        .congestion_control(CongestionControl::Drop)
        .reliability(Reliability::Reliable))
    .unwrap();
    wait_matching(&publisher).await;

    proxy.stalled.store(true, Ordering::SeqCst);
    overfill(&publisher).await;
    wait_queued_loss(
        subscribing.zid(),
        |reason| matches!(reason, PushDropReason::LinkClosedQueued { batches } if *batches > 0),
    )
    .await;
    assert!(!push_drops_to(subscribing.zid())
        .iter()
        .any(|dropped| matches!(dropped.reason, PushDropReason::LinkResetQueued { .. })));

    proxy.kill();
    sessions.close().await;
}

/// A link that fails while a blocking put waits on it does not hold up the process's other
/// links: a healthy subscriber of the same publisher keeps receiving, with nothing dropped and
/// nothing lost. The process's links share one TX thread, which must not wait for that put.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_link_does_not_stall_the_other_links() {
    zenoh::init_log_from_env_or("error");
    install_observers();
    const DATA: &str = "test/drop_observer/stall/data";
    const BLOCKED: &str = "test/drop_observer/stall/blocked";
    const FILL: &str = "test/drop_observer/stall/fill";
    let mut sessions = TestSessions::new();
    let mut listener = sessions.get_listener_config(
        &format!("tcp/127.0.0.1:0#so_sndbuf={SOCKET_BUFFER};so_rcvbuf={SOCKET_BUFFER}"),
        1,
    );
    // A queue that holds seconds of the healthy stream, so that the one TX thread, shared with
    // the tests running beside this one, can lag without the healthy link dropping.
    listener
        .insert_json5("transport/link/tx/queue/size/data", "16")
        .unwrap();
    let publishing = sessions.open_listener_with_cfg(listener).await;
    let healthy = sessions.open_connector().await;
    let proxy = Proxy::start(&get_tcp_locator(&publishing).await);
    let stalled = sessions
        .open_connector_with_cfg(
            sessions.get_connector_config_with_endpoint(vec![proxy.endpoint.clone()]),
        )
        .await;

    let healthy_received = Arc::new(AtomicUsize::new(0));
    let stalled_received = Arc::new(AtomicUsize::new(0));
    let _healthy_subscriber = ztimeout!(healthy.declare_subscriber(DATA).callback({
        let received = healthy_received.clone();
        move |_| {
            received.fetch_add(1, Ordering::SeqCst);
        }
    }))
    .unwrap();
    let _stalled_subscriber = ztimeout!(stalled
        .declare_subscriber("test/drop_observer/stall/**")
        .callback({
            let received = stalled_received.clone();
            move |_| {
                received.fetch_add(1, Ordering::SeqCst);
            }
        }))
    .unwrap();
    let publisher = ztimeout!(publishing
        .declare_publisher(DATA)
        .congestion_control(CongestionControl::Drop)
        .reliability(Reliability::Reliable))
    .unwrap();
    // Only the stalled subscriber matches the fill, which must be routed to it before the proxy
    // stops: the declarations that route it would not get through after.
    let filling = ztimeout!(publishing
        .declare_publisher(FILL)
        .congestion_control(CongestionControl::Drop)
        .reliability(Reliability::Reliable))
    .unwrap();
    wait_matching(&filling).await;
    // Both subscribers receive before the test begins.
    let start = Instant::now();
    while healthy_received.load(Ordering::SeqCst) == 0
        || stalled_received.load(Ordering::SeqCst) == 0
    {
        assert!(start.elapsed() < TIMEOUT, "a subscriber never received");
        ztimeout!(publisher.put(vec![0_u8; PAYLOAD_SIZE])).unwrap();
        tokio::time::sleep(POLL).await;
    }

    // About 1,000 small puts a second to both, which the healthy subscriber keeps up with.
    let streaming = Arc::new(AtomicBool::new(true));
    let stream = std::thread::spawn({
        let streaming = streaming.clone();
        move || {
            while streaming.load(Ordering::SeqCst) {
                publisher.put(vec![0_u8; 64]).wait().unwrap();
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    });

    // The proxy stops reading, and the link to the stalled subscriber fills until a blocking
    // put to it waits for a batch, holding its queue's lock. A fill put dropped after the
    // blocking put began, with the blocking put still not done, shows the queue full with no
    // batch given back since. Until that is seen, the link is filled further and a new blocking
    // put is tried.
    proxy.stalled.store(true, Ordering::SeqCst);
    let fill_drops = || {
        push_drops_to(stalled.zid())
            .iter()
            .filter(|dropped| dropped.key_expr.as_deref() == Some(FILL))
            .count()
    };
    let start = Instant::now();
    let blocked = loop {
        assert!(start.elapsed() < TIMEOUT, "no blocking put waited");
        overfill(&filling).await;
        let blocking = Arc::new(AtomicBool::new(false));
        let blocked = std::thread::spawn({
            let (publishing, blocking) = (publishing.clone(), blocking.clone());
            move || {
                blocking.store(true, Ordering::SeqCst);
                publishing
                    .put(BLOCKED, vec![0_u8; PAYLOAD_SIZE])
                    .congestion_control(CongestionControl::Block)
                    .reliability(Reliability::Reliable)
                    .wait()
            }
        });
        wait_until("the blocking put begins", || {
            blocking.load(Ordering::SeqCst)
        })
        .await;
        let drops = fill_drops();
        while fill_drops() == drops {
            assert!(
                start.elapsed() < TIMEOUT,
                "no put to the stalled link dropped"
            );
            ztimeout!(filling.put(vec![0_u8; PAYLOAD_SIZE])).unwrap();
        }
        if !blocked.is_finished() {
            break blocked;
        }
    };

    // The other links go on while the reset link is torn down, which is within
    // `wait_before_close` of the blocking put, the time a TX thread waiting for it would stop.
    let before = healthy_received.load(Ordering::SeqCst);
    proxy.kill();
    let start = Instant::now();
    while healthy_received.load(Ordering::SeqCst) < before + 100 {
        assert!(
            start.elapsed() < Duration::from_millis(2_500),
            "the healthy subscriber stalled after the reset: {} received",
            healthy_received.load(Ordering::SeqCst) - before
        );
        tokio::time::sleep(POLL).await;
    }
    streaming.store(false, Ordering::SeqCst);
    tokio::task::spawn_blocking(move || {
        stream.join().unwrap();
        let _ = blocked.join().unwrap();
    })
    .await
    .unwrap();

    assert_eq!(push_drops_to(healthy.zid()), []);
    assert_eq!(rx_losses_from(publishing.zid()), []);

    sessions.close().await;
}

/// Whether the rx loss observer has reported a local close of the transport to `peer`.
fn closed_locally(peer: ZenohId, reason: LocalClose) -> bool {
    rx_losses_from(peer).iter().any(|loss| {
        loss.kind == RxLossKind::TransportClosedLocally { reason }
            && loss.priority.is_none()
            && loss.reliability == Reliability::Reliable
    })
}

fn any_local_close(peer: ZenohId) -> bool {
    rx_losses_from(peer)
        .iter()
        .any(|loss| matches!(loss.kind, RxLossKind::TransportClosedLocally { .. }))
}

/// A blocking put that finds no batch before `wait_before_close`, because the peer reads
/// nothing, closes the transport, and the putting process's rx loss observer reports it,
/// naming the peer: what the peer had sent it and it had not read is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_blocking_put_that_closes_the_transport_is_reported() {
    zenoh::init_log_from_env_or("error");
    install_observers();
    const KEY: &str = "test/drop_observer/blocking_put";
    let mut sessions = TestSessions::new();
    let (writing, reading) = open_pair(&mut sessions, |config| {
        small_socket_buffers(config);
        config
            .insert_json5(
                "transport/link/tx/queue/congestion_control/block/wait_before_close",
                "300000",
            )
            .unwrap();
    })
    .await;

    // The writing peer holds its receive thread at the reading peer's first put, and so reads
    // nothing more.
    let gate = Arc::new(Gate::default());
    let markers = Arc::new(AtomicUsize::new(0));
    let _held = held_subscriber(&writing, KEY, &gate, &markers).await;
    let publisher = ztimeout!(reading
        .declare_publisher(KEY)
        .congestion_control(CongestionControl::Block)
        .reliability(Reliability::Reliable))
    .unwrap();
    wait_matching(&publisher).await;

    let putting = std::thread::spawn({
        let writer = writing.zid();
        move || {
            let start = Instant::now();
            while !closed_locally(writer, LocalClose::BlockingPushFailed) {
                assert!(start.elapsed() < TIMEOUT, "no local close reported");
                publisher.put(vec![0_u8; 8 * PAYLOAD_SIZE]).wait().unwrap();
            }
        }
    });
    tokio::task::spawn_blocking(move || putting.join().unwrap())
        .await
        .unwrap();
    gate.release();
    assert!(!any_local_close(reading.zid()));

    sessions.close().await;
}

/// A lease that expires closes the transport, and each peer whose lease of the other expired
/// reports it, naming the other: with the proxy between them stopped, both do.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lease_that_expires_is_reported_by_each_side_it_expires_on() {
    zenoh::init_log_from_env_or("error");
    install_observers();
    let mut sessions = TestSessions::new();
    let (connecting, listening, proxy) = open_pair_through_proxy(&mut sessions, |config| {
        config
            .insert_json5("transport/link/tx/lease", "1000")
            .unwrap();
    })
    .await;
    wait_for_peers(&connecting, &listening).await;

    proxy.stalled.store(true, Ordering::SeqCst);
    wait_until("both leases expire", || {
        closed_locally(listening.zid(), LocalClose::LeaseExpired)
            && closed_locally(connecting.zid(), LocalClose::LeaseExpired)
    })
    .await;

    proxy.kill();
    sessions.close().await;
}

/// A peer that closes its session, a connection reset at the peer's end, as a peer whose
/// process ends gives, and this process closing its own session are not reported as local
/// closes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_that_leaves_and_an_own_close_are_not_reported() {
    zenoh::init_log_from_env_or("error");
    install_observers();
    async fn wait_gone(session: &Session, peer: ZenohId) {
        let start = Instant::now();
        while session.info().peers_zid().await.any(|zid| zid == peer) {
            assert!(start.elapsed() < TIMEOUT, "the transport stayed open");
            tokio::time::sleep(POLL).await;
        }
    }

    // The peer closes its session, and this process then closes its own.
    let mut sessions = TestSessions::new();
    let (staying, leaving) = sessions.open_pairs().await;
    wait_for_peers(&staying, &leaving).await;
    ztimeout!(leaving.close()).unwrap();
    wait_gone(&staying, leaving.zid()).await;
    ztimeout!(staying.close()).unwrap();
    assert!(!any_local_close(leaving.zid()));
    assert!(!any_local_close(staying.zid()));
    sessions.close().await;

    // The connection is reset.
    let mut sessions = TestSessions::new();
    let (connecting, listening, proxy) = open_pair_through_proxy(&mut sessions, |_| {}).await;
    wait_for_peers(&connecting, &listening).await;
    proxy.kill();
    wait_gone(&connecting, listening.zid()).await;
    wait_gone(&listening, connecting.zid()).await;
    assert!(!any_local_close(listening.zid()));
    assert!(!any_local_close(connecting.zid()));
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
