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
//! What a unicast link that fails reports, and what it does with the pushes after (BER-2554).
//!
//! The link runs through a TCP proxy that can stop reading and then drop its sockets, which
//! resets the connection. A link whose write fails refuses every later push at once. A link
//! that fails while its transport keeps another link to the peer reports the batches it held
//! as lost to a live reader, the pushes it refuses as dropped, and the receiver sees the gap.
//!
//! The loss observers are installed once per process, so each test reads its own peers' only.
#![cfg(all(feature = "transport_tcp", feature = "transport_multilink"))]

use std::{
    any::Any,
    collections::HashMap,
    convert::TryFrom,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex, Once,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use zenoh_config::QueueSizeConf;
use zenoh_core::ztimeout;
use zenoh_link::{EndPoint, Link};
use zenoh_protocol::{
    core::{CongestionControl, Priority, WhatAmI, ZenohIdProto},
    network::{push::ext::QoSType, NetworkMessage, NetworkMessageMut, Push},
};
use zenoh_result::ZResult;
use zenoh_test::get_free_tcp_port;
use zenoh_transport::{
    common::loss::{self, RxLoss, RxLossKind, TxLoss, TxLossKind},
    multicast::TransportMulticast,
    unicast::{TransportManagerBuilderUnicast, TransportUnicast},
    DummyTransportPeerEventHandler, TransportEventHandler, TransportManager,
    TransportMulticastEventHandler, TransportPeer, TransportPeerEventHandler,
};

const TIMEOUT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(10);

type OnTxLoss = Arc<dyn Fn() + Send + Sync>;

static TX_LOSSES: Mutex<Vec<TxLoss>> = Mutex::new(Vec::new());
static RX_LOSSES: Mutex<Vec<RxLoss>> = Mutex::new(Vec::new());
/// Run by the tx loss observer, on the thread that reports the loss, for the peer it names.
static ON_TX_LOSS: Mutex<Option<HashMap<ZenohIdProto, OnTxLoss>>> = Mutex::new(None);

fn install_observers() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        loss::set_tx_loss_observer(Box::new(|loss| {
            TX_LOSSES.lock().unwrap().push(*loss);
            let on_loss = ON_TX_LOSS
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|on_loss| on_loss.get(&loss.peer).cloned());
            if let Some(on_loss) = on_loss {
                on_loss();
            }
        }))
        .unwrap();
        loss::set_rx_loss_observer(Box::new(|loss| RX_LOSSES.lock().unwrap().push(*loss))).unwrap();
    });
}

fn on_tx_loss(peer: ZenohIdProto, on_loss: OnTxLoss) {
    ON_TX_LOSS
        .lock()
        .unwrap()
        .get_or_insert_with(HashMap::new)
        .insert(peer, on_loss);
}

fn tx_losses_to(peer: ZenohIdProto) -> Vec<TxLoss> {
    TX_LOSSES
        .lock()
        .unwrap()
        .iter()
        .filter(|loss| loss.peer == peer)
        .copied()
        .collect()
}

fn rx_losses_from(peer: ZenohIdProto) -> Vec<RxLoss> {
    RX_LOSSES
        .lock()
        .unwrap()
        .iter()
        .filter(|loss| loss.peer == peer)
        .copied()
        .collect()
}

/// Counts the messages each transport delivers.
#[derive(Default)]
struct Counting(Arc<AtomicUsize>);

impl TransportEventHandler for Counting {
    fn new_unicast(
        &self,
        _peer: TransportPeer,
        _transport: TransportUnicast,
    ) -> ZResult<Arc<dyn TransportPeerEventHandler>> {
        Ok(Arc::new(CountingPeer(self.0.clone())))
    }

    fn new_multicast(
        &self,
        _transport: TransportMulticast,
    ) -> ZResult<Arc<dyn TransportMulticastEventHandler>> {
        panic!("not used")
    }
}

struct CountingPeer(Arc<AtomicUsize>);

impl TransportPeerEventHandler for CountingPeer {
    fn handle_message(&self, _message: NetworkMessageMut) -> ZResult<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn new_link(&self, _link: Link) {}
    fn del_link(&self, _link: Link) {}
    fn closed(&self) {}

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Sends nothing up.
struct Sender;

impl TransportEventHandler for Sender {
    fn new_unicast(
        &self,
        _peer: TransportPeer,
        _transport: TransportUnicast,
    ) -> ZResult<Arc<dyn TransportPeerEventHandler>> {
        Ok(Arc::new(DummyTransportPeerEventHandler))
    }

    fn new_multicast(
        &self,
        _transport: TransportMulticast,
    ) -> ZResult<Arc<dyn TransportMulticastEventHandler>> {
        panic!("not used")
    }
}

fn manager(zid: u8, handler: Arc<dyn TransportEventHandler>) -> TransportManager {
    TransportManager::builder()
        .whatami(WhatAmI::Peer)
        .zid(ZenohIdProto::try_from([zid]).unwrap())
        .unicast(TransportManagerBuilderUnicast::default().max_links(2))
        .build_test(handler)
        .unwrap()
}

fn droppable() -> NetworkMessage {
    NetworkMessage::from(Push {
        wire_expr: "test/link_loss".into(),
        ext_qos: QoSType::new(Priority::Data, CongestionControl::Drop, false),
        ..Push::from(vec![0_u8; 1024])
    })
}

/// A TCP proxy that forwards between one accepted connection and `target`, stops reading
/// while stalled, and drops both sockets once killed, so that a peer with data unread in them
/// sees its connection reset.
struct Proxy {
    endpoint: EndPoint,
    stalled: Arc<AtomicBool>,
    killed: Arc<AtomicBool>,
    pumps: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl Proxy {
    fn start(target_port: u16) -> Self {
        let port = get_free_tcp_port();
        let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
        let stalled = Arc::new(AtomicBool::new(false));
        let killed = Arc::new(AtomicBool::new(false));
        let pumps = Arc::new(Mutex::new(vec![]));
        std::thread::spawn({
            let (stalled, killed, pumps) = (stalled.clone(), killed.clone(), pumps.clone());
            move || {
                let (downstream, _) = listener.accept().unwrap();
                let upstream = TcpStream::connect(("127.0.0.1", target_port)).unwrap();
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

fn wait_until(what: &str, condition: impl Fn() -> bool) {
    let start = Instant::now();
    while !condition() {
        assert!(start.elapsed() < TIMEOUT, "timed out waiting until {what}");
        std::thread::sleep(POLL);
    }
}

/// Pushes more than the sockets between the sender and a stopped proxy can hold, so that the
/// link's write blocks and its queue fills: a drop alone can come from the TX thread being
/// busy with other links.
fn overfill(transport: &TransportUnicast) {
    let mut dropped = false;
    for _ in 0..4_000 {
        let mut msg = NetworkMessage::from(Push {
            wire_expr: "test/link_loss".into(),
            ext_qos: QoSType::new(Priority::Data, CongestionControl::Drop, false),
            ..Push::from(vec![0_u8; 8192])
        });
        dropped |= !transport.schedule(msg.as_mut()).unwrap();
    }
    assert!(dropped, "the link's queue never filled");
}

// Each test's managers have ZenohIds of their own: 20 to 59, 80 to 119 and 140 to 141 for the
// trials, 200 and 201 for the close.

/// What the tx loss observer saw when a link through the proxy failed: whether the failed
/// link was listed both before and after a push made right then, which it is when its own
/// failed write reports the loss and the transport has not removed it meanwhile, and what that
/// push returned.
struct Failure {
    listed: bool,
    pushed: ZResult<bool>,
}

/// Opens a transport through a proxy, and a second link beside it if `two_links`, overfills the
/// proxied link, resets it, and returns what the tx loss observer saw.
async fn fail_proxied_link(
    trial: u8,
    two_links: bool,
) -> (
    Failure,
    TransportManager,
    TransportManager,
    TransportUnicast,
) {
    let received = Arc::new(AtomicUsize::new(0));
    let receiving = manager(2 * trial, Arc::new(Counting(received)));
    let port = get_free_tcp_port();
    let listen: EndPoint = format!("tcp/127.0.0.1:{port}").parse().unwrap();
    ztimeout!(receiving.add_listener(listen.clone())).unwrap();
    let proxy = Proxy::start(port);
    let sending = manager(2 * trial + 1, Arc::new(Sender));
    // The link through the proxy is the transport's first, which its pushes go to.
    let transport = ztimeout!(sending.open_transport_unicast(proxy.endpoint.clone())).unwrap();
    let links = if two_links {
        let same = ztimeout!(sending.open_transport_unicast(listen)).unwrap();
        assert_eq!(same.get_zid().unwrap(), transport.get_zid().unwrap());
        2
    } else {
        1
    };
    assert_eq!(transport.get_links().unwrap().len(), links);

    let failure = Arc::new(Mutex::new(None));
    on_tx_loss(receiving.zid(), {
        let (transport, failure) = (transport.clone(), failure.clone());
        Arc::new(move || {
            let listed = || transport.get_links().is_ok_and(|l| l.len() == links);
            failure.lock().unwrap().get_or_insert_with(|| {
                let before = listed();
                let pushed = transport.schedule(droppable().as_mut());
                Failure {
                    listed: before && listed(),
                    pushed,
                }
            });
        })
    });
    proxy.stalled.store(true, Ordering::SeqCst);
    overfill(&transport);
    proxy.kill();
    wait_until("the link reports its loss", || {
        failure.lock().unwrap().is_some()
    });
    let failure = failure.lock().unwrap().take().unwrap();
    (failure, receiving, sending, transport)
}

/// The failure of a proxied link, retried until the failed link's own write reports it, which
/// it does unless the transport has removed the link first.
async fn fail_proxied_link_listed(first_trial: u8, two_links: bool) -> (Failure, ZenohIdProto) {
    for trial in first_trial..first_trial + 20 {
        let (failure, receiving, sending, _) = fail_proxied_link(trial, two_links).await;
        let peer = receiving.zid();
        ztimeout!(sending.close());
        ztimeout!(receiving.close());
        if failure.listed {
            return (failure, peer);
        }
    }
    panic!("the failed link's write never reported its loss");
}

/// A link whose write fails refuses the next push at once, before the transport removes the
/// link, and reports what it held as lost with a connection reset at the peer's end.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_write_refuses_later_pushes() {
    zenoh_util::init_log_from_env_or("error");
    install_observers();
    let (failure, peer) = fail_proxied_link_listed(10, false).await;

    assert!(
        failure.pushed.is_err(),
        "a push after the failed write was taken: {:?}",
        failure.pushed
    );
    let losses = tx_losses_to(peer);
    assert!(
        losses.iter().any(|loss| matches!(
            loss.kind,
            TxLossKind::ResetQueued { batches } if batches > 0
        )),
        "{losses:?}"
    );
}

/// A link that fails while its transport keeps another link to the peer reports the batches
/// it held as lost to a live reader, reports a push it then refuses as dropped rather than
/// failing it as for a closed transport, and leaves the receiver a gap on the other link.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_link_that_fails_beside_another_reports_a_live_loss() {
    zenoh_util::init_log_from_env_or("error");
    install_observers();
    let (failure, _) = fail_proxied_link_listed(40, true).await;
    assert!(
        matches!(failure.pushed, Ok(false)),
        "a push the failed link refused is a drop: {:?}",
        failure.pushed
    );

    let (_, receiving, sending, transport) = fail_proxied_link(70, true).await;
    let losses = tx_losses_to(receiving.zid());
    assert!(
        losses.iter().any(|loss| matches!(
            loss.kind,
            TxLossKind::ClosedQueued { batches } if batches > 0
        )),
        "{losses:?}"
    );
    assert!(!losses
        .iter()
        .any(|loss| matches!(loss.kind, TxLossKind::ResetQueued { .. })));

    // The pushes go to the other link once the failed one is removed, and arrive behind the
    // gap the lost batches left.
    let start = Instant::now();
    while rx_losses_from(sending.zid()).is_empty() {
        assert!(start.elapsed() < TIMEOUT, "no gap reported");
        let _ = transport.schedule(droppable().as_mut()).unwrap();
        std::thread::sleep(POLL);
    }
    assert!(rx_losses_from(sending.zid())
        .iter()
        .all(|loss| matches!(loss.kind, RxLossKind::SnGap { .. })));
    assert_eq!(transport.get_links().unwrap().len(), 1);

    ztimeout!(sending.close());
    ztimeout!(receiving.close());
}

/// A link that closes at this end while its write is cut short counts the batch it was
/// writing, even with nothing else of that priority queued.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_close_counts_the_batch_being_written() {
    zenoh_util::init_log_from_env_or("error");
    install_observers();
    let receiving = manager(200, Arc::new(Counting::default()));
    let port = get_free_tcp_port();
    let listen: EndPoint = format!("tcp/127.0.0.1:{port}").parse().unwrap();
    ztimeout!(receiving.add_listener(listen)).unwrap();
    let proxy = Proxy::start(port);
    // One batch per priority, so that the one being written is all the link holds of `Data`,
    // and a short lease, whose keep-alive period bounds the writes at close.
    let mut sizes = QueueSizeConf::default();
    sizes.set_data(1).unwrap();
    let sending = TransportManager::builder()
        .whatami(WhatAmI::Peer)
        .zid(ZenohIdProto::try_from([201]).unwrap())
        .queue_size(sizes)
        .unicast(TransportManagerBuilderUnicast::default().lease(Duration::from_secs(1)))
        .build_test(Arc::new(Sender))
        .unwrap();
    let transport = ztimeout!(sending.open_transport_unicast(proxy.endpoint.clone())).unwrap();

    proxy.stalled.store(true, Ordering::SeqCst);
    overfill(&transport);
    ztimeout!(transport.close()).unwrap();
    wait_until("the batch being written is reported", || {
        tx_losses_to(receiving.zid()).iter().any(|loss| {
            loss.priority == Priority::Data
                && matches!(loss.kind, TxLossKind::ClosedQueued { batches: 1 })
        })
    });

    proxy.kill();
    ztimeout!(sending.close());
    ztimeout!(receiving.close());
}
