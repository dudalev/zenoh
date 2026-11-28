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
//! Also, what a lease that expires reports, for a receive task held past it and for a transport
//! with two links, and how a peer that reopens its side of a transport this side still holds is
//! admitted (BER-2625, BER-2627).
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
        Arc, Condvar, Mutex, Once,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use zenoh_buffers::{buffer::SplitBuffer, reader::Reader};
use zenoh_codec::{RCodec, Zenoh080};
use zenoh_config::QueueSizeConf;
use zenoh_core::ztimeout;
use zenoh_link::{EndPoint, Link};
use zenoh_protocol::{
    core::{CongestionControl, Priority, WhatAmI, ZenohIdProto},
    network::{
        push::ext::QoSType, NetworkBody, NetworkBodyMut, NetworkMessage, NetworkMessageMut, Push,
    },
    transport::{TransportBody, TransportMessage},
    zenoh::PushBody,
};
use zenoh_result::ZResult;
use zenoh_test::get_free_tcp_port;
use zenoh_transport::{
    common::loss::{self, LocalClose, RxLoss, RxLossKind, TxLoss, TxLossKind},
    multicast::TransportMulticast,
    unicast::{TransportManagerBuilderUnicast, TransportUnicast},
    DummyTransportPeerEventHandler, TransportEventHandler, TransportManager,
    TransportMulticastEventHandler, TransportPeer, TransportPeerEventHandler,
};

const TIMEOUT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(10);

type OnTxLoss = Arc<dyn Fn(&TxLoss) + Send + Sync>;

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
                on_loss(loss);
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

fn zid(n: u16) -> ZenohIdProto {
    ZenohIdProto::try_from([0x5a, n as u8, (n >> 8) as u8 + 1]).unwrap()
}

/// A manager with ZenohId `n`, and `data_queue` batches for the `Data` priority if given.
fn manager(
    n: u16,
    handler: Arc<dyn TransportEventHandler>,
    data_queue: Option<usize>,
) -> TransportManager {
    let mut sizes = QueueSizeConf::default();
    if let Some(batches) = data_queue {
        sizes.set_data(batches).unwrap();
    }
    TransportManager::builder()
        .whatami(WhatAmI::Peer)
        .zid(zid(n))
        .queue_size(sizes)
        .unicast(TransportManagerBuilderUnicast::default().max_links(2))
        .build_test(handler)
        .unwrap()
}

/// A manager with ZenohId `n` that sends payloads in its batches, never as shared memory.
fn manager_without_shm(n: u16, handler: Arc<dyn TransportEventHandler>) -> TransportManager {
    let builder = TransportManager::builder()
        .whatami(WhatAmI::Peer)
        .zid(zid(n))
        .unicast(TransportManagerBuilderUnicast::default().max_links(2));
    #[cfg(feature = "shared-memory")]
    let builder = {
        let mut shm = zenoh_config::ShmConf::default();
        shm.set_enabled(false).unwrap();
        builder.shm(shm)
    };
    builder.build_test(handler).unwrap()
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
/// sees its connection reset. It keeps what it read from the accepted connection.
struct Proxy {
    endpoint: EndPoint,
    stalled: Arc<AtomicBool>,
    killed: Arc<AtomicBool>,
    pumps: Arc<Mutex<Vec<JoinHandle<()>>>>,
    sent: Arc<Mutex<Vec<u8>>>,
}

impl Proxy {
    fn start(target_port: u16) -> Self {
        let port = get_free_tcp_port();
        let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
        let stalled = Arc::new(AtomicBool::new(false));
        let killed = Arc::new(AtomicBool::new(false));
        let pumps = Arc::new(Mutex::new(vec![]));
        let sent = Arc::new(Mutex::new(vec![]));
        std::thread::spawn({
            let (stalled, killed, pumps) = (stalled.clone(), killed.clone(), pumps.clone());
            let sent = sent.clone();
            move || {
                let (downstream, _) = listener.accept().unwrap();
                let upstream = TcpStream::connect(("127.0.0.1", target_port)).unwrap();
                for (from, to, kept) in [
                    (
                        downstream.try_clone().unwrap(),
                        upstream.try_clone().unwrap(),
                        Some(sent),
                    ),
                    (upstream, downstream, None),
                ] {
                    let (stalled, killed) = (stalled.clone(), killed.clone());
                    pumps.lock().unwrap().push(std::thread::spawn(move || {
                        pump(from, to, &stalled, &killed, kept.as_deref())
                    }));
                }
            }
        });
        Proxy {
            endpoint: format!("tcp/127.0.0.1:{port}").parse().unwrap(),
            stalled,
            killed,
            pumps,
            sent,
        }
    }

    /// What the accepted connection sent, once it has ended.
    fn sent_until_closed(&self) -> Vec<u8> {
        wait_until("the proxy has two pumps", || {
            self.pumps.lock().unwrap().len() == 2
        });
        let pump = self.pumps.lock().unwrap().remove(0);
        pump.join().unwrap();
        self.sent.lock().unwrap().clone()
    }

    fn kill(&self) {
        self.killed.store(true, Ordering::SeqCst);
        for pump in self.pumps.lock().unwrap().drain(..) {
            pump.join().unwrap();
        }
    }
}

fn pump(
    mut from: TcpStream,
    mut to: TcpStream,
    stalled: &AtomicBool,
    killed: &AtomicBool,
    kept: Option<&Mutex<Vec<u8>>>,
) {
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
                if let Some(kept) = kept {
                    kept.lock().unwrap().extend_from_slice(&buf[..n]);
                }
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

/// The size of the pushes that fill a link, and the byte they repeat: a stream read from
/// inside one of them reads a batch length, and then a header, that this file never sends.
const FILL_SIZE: usize = 8192;
const FILL_BYTE: u8 = 0x5a;

/// Pushes more than the sockets between the sender and a stopped proxy can hold, so that the
/// link's write blocks and its queue fills: a drop alone can come from the TX thread being
/// busy with other links.
fn overfill(transport: &TransportUnicast) {
    let mut dropped = false;
    for _ in 0..4_000 {
        let mut msg = NetworkMessage::from(Push {
            wire_expr: "test/link_loss".into(),
            ext_qos: QoSType::new(Priority::Data, CongestionControl::Drop, false),
            ..Push::from(vec![FILL_BYTE; FILL_SIZE])
        });
        dropped |= !transport.schedule(msg.as_mut()).unwrap();
    }
    assert!(dropped, "the link's queue never filled");
}

// Each test's managers have ZenohIds of their own: 20 to 59, 80 to 119, 140 to 141, 210 to
// 249 and 300 to 339 for the trials, 200 and 201 for the close, and from 400 on for the tests
// of leases and of reopening.

/// What the tx loss observer saw when a link through the proxy failed: whether the failed
/// link was listed both before and after a push made right then, which it is when its own
/// failed write reports the loss and the transport has not removed it meanwhile, and what that
/// push returned.
struct Failure {
    listed: bool,
    pushed: ZResult<bool>,
    loss: TxLoss,
}

/// Opens a transport through a proxy, and a second link beside it if `two_links`, overfills the
/// proxied link, resets it, and returns what the tx loss observer saw.
async fn fail_proxied_link(
    trial: u16,
    two_links: bool,
    data_queue: Option<usize>,
) -> (
    Failure,
    TransportManager,
    TransportManager,
    TransportUnicast,
) {
    let received = Arc::new(AtomicUsize::new(0));
    let receiving = manager(2 * trial, Arc::new(Counting(received)), None);
    let port = get_free_tcp_port();
    let listen: EndPoint = format!("tcp/127.0.0.1:{port}").parse().unwrap();
    ztimeout!(receiving.add_listener(listen.clone())).unwrap();
    let proxy = Proxy::start(port);
    let sending = manager(2 * trial + 1, Arc::new(Sender), data_queue);
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
        Arc::new(move |loss: &TxLoss| {
            let listed = || transport.get_links().is_ok_and(|l| l.len() == links);
            failure.lock().unwrap().get_or_insert_with(|| {
                let before = listed();
                let pushed = transport.schedule(droppable().as_mut());
                Failure {
                    listed: before && listed(),
                    pushed,
                    loss: *loss,
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
async fn fail_proxied_link_listed(
    first_trial: u16,
    two_links: bool,
    data_queue: Option<usize>,
) -> (Failure, ZenohIdProto) {
    for trial in first_trial..first_trial + 20 {
        let (failure, receiving, sending, _) =
            fail_proxied_link(trial, two_links, data_queue).await;
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
    let (failure, peer) = fail_proxied_link_listed(10, false, None).await;

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
    let (failure, _) = fail_proxied_link_listed(40, true, None).await;
    assert!(
        matches!(failure.pushed, Ok(false)),
        "a push the failed link refused is a drop: {:?}",
        failure.pushed
    );

    let (_, receiving, sending, transport) = fail_proxied_link(70, true, None).await;
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
/// writing, even with nothing else of that priority queued, and not the batch that holds only
/// the transport's `Close`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_close_counts_the_batch_being_written() {
    zenoh_util::init_log_from_env_or("error");
    install_observers();
    let receiving = manager(200, Arc::new(Counting::default()), None);
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
        .zid(zid(201))
        .queue_size(sizes)
        .unicast(TransportManagerBuilderUnicast::default().lease(Duration::from_secs(1)))
        .build_test(Arc::new(Sender))
        .unwrap();
    let transport = ztimeout!(sending.open_transport_unicast(proxy.endpoint.clone())).unwrap();

    proxy.stalled.store(true, Ordering::SeqCst);
    overfill(&transport);
    ztimeout!(transport.close()).unwrap();
    // The close returns once the link's TX task has reported.
    let losses = tx_losses_to(receiving.zid());
    assert!(
        losses.iter().any(|loss| loss.priority == Priority::Data
            && matches!(loss.kind, TxLossKind::ClosedQueued { batches: 1 })),
        "{losses:?}"
    );
    assert!(
        losses
            .iter()
            .all(|loss| loss.priority != Priority::Background),
        "{losses:?}"
    );

    proxy.kill();
    ztimeout!(sending.close());
    ztimeout!(receiving.close());
}

/// Whether `stream`, the bytes a TCP link wrote, is whole batches of messages this file sends,
/// with possibly the last batch cut short and nothing after it. Bytes written after a batch
/// cut short are read as its rest and then as batches of their own, and a read of them, which
/// starts inside a push, fails this.
fn ends_inside_a_batch(stream: &[u8]) -> Result<bool, String> {
    let codec = Zenoh080::new();
    let mut at = 0;
    while at < stream.len() {
        let Some(prefix) = stream.get(at..at + 2) else {
            return Ok(true);
        };
        let len = u16::from_le_bytes([prefix[0], prefix[1]]) as usize;
        at += 2;
        if len == 0 {
            return Err(format!("an empty batch at byte {at}"));
        }
        let whole = stream.len() - at >= len;
        let mut reader = &stream[at..(at + len).min(stream.len())];
        while reader.can_read() {
            let message: TransportMessage = match codec.read(&mut reader) {
                Ok(message) => message,
                Err(_) if !whole => return Ok(true),
                Err(_) => return Err(format!("a batch at byte {at} does not decode")),
            };
            match message.body {
                TransportBody::InitSyn(_)
                | TransportBody::OpenSyn(_)
                | TransportBody::KeepAlive(_)
                | TransportBody::Close(_) => {}
                TransportBody::Frame(frame) => {
                    for message in frame.payload {
                        let NetworkBody::Push(Push {
                            payload: PushBody::Put(put),
                            ..
                        }) = message.body
                        else {
                            return Err(format!("a batch at byte {at} holds {message:?}"));
                        };
                        let payload = put.payload.contiguous();
                        if payload.as_ref() != [FILL_BYTE; FILL_SIZE] {
                            let first = payload.iter().position(|b| *b != FILL_BYTE);
                            return Err(format!(
                                "a push in the batch at byte {at} of {}, {len} long, holds {} \
                                 bytes, the first not sent at {first:?}: {:02x?}",
                                stream.len(),
                                payload.len(),
                                first.map(|i| &payload[i..(i + 16).min(payload.len())])
                            ));
                        }
                    }
                }
                body => return Err(format!("a batch at byte {at} holds {body:?}")),
            }
        }
        if !whole {
            return Ok(true);
        }
        at += len;
    }
    Ok(false)
}

/// A link whose write a close cuts short writes nothing more: the cut can leave the stream
/// inside a batch, whose rest the peer would take from the bytes written next. The proxy
/// starts reading again as the close takes the link, which a write after the cut would then
/// complete into.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_close_that_cuts_a_write_writes_nothing_more() {
    zenoh_util::init_log_from_env_or("error");
    install_observers();
    for trial in 105..125 {
        let receiving = manager_without_shm(2 * trial, Arc::new(Counting::default()));
        let port = get_free_tcp_port();
        let listen: EndPoint = format!("tcp/127.0.0.1:{port}").parse().unwrap();
        ztimeout!(receiving.add_listener(listen)).unwrap();
        let proxy = Proxy::start(port);
        let sending = manager_without_shm(2 * trial + 1, Arc::new(Sender));
        let transport = ztimeout!(sending.open_transport_unicast(proxy.endpoint.clone())).unwrap();

        proxy.stalled.store(true, Ordering::SeqCst);
        overfill(&transport);
        let closing = tokio::spawn({
            let transport = transport.clone();
            async move { transport.close().await }
        });
        wait_until("the close takes the link", || {
            transport.get_links().map_or(true, |links| links.is_empty())
        });
        proxy.stalled.store(false, Ordering::SeqCst);
        ztimeout!(closing).unwrap().unwrap();
        let stream = proxy.sent_until_closed();
        proxy.kill();
        ztimeout!(sending.close());
        ztimeout!(receiving.close());

        // The trial counts when the close cut a write that had begun, which the proxy reading
        // again before the close came can let finish: the link then reports the batch it was
        // writing, and the stream ends inside it.
        let ended_inside = ends_inside_a_batch(&stream).unwrap();
        let cut = tx_losses_to(receiving.zid()).iter().any(|loss| {
            loss.priority == Priority::Data && matches!(loss.kind, TxLossKind::ClosedQueued { .. })
        });
        if cut && ended_inside {
            return;
        }
    }
    panic!("no close cut a write");
}

/// A link whose write fails counts the batch it was writing, even with nothing else of that
/// priority queued.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_write_counts_the_batch_being_written() {
    zenoh_util::init_log_from_env_or("error");
    install_observers();
    let (failure, _) = fail_proxied_link_listed(150, false, Some(1)).await;
    assert_eq!(failure.loss.priority, Priority::Data);
    assert_eq!(failure.loss.kind, TxLossKind::ResetQueued { batches: 1 });
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

/// The first payload byte of a message that holds the receive task at [`Marking`].
const HOLD: u8 = 0xff;

/// Counts the pushes each transport delivers by the byte their payload starts with, and holds
/// the receive task at a push that starts with [`HOLD`] until `gate` is released.
#[derive(Clone, Default)]
struct Marking {
    delivered: Arc<Mutex<HashMap<u8, usize>>>,
    gate: Arc<Gate>,
    held: Arc<AtomicBool>,
}

impl Marking {
    fn delivered(&self, marker: u8) -> usize {
        self.delivered
            .lock()
            .unwrap()
            .get(&marker)
            .copied()
            .unwrap_or(0)
    }
}

impl TransportEventHandler for Marking {
    fn new_unicast(
        &self,
        _peer: TransportPeer,
        _transport: TransportUnicast,
    ) -> ZResult<Arc<dyn TransportPeerEventHandler>> {
        Ok(Arc::new(MarkingPeer(self.clone())))
    }

    fn new_multicast(
        &self,
        _transport: TransportMulticast,
    ) -> ZResult<Arc<dyn TransportMulticastEventHandler>> {
        panic!("not used")
    }
}

struct MarkingPeer(Marking);

impl TransportPeerEventHandler for MarkingPeer {
    fn handle_message(&self, message: NetworkMessageMut) -> ZResult<()> {
        let NetworkBodyMut::Push(Push {
            payload: PushBody::Put(put),
            ..
        }) = message.body
        else {
            return Ok(());
        };
        let marker = put.payload.contiguous().first().copied().unwrap_or(0);
        if marker == HOLD {
            self.0.held.store(true, Ordering::SeqCst);
            self.0.gate.wait();
        } else {
            *self.0.delivered.lock().unwrap().entry(marker).or_default() += 1;
        }
        Ok(())
    }

    fn new_link(&self, _link: Link) {}
    fn del_link(&self, _link: Link) {}
    fn closed(&self) {}

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A reliable push that may not be dropped, whose payload starts with `marker`.
fn marked(marker: u8) -> NetworkMessage {
    NetworkMessage::from(Push {
        wire_expr: "test/link_loss".into(),
        ext_qos: QoSType::new(Priority::Data, CongestionControl::Block, false),
        ..Push::from(vec![marker; 64])
    })
}

/// A manager with ZenohId `n` and `lease`, which negotiates multilink only with `max_links`
/// above 1.
fn manager_with(
    n: u16,
    handler: Arc<dyn TransportEventHandler>,
    max_links: usize,
    lease: Duration,
) -> TransportManager {
    TransportManager::builder()
        .whatami(WhatAmI::Peer)
        .zid(zid(n))
        .unicast(
            TransportManagerBuilderUnicast::default()
                .max_links(max_links)
                .lease(lease),
        )
        .build_test(handler)
        .unwrap()
}

/// A TCP listener added to `manager`, on a free port.
async fn listen(manager: &TransportManager) -> (EndPoint, u16) {
    let port = get_free_tcp_port();
    let endpoint: EndPoint = format!("tcp/127.0.0.1:{port}").parse().unwrap();
    ztimeout!(manager.add_listener(endpoint.clone())).unwrap();
    (endpoint, port)
}

/// The transport `manager` holds with `peer`, once it has one.
async fn transport_with(manager: &TransportManager, peer: ZenohIdProto) -> TransportUnicast {
    let start = Instant::now();
    loop {
        if let Some(transport) = manager.get_transport_unicast(&peer).await {
            return transport;
        }
        assert!(start.elapsed() < TIMEOUT, "no transport with {peer}");
        tokio::time::sleep(POLL).await;
    }
}

/// Waits until `manager` holds no transport with `peer`.
async fn wait_closed(manager: &TransportManager, peer: ZenohIdProto) {
    let start = Instant::now();
    while manager.get_transport_unicast(&peer).await.is_some() {
        assert!(
            start.elapsed() < TIMEOUT,
            "the transport with {peer} stayed open"
        );
        tokio::time::sleep(POLL).await;
    }
}

fn lease_expiries_of(peer: ZenohIdProto) -> usize {
    rx_losses_from(peer)
        .iter()
        .filter(|loss| {
            loss.kind
                == RxLossKind::TransportClosedLocally {
                    reason: LocalClose::LeaseExpired,
                }
        })
        .count()
}

/// A peer that closes its side of a transport and opens a new one while this side still holds
/// the old, its receive task held by a callback, is refused until the old one has closed, and
/// then gets a fresh transport, which delivers every message it is sent. Joined to the old
/// transport, the new link would meet the old receive SNs, which the peer's new SNs start
/// below, and its messages would be dropped as old with no report.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_that_reopens_beside_a_held_transport_gets_a_fresh_one() {
    zenoh_util::init_log_from_env_or("error");
    install_observers();
    const BEFORE: usize = 1_000;
    const AFTER: usize = 200;
    // Leases long enough that neither side writes to the closed link, which would end the old
    // transport before the peer reopens.
    let held = Marking::default();
    let holding = manager_with(400, Arc::new(held.clone()), 1, Duration::from_secs(60));
    let sending = manager_with(401, Arc::new(Sender), 1, Duration::from_secs(60));
    let (holding_endpoint, _) = listen(&holding).await;
    let (sending_endpoint, _) = listen(&sending).await;
    // The holding side dials, so that the peer's new link is an inbound one beside it.
    ztimeout!(holding.open_transport_unicast(sending_endpoint)).unwrap();
    let old = transport_with(&sending, holding.zid()).await;
    for _ in 0..BEFORE {
        assert!(old.schedule(marked(1).as_mut()).unwrap());
    }
    wait_until("the first messages are delivered", || {
        held.delivered(1) == BEFORE
    });
    assert!(old.schedule(marked(HOLD).as_mut()).unwrap());
    wait_until("the receive task is held", || {
        held.held.load(Ordering::SeqCst)
    });
    ztimeout!(old.close()).unwrap();

    let refused = ztimeout!(sending.open_transport_unicast(holding_endpoint.clone()));
    held.gate.release();
    let reopened = match &refused {
        Ok(transport) => transport.clone(),
        Err(_) => {
            let start = Instant::now();
            loop {
                if let Ok(transport) =
                    ztimeout!(sending.open_transport_unicast(holding_endpoint.clone()))
                {
                    break transport;
                }
                assert!(start.elapsed() < TIMEOUT, "the peer never reconnected");
                tokio::time::sleep(POLL).await;
            }
        }
    };
    for _ in 0..AFTER {
        assert!(reopened.schedule(marked(2).as_mut()).unwrap());
    }
    let start = Instant::now();
    while held.delivered(2) < AFTER {
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "{} of {AFTER} delivered; reported: {:?}",
            held.delivered(2),
            rx_losses_from(sending.zid())
        );
        tokio::time::sleep(POLL).await;
    }
    assert!(
        refused.is_err(),
        "the new link was joined to the old transport"
    );
    assert_eq!(rx_losses_from(sending.zid()), []);

    ztimeout!(sending.close());
    ztimeout!(holding.close());
}

/// A peer that dials this process while a transport between them stands, opened the other way,
/// is refused, and the transport keeps one link and carries messages both ways: without
/// multilink, a link beside another is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dial_beside_a_transport_opened_the_other_way_is_refused() {
    zenoh_util::init_log_from_env_or("error");
    install_observers();
    let a_marks = Marking::default();
    let b_marks = Marking::default();
    let a = manager_with(410, Arc::new(a_marks.clone()), 1, Duration::from_secs(10));
    let b = manager_with(411, Arc::new(b_marks.clone()), 1, Duration::from_secs(10));
    let (a_endpoint, _) = listen(&a).await;
    let (b_endpoint, _) = listen(&b).await;
    let from_a = ztimeout!(a.open_transport_unicast(b_endpoint)).unwrap();
    let from_b = transport_with(&b, a.zid()).await;

    assert!(ztimeout!(b.open_transport_unicast(a_endpoint)).is_err());
    assert_eq!(from_a.get_links().unwrap().len(), 1);
    assert_eq!(from_b.get_links().unwrap().len(), 1);
    assert!(from_a.schedule(marked(1).as_mut()).unwrap());
    assert!(from_b.schedule(marked(1).as_mut()).unwrap());
    wait_until("messages cross both ways", || {
        a_marks.delivered(1) == 1 && b_marks.delivered(1) == 1
    });

    ztimeout!(a.close());
    ztimeout!(b.close());
}

/// Peers that dial each other at once end up with a transport each, which carries messages
/// both ways, whichever of their handshakes ends first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peers_that_dial_each_other_at_once_both_connect() {
    zenoh_util::init_log_from_env_or("error");
    install_observers();
    for trial in 0..20 {
        let a_marks = Marking::default();
        let b_marks = Marking::default();
        let a = manager_with(
            420 + 2 * trial,
            Arc::new(a_marks.clone()),
            1,
            Duration::from_secs(10),
        );
        let b = manager_with(
            421 + 2 * trial,
            Arc::new(b_marks.clone()),
            1,
            Duration::from_secs(10),
        );
        let (a_endpoint, _) = listen(&a).await;
        let (b_endpoint, _) = listen(&b).await;
        let _ = tokio::join!(
            a.open_transport_unicast(b_endpoint),
            b.open_transport_unicast(a_endpoint)
        );
        let from_a = transport_with(&a, b.zid()).await;
        let from_b = transport_with(&b, a.zid()).await;
        let start = Instant::now();
        while a_marks.delivered(1) == 0 || b_marks.delivered(1) == 0 {
            assert!(
                start.elapsed() < TIMEOUT,
                "trial {trial}: a side received nothing"
            );
            let _ = from_a.schedule(marked(1).as_mut());
            let _ = from_b.schedule(marked(1).as_mut());
            tokio::time::sleep(POLL).await;
        }
        ztimeout!(a.close());
        ztimeout!(b.close());
    }
}

/// A short lease, which a hold several times as long outlasts.
const SHORT_LEASE: Duration = Duration::from_millis(400);

/// A reader whose receive task its own callback holds past the lease reads what waited once
/// released, and the lease runs again from it. With the peer alive, the reader keeps the
/// transport, reports no local close, and later messages are delivered; with the peer having
/// closed the transport meanwhile, the reader sees the close and reports no expired lease.
/// Each hold lasts three leases, which is the scenario itself rather than a wait for a state;
/// the trials run one after the other, so that only one receive thread is held at a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reader_held_past_its_lease_reads_what_waited() {
    zenoh_util::init_log_from_env_or("error");
    install_observers();
    for trial in 0..12 {
        let peer_closes = trial % 2 == 1;
        let held = Marking::default();
        let reading = manager_with(460 + 2 * trial, Arc::new(held.clone()), 1, SHORT_LEASE);
        let sending = manager_with(461 + 2 * trial, Arc::new(Sender), 1, SHORT_LEASE);
        let (endpoint, _) = listen(&reading).await;
        let transport = ztimeout!(sending.open_transport_unicast(endpoint)).unwrap();
        assert!(transport.schedule(marked(HOLD).as_mut()).unwrap());
        wait_until("the receive task is held", || {
            held.held.load(Ordering::SeqCst)
        });
        if peer_closes {
            ztimeout!(transport.close()).unwrap();
        }
        tokio::time::sleep(3 * SHORT_LEASE).await;
        held.gate.release();

        if peer_closes {
            wait_closed(&reading, sending.zid()).await;
            assert_eq!(lease_expiries_of(sending.zid()), 0, "trial {trial}");
        } else {
            for _ in 0..10 {
                assert!(transport.schedule(marked(1).as_mut()).unwrap());
            }
            let start = Instant::now();
            while held.delivered(1) < 10 {
                assert!(
                    start.elapsed() < Duration::from_secs(5),
                    "trial {trial}: {} of 10 delivered; reported: {:?}",
                    held.delivered(1),
                    rx_losses_from(sending.zid())
                );
                tokio::time::sleep(POLL).await;
            }
            assert_eq!(rx_losses_from(sending.zid()), [], "trial {trial}");
            let links = transport_with(&reading, sending.zid())
                .await
                .get_links()
                .unwrap();
            assert_eq!(links.len(), 1, "trial {trial}");
        }
        ztimeout!(sending.close());
        ztimeout!(reading.close());
    }
}

/// A link whose lease expires while its transport keeps another link reports no local close,
/// and when the other one expires too, the transport reports it once at each end.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_the_last_link_to_expire_reports_its_lease() {
    zenoh_util::init_log_from_env_or("error");
    install_observers();
    let reading = manager_with(500, Arc::new(Counting::default()), 2, SHORT_LEASE);
    let sending = manager_with(501, Arc::new(Sender), 2, SHORT_LEASE);
    let (_, port) = listen(&reading).await;
    let proxies = [Proxy::start(port), Proxy::start(port)];
    for proxy in &proxies {
        ztimeout!(sending.open_transport_unicast(proxy.endpoint.clone())).unwrap();
    }
    let at_reading = transport_with(&reading, sending.zid()).await;
    let at_sending = transport_with(&sending, reading.zid()).await;
    let links = |transport: &TransportUnicast| transport.get_links().map_or(0, |l| l.len());
    wait_until("both links are listed", || {
        links(&at_reading) == 2 && links(&at_sending) == 2
    });

    proxies[0].stalled.store(true, Ordering::SeqCst);
    wait_until("the first link expires at both ends", || {
        links(&at_reading) == 1 && links(&at_sending) == 1
    });
    assert_eq!(lease_expiries_of(sending.zid()), 0);
    assert_eq!(lease_expiries_of(reading.zid()), 0);

    proxies[1].stalled.store(true, Ordering::SeqCst);
    wait_closed(&reading, sending.zid()).await;
    wait_closed(&sending, reading.zid()).await;
    assert_eq!(lease_expiries_of(sending.zid()), 1);
    assert_eq!(lease_expiries_of(reading.zid()), 1);

    for proxy in &proxies {
        proxy.kill();
    }
    ztimeout!(sending.close());
    ztimeout!(reading.close());
}
