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
//! A transport manager that has begun to close delivers nothing more and maps no
//! shared-memory buffer, while an open one still reports a failed map at ERROR
//! (BER-2230, BER-2167). Each case runs over the universal and the low-latency
//! unicast transport.
//!
//! `TransportManager::close` drains its transport table, then closes the drained
//! transports one at a time. The closing case holds `close` inside the first
//! transport's `closed()` callback, which `close` itself runs, so no receive thread is
//! ever blocked, and then sends to the receiver on the transport `close` has not closed
//! yet. That a closing manager admits no new transport at all is
//! `unicast_admission_after_close.rs`'s subject.
//!
//! A map is observed as an attach: the receiver's shared-memory client counts every
//! attach, and a receiver attaches a segment the first time it maps a buffer from it.
//! The probes sent after `close` has begun come from a pool the receiver has never
//! mapped, so any attempt to map one of them shows up as an attach.
//!
//! What a closing manager does not deliver it counts, and the open manager counts nothing.
#![cfg(all(feature = "shared-memory", feature = "test", feature = "transport_tcp"))]

use std::{
    any::Any,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    time::Duration,
};

use zenoh_buffers::ZBuf;
use zenoh_core::{ztimeout, Wait};
use zenoh_link::Link;
use zenoh_protocol::{
    core::{CongestionControl, EndPoint, Priority, WhatAmI, ZenohIdProto},
    network::{push::ext::QoSType, NetworkBodyMut, NetworkMessage, NetworkMessageMut, Push},
    zenoh::{PushBody, Put},
};
use zenoh_result::{bail, ZResult};
use zenoh_shm::{
    api::{
        client::{shm_client::ShmClient, shm_segment::ShmSegment},
        client_storage::ShmClientStorage,
        common::{
            types::{ProtocolID, SegmentID},
            with_id::WithProtocolID,
        },
        protocol_implementations::posix::{
            posix_shm_client::PosixShmClient,
            posix_shm_provider_backend_binary_heap::PosixShmProviderBackendBinaryHeap,
            protocol_id::POSIX_PROTOCOL_ID,
        },
        provider::shm_provider::{BlockOn, GarbageCollect, ShmProvider, ShmProviderBuilder},
    },
    reader::ShmReader,
    ShmBufInner,
};
use zenoh_test::get_free_tcp_port;
use zenoh_transport::{
    common::shm::interop::shm_rx_drop_state, multicast::TransportMulticast,
    unicast::TransportUnicast, DummyTransportPeerEventHandler, TransportEventHandler,
    TransportManager, TransportMulticastEventHandler, TransportPeer, TransportPeerEventHandler,
};

const TIMEOUT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(10);
/// How long the receiver is given to process probes already sent before an absence
/// is asserted: a margin for a loaded host, not an estimate of the work.
const SETTLE: Duration = Duration::from_secs(1);
const POOL_SIZE: usize = 64 * 1024;
const PROBE_SIZE: usize = 64;

/// The receiver's only shared-memory client: POSIX, counting every attach.
#[derive(Debug)]
struct CountingClient {
    attaches: AtomicUsize,
    refuse: bool,
}

impl CountingClient {
    fn new(refuse: bool) -> Arc<Self> {
        Arc::new(Self {
            attaches: AtomicUsize::new(0),
            refuse,
        })
    }

    fn attaches(&self) -> usize {
        self.attaches.load(Ordering::SeqCst)
    }
}

impl WithProtocolID for CountingClient {
    fn id(&self) -> ProtocolID {
        POSIX_PROTOCOL_ID
    }
}

impl ShmClient for CountingClient {
    fn attach(&self, segment: SegmentID) -> ZResult<Arc<dyn ShmSegment>> {
        self.attaches.fetch_add(1, Ordering::SeqCst);
        if self.refuse {
            bail!("this test's client refuses to attach segment {segment}");
        }
        PosixShmClient.attach(segment)
    }
}

/// Once armed, holds the first `closed()` callback, and with it the
/// `TransportManager::close` that runs it, until released.
///
/// Unarmed, every `closed()` returns at once: a close the remote starts runs this
/// callback on zenoh's `Net` runtime, which a held callback would stall.
#[derive(Default)]
struct Hold {
    armed: AtomicBool,
    first: Mutex<Option<ZenohIdProto>>,
    released: Mutex<bool>,
    cv: Condvar,
}

impl Hold {
    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    fn on_closed(&self, zid: ZenohIdProto) {
        if !self.armed.load(Ordering::SeqCst) {
            return;
        }
        {
            let mut first = self.first.lock().unwrap();
            if first.is_some() {
                return;
            }
            *first = Some(zid);
        }
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.cv.wait(released).unwrap();
        }
    }

    fn first_closed(&self) -> Option<ZenohIdProto> {
        *self.first.lock().unwrap()
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.cv.notify_all();
    }
}

/// What the receiver's handler has been given, by kind.
#[derive(Default)]
struct Received {
    raw: AtomicUsize,
    shm: AtomicUsize,
    hold: Hold,
}

impl Received {
    /// `(raw, shm)` messages delivered so far.
    fn counts(&self) -> (usize, usize) {
        (
            self.raw.load(Ordering::SeqCst),
            self.shm.load(Ordering::SeqCst),
        )
    }
}

/// Releases the hold when a test unwinds, so a failed assertion does not leave
/// `close` blocked.
struct ReleaseOnDrop(Arc<Received>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.hold.release();
    }
}

struct ReceiverHandler(Arc<Received>);

impl TransportEventHandler for ReceiverHandler {
    fn new_unicast(
        &self,
        peer: TransportPeer,
        _transport: TransportUnicast,
    ) -> ZResult<Arc<dyn TransportPeerEventHandler>> {
        Ok(Arc::new(ReceiverPeer {
            zid: peer.zid,
            received: self.0.clone(),
        }))
    }

    fn new_multicast(
        &self,
        _transport: TransportMulticast,
    ) -> ZResult<Arc<dyn TransportMulticastEventHandler>> {
        panic!("not used")
    }
}

struct ReceiverPeer {
    zid: ZenohIdProto,
    received: Arc<Received>,
}

impl TransportPeerEventHandler for ReceiverPeer {
    fn handle_message(&self, message: NetworkMessageMut) -> ZResult<()> {
        if let NetworkBodyMut::Push(push) = message.body {
            if let PushBody::Put(Put { payload, .. }) = &push.payload {
                let mapped = payload
                    .zslices()
                    .any(|zs| zs.downcast_ref::<ShmBufInner>().is_some());
                let counter = if mapped {
                    &self.received.shm
                } else {
                    &self.received.raw
                };
                counter.fetch_add(1, Ordering::SeqCst);
            }
        }
        Ok(())
    }

    fn new_link(&self, _link: Link) {}
    fn del_link(&self, _link: Link) {}

    fn closed(&self) {
        self.received.hold.on_closed(self.zid);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct SenderHandler;

impl TransportEventHandler for SenderHandler {
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

/// Shared memory on, in `lazy` mode: nothing is pre-mounted at establishment, so the
/// receiver's first attach of a pool is a map.
fn shm_conf() -> zenoh_config::ShmConf {
    let mut shm = zenoh_config::ShmConf::default();
    shm.set_enabled(true).unwrap();
    shm.set_mode(zenoh_config::ShmInitMode::Lazy).unwrap();
    shm
}

fn receiver(
    zid: ZenohIdProto,
    lowlatency: bool,
    client: Arc<CountingClient>,
    received: Arc<Received>,
) -> TransportManager {
    let storage = Arc::new(ShmClientStorage::builder().with_client(client).build());
    TransportManager::builder()
        .whatami(WhatAmI::Peer)
        .zid(zid)
        .shm(shm_conf())
        .shm_reader(Some(ShmReader::new(storage)))
        .unicast(
            TransportManager::config_unicast()
                .lowlatency(lowlatency)
                .qos(!lowlatency),
        )
        .build_test(Arc::new(ReceiverHandler(received)))
        .unwrap()
}

fn sender(zid: ZenohIdProto, lowlatency: bool) -> TransportManager {
    TransportManager::builder()
        .whatami(WhatAmI::Peer)
        .zid(zid)
        .shm(shm_conf())
        .unicast(
            TransportManager::config_unicast()
                .lowlatency(lowlatency)
                .qos(!lowlatency),
        )
        .build_test(Arc::new(SenderHandler))
        .unwrap()
}

fn zid(byte: u8) -> ZenohIdProto {
    ZenohIdProto::try_from([byte]).unwrap()
}

fn tcp_endpoint() -> EndPoint {
    format!("tcp/127.0.0.1:{}", get_free_tcp_port())
        .parse()
        .unwrap()
}

/// A pool of its own, so the receiver's first map of a buffer from it is an attach.
fn pool() -> ShmProvider<PosixShmProviderBackendBinaryHeap> {
    let backend = PosixShmProviderBackendBinaryHeap::builder(POOL_SIZE)
        .wait()
        .unwrap();
    ShmProviderBuilder::backend(backend).wait()
}

fn put(payload: ZBuf) -> NetworkMessage {
    NetworkMessage::from(Push {
        wire_expr: "test/shm-rx-after-close".into(),
        ext_qos: QoSType::new(Priority::DEFAULT, CongestionControl::Block, false),
        ..Push::from(Put {
            payload,
            ..Put::default()
        })
    })
}

fn raw_probe() -> NetworkMessage {
    put(vec![0_u8; PROBE_SIZE].into())
}

async fn shm_probe(pool: &ShmProvider<PosixShmProviderBackendBinaryHeap>) -> NetworkMessage {
    let mut buf = ztimeout!(pool
        .alloc_layout(PROBE_SIZE)
        .unwrap()
        .alloc()
        .with_policy::<BlockOn<GarbageCollect>>())
    .unwrap();
    buf.fill(0xa5);
    put(buf.into())
}

/// Send one raw and one shared-memory probe, in that order.
async fn send_probes(
    transport: &TransportUnicast,
    pool: &ShmProvider<PosixShmProviderBackendBinaryHeap>,
) {
    assert!(transport.schedule(raw_probe().as_mut()).unwrap());
    assert!(transport.schedule(shm_probe(pool).await.as_mut()).unwrap());
}

async fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while !done() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(POLL).await;
    }
}

async fn transport_to(manager: &TransportManager, peer: &ZenohIdProto) -> TransportUnicast {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        if let Some(transport) = manager.get_transport_unicast(peer).await {
            return transport;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for a transport to {peer}"
        );
        tokio::time::sleep(POLL).await;
    }
}

/// `close` is held on whichever of two transports it closes first, and the other sender
/// sends to the receiver while `close` is still in progress.
async fn not_yet_closed(lowlatency: bool, first_zid: u8) {
    zenoh_util::init_log_from_env_or("error");
    let endpoint = tcp_endpoint();
    let receiver_zid = zid(first_zid);
    let sender_zids = [zid(first_zid + 1), zid(first_zid + 2)];

    let client = CountingClient::new(false);
    let received = Arc::new(Received::default());
    let receiver = receiver(receiver_zid, lowlatency, client.clone(), received.clone());
    ztimeout!(receiver.add_listener(endpoint.clone())).unwrap();

    let senders = sender_zids.map(|zid| sender(zid, lowlatency));
    let mut to_receiver = Vec::new();
    for sender in &senders {
        let transport = ztimeout!(sender.open_transport_unicast(endpoint.clone())).unwrap();
        assert!(
            transport.is_shm().unwrap(),
            "shm must negotiate, or nothing here is about mapping"
        );
        to_receiver.push(transport);
    }

    // THE CONTROL: while the manager is open, both transports map and deliver.
    let early = pool();
    for transport in &to_receiver {
        send_probes(transport, &early).await;
    }
    wait_until("the open manager to deliver every probe", || {
        received.counts() == (2, 2)
    })
    .await;
    assert_eq!(
        client.attaches(),
        1,
        "the open manager must map the pool, attaching it once"
    );
    let mut receiver_side = Vec::new();
    for zid in &sender_zids {
        receiver_side.push(transport_to(&receiver, zid).await);
    }

    let _release = ReleaseOnDrop(received.clone());
    received.hold.arm();
    let closing = tokio::spawn({
        let receiver = receiver.clone();
        async move { receiver.close().await }
    });
    wait_until("close to reach its first transport", || {
        received.hold.first_closed().is_some()
    })
    .await;
    let not_yet = usize::from(received.hold.first_closed() == Some(sender_zids[0]));

    // PRECONDITION: `close` has not reached this transport, so it is still open.
    assert!(
        receiver_side[not_yet].get_callback().unwrap().is_some(),
        "the second transport must still be open while close is held on the first"
    );

    let late = pool();
    send_probes(&to_receiver[not_yet], &late).await;
    tokio::time::sleep(SETTLE).await;

    assert_eq!(
        (received.counts(), client.attaches()),
        ((2, 2), 1),
        "a closing manager must deliver nothing more and map no shared-memory buffer, \
         even on a transport it has not closed yet: ((raw, shm) delivered, attaches)"
    );

    received.hold.release();
    ztimeout!(closing).unwrap();
    assert_eq!(
        receiver.undelivered_after_close(),
        2,
        "a closing manager must count the two probes it did not deliver"
    );
    for sender in &senders {
        ztimeout!(sender.close());
    }
}

/// The receiver's client refuses every attach, so the map fails the way it does on a
/// live receiver that cannot open a sender's pool.
async fn open_manager_reports(lowlatency: bool, first_zid: u8) {
    zenoh_util::init_log_from_env_or("error");
    let endpoint = tcp_endpoint();
    let receiver_zid = zid(first_zid);
    let sender_zid = zid(first_zid + 1);

    let client = CountingClient::new(true);
    let received = Arc::new(Received::default());
    let receiver = receiver(receiver_zid, lowlatency, client.clone(), received.clone());
    ztimeout!(receiver.add_listener(endpoint.clone())).unwrap();

    let sender = sender(sender_zid, lowlatency);
    let to_receiver = ztimeout!(sender.open_transport_unicast(endpoint.clone())).unwrap();
    assert!(to_receiver.is_shm().unwrap());

    // The shared-memory probe first and the raw one after it, on the same channel, so
    // the raw probe's arrival means the shared-memory one has been through the receive
    // path.
    let pool = pool();
    assert!(to_receiver
        .schedule(shm_probe(&pool).await.as_mut())
        .unwrap());
    assert!(to_receiver.schedule(raw_probe().as_mut()).unwrap());
    wait_until("the raw probe", || received.counts().0 == 1).await;

    assert_eq!(
        received.counts(),
        (1, 0),
        "a buffer the receiver cannot map must not be delivered"
    );
    assert_eq!(client.attaches(), 1, "the map must have been attempted");
    let (total, reported, _) = shm_rx_drop_state(&sender_zid)
        .expect("an open manager's failed map must be counted against its peer");
    assert_eq!(
        (total, reported),
        (1, 1),
        "an open manager's failed map must be reported at ERROR"
    );
    assert_eq!(
        receiver.undelivered_after_close(),
        0,
        "an open manager drops nothing as closed"
    );

    ztimeout!(to_receiver.close()).unwrap();
    ztimeout!(sender.close());
    ztimeout!(receiver.close());
}

/// **A CLOSING MANAGER NEITHER MAPS NOR DELIVERS ON A TRANSPORT IT HAS NOT CLOSED
/// YET.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_closing_manager_neither_maps_nor_delivers_on_a_transport_it_has_not_closed_yet() {
    not_yet_closed(false, 0x31).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_same_over_the_lowlatency_transport_on_a_transport_not_closed_yet() {
    not_yet_closed(true, 0x61).await;
}

/// **AN OPEN MANAGER STILL REPORTS A FAILED MAP AT ERROR** (BER-2167): the control for
/// the tests above.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_manager_still_reports_a_failed_map_at_error() {
    open_manager_reports(false, 0x51).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_same_over_the_lowlatency_transport_for_an_open_manager() {
    open_manager_reports(true, 0x81).await;
}
