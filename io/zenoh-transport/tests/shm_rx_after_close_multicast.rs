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
//! On a MULTICAST transport too, a transport manager that has begun to close delivers
//! nothing more and maps no shared-memory buffer, while an open one still maps, delivers
//! and reports a failed map at ERROR (BER-2230, BER-2167).
//!
//! `TransportManager::close` closes its unicast transports before its multicast ones. The
//! closing case holds `close` inside the `closed()` callback of a unicast transport to a
//! helper peer, so the multicast transport is still open, and then sends to the group. A
//! witness, an open manager in the same group, receives what was sent, so the receiver's
//! silence is the gate's doing and not a lost datagram.
//!
//! A map is observed as an attach, as in `shm_rx_after_close.rs`: the receiver's
//! shared-memory client counts every attach, and the probes sent after `close` has begun
//! come from a pool the receiver has never mapped.
//!
//! The group is joined on the loopback interface, so that the host's firewall and routes
//! cannot decide the outcome. `ZENOH_TEST_MULTICAST_IFACE` names another interface, by
//! name or by address, to run the same tests over it.
#![cfg(all(
    feature = "shared-memory",
    feature = "test",
    feature = "transport_tcp",
    feature = "transport_udp"
))]

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
use zenoh_test::{get_free_tcp_port, get_free_udp_port, multicast_iface};
use zenoh_transport::{
    common::shm::interop::shm_rx_drop_state, multicast::TransportMulticast,
    unicast::TransportUnicast, DummyTransportMulticastEventHandler, DummyTransportPeerEventHandler,
    TransportEventHandler, TransportManager, TransportMulticastEventHandler, TransportPeer,
    TransportPeerEventHandler,
};

const TIMEOUT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(10);
/// How long the receiver is given to process probes the witness has already received
/// before an absence is asserted: a margin for a loaded host, not an estimate of the work.
const SETTLE: Duration = Duration::from_secs(1);
const POOL_SIZE: usize = 64 * 1024;
const PROBE_SIZE: usize = 64;
/// The multicast group, joined on `multicast_iface()`.
const GROUP: &str = "224.0.0.1";

fn multicast_endpoint() -> EndPoint {
    format!(
        "udp/{GROUP}:{}#iface={}",
        get_free_udp_port(),
        multicast_iface()
    )
    .parse()
    .unwrap()
}

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

/// Once armed, holds the first unicast `closed()` callback, and with it the
/// `TransportManager::close` that runs it, until released.
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

/// What a manager's multicast peers have delivered, by kind.
#[derive(Default)]
struct Received {
    raw: AtomicUsize,
    shm: AtomicUsize,
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
struct ReleaseOnDrop(Arc<Hold>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

/// Counts what a multicast peer delivers.
struct Counting(Arc<Received>);

impl TransportPeerEventHandler for Counting {
    fn handle_message(&self, message: NetworkMessageMut) -> ZResult<()> {
        if let NetworkBodyMut::Push(push) = message.body {
            if let PushBody::Put(Put { payload, .. }) = &push.payload {
                let mapped = payload
                    .zslices()
                    .any(|zs| zs.downcast_ref::<ShmBufInner>().is_some());
                let counter = if mapped { &self.0.shm } else { &self.0.raw };
                counter.fetch_add(1, Ordering::SeqCst);
            }
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

impl TransportMulticastEventHandler for Counting {
    fn new_peer(&self, _peer: TransportPeer) -> ZResult<Arc<dyn TransportPeerEventHandler>> {
        Ok(Arc::new(Counting(self.0.clone())))
    }

    fn closed(&self) {}

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// The unicast peer `close` is held on.
struct Held {
    zid: ZenohIdProto,
    hold: Arc<Hold>,
}

impl TransportPeerEventHandler for Held {
    fn handle_message(&self, _message: NetworkMessageMut) -> ZResult<()> {
        Ok(())
    }

    fn new_link(&self, _link: Link) {}
    fn del_link(&self, _link: Link) {}

    fn closed(&self) {
        self.hold.on_closed(self.zid);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A manager whose multicast peers count what they deliver and whose unicast peers are
/// held on close once the hold is armed.
struct ReceiverHandler {
    received: Arc<Received>,
    hold: Arc<Hold>,
}

impl TransportEventHandler for ReceiverHandler {
    fn new_unicast(
        &self,
        peer: TransportPeer,
        _transport: TransportUnicast,
    ) -> ZResult<Arc<dyn TransportPeerEventHandler>> {
        Ok(Arc::new(Held {
            zid: peer.zid,
            hold: self.hold.clone(),
        }))
    }

    fn new_multicast(
        &self,
        _transport: TransportMulticast,
    ) -> ZResult<Arc<dyn TransportMulticastEventHandler>> {
        Ok(Arc::new(Counting(self.received.clone())))
    }
}

/// A manager that delivers nothing anywhere.
struct QuietHandler;

impl TransportEventHandler for QuietHandler {
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
        Ok(Arc::new(DummyTransportMulticastEventHandler))
    }
}

/// Shared memory on, in `lazy` mode: nothing is pre-mounted, so a receiver's first
/// attach of a pool is a map.
fn shm_conf() -> zenoh_config::ShmConf {
    let mut shm = zenoh_config::ShmConf::default();
    shm.set_enabled(true).unwrap();
    shm.set_mode(zenoh_config::ShmInitMode::Lazy).unwrap();
    shm
}

fn manager(
    zid: ZenohIdProto,
    handler: Arc<dyn TransportEventHandler>,
    client: Option<Arc<CountingClient>>,
) -> TransportManager {
    let reader = client.map(|client| {
        ShmReader::new(Arc::new(
            ShmClientStorage::builder().with_client(client).build(),
        ))
    });
    TransportManager::builder()
        .whatami(WhatAmI::Peer)
        .zid(zid)
        .shm(shm_conf())
        .shm_reader(reader)
        .build_test(handler)
        .unwrap()
}

fn zid(byte: u8) -> ZenohIdProto {
    ZenohIdProto::try_from([byte]).unwrap()
}

/// A pool of its own, so a receiver's first map of a buffer from it is an attach.
fn pool() -> ShmProvider<PosixShmProviderBackendBinaryHeap> {
    let backend = PosixShmProviderBackendBinaryHeap::builder(POOL_SIZE)
        .wait()
        .unwrap();
    ShmProviderBuilder::backend(backend).wait()
}

fn put(payload: ZBuf) -> NetworkMessage {
    NetworkMessage::from(Push {
        wire_expr: "test/shm-rx-after-close-multicast".into(),
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

/// The multicast transport of `manager` once `peer` has joined it.
async fn joined(manager: &TransportManager, peer: &ZenohIdProto) -> TransportMulticast {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        if let Some(transport) = manager.get_transport_multicast(peer).await {
            return transport;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {peer} to join the group"
        );
        tokio::time::sleep(POLL).await;
    }
}

/// `close` is held on the receiver's unicast transport, and the sender multicasts while
/// it is: the receiver's multicast transport is open and must deliver and map nothing.
async fn closing_on_multicast(first_zid: u8) {
    zenoh_util::init_log_from_env_or("error");
    let group = multicast_endpoint();
    let (receiver_zid, sender_zid, witness_zid, helper_zid) = (
        zid(first_zid),
        zid(first_zid + 1),
        zid(first_zid + 2),
        zid(first_zid + 3),
    );

    let client = CountingClient::new(false);
    let received = Arc::new(Received::default());
    let hold = Arc::new(Hold::default());
    let receiver = manager(
        receiver_zid,
        Arc::new(ReceiverHandler {
            received: received.clone(),
            hold: hold.clone(),
        }),
        Some(client.clone()),
    );
    let sender = manager(sender_zid, Arc::new(QuietHandler), None);
    let witnessed = Arc::new(Received::default());
    let witness = manager(
        witness_zid,
        Arc::new(ReceiverHandler {
            received: witnessed.clone(),
            hold: Arc::new(Hold::default()),
        }),
        None,
    );
    let helper = manager(helper_zid, Arc::new(QuietHandler), None);

    for manager in [&receiver, &sender, &witness] {
        ztimeout!(manager.open_transport_multicast(group.clone())).unwrap();
    }
    let to_group = joined(&sender, &receiver_zid).await;
    joined(&receiver, &sender_zid).await;
    joined(&witness, &sender_zid).await;
    assert!(
        to_group.is_shm().unwrap(),
        "shm must be on, or nothing here is about mapping"
    );

    let helper_endpoint: EndPoint = format!("tcp/127.0.0.1:{}", get_free_tcp_port())
        .parse()
        .unwrap();
    ztimeout!(helper.add_listener(helper_endpoint.clone())).unwrap();
    ztimeout!(receiver.open_transport_unicast(helper_endpoint)).unwrap();

    // THE CONTROL: while the manager is open, its multicast transport maps and delivers.
    let early = pool();
    assert!(to_group.schedule(raw_probe().as_mut()).unwrap());
    assert!(to_group.schedule(shm_probe(&early).await.as_mut()).unwrap());
    wait_until(
        "the open receiver and the witness to deliver both probes",
        || received.counts() == (1, 1) && witnessed.counts() == (1, 1),
    )
    .await;
    assert_eq!(
        client.attaches(),
        1,
        "the open manager must map the pool, attaching it once"
    );

    let _release = ReleaseOnDrop(hold.clone());
    hold.arm();
    let closing = tokio::spawn({
        let receiver = receiver.clone();
        async move { receiver.close().await }
    });
    wait_until("close to reach the unicast transport", || {
        hold.first_closed() == Some(helper_zid)
    })
    .await;

    // PRECONDITION: `close` has not reached the multicast transport yet.
    assert!(
        !ztimeout!(receiver.get_transports_multicast()).is_empty(),
        "the multicast transport must still be open while close is held"
    );

    let late = pool();
    assert!(to_group.schedule(raw_probe().as_mut()).unwrap());
    assert!(to_group.schedule(shm_probe(&late).await.as_mut()).unwrap());
    wait_until(
        "the witness to deliver the probes sent during close",
        || witnessed.counts() == (2, 2),
    )
    .await;
    tokio::time::sleep(SETTLE).await;

    assert_eq!(
        (received.counts(), client.attaches()),
        ((1, 1), 1),
        "a closing manager must deliver nothing more and map no shared-memory buffer on \
         its multicast transport: ((raw, shm) delivered, attaches)"
    );

    hold.release();
    ztimeout!(closing).unwrap();
    for manager in [&sender, &witness, &helper] {
        ztimeout!(manager.close());
    }
}

/// The receiver's client refuses every attach, so the map fails the way it does on a
/// live receiver that cannot open a sender's pool.
async fn open_manager_reports_on_multicast(first_zid: u8) {
    zenoh_util::init_log_from_env_or("error");
    let group = multicast_endpoint();
    let (receiver_zid, sender_zid) = (zid(first_zid), zid(first_zid + 1));

    let client = CountingClient::new(true);
    let received = Arc::new(Received::default());
    let receiver = manager(
        receiver_zid,
        Arc::new(ReceiverHandler {
            received: received.clone(),
            hold: Arc::new(Hold::default()),
        }),
        Some(client.clone()),
    );
    let sender = manager(sender_zid, Arc::new(QuietHandler), None);
    for manager in [&receiver, &sender] {
        ztimeout!(manager.open_transport_multicast(group.clone())).unwrap();
    }
    let to_group = joined(&sender, &receiver_zid).await;
    joined(&receiver, &sender_zid).await;

    // The shared-memory probe first and the raw one after it, on the same channel, so
    // the raw probe's arrival means the shared-memory one has been through the receive
    // path.
    let pool = pool();
    assert!(to_group.schedule(shm_probe(&pool).await.as_mut()).unwrap());
    assert!(to_group.schedule(raw_probe().as_mut()).unwrap());
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

    for manager in [&sender, &receiver] {
        ztimeout!(manager.close());
    }
}

/// **A CLOSING MANAGER NEITHER MAPS NOR DELIVERS ON ITS MULTICAST TRANSPORT.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_closing_manager_neither_maps_nor_delivers_on_its_multicast_transport() {
    closing_on_multicast(0x91).await;
}

/// **AN OPEN MANAGER STILL REPORTS A FAILED MAP ON MULTICAST AT ERROR** (BER-2167): the
/// control for the test above.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_manager_still_reports_a_failed_map_on_multicast_at_error() {
    open_manager_reports_on_multicast(0xa1).await;
}
