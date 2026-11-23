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
//! A received message takes the exit hold only if it carries a shared-memory buffer
//! (BER-2474). The hold makes process exit wait for a map in flight (BER-2456), and only such a
//! message is mapped.
//!
//! The holds are counted process-wide, so this binary holds this one test.
#![cfg(all(feature = "shared-memory", feature = "test", feature = "transport_tcp"))]

use std::{
    any::Any,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
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
use zenoh_result::ZResult;
use zenoh_shm::{
    api::{
        protocol_implementations::posix::posix_shm_provider_backend_binary_heap::PosixShmProviderBackendBinaryHeap,
        provider::shm_provider::{GarbageCollect, ShmProvider, ShmProviderBuilder},
    },
    ShmBufInner,
};
use zenoh_transport::{
    common::shm::interop::exit_holds_taken, multicast::TransportMulticast,
    unicast::TransportUnicast, DummyTransportPeerEventHandler, TransportEventHandler,
    TransportManager, TransportMulticastEventHandler, TransportPeer, TransportPeerEventHandler,
};

const TIMEOUT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(10);
/// How many messages of each kind are sent.
const MESSAGES: usize = 20;
const POOL_SIZE: usize = 64 * 1024;
const PROBE_SIZE: usize = 64;

/// The messages a receiver delivered: `(raw, shared memory)`.
#[derive(Default)]
struct Delivered {
    raw: AtomicUsize,
    shm: AtomicUsize,
}

impl Delivered {
    fn counts(&self) -> (usize, usize) {
        (
            self.raw.load(Ordering::SeqCst),
            self.shm.load(Ordering::SeqCst),
        )
    }
}

struct Counting(Arc<Delivered>);

impl TransportPeerEventHandler for Counting {
    fn handle_message(&self, message: NetworkMessageMut) -> ZResult<()> {
        if let NetworkBodyMut::Push(push) = message.body {
            if let PushBody::Put(Put { payload, .. }) = &push.payload {
                if payload
                    .zslices()
                    .any(|zs| zs.downcast_ref::<ShmBufInner>().is_some())
                {
                    self.0.shm.fetch_add(1, Ordering::SeqCst);
                } else {
                    self.0.raw.fetch_add(1, Ordering::SeqCst);
                }
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

struct ReceiverHandler(Arc<Delivered>);

impl TransportEventHandler for ReceiverHandler {
    fn new_unicast(
        &self,
        _peer: TransportPeer,
        _transport: TransportUnicast,
    ) -> ZResult<Arc<dyn TransportPeerEventHandler>> {
        Ok(Arc::new(Counting(self.0.clone())))
    }

    fn new_multicast(
        &self,
        _transport: TransportMulticast,
    ) -> ZResult<Arc<dyn TransportMulticastEventHandler>> {
        panic!("not used")
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

/// A manager with shared memory on, in `lazy` mode.
fn manager(byte: u8, handler: Arc<dyn TransportEventHandler>) -> TransportManager {
    let mut shm = zenoh_config::ShmConf::default();
    shm.set_enabled(true).unwrap();
    shm.set_mode(zenoh_config::ShmInitMode::Lazy).unwrap();
    TransportManager::builder()
        .whatami(WhatAmI::Peer)
        .zid(ZenohIdProto::try_from([byte]).unwrap())
        .shm(shm)
        .build_test(handler)
        .unwrap()
}

fn put(payload: ZBuf) -> NetworkMessage {
    NetworkMessage::from(Push {
        wire_expr: "test/shm-rx-exit-hold".into(),
        ext_qos: QoSType::new(Priority::DEFAULT, CongestionControl::Block, false),
        ..Push::from(Put {
            payload,
            ..Put::default()
        })
    })
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

/// **ONLY A MESSAGE THAT CARRIES A SHARED-MEMORY BUFFER TAKES THE EXIT HOLD**, each exactly
/// once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_a_message_carrying_a_shared_memory_buffer_takes_the_exit_hold() {
    zenoh_util::init_log_from_env_or("error");
    let delivered = Arc::new(Delivered::default());
    let receiver = manager(0x71, Arc::new(ReceiverHandler(delivered.clone())));
    let locator = ztimeout!(receiver.add_listener("tcp/127.0.0.1:0".parse().unwrap())).unwrap();
    let sender = manager(0x72, Arc::new(SenderHandler));
    let endpoint: EndPoint = locator.to_string().parse().unwrap();
    let to_receiver = ztimeout!(sender.open_transport_unicast(endpoint)).unwrap();
    assert!(
        to_receiver.is_shm().unwrap(),
        "shm must negotiate, or nothing here is about mapping"
    );

    // A message with no shared-memory buffer, on a shared-memory transport, takes no hold.
    let before = exit_holds_taken();
    for i in 0..MESSAGES {
        let payload = ZBuf::from(vec![i as u8; PROBE_SIZE]);
        assert!(to_receiver.schedule(put(payload).as_mut()).unwrap());
    }
    wait_until("the raw messages", || delivered.counts().0 == MESSAGES).await;
    assert_eq!(
        exit_holds_taken() - before,
        0,
        "a message that carries no shared-memory buffer must take no exit hold"
    );

    // THE OTHER SIDE: a message with a buffer takes the hold, once.
    let backend = PosixShmProviderBackendBinaryHeap::builder(POOL_SIZE)
        .wait()
        .unwrap();
    let pool: ShmProvider<PosixShmProviderBackendBinaryHeap> =
        ShmProviderBuilder::backend(backend).wait();
    for _ in 0..MESSAGES {
        let mut buf = pool
            .alloc_layout(PROBE_SIZE)
            .unwrap()
            .alloc()
            .with_policy::<GarbageCollect>()
            .wait()
            .unwrap();
        buf.fill(0xa5);
        assert!(to_receiver.schedule(put(buf.into()).as_mut()).unwrap());
    }
    wait_until("the shared-memory messages", || {
        delivered.counts().1 == MESSAGES
    })
    .await;
    assert_eq!(
        exit_holds_taken() - before,
        MESSAGES,
        "each message that carries a shared-memory buffer must take the exit hold once"
    );
    assert_eq!(
        delivered.counts(),
        (MESSAGES, MESSAGES),
        "every message must be delivered, as what it was sent as"
    );

    ztimeout!(to_receiver.close()).unwrap();
    ztimeout!(sender.close());
    ztimeout!(receiver.close());
}
