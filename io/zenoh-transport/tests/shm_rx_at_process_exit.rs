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
//! A process that exits with its transport manager still open maps no shared-memory buffer
//! once exit has begun, so it reports nothing as discarded (BER-2456).
//!
//! The test runs itself again as a child, in the shape of a FATAL exit: the child receives
//! a stream of shared-memory messages from this process and, while they keep coming, calls
//! `std::process::exit` without closing anything. `linger` then holds the child in exit
//! after zenoh-shm's statics are finalized, as a slow exit does, so a map that ran then
//! would fail and be reported. This process reads the child's log.
#![cfg(all(
    unix,
    feature = "shared-memory",
    feature = "test",
    feature = "transport_tcp"
))]

use std::{
    any::Any,
    io::{BufRead, BufReader, Read},
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
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
use zenoh_test::get_free_tcp_port;
use zenoh_transport::{
    multicast::TransportMulticast, unicast::TransportUnicast, DummyTransportPeerEventHandler,
    TransportEventHandler, TransportManager, TransportMulticastEventHandler, TransportPeer,
    TransportPeerEventHandler,
};

const TIMEOUT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(10);
/// Set in the child, to the endpoint it connects to.
const CHILD: &str = "ZENOH_TEST_SHM_EXIT_CHILD";
const TEST_NAME: &str = "an_exiting_process_maps_nothing_and_reports_no_discard";
/// How long the child stays in exit after zenoh-shm's statics are finalized.
const LINGER: Duration = Duration::from_millis(500);
/// The shared-memory messages the child maps before it exits: the control that maps work.
const MAPPED_BEFORE_EXIT: usize = 20;
/// The child's log line just before it exits.
const EXITING: &str = "CHILD EXITING";
/// The child's first log line, an ERROR, so that its log is known to reach this process.
const LOGGING: &str = "CHILD LOGGING";
const POOL_SIZE: usize = 1024 * 1024;
const PROBE_SIZE: usize = 64;

/// Runs after static_init has finalized its statics, which it does at priority 0, so the
/// child stays in exit with zenoh-shm finalized while its receive threads run.
#[static_init::destructor(1)]
extern "C" fn linger() {
    if std::env::var_os(CHILD).is_some() {
        std::thread::sleep(LINGER);
    }
}

/// Counts the shared-memory messages a peer delivers.
struct Counting(Arc<AtomicUsize>);

impl TransportPeerEventHandler for Counting {
    fn handle_message(&self, message: NetworkMessageMut) -> ZResult<()> {
        if let NetworkBodyMut::Push(push) = message.body {
            if let PushBody::Put(Put { payload, .. }) = &push.payload {
                if payload
                    .zslices()
                    .any(|zs| zs.downcast_ref::<ShmBufInner>().is_some())
                {
                    self.0.fetch_add(1, Ordering::SeqCst);
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

struct ReceiverHandler(Arc<AtomicUsize>);

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

/// Shared memory on, in `lazy` mode.
fn shm_conf() -> zenoh_config::ShmConf {
    let mut shm = zenoh_config::ShmConf::default();
    shm.set_enabled(true).unwrap();
    shm.set_mode(zenoh_config::ShmInitMode::Lazy).unwrap();
    shm
}

fn manager(byte: u8, handler: Arc<dyn TransportEventHandler>) -> TransportManager {
    TransportManager::builder()
        .whatami(WhatAmI::Peer)
        .zid(ZenohIdProto::try_from([byte]).unwrap())
        .shm(shm_conf())
        .build_test(handler)
        .unwrap()
}

fn put(payload: ZBuf) -> NetworkMessage {
    NetworkMessage::from(Push {
        wire_expr: "test/shm-rx-at-process-exit".into(),
        ext_qos: QoSType::new(Priority::DEFAULT, CongestionControl::Block, false),
        ..Push::from(Put {
            payload,
            ..Put::default()
        })
    })
}

fn pool() -> ShmProvider<PosixShmProviderBackendBinaryHeap> {
    let backend = PosixShmProviderBackendBinaryHeap::builder(POOL_SIZE)
        .wait()
        .unwrap();
    ShmProviderBuilder::backend(backend).wait()
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}

/// **A PROCESS THAT HAS BEGUN TO EXIT MAPS NOTHING AND REPORTS NO DISCARD.**
#[test]
fn an_exiting_process_maps_nothing_and_reports_no_discard() {
    if let Some(endpoint) = std::env::var_os(CHILD) {
        child(endpoint.into_string().unwrap());
    }
    parent();
}

/// Connects to the parent, maps `MAPPED_BEFORE_EXIT` of its messages, and exits with the
/// transport open.
fn child(endpoint: String) -> ! {
    zenoh_util::init_log_from_env_or("error");
    tracing::error!("{LOGGING}: the child's log reaches the parent");
    runtime().block_on(async {
        let mapped = Arc::new(AtomicUsize::new(0));
        let receiver = manager(0xe2, Arc::new(ReceiverHandler(mapped.clone())));
        let to_parent =
            ztimeout!(receiver.open_transport_unicast(endpoint.parse().unwrap())).unwrap();
        assert!(
            to_parent.is_shm().unwrap(),
            "shm must negotiate, or nothing here is about mapping"
        );
        ztimeout!(async {
            while mapped.load(Ordering::SeqCst) < MAPPED_BEFORE_EXIT {
                tokio::time::sleep(POLL).await;
            }
        });
        // The same stream as zenoh's log, so that the parent can tell what came after.
        println!(
            "{EXITING}: mapped {} shared-memory messages; exiting with the transport open",
            mapped.load(Ordering::SeqCst)
        );
        std::process::exit(0)
    });
    unreachable!("the child exits from inside its runtime")
}

/// Streams shared-memory messages to the child until it has exited, then reads its log.
fn parent() {
    runtime().block_on(async {
        zenoh_util::init_log_from_env_or("error");
        let endpoint: EndPoint = format!("tcp/127.0.0.1:{}", get_free_tcp_port())
            .parse()
            .unwrap();
        let sender = manager(0xe1, Arc::new(SenderHandler));
        ztimeout!(sender.add_listener(endpoint.clone())).unwrap();

        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST_NAME, "--nocapture", "--test-threads=1"])
            .env(CHILD, endpoint.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        // Read the child's two streams as they come, so that neither pipe fills up, and
        // note when it says it is exiting.
        let exiting = Arc::new(AtomicBool::new(false));
        let stdout = Arc::new(Mutex::new(String::new()));
        let stdout_reader = std::thread::spawn({
            let pipe = child.stdout.take().unwrap();
            let (exiting, stdout) = (exiting.clone(), stdout.clone());
            move || {
                for line in BufReader::new(pipe).lines() {
                    let line = line.unwrap();
                    if line.contains(EXITING) {
                        exiting.store(true, Ordering::SeqCst);
                    }
                    let mut stdout = stdout.lock().unwrap();
                    stdout.push_str(&line);
                    stdout.push('\n');
                }
            }
        });
        let stderr_reader = std::thread::spawn({
            let mut pipe = child.stderr.take().unwrap();
            move || {
                let mut stderr = String::new();
                pipe.read_to_string(&mut stderr).unwrap();
                stderr
            }
        });

        let to_child = ztimeout!(async {
            loop {
                if let Some(transport) = sender.get_transports_unicast().await.pop() {
                    break transport;
                }
                tokio::time::sleep(POLL).await;
            }
        });

        let pool = pool();
        let mut sent_while_exiting = 0_usize;
        let status = ztimeout!(async {
            loop {
                if let Some(status) = child.try_wait().unwrap() {
                    break status;
                }
                let exiting_now = exiting.load(Ordering::SeqCst);
                if let Ok(mut buf) = pool
                    .alloc_layout(PROBE_SIZE)
                    .unwrap()
                    .alloc()
                    .with_policy::<GarbageCollect>()
                    .wait()
                {
                    buf.fill(0xa5);
                    if matches!(to_child.schedule(put(buf.into()).as_mut()), Ok(true))
                        && exiting_now
                    {
                        sent_while_exiting += 1;
                    }
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });
        stdout_reader.join().unwrap();
        let stdout = stdout.lock().unwrap().clone();
        let stderr = stderr_reader.join().unwrap();
        let log = format!("child stdout:\n{stdout}\nchild stderr:\n{stderr}");

        assert!(
            status.success(),
            "the child must exit as it chose to: {status}\n{log}"
        );
        assert!(
            stdout.contains(LOGGING),
            "the child's log must reach this process, or the absence below proves nothing\n{log}"
        );
        let (_, after) = stdout
            .split_once(EXITING)
            .unwrap_or_else(|| panic!("the child must map messages and then exit\n{log}"));
        assert!(
            sent_while_exiting > 0,
            "messages must still be arriving while the child exits\n{log}"
        );
        assert!(
            !after.contains("DISCARDED"),
            "a process that has begun to exit must map nothing, and so report no discarded \
             message\n{log}"
        );

        ztimeout!(sender.close());
    });
}
