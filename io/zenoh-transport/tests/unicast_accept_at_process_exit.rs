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
//! A process that has begun to exit runs no accept handshake (BER-2456).
//!
//! The test runs itself again as a child, in the shape of a FATAL exit: the child listens
//! with shared memory on, admits one transport from this process while it is live, and
//! calls `std::process::exit` with its listener open. `linger` then holds the child in exit
//! after static_init has finalized its statics, and this process dials it again. An accept
//! handshake run then reads the transport's own shared-memory static, finalized by now, and
//! panics. This process reads the child's log.
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
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use zenoh_core::ztimeout;
use zenoh_link::Link;
use zenoh_protocol::{
    core::{EndPoint, WhatAmI, ZenohIdProto},
    network::NetworkMessageMut,
};
use zenoh_result::ZResult;
use zenoh_transport::{
    multicast::TransportMulticast, unicast::TransportUnicast, TransportEventHandler,
    TransportManager, TransportMulticastEventHandler, TransportPeer, TransportPeerEventHandler,
};

const TIMEOUT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(10);
/// Set in the child.
const CHILD: &str = "ZENOH_TEST_ACCEPT_EXIT_CHILD";
const TEST_NAME: &str = "an_exiting_process_runs_no_accept_handshake";
/// How long the child stays in exit after static_init has finalized its statics.
const LINGER: Duration = Duration::from_millis(1500);
/// The child's first log line, an ERROR, so that its log is known to reach this process.
const LOGGING: &str = "CHILD LOGGING";
/// The child's line naming the endpoint it listens on.
const LISTENING: &str = "CHILD LISTENING";
/// The child's line just before it exits.
const EXITING: &str = "CHILD EXITING";
/// The child's line once static_init has finalized its statics, from `linger`.
const FINALIZED: &str = "CHILD FINALIZED";
/// How many times this process dials the child in exit.
const DIALS: u8 = 3;

/// Runs after static_init has finalized its statics, which it does at priority 0, so the
/// child stays in exit with them finalized while its accept threads run.
#[static_init::destructor(1)]
extern "C" fn linger() {
    if std::env::var_os(CHILD).is_some() {
        println!("{FINALIZED}");
        std::thread::sleep(LINGER);
    }
}

struct Peer;

impl TransportPeerEventHandler for Peer {
    fn handle_message(&self, _message: NetworkMessageMut) -> ZResult<()> {
        Ok(())
    }
    fn new_link(&self, _link: Link) {}
    fn del_link(&self, _link: Link) {}
    fn closed(&self) {}
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Counts the transports a manager admits.
struct Handler(Arc<AtomicUsize>);

impl TransportEventHandler for Handler {
    fn new_unicast(
        &self,
        _peer: TransportPeer,
        _transport: TransportUnicast,
    ) -> ZResult<Arc<dyn TransportPeerEventHandler>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(Peer))
    }

    fn new_multicast(
        &self,
        _transport: TransportMulticast,
    ) -> ZResult<Arc<dyn TransportMulticastEventHandler>> {
        panic!("not used")
    }
}

/// A manager with shared memory on, in `lazy` mode, counting what it admits.
fn manager(byte: u8, admitted: Arc<AtomicUsize>) -> TransportManager {
    let mut shm = zenoh_config::ShmConf::default();
    shm.set_enabled(true).unwrap();
    shm.set_mode(zenoh_config::ShmInitMode::Lazy).unwrap();
    TransportManager::builder()
        .whatami(WhatAmI::Peer)
        .zid(ZenohIdProto::try_from([byte]).unwrap())
        .shm(shm)
        .build_test(Arc::new(Handler(admitted)))
        .unwrap()
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}

/// **A PROCESS THAT HAS BEGUN TO EXIT RUNS NO ACCEPT HANDSHAKE.**
#[test]
fn an_exiting_process_runs_no_accept_handshake() {
    if std::env::var_os(CHILD).is_some() {
        child();
    }
    parent();
}

/// Listens, admits one transport, and exits with its listener open.
fn child() -> ! {
    zenoh_util::init_log_from_env_or("error");
    tracing::error!("{LOGGING}: the child's log reaches the parent");
    runtime().block_on(async {
        let admitted = Arc::new(AtomicUsize::new(0));
        let listener = manager(0xc1, admitted.clone());
        let locator =
            ztimeout!(listener.add_listener("tcp/127.0.0.1:0".parse().unwrap())).unwrap();
        println!("{LISTENING} {locator}");
        ztimeout!(async {
            while admitted.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(POLL).await;
            }
        });
        println!(
            "{EXITING}: admitted {} transport(s); exiting with the listener open",
            admitted.load(Ordering::SeqCst)
        );
        std::process::exit(0)
    });
    unreachable!("the child exits from inside its runtime")
}

/// What the child has said so far on stdout.
#[derive(Default)]
struct ChildOut {
    endpoint: Option<EndPoint>,
    finalized: bool,
    lines: String,
}

/// Dials the child while it is live and again once it is in exit, then reads its log.
fn parent() {
    zenoh_util::init_log_from_env_or("error");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST_NAME, "--nocapture", "--test-threads=1"])
        .env(CHILD, "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    // Read the child's two streams as they come, so that neither pipe fills up.
    let out = Arc::new(Mutex::new(ChildOut::default()));
    let stdout_reader = std::thread::spawn({
        let pipe = child.stdout.take().unwrap();
        let out = out.clone();
        move || {
            for line in BufReader::new(pipe).lines() {
                let line = line.unwrap();
                let mut out = out.lock().unwrap();
                if let Some(endpoint) = line.strip_prefix(LISTENING) {
                    out.endpoint = Some(endpoint.trim().parse().unwrap());
                }
                if line.contains(FINALIZED) {
                    out.finalized = true;
                }
                out.lines.push_str(&line);
                out.lines.push('\n');
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

    let (live, late, alive_after) = runtime().block_on(async {
        let endpoint = ztimeout!(async {
            loop {
                if let Some(endpoint) = out.lock().unwrap().endpoint.clone() {
                    break endpoint;
                }
                tokio::time::sleep(POLL).await;
            }
        });

        // THE CONTROL: while the child is live, a dial is admitted, with shared memory.
        let first = manager(0xa1, Arc::new(AtomicUsize::new(0)));
        let live = ztimeout!(first.open_transport_unicast(endpoint.clone()))
            .map(|transport| transport.is_shm());

        ztimeout!(async {
            while !out.lock().unwrap().finalized {
                tokio::time::sleep(POLL).await;
            }
        });
        let mut late = Vec::new();
        for i in 0..DIALS {
            let dialler = manager(0xb0 + i, Arc::new(AtomicUsize::new(0)));
            late.push(
                ztimeout!(dialler.open_transport_unicast(endpoint.clone()))
                    .map(|transport| transport.is_shm()),
            );
            ztimeout!(dialler.close());
        }
        // The dials met the child in exit only if it is still there.
        let alive_after = child.try_wait().unwrap().is_none();
        ztimeout!(first.close());
        (live, late, alive_after)
    });

    let status = child.wait().unwrap();
    stdout_reader.join().unwrap();
    let stdout = out.lock().unwrap().lines.clone();
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
    assert!(
        matches!(live, Ok(Ok(true))),
        "while the child is live a dial must be admitted, with shared memory: {live:?}\n{log}"
    );
    assert!(
        alive_after,
        "the child must still be in exit after the last dial, or no dial met it there\n{log}"
    );
    assert!(
        late.iter().all(Result::is_err),
        "a process in exit must admit no transport: {late:?}\n{log}"
    );
    assert!(
        !stderr.contains("panicked"),
        "a process that has begun to exit must run no accept handshake, which panics on a \
         finalized static\n{log}"
    );
}
