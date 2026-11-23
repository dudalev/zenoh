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
//! A transport manager that has begun to close admits no new unicast transport
//! (BER-2230), whichever side began it: a link it dials, or one whose accept was already
//! in flight when it began to close. Each case has an open-manager control.
//!
//! `TransportManager::close` drains its transport table before closing what it drained,
//! so a transport admitted after the drain would never be closed and would outlive
//! `close`.
//!
//! The peer whose dial is refused is healthy, so it logs no ERROR. What it logs is read
//! from its own span: every test in this binary logs through [`init_log`].
#![cfg(all(feature = "test", feature = "transport_tcp"))]

use std::{
    any::Any,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
};
use tracing::Instrument;
use zenoh_core::ztimeout;
use zenoh_link::Link;
use zenoh_protocol::{
    core::{EndPoint, WhatAmI, ZenohIdProto},
    network::NetworkMessageMut,
};
use zenoh_result::ZResult;
use zenoh_test::get_free_tcp_port;
use zenoh_transport::{
    multicast::TransportMulticast, unicast::TransportUnicast, TransportEventHandler,
    TransportManager, TransportMulticastEventHandler, TransportPeer, TransportPeerEventHandler,
};

const TIMEOUT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(10);
/// The span a dialling peer's handshake runs in, with a `test` field naming its test.
const DIALLER: &str = "dialler";
/// Logged at ERROR in the dialler's span, so that its log is known to reach the test.
const PROBE: &str = "the dialler's log reaches the test";

/// Every WARN and ERROR event of this binary, each with the fields of its spans.
static LOGGED: Mutex<Vec<zenoh_util::LogRecord>> = Mutex::new(Vec::new());

/// Logs WARN and above to stderr and into [`LOGGED`].
fn init_log() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        zenoh_util::init_log_with_callback(
            |meta| {
                if meta.is_span() {
                    meta.name() == DIALLER
                } else {
                    *meta.level() <= tracing::Level::WARN
                }
            },
            |record| {
                eprintln!(
                    "{} {}: {}",
                    record.level,
                    record.target,
                    record.message.as_deref().unwrap_or_default()
                );
                LOGGED
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(record);
            },
        );
    });
}

/// The ERROR events logged in the span of the dialler whose `test` field is `test`.
fn dialler_errors(test: u8) -> Vec<String> {
    LOGGED
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter(|record| record.level == tracing::Level::ERROR)
        .filter(|record| {
            record
                .attributes
                .iter()
                .any(|(name, value)| *name == "test" && *value == test.to_string())
        })
        .map(|record| record.message.clone().unwrap_or_default())
        .collect()
}

/// Counts the transports a manager admitted: `new_unicast` is how one is announced.
#[derive(Default)]
struct Admissions(AtomicUsize);

impl Admissions {
    fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

struct Handler(Arc<Admissions>);

impl TransportEventHandler for Handler {
    fn new_unicast(
        &self,
        _peer: TransportPeer,
        _transport: TransportUnicast,
    ) -> ZResult<Arc<dyn TransportPeerEventHandler>> {
        self.0 .0.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(Peer))
    }

    fn new_multicast(
        &self,
        _transport: TransportMulticast,
    ) -> ZResult<Arc<dyn TransportMulticastEventHandler>> {
        panic!("not used")
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

fn manager(byte: u8) -> (TransportManager, Arc<Admissions>) {
    let admissions = Arc::new(Admissions::default());
    let manager = TransportManager::builder()
        .whatami(WhatAmI::Peer)
        .zid(zid(byte))
        .unicast(TransportManager::config_unicast())
        .build_test(Arc::new(Handler(admissions.clone())))
        .unwrap();
    (manager, admissions)
}

fn zid(byte: u8) -> ZenohIdProto {
    ZenohIdProto::try_from([byte]).unwrap()
}

fn endpoint(port: u16) -> EndPoint {
    format!("tcp/127.0.0.1:{port}").parse().unwrap()
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

/// A TCP proxy in front of an acceptor that holds the opener's handshake half way.
///
/// The opener's first batch, its InitSyn, goes through at once; its second, the OpenSyn,
/// is held until `release`. The acceptor's accept is therefore in flight, waiting for the
/// OpenSyn, for as long as the test likes.
struct HeldHandshake {
    addr: SocketAddr,
    holding: Arc<AtomicBool>,
    release: Arc<Notify>,
}

impl HeldHandshake {
    async fn start(acceptor: u16) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let holding = Arc::new(AtomicBool::new(false));
        let release = Arc::new(Notify::new());
        let (c_holding, c_release) = (holding.clone(), release.clone());
        tokio::spawn(async move {
            let (opener, _) = listener.accept().await.unwrap();
            let acceptor = TcpStream::connect(("127.0.0.1", acceptor)).await.unwrap();
            let (mut from_opener, mut to_opener) = opener.into_split();
            let (mut from_acceptor, mut to_acceptor) = acceptor.into_split();
            tokio::spawn(async move {
                let _ = tokio::io::copy(&mut from_acceptor, &mut to_opener).await;
            });
            let init_syn = read_batch(&mut from_opener).await;
            to_acceptor.write_all(&init_syn).await.unwrap();
            let open_syn = read_batch(&mut from_opener).await;
            c_holding.store(true, Ordering::SeqCst);
            c_release.notified().await;
            // The acceptor may have gone by now, which is the closing case's point.
            if to_acceptor.write_all(&open_syn).await.is_ok() {
                let _ = tokio::io::copy(&mut from_opener, &mut to_acceptor).await;
            }
        });
        Self {
            addr,
            holding,
            release,
        }
    }

    fn endpoint(&self) -> EndPoint {
        format!("tcp/{}", self.addr).parse().unwrap()
    }

    fn holding(&self) -> bool {
        self.holding.load(Ordering::SeqCst)
    }

    fn release(&self) {
        self.release.notify_one();
    }
}

/// One batch of a streamed link, with its two-byte little-endian length prefix.
async fn read_batch(from: &mut tokio::net::tcp::OwnedReadHalf) -> Vec<u8> {
    let mut len = [0_u8; 2];
    from.read_exact(&mut len).await.unwrap();
    let mut batch = vec![0_u8; u16::from_le_bytes(len) as usize];
    from.read_exact(&mut batch).await.unwrap();
    [len.to_vec(), batch].concat()
}

/// A link the manager dials itself, as a re-dial during close would.
async fn dial(close_first: bool, first_zid: u8) {
    init_log();
    let port = get_free_tcp_port();
    let (peer, _) = manager(first_zid);
    ztimeout!(peer.add_listener(endpoint(port))).unwrap();
    let (dialer, dialer_admissions) = manager(first_zid + 1);

    if close_first {
        ztimeout!(dialer.close());
    }
    let dialled = ztimeout!(dialer.open_transport_unicast(endpoint(port)));

    if close_first {
        assert!(
            dialled.is_err(),
            "a closed manager must refuse the transport it dials"
        );
        assert!(ztimeout!(dialer.get_transport_unicast(&zid(first_zid))).is_none());
        assert_eq!(dialer_admissions.count(), 0, "nothing may be admitted");
    } else {
        // THE CONTROL: open, the same dial is admitted.
        assert!(dialled.is_ok(), "an open manager must admit what it dials");
        assert!(ztimeout!(dialer.get_transport_unicast(&zid(first_zid))).is_some());
        assert_eq!(dialer_admissions.count(), 1);
        ztimeout!(dialer.close());
    }
    ztimeout!(peer.close());
}

/// A link whose accept was in flight when the acceptor began to close.
async fn held_accept(close: bool, first_zid: u8) {
    init_log();
    let port = get_free_tcp_port();
    let (acceptor, acceptor_admissions) = manager(first_zid);
    ztimeout!(acceptor.add_listener(endpoint(port))).unwrap();
    let proxy = HeldHandshake::start(port).await;
    let (opener, _) = manager(first_zid + 1);

    let proxy_endpoint = proxy.endpoint();
    let opening = tokio::spawn({
        let opener = opener.clone();
        async move {
            tracing::error!("{PROBE}");
            opener.open_transport_unicast(proxy_endpoint).await
        }
        .instrument(tracing::error_span!(DIALLER, test = first_zid))
    });
    wait_until("the acceptor to wait for the OpenSyn", || proxy.holding()).await;

    let closing = if close {
        let closing = tokio::spawn({
            let acceptor = acceptor.clone();
            async move { acceptor.close().await }
        });
        // `close` drains the listeners after it sets its closed flag, so an empty list
        // means the accept is now finishing on a closed manager.
        ztimeout!(async {
            while !acceptor.get_listeners().await.is_empty() {
                tokio::time::sleep(POLL).await;
            }
        });
        Some(closing)
    } else {
        None
    };

    proxy.release();
    let opened = ztimeout!(opening).unwrap();
    let logged = dialler_errors(first_zid);
    assert!(
        logged.iter().any(|message| message == PROBE),
        "the dialler's log must reach this test, or the absence below proves nothing: \
         {logged:?}"
    );
    assert!(
        logged.iter().all(|message| message == PROBE),
        "the dialler is healthy and must log no ERROR, refused or not: {logged:?}; it was \
         told {opened:?}"
    );

    if let Some(closing) = closing {
        ztimeout!(closing).unwrap();
        assert!(
            opened.is_err(),
            "the opener must not get a transport the acceptor refused"
        );
        assert!(ztimeout!(acceptor.get_transport_unicast(&zid(first_zid + 1))).is_none());
        assert_eq!(
            acceptor_admissions.count(),
            0,
            "a closed manager must admit nothing, even an accept already in flight"
        );
    } else {
        // THE CONTROL: open, the same held accept is admitted once released.
        assert!(opened.is_ok(), "an open manager must admit a held accept");
        wait_until("the acceptor to admit the transport", || {
            acceptor_admissions.count() == 1
        })
        .await;
        assert!(ztimeout!(acceptor.get_transport_unicast(&zid(first_zid + 1))).is_some());
        ztimeout!(acceptor.close());
    }
    ztimeout!(opener.close());
}

/// **A CLOSED MANAGER ADMITS NO TRANSPORT IT DIALS.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_closed_manager_admits_no_transport_it_dials() {
    dial(true, 0x11).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_manager_admits_the_transport_it_dials() {
    dial(false, 0x13).await;
}

/// **A CLOSING MANAGER ADMITS NO TRANSPORT WHOSE ACCEPT WAS ALREADY IN FLIGHT.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_closing_manager_admits_no_transport_whose_accept_was_in_flight() {
    held_accept(true, 0x21).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_manager_admits_a_transport_whose_accept_was_held() {
    held_accept(false, 0x23).await;
}
