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
//! A query is not routed to a peer that has closed (BER-2457).
//!
//! A peer routes every query to a peer whose answer to its initial interest has not come
//! yet, and caches that route. The test's other peer is a bare transport that never
//! answers, so the session routes its query there. When that peer closes, the query it
//! held must finish at once, and so must the next query, answered by the session's own
//! queryable alone: neither may wait out its timeout on the closed peer.
//!
//! No router: the defect is the peer's own routing, so the session's scouting group is one
//! the test has to itself, with nothing else in it.
#![cfg(feature = "transport_tcp")]

use std::{
    any::Any,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use zenoh::{config::Config, Wait};
use zenoh_core::ztimeout;
use zenoh_link::Link;
use zenoh_protocol::{
    core::{WhatAmI, ZenohIdProto},
    network::{NetworkBodyMut, NetworkMessageMut},
};
use zenoh_result::ZResult;
use zenoh_test::{get_free_tcp_port, ScoutingGroup};
use zenoh_transport::{
    multicast::TransportMulticast, unicast::TransportUnicast, TransportEventHandler,
    TransportManager, TransportMulticastEventHandler, TransportPeer, TransportPeerEventHandler,
};

const TIMEOUT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(10);
/// Each query's own timeout: long enough that a query answered at once is told apart from
/// one that waited for it.
const QUERY_TIMEOUT: Duration = Duration::from_secs(5);
const KEY: &str = "test/query-after-peer-close";

/// Counts the queries a peer receives, and answers nothing.
struct Silent(Arc<AtomicUsize>);

impl TransportEventHandler for Silent {
    fn new_unicast(
        &self,
        _peer: TransportPeer,
        _transport: TransportUnicast,
    ) -> ZResult<Arc<dyn TransportPeerEventHandler>> {
        Ok(Arc::new(Silent(self.0.clone())))
    }

    fn new_multicast(
        &self,
        _transport: TransportMulticast,
    ) -> ZResult<Arc<dyn TransportMulticastEventHandler>> {
        panic!("not used")
    }
}

impl TransportPeerEventHandler for Silent {
    fn handle_message(&self, message: NetworkMessageMut) -> ZResult<()> {
        if let NetworkBodyMut::Request(_) = message.body {
            self.0.fetch_add(1, Ordering::SeqCst);
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

/// Drains a query's replies: `(answers, error payloads)`, and how long the query had run when
/// its last reply came, from `started`.
async fn outcome(
    replies: zenoh::handlers::FifoChannelHandler<zenoh::query::Reply>,
    started: Instant,
) -> ((usize, Vec<String>), Duration) {
    let (mut answered, mut failed) = (0, Vec::new());
    while let Ok(reply) = ztimeout!(replies.recv_async()) {
        match reply.result() {
            Ok(_) => answered += 1,
            Err(e) => failed.push(e.payload().try_to_string().unwrap().to_string()),
        }
    }
    ((answered, failed), started.elapsed())
}

async fn linked(session: &zenoh::Session, peer: ZenohIdProto) -> bool {
    session
        .info()
        .peers_zid()
        .await
        .any(|zid| zid.to_string() == peer.to_string())
}

/// **A QUERY IS NOT ROUTED TO A PEER THAT HAS CLOSED.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_query_is_not_routed_to_a_peer_that_has_closed() {
    zenoh_util::init_log_from_env_or("error");
    let endpoint = format!("tcp/127.0.0.1:{}", get_free_tcp_port());

    let silent_zid = ZenohIdProto::try_from([0x57]).unwrap();
    let received = Arc::new(AtomicUsize::new(0));
    let silent = TransportManager::builder()
        .whatami(WhatAmI::Peer)
        .zid(silent_zid)
        .build_test(Arc::new(Silent(received.clone())))
        .unwrap();
    ztimeout!(silent.add_listener(endpoint.parse().unwrap())).unwrap();

    // A peer session that knows nothing but the silent peer.
    let group = ScoutingGroup::new();
    let mut config: Config = group.config();
    config
        .insert_json5("listen/endpoints", "[\"tcp/127.0.0.1:0\"]")
        .unwrap();
    config
        .insert_json5("connect/endpoints", &format!("[\"{endpoint}\"]"))
        .unwrap();
    let session = ztimeout!(zenoh::open(config)).unwrap();
    ztimeout!(async {
        while !linked(&session, silent_zid).await {
            tokio::time::sleep(POLL).await;
        }
    });

    let _queryable = ztimeout!(session.declare_queryable(KEY).callback(|query| {
        query
            .reply(query.key_expr().clone(), "local")
            .wait()
            .unwrap();
    }))
    .unwrap();

    // PRECONDITION: while the silent peer is open, a query is routed to it too.
    let held_since = Instant::now();
    let held = ztimeout!(session.get(KEY).timeout(QUERY_TIMEOUT)).unwrap();
    ztimeout!(async {
        while received.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(POLL).await;
        }
    });

    ztimeout!(silent.close());
    ztimeout!(async {
        while linked(&session, silent_zid).await {
            tokio::time::sleep(POLL).await;
        }
    });

    let (held, held_took) = outcome(held, held_since).await;
    assert_eq!(
        held,
        (1, Vec::<String>::new()),
        "the query the closed peer held must finish when it closes, answered only by the \
         session's own queryable, after {held_took:?}"
    );
    assert!(
        held_took < QUERY_TIMEOUT / 2,
        "the query the closed peer held must not wait out its timeout: it took {held_took:?}"
    );

    let (next, next_took) = outcome(
        ztimeout!(session.get(KEY).timeout(QUERY_TIMEOUT)).unwrap(),
        Instant::now(),
    )
    .await;
    assert_eq!(
        next,
        (1, Vec::<String>::new()),
        "the next query must be answered by the session's own queryable alone, and nothing \
         may fail, after {next_took:?}"
    );
    assert!(
        next_took < QUERY_TIMEOUT / 2,
        "a query must not wait on a peer that has closed: it took {next_took:?}"
    );

    ztimeout!(session.close()).unwrap();
}
