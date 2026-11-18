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
//! End-to-end cover for eager SHM setup (BER-2169, BER-2170, BER-2173).
//!
//! Every other test of this work asserts a config value, a codec's bytes or a
//! pure predicate. None of them establishes a link, so none could catch the
//! handshake advertising nothing, the pre-mount never running, or the lease
//! releasing at the wrong moment.
#![cfg(all(feature = "shared-memory", feature = "test"))]
use std::{sync::Arc, time::Duration};

use zenoh_core::ztimeout;
use zenoh_link::EndPoint;
use zenoh_protocol::core::{WhatAmI, ZenohIdProto};
use zenoh_result::ZResult;
use zenoh_shm::{api::client_storage::ShmClientStorage, reader::ShmReader};
use zenoh_transport::{
    multicast::TransportMulticast,
    unicast::{test_helpers::make_transport_manager_builder, TransportUnicast},
    DummyTransportPeerEventHandler, TransportEventHandler, TransportManager,
    TransportMulticastEventHandler, TransportPeer, TransportPeerEventHandler,
};

const TIMEOUT: Duration = Duration::from_secs(60);
const SLEEP: Duration = Duration::from_millis(100);

struct Handler;

impl TransportEventHandler for Handler {
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

/// A manager with shared memory on, in the requested init mode, reading through
/// a storage the test keeps a handle on.
fn manager(
    zid: ZenohIdProto,
    mode: zenoh_config::ShmInitMode,
) -> (TransportManager, Arc<ShmClientStorage>) {
    let storage = Arc::new(
        ShmClientStorage::builder()
            .with_default_client_set()
            .build(),
    );

    let mut shm = zenoh_config::ShmConf::default();
    shm.set_enabled(true).unwrap();
    shm.set_mode(mode).unwrap();
    // A SMALL POOL, because the default is 16 MiB and the owner mlocks it: on
    // any host whose RLIMIT_MEMLOCK is below that -- every CI and dev box -- the
    // eager build fails with ENOMEM, the provider lands in `Error`, and there
    // is no pool id to advertise. The test would then pass or fail for a reason
    // that has nothing to do with the handshake.
    shm.transport_optimization
        .set_pool_size(std::num::NonZeroUsize::new(64 * 1024).unwrap())
        .unwrap();

    let manager = TransportManager::builder()
        .whatami(WhatAmI::Peer)
        .zid(zid)
        .shm(shm)
        .shm_reader(Some(ShmReader::new(storage.clone())))
        .unicast(make_transport_manager_builder(
            #[cfg(feature = "transport_multilink")]
            1,
            false,
        ))
        .build_test(Arc::new(Handler))
        .unwrap();

    (manager, storage)
}

/// **IN `init` MODE A PEER'S POOL IS MOUNTED AT ESTABLISHMENT, BEFORE ANY
/// MESSAGE REFERENCES IT -- AND RELEASED WHEN THE LINK CLOSES.**
///
/// The three properties this covers, none of which any other test can see:
///
/// 1. the handshake ADVERTISES a pool id at all (an empty advertisement would
///    leave the count at zero and look like success);
/// 2. the receiver MOUNTS it before a single message has flowed;
/// 3. closing the link RELEASES it, which is the regression that made the
///    pre-mount worse than the lazy behaviour it replaced.
///
/// Asserted on the kernel-visible mount count rather than on throughput,
/// because a working data path proves only that SOMETHING mapped the pool --
/// which is equally true of the lazy build.
#[test]
fn eager_mode_mounts_the_peers_pool_at_establishment_and_releases_it_on_close() {
    zenoh_util::init_log_from_env_or("error");
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let endpoint: EndPoint = "tcp/127.0.0.1:17451".parse().unwrap();

        let (listener, listener_storage) = manager(
            ZenohIdProto::try_from([1]).unwrap(),
            zenoh_config::ShmInitMode::Init,
        );
        let (opener, opener_storage) = manager(
            ZenohIdProto::try_from([2]).unwrap(),
            zenoh_config::ShmInitMode::Init,
        );

        // NOTHING is mounted before a link exists. Without this the assertions
        // below could pass against a build that mounts at startup for some
        // other reason.
        assert_eq!(listener_storage.mounted_segment_count(), 0);
        assert_eq!(opener_storage.mounted_segment_count(), 0);

        ztimeout!(listener.add_listener(endpoint.clone())).unwrap();
        let transport = ztimeout!(opener.open_transport_unicast(endpoint.clone())).unwrap();

        // Give the acceptor's side of the handshake a moment to finish; the
        // opener's call returns once ITS state machine is done.
        tokio::time::sleep(SLEEP).await;

        // PRECONDITION, not the subject: if shm did not negotiate at all then
        // nothing below is about the pre-mount.
        assert!(
            transport.is_shm().unwrap(),
            "shm must negotiate, or this test is not exercising the pre-mount"
        );

        // THE POINT: both sides hold the other's pool, and not one message has
        // been sent.
        assert_eq!(
            opener_storage.mounted_segment_count(),
            1,
            "the opener must mount the acceptor's pool from InitAck, before any message"
        );
        assert_eq!(
            listener_storage.mounted_segment_count(),
            1,
            "the acceptor must mount the opener's pool once the challenge is answered"
        );

        // And the lease is released with the link (BER-2170).
        ztimeout!(transport.close()).unwrap();
        tokio::time::sleep(SLEEP).await;

        assert_eq!(
            opener_storage.mounted_segment_count(),
            0,
            "closing the link must release the pre-mounted pool, or a process accumulates a \
             mapping and an flock per peer it has ever met"
        );
        assert_eq!(
            listener_storage.mounted_segment_count(),
            0,
            "the acceptor must release it too"
        );

        ztimeout!(listener.close());
        ztimeout!(opener.close());
    });
}

/// **IN `lazy` MODE NOTHING IS MOUNTED AT ESTABLISHMENT.**
///
/// The control for the test above. Without it, a build that mounted peer pools
/// for some unrelated reason -- or one where the count means something other
/// than a pre-mount -- would satisfy every assertion there.
#[test]
fn lazy_mode_mounts_nothing_at_establishment() {
    zenoh_util::init_log_from_env_or("error");
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let endpoint: EndPoint = "tcp/127.0.0.1:17452".parse().unwrap();

        let (listener, listener_storage) = manager(
            ZenohIdProto::try_from([3]).unwrap(),
            zenoh_config::ShmInitMode::Lazy,
        );
        let (opener, opener_storage) = manager(
            ZenohIdProto::try_from([4]).unwrap(),
            zenoh_config::ShmInitMode::Lazy,
        );

        ztimeout!(listener.add_listener(endpoint.clone())).unwrap();
        let transport = ztimeout!(opener.open_transport_unicast(endpoint.clone())).unwrap();
        tokio::time::sleep(SLEEP).await;

        // In lazy mode the provider does not exist at establishment, so there
        // is no pool id to advertise and nothing to mount. The link is
        // established all the same.
        assert_eq!(
            opener_storage.mounted_segment_count(),
            0,
            "lazy mode must not pre-mount"
        );
        assert_eq!(
            listener_storage.mounted_segment_count(),
            0,
            "lazy mode must not pre-mount"
        );

        ztimeout!(transport.close()).unwrap();
        ztimeout!(listener.close());
        ztimeout!(opener.close());
    });
}
