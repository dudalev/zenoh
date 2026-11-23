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
//! A session that scouts in a test's own scouting group finds the router in that group,
//! and no zenoh outside it (BER-2469). Every fork test that scouts relies on this to stay
//! clear of the zenohd the host runs, which scouts the default group.
use std::time::{Duration, Instant};

use zenoh::{config::Config, Wait};
use zenoh_test::{ScoutingGroup, Zenohd};

/// The least time a session outside the router's group is given to find it.
const SETTLE: Duration = Duration::from_secs(3);
const POLL: Duration = Duration::from_millis(50);

/// **A SESSION FINDS THE ROUTER IN ITS SCOUTING GROUP, AND NOTHING OUTSIDE IT.**
#[test]
fn a_session_finds_the_router_in_its_scouting_group_and_nothing_outside_it() {
    let group = ScoutingGroup::new();
    let other = ScoutingGroup::new();
    let router = Zenohd::start(env!("CARGO_BIN_EXE_zenohd"), &group);

    // THE CONTROL: in the router's group, a session finds the router by scouting.
    let started = Instant::now();
    let inside = zenoh::open(group.config::<Config>()).wait().unwrap();
    router.wait_linked(&inside);
    let found_in = started.elapsed();

    // In another group, a session scouts for several times as long and finds nothing: not
    // the router, not the session in the router's group, and no zenoh the host runs.
    let outside = zenoh::open(other.config::<Config>()).wait().unwrap();
    let deadline = Instant::now() + SETTLE.max(found_in * 5);
    while Instant::now() < deadline {
        let routers: Vec<_> = outside.info().routers_zid().wait().collect();
        let peers: Vec<_> = outside.info().peers_zid().wait().collect();
        assert!(
            routers.is_empty() && peers.is_empty(),
            "a session in a scouting group of its own must find nothing outside it: routers \
             {routers:?}, peers {peers:?}; the router {} was found inside in {found_in:?}",
            router.zid()
        );
        std::thread::sleep(POLL);
    }

    outside.close().wait().unwrap();
    inside.close().wait().unwrap();
}
