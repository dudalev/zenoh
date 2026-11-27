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
//! A put dropped for congestion reaches the drop observers of both peers (BER-2554).
//!
//! Two peers, no router, over TCP on loopback with explicit endpoints. The subscriber's
//! callback holds its receive thread until the test releases it, so the subscriber reads
//! nothing, the publisher's link backs up, and the publisher's droppable puts are dropped. The
//! publisher's push drop observer names the subscriber and the key. Once the callback is
//! released, a blocking put after the drops arrives behind the gap the drops left, and the
//! subscriber's rx loss observer reports it. A subscriber that keeps up reaches neither.
//!
//! The observers are installed once per process, so each test reads the events of its own
//! sessions only.
#![cfg(all(feature = "unstable", feature = "transport_tcp"))]
// The lint set of the project that carries this fork, as in `expr_id_rollover.rs`.
#![deny(warnings)]
#![deny(missing_docs)]
#![deny(clippy::all)]
#![deny(clippy::too_many_lines)]
#![deny(clippy::equatable_if_let)]
#![deny(clippy::cognitive_complexity)]
#![deny(clippy::unnested_or_patterns)]

use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Condvar, Mutex, Once,
    },
    time::{Duration, Instant},
};

use zenoh::{
    drop_observer::{self, PushDropReason, RxLoss, RxLossKind},
    pubsub::Publisher,
    qos::{CongestionControl, Priority, Reliability},
    session::ZenohId,
};
use zenoh_core::ztimeout;
use zenoh_test::TestSessions;

const TIMEOUT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(10);
const PAYLOAD_SIZE: usize = 1024;
const MARKER: &[u8] = b"marker";
/// Puts the subscriber that keeps up is sent, one at a time.
const IN_STEP: usize = 200;

/// A push drop as the observer saw it, with its key owned.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PushDropRecord {
    zid: ZenohId,
    key_expr: Option<String>,
    priority: Option<Priority>,
    reason: PushDropReason,
}

static PUSH_DROPS: Mutex<Vec<PushDropRecord>> = Mutex::new(Vec::new());
static RX_LOSSES: Mutex<Vec<RxLoss>> = Mutex::new(Vec::new());

fn install_observers() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        drop_observer::set_push_drop_observer(|dropped| {
            PUSH_DROPS.lock().unwrap().push(PushDropRecord {
                zid: dropped.zid,
                key_expr: dropped.key_expr.map(str::to_owned),
                priority: dropped.priority,
                reason: dropped.reason,
            });
        })
        .unwrap();
        drop_observer::set_rx_loss_observer(|loss| RX_LOSSES.lock().unwrap().push(*loss)).unwrap();
    });
}

/// The pushes dropped on the way to `zid`.
fn push_drops_to(zid: ZenohId) -> Vec<PushDropRecord> {
    PUSH_DROPS
        .lock()
        .unwrap()
        .iter()
        .filter(|dropped| dropped.zid == zid)
        .cloned()
        .collect()
}

/// The losses detected in what `zid` sent.
fn rx_losses_from(zid: ZenohId) -> Vec<RxLoss> {
    RX_LOSSES
        .lock()
        .unwrap()
        .iter()
        .filter(|loss| loss.zid == zid)
        .copied()
        .collect()
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

async fn wait_until(what: &str, condition: impl Fn() -> bool) {
    let start = Instant::now();
    while !condition() {
        assert!(start.elapsed() < TIMEOUT, "timed out waiting until {what}");
        tokio::time::sleep(POLL).await;
    }
}

async fn wait_matching(publisher: &Publisher<'_>) {
    let start = Instant::now();
    while !ztimeout!(publisher.matching_status()).unwrap().matching() {
        assert!(
            start.elapsed() < TIMEOUT,
            "the publisher found no subscriber"
        );
        tokio::time::sleep(POLL).await;
    }
}

/// A put dropped because the subscriber reads nothing is reported by the publisher, naming the
/// subscriber and the key, and by the subscriber once it reads again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_put_reaches_both_observers() {
    zenoh::init_log_from_env_or("error");
    install_observers();
    const KEY: &str = "test/drop_observer/held";
    let mut sessions = TestSessions::new();
    let (publishing, subscribing) = sessions.open_pairs().await;

    let gate = Arc::new(Gate::default());
    let marker_received = Arc::new(AtomicBool::new(false));
    let _subscriber = ztimeout!(subscribing.declare_subscriber(KEY).callback({
        let gate = gate.clone();
        let marker_received = marker_received.clone();
        move |sample| {
            if sample.payload().to_bytes().as_ref() == MARKER {
                marker_received.store(true, Ordering::SeqCst);
            }
            gate.wait();
        }
    }))
    .unwrap();
    let publisher = ztimeout!(publishing
        .declare_publisher(KEY)
        .congestion_control(CongestionControl::Drop)
        .reliability(Reliability::Reliable))
    .unwrap();
    wait_matching(&publisher).await;

    // The first put holds the subscriber's receive thread, and the puts after it fill the
    // link until one is dropped.
    let start = Instant::now();
    let mut puts = 0;
    while push_drops_to(subscribing.zid()).is_empty() {
        assert!(start.elapsed() < TIMEOUT, "no put dropped in {puts}");
        ztimeout!(publisher.put(vec![0_u8; PAYLOAD_SIZE])).unwrap();
        puts += 1;
    }
    assert!(rx_losses_from(publishing.zid()).is_empty());

    gate.release();
    ztimeout!(publishing
        .put(KEY, MARKER)
        .congestion_control(CongestionControl::Block)
        .reliability(Reliability::Reliable))
    .unwrap();
    wait_until("the marker arrives", || {
        marker_received.load(Ordering::SeqCst)
    })
    .await;

    let drops = push_drops_to(subscribing.zid());
    assert!(!drops.is_empty());
    for dropped in &drops {
        assert_eq!(
            dropped,
            &PushDropRecord {
                zid: subscribing.zid(),
                key_expr: Some(KEY.to_owned()),
                priority: Some(Priority::Data),
                reason: PushDropReason::Dropped,
            }
        );
    }
    let losses = rx_losses_from(publishing.zid());
    assert!(!losses.is_empty(), "the subscriber saw no gap");
    for loss in &losses {
        assert_eq!(loss.kind, RxLossKind::SnGap { gap: 2 });
        assert_eq!(loss.priority, Some(Priority::Data));
        assert_eq!(loss.reliability, Reliability::Reliable);
    }
    assert!(push_drops_to(publishing.zid()).is_empty());
    assert!(rx_losses_from(subscribing.zid()).is_empty());

    sessions.close().await;
}

/// Puts that a subscriber keeps up with all arrive, and reach neither observer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_subscriber_that_keeps_up_reaches_neither_observer() {
    zenoh::init_log_from_env_or("error");
    install_observers();
    const KEY: &str = "test/drop_observer/fast";
    let mut sessions = TestSessions::new();
    let (publishing, subscribing) = sessions.open_pairs().await;

    let received = Arc::new(AtomicUsize::new(0));
    let _subscriber = ztimeout!(subscribing.declare_subscriber(KEY).callback({
        let received = received.clone();
        move |_| {
            received.fetch_add(1, Ordering::SeqCst);
        }
    }))
    .unwrap();
    let publisher = ztimeout!(publishing
        .declare_publisher(KEY)
        .congestion_control(CongestionControl::Drop)
        .reliability(Reliability::Reliable))
    .unwrap();
    wait_matching(&publisher).await;

    // One put in flight at a time, so that no queue can fill.
    for sent in 1..=IN_STEP {
        ztimeout!(publisher.put(vec![0_u8; PAYLOAD_SIZE])).unwrap();
        wait_until("the put arrives", || {
            received.load(Ordering::SeqCst) == sent
        })
        .await;
    }

    assert_eq!(received.load(Ordering::SeqCst), IN_STEP);
    for zid in [publishing.zid(), subscribing.zid()] {
        assert_eq!(push_drops_to(zid), []);
        assert_eq!(rx_losses_from(zid), []);
    }

    sessions.close().await;
}
