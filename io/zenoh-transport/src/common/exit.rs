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
//! Process exit, treated by the receive paths as a close (BER-2456).
//!
//! `std::process::exit` with a transport manager still open never runs
//! [`TransportManager::close`](crate::TransportManager::close), and it finalizes zenoh-shm's
//! statics while the receive threads still run, so a map then fails and is reported as a
//! discarded message. An `atexit` hook marks exit as begun before that happens. From then on
//! the receive paths deliver nothing, as they do once `close` has begun, and the hook waits
//! for the maps already in flight, so that none overlaps the finalization.
//!
//! The order is the C runtime's: `atexit` hooks run before the `.fini_array` destructor
//! that static_init finalizes its statics from, whichever was registered first.
use std::{
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::{Duration, Instant},
};

use crossbeam_utils::CachePadded;

/// The longest exit waits for the maps in flight when it begins.
#[cfg(unix)]
const MAX_WAIT: Duration = Duration::from_secs(1);

static GATE: ExitGate = ExitGate::new();

/// Registers the hook that begins exit, once per process.
pub(crate) fn register() {
    #[cfg(unix)]
    {
        static REGISTER: std::sync::Once = std::sync::Once::new();
        REGISTER.call_once(|| {
            // SAFETY: `on_exit` takes no arguments and does not unwind.
            if unsafe { libc::atexit(on_exit) } != 0 {
                tracing::error!(
                    "Could not register the exit hook: a process that exits with a session \
                     open may report its last shared-memory messages as discarded (BER-2456)"
                );
            }
        });
    }
}

#[cfg(unix)]
extern "C" fn on_exit() {
    GATE.begin(MAX_WAIT);
}

/// Whether the process has begun to exit.
pub(crate) fn has_begun() -> bool {
    GATE.has_begun()
}

/// Holds exit back while one map runs, or `None` once exit has begun.
pub(crate) fn hold() -> Option<MapHold<'static>> {
    #[cfg(any(test, feature = "test"))]
    HOLDS_TAKEN.fetch_add(1, Ordering::Relaxed);
    GATE.hold()
}

/// How many times [`hold`] has been called, for tests only.
#[cfg(any(test, feature = "test"))]
static HOLDS_TAKEN: AtomicUsize = AtomicUsize::new(0);

#[cfg(any(test, feature = "test"))]
pub(crate) fn holds_taken() -> usize {
    HOLDS_TAKEN.load(Ordering::Relaxed)
}

/// Whether exit has begun, and how many maps it has to wait for.
pub(crate) struct ExitGate {
    begun: AtomicBool,
    in_flight: CachePadded<AtomicUsize>,
}

impl ExitGate {
    pub(crate) const fn new() -> Self {
        Self {
            begun: AtomicBool::new(false),
            in_flight: CachePadded::new(AtomicUsize::new(0)),
        }
    }

    pub(crate) fn has_begun(&self) -> bool {
        self.begun.load(Ordering::Acquire)
    }

    /// A map is counted BEFORE `begun` is read, and [`ExitGate::begin`] sets `begun` BEFORE
    /// it reads the count, all `SeqCst`: so either this sees that exit has begun, or
    /// `begin` sees this map and waits for it.
    pub(crate) fn hold(&self) -> Option<MapHold<'_>> {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        let hold = MapHold(&self.in_flight);
        if self.begun.load(Ordering::SeqCst) {
            // Dropping `hold` uncounts this map.
            return None;
        }
        Some(hold)
    }

    /// Marks exit as begun, then waits up to `max_wait` for the maps in flight. Returns
    /// whether none was left.
    // Outside tests, only the unix exit hook calls this.
    #[cfg_attr(not(unix), allow(dead_code))]
    pub(crate) fn begin(&self, max_wait: Duration) -> bool {
        self.begun.store(true, Ordering::SeqCst);
        let deadline = Instant::now() + max_wait;
        while self.in_flight.load(Ordering::SeqCst) != 0 {
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::yield_now();
        }
        true
    }
}

/// One map in flight: exit waits until it is dropped.
pub(crate) struct MapHold<'a>(&'a AtomicUsize);

impl Drop for MapHold<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// How long a test keeps a map in flight while exit waits for it.
    const HELD: Duration = Duration::from_millis(200);

    /// **ONCE EXIT HAS BEGUN NO MAP STARTS**, with the open control first.
    #[test]
    fn a_map_is_admitted_until_exit_begins_and_refused_after() {
        let gate = ExitGate::new();
        drop(gate.hold().expect("before exit a map must be admitted"));

        assert!(gate.begin(Duration::ZERO), "no map was in flight");
        assert!(gate.has_begun());
        assert!(
            gate.hold().is_none(),
            "once exit has begun no map may start"
        );
        assert_eq!(
            gate.in_flight.load(Ordering::SeqCst),
            0,
            "a refused map must not stay counted"
        );
    }

    /// **EXIT WAITS FOR A MAP IN FLIGHT**, and returns once it is done.
    #[test]
    fn exit_waits_for_a_map_in_flight() {
        let gate = ExitGate::new();
        let hold = gate.hold().unwrap();
        // Timed from before the spawn: the thread may start late on a loaded host.
        let started = Instant::now();
        std::thread::scope(|scope| {
            let exiting = scope.spawn(|| (gate.begin(Duration::from_secs(60)), started.elapsed()));
            std::thread::sleep(HELD);
            assert!(
                !exiting.is_finished(),
                "exit must wait while a map is in flight"
            );
            drop(hold);
            let (drained, waited) = exiting.join().unwrap();
            assert!(drained, "exit must see the map end");
            assert!(waited >= HELD);
        });
    }

    /// **EXIT STOPS WAITING AT ITS BOUND**, so a map that never ends cannot hold the
    /// process.
    #[test]
    fn exit_stops_waiting_at_its_bound() {
        let gate = ExitGate::new();
        let _hold = gate.hold().unwrap();
        let started = Instant::now();
        assert!(
            !gate.begin(Duration::from_millis(50)),
            "the map is still in flight at the bound"
        );
        assert!(started.elapsed() >= Duration::from_millis(50));
    }
}
