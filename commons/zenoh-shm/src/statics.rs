//
// Copyright (c) 2023 ZettaScale Technology
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

//! Reading a `#[dynamic(lazy, drop)]` static without panicking at process exit.
//!
//! The generated accessors are not interchangeable, and the difference is the
//! whole subject of this module:
//!
//! | accessor | initializes? | on inaccessible |
//! | -- | -- | -- |
//! | `.read()` | yes (`init_then_read_lock`) | **PANICS** |
//! | `.try_read()` | **no** (`try_read_lock`) | `Err(AccessError)` |
//!
//! So `.try_read()` is not `.read()` without the panic: on a static that has
//! never been touched it fails, where `.read()` would have initialized it. A
//! live code path needs the initializing one.
//!
//! But `.read()` panics once the static is FINALIZED, which happens at process
//! exit while zenoh's own threads are still running -- they belong to the
//! runtime, not to these statics, so nothing stops them first. Measured on giedi
//! 2026-11-17 and 2026-11-18: `thread 'rx-1' panicked ... AccessError { phase:
//! ... FINALIZED ... }`, one second after a deliberate non-zero exit, three
//! times. The exit's own message was already written; the panic landed after it
//! and pointed here, so the real cause read as a footnote.
//!
//! `AccessError` carries the `Phase`, and the two failure causes are opposite:
//!
//! * **empty phase** -- never initialized. The static is wanted, so initialize.
//! * **FINALIZED** -- the process is exiting. Initializing is exactly what
//!   panics, so fail instead and let the caller treat it as the unavailability
//!   it is.
//!
//! A DROP must not use this. At drop time the static was necessarily
//! initialized already (the object being dropped came from it), so a plain
//! `.try_read()` is both sufficient and correct there -- and re-initializing a
//! static during its own teardown is the thing to avoid. `Segment::drop` set
//! that precedent before this module existed.

/// Read a finalizable static for a LIVE operation: initialize it if it has not
/// been, but never once it is FINALIZED.
///
/// Yields `Result<ReadGuard<_>, static_init::AccessError>`, where `Err` means
/// FINALIZED and nothing else -- so a caller can report "shutting down" rather
/// than guessing.
///
/// RESIDUAL, stated because it is not zero: a static touched for the very first
/// time once static_init has finalized the others is not initialized. static_init
/// refuses to register a static after its exit sequence, so the `read` below
/// panics, `AccessError { phase: INITIALIZATION_SKIPED | REGISTRATION_REFUSED }`,
/// on every such first touch: no narrow window. zenoh-transport's
/// `a_first_accept_in_flight_at_exit_is_refused_cleanly` measured it on that crate's
/// own static (BER-2487). This crate cannot see exit begin, so a caller that can
/// make such a first touch must refuse once exit has begun, as zenoh-transport's
/// handoff reactor does. In `init` mode `init::init` initializes them all at session
/// open, so there it cannot happen. Closing it here needs
/// `init_then_try_read_lock`, which the `dynamic` macro does not expose.
macro_rules! try_live_read {
    ($st:path) => {
        match $st.try_read() {
            Ok(guard) => Ok(guard),
            // The process is exiting. Do NOT initialize -- that is the panic.
            Err(e) if e.phase.intersects(::static_init::Phase::FINALIZED) => Err(e),
            // Never initialized: this is ordinary first use.
            Err(_) => Ok($st.read()),
        }
    };
}

pub(crate) use try_live_read;

#[cfg(test)]
mod tests {
    use static_init::Phase;

    /// The discriminator this module rests on: the two Err causes are
    /// distinguishable, and only one of them must skip initialization.
    #[test]
    fn finalized_is_distinguishable_from_never_initialized() {
        assert!(
            Phase::FINALIZED.intersects(Phase::FINALIZED),
            "a finalized phase must be recognised as finalized"
        );

        // CONTROL: the never-initialized phase is EMPTY, so it must NOT be read
        // as finalized -- if it were, first use would fail instead of
        // initializing, which is the regression this control exists to catch.
        assert!(
            !Phase::empty().intersects(Phase::FINALIZED),
            "an uninitialized static must not be mistaken for a finalized one"
        );

        // And a healthy, initialized static is not finalized either.
        assert!(
            !Phase::INITIALIZED_AND_REGISTERED.intersects(Phase::FINALIZED),
            "an initialized static must not be mistaken for a finalized one"
        );
    }
}
