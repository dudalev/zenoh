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
//! Losses the receive side of a unicast transport detects (BER-2554).
//!
//! A sender built from this fork leaves one frame SN unused on a channel after it drops a
//! message there, so its next frame on that channel arrives with a gap of 2. A gap reaches the
//! observer as [`RxLossKind::SnGap`], and it is the one kind that a lost message from such a
//! sender always produces. The fragment kinds report a reassembly that ended without a message.
//!
//! The multicast receive path reports nothing here.
use std::sync::OnceLock;

use zenoh_protocol::{
    core::{Priority, Reliability, ZenohIdProto},
    transport::TransportSn,
};
use zenoh_result::{bail, ZResult};

/// A loss the receive side of a unicast transport detected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct RxLoss {
    /// The peer that sent the frames.
    pub peer: ZenohIdProto,
    /// The priority of the channel the loss is on.
    pub priority: Priority,
    /// The reliability of the channel the loss is on.
    pub reliability: Reliability,
    /// What was lost.
    pub kind: RxLossKind,
}

/// What a receiver lost.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RxLossKind {
    /// A frame or fragment was accepted `gap` SNs after the last one on its channel, so
    /// `gap - 1` SNs never arrived. From a sender built from this fork, each such gap is one or
    /// more lost messages: messages it dropped, or frames that never arrived.
    SnGap { gap: TransportSn },
    /// A stop fragment arrived: the sender abandoned a fragmented message it had begun to
    /// send, and the fragments received of it were discarded. A sender built from this fork
    /// sends no stop fragment, because one carries no payload and so does not encode: what
    /// reports such a message from it is the gap at its next frame.
    FragmentDropped,
    /// A fragment that does not start a message arrived with no message being reassembled, and
    /// was discarded. The fragments that follow one already discarded each report this.
    FragmentWithoutStart,
    /// Reassembly failed: a fragment out of sequence, a message larger than the
    /// defragmentation buffer, or reassembled bytes that do not decode. The message was
    /// discarded.
    Defragmentation,
}

/// Called with every loss a unicast receive path detects.
pub type RxLossObserver = Box<dyn Fn(&RxLoss) + Send + Sync>;

static OBSERVER: OnceLock<RxLossObserver> = OnceLock::new();

/// Installs the process's rx loss observer. It is called on the receive thread that detected
/// the loss, so it must not block. It can be installed once per process.
pub fn set_rx_loss_observer(observer: RxLossObserver) -> ZResult<()> {
    if OBSERVER.set(observer).is_err() {
        bail!("rx loss observer already installed");
    }
    Ok(())
}

/// Reports a loss to the observer, when one is installed.
pub(crate) fn report(
    peer: ZenohIdProto,
    priority: Priority,
    reliability: Reliability,
    kind: RxLossKind,
) {
    if let Some(observer) = OBSERVER.get() {
        observer(&RxLoss {
            peer,
            priority,
            reliability,
            kind,
        });
    }
}

/// A recording observer for the unit tests, which share one process and so one observer.
#[cfg(test)]
pub(crate) mod recorder {
    use std::sync::{Mutex, Once};

    use zenoh_protocol::core::ZenohIdProto;

    use super::RxLoss;

    static LOSSES: Mutex<Vec<RxLoss>> = Mutex::new(Vec::new());

    /// Installs the recording observer, once per process.
    pub(crate) fn install() {
        static INSTALL: Once = Once::new();
        INSTALL.call_once(|| {
            super::set_rx_loss_observer(Box::new(|loss| LOSSES.lock().unwrap().push(*loss)))
                .unwrap();
        });
    }

    /// The losses recorded from `peer`, in the order they were reported.
    pub(crate) fn losses_from(peer: ZenohIdProto) -> Vec<RxLoss> {
        LOSSES
            .lock()
            .unwrap()
            .iter()
            .filter(|loss| loss.peer == peer)
            .copied()
            .collect()
    }
}
