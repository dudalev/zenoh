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
//! Losses a unicast transport detects, on the receive side and on the send side (BER-2554).
//!
//! A sender built from this fork leaves one frame SN unused on a channel after it drops a
//! message there, so its next frame on that channel arrives with a gap of 2, which the
//! receiver reports as [`RxLossKind::SnGap`]. A drop the sender never follows with a frame on
//! that channel, and the batches a link held when it failed or closed, reach no receiver: the
//! new transport that replaces a failed link starts its SNs afresh. The sender reports the
//! latter as a [`TxLoss`]. Batches already written to the socket are not counted, nor a push
//! already under way in the instant the link fails or closes. The fragment kinds report a
//! reassembly that ended without a message.
//!
//! The multicast receive path reports nothing here.
//!
//! An observer is called on the thread that detected the loss, and must neither block nor
//! panic. A panic is caught there, after the panic hook has reported it, so that it does not
//! unwind through the transport's locks.
use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    sync::OnceLock,
};

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
    /// `gap - 1` SNs never arrived. From a sender built from this fork, a gap means that one
    /// or more messages on that channel were lost. A sender that takes an SN after the last
    /// fragment of a message, as upstream zenoh does, leaves a gap after every fragmented
    /// message it sends, with nothing lost.
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
/// the loss, holding that channel's lock, so it must neither block nor panic. It can be
/// installed once per process.
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
        let loss = RxLoss {
            peer,
            priority,
            reliability,
            kind,
        };
        // The panic hook has already reported a panic by the time it is caught.
        let _ = catch_unwind(AssertUnwindSafe(|| observer(&loss)));
    }
}

/// A loss the send side of a unicast transport detected, of messages it had accepted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct TxLoss {
    /// The peer the messages were for.
    pub peer: ZenohIdProto,
    /// The priority of the messages.
    pub priority: Priority,
    /// What was lost.
    pub kind: TxLossKind,
}

/// What a sender lost. Exhaustive, so that zenoh maps every kind to a push drop reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxLossKind {
    /// A link's write found its connection closed or reset at the peer's end, with `batches`
    /// batches of that priority queued or being written, and the transport had no other link
    /// to the peer. This is what a peer whose process ends gives: its reader, gone with it,
    /// loses nothing it would have read. A peer whose zenoh closes the link while its process
    /// lives gives it too, and then its application has lost these messages.
    ResetQueued { batches: usize },
    /// A link held `batches` batches of that priority, queued or being written, that a reader
    /// that is alive has lost: the link closed at this end while its peer was not reading (a
    /// write at close found no room before the keep-alive timeout, or was cut short by the
    /// close), or its write failed while the transport kept another link to the peer.
    ClosedQueued { batches: usize },
    /// A `BlockFirst` push the transport had accepted found no batch before
    /// `wait_before_close`. The transport then closes.
    BlockFirstDropped,
    /// A `BlockFirst` push the transport had accepted found the transport closed.
    BlockFirstClosed,
}

/// Called with every loss a unicast send path detects.
pub type TxLossObserver = Box<dyn Fn(&TxLoss) + Send + Sync>;

static TX_OBSERVER: OnceLock<TxLossObserver> = OnceLock::new();

/// Installs the process's tx loss observer. It is called on the thread that detected the
/// loss, so it must neither block nor panic. It can be installed once per process.
pub fn set_tx_loss_observer(observer: TxLossObserver) -> ZResult<()> {
    if TX_OBSERVER.set(observer).is_err() {
        bail!("tx loss observer already installed");
    }
    Ok(())
}

/// Reports a loss to the tx observer, when one is installed.
pub(crate) fn report_tx(peer: ZenohIdProto, priority: Priority, kind: TxLossKind) {
    if let Some(observer) = TX_OBSERVER.get() {
        let loss = TxLoss {
            peer,
            priority,
            kind,
        };
        // The panic hook has already reported a panic by the time it is caught.
        let _ = catch_unwind(AssertUnwindSafe(|| observer(&loss)));
    }
}

/// A recording observer for the unit tests, which share one process and so one observer.
#[cfg(test)]
pub(crate) mod recorder {
    use std::sync::{Mutex, Once};

    use zenoh_protocol::core::ZenohIdProto;

    use super::RxLoss;

    static LOSSES: Mutex<Vec<RxLoss>> = Mutex::new(Vec::new());
    static PANICKING: Mutex<Vec<ZenohIdProto>> = Mutex::new(Vec::new());

    /// Installs the recording observer, once per process.
    pub(crate) fn install() {
        static INSTALL: Once = Once::new();
        INSTALL.call_once(|| {
            super::set_rx_loss_observer(Box::new(|loss| {
                LOSSES.lock().unwrap().push(*loss);
                let panics = PANICKING.lock().unwrap().contains(&loss.peer);
                if panics {
                    panic!("the observer panics for this peer");
                }
            }))
            .unwrap();
        });
    }

    /// Makes the observer panic, once it has recorded a loss from `peer`.
    pub(crate) fn panic_for(peer: ZenohIdProto) {
        PANICKING.lock().unwrap().push(peer);
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
