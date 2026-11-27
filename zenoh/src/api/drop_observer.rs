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
//! Observers of the messages a process's transports drop (BER-2554).
//!
//! An observer is called on the thread that detected the loss, and must neither block nor
//! panic. A panic is caught there, after the panic hook has reported it.
use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    sync::OnceLock,
};

use zenoh_config::ZenohId;
use zenoh_protocol::core::{Priority as ProtocolPriority, Reliability, ZenohIdProto};
use zenoh_result::{bail, ZResult};
use zenoh_transport::common::loss::{self, RxLossKind, TxLoss, TxLossKind};

use crate::api::publisher::Priority;

/// A push that the transport to one of the faces it was routed to did not deliver. The other
/// faces it was routed to are not affected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct PushDrop<'a> {
    /// The face the push was routed to. A multicast group's face has the placeholder ZenohId
    /// zenoh gives such faces.
    pub zid: ZenohId,
    /// The push's key expression. `None` when the face no longer maps its wire expression,
    /// and for a loss the transport reports after it accepted the push, where only the wire
    /// expression is known: [`PushDropReason::LinkClosedQueued`] and a `BlockFirst` push.
    pub key_expr: Option<&'a str>,
    /// The push's priority, or `None` for zenoh's own control priority.
    pub priority: Option<Priority>,
    /// Why the transport did not deliver it.
    pub reason: PushDropReason,
}

/// Why a transport did not deliver a push.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PushDropReason {
    /// The transport discarded it: no batch of its priority's queue came free before the drop
    /// deadline (for a blocking push, before `wait_before_close`, and the transport then
    /// closes), a `BlockFirst` push waited out its turn, or the push does not fit the link's
    /// batches even as fragments.
    Dropped,
    /// The transport had closed or begun to close, or had lost its last link, which it loses
    /// only as it closes.
    TransportClosed,
    /// The link failed, or could not flush at close, with `batches` batches of the push's
    /// priority queued or being written: every message in them is lost. Batches already
    /// written to the socket are not counted.
    LinkClosedQueued { batches: usize },
}

impl PushDropReason {
    /// The reason a scheduling result names, or `None` when the push was queued.
    pub(crate) fn of(sent: &ZResult<bool>) -> Option<Self> {
        match sent {
            Ok(true) => None,
            Ok(false) => Some(Self::Dropped),
            Err(_) => Some(Self::TransportClosed),
        }
    }

    fn of_tx_loss(kind: TxLossKind) -> Self {
        match kind {
            TxLossKind::Unsent { batches } => Self::LinkClosedQueued { batches },
            TxLossKind::BlockFirstDropped => Self::Dropped,
            TxLossKind::BlockFirstClosed => Self::TransportClosed,
        }
    }
}

type PushDropObserver = Box<dyn Fn(&PushDrop<'_>) + Send + Sync>;

static PUSH_DROP_OBSERVER: OnceLock<PushDropObserver> = OnceLock::new();

/// Installs the process's push drop observer. It is called on the thread that routed the
/// push, once the transport has refused it, or on the transport's own thread for a loss it
/// detects after accepting the push, so it must neither block nor panic. It can be installed
/// once per process. A push that an ACL or another interceptor refuses does not reach it.
pub fn set_push_drop_observer(
    observer: impl Fn(&PushDrop<'_>) + Send + Sync + 'static,
) -> ZResult<()> {
    if PUSH_DROP_OBSERVER.set(Box::new(observer)).is_err() {
        bail!("push drop observer already installed");
    }
    loss::set_tx_loss_observer(Box::new(|loss: &TxLoss| {
        report_push_drop(
            loss.peer,
            None,
            loss.priority,
            PushDropReason::of_tx_loss(loss.kind),
        )
    }))
}

/// Whether a push drop observer is installed.
pub(crate) fn is_push_drop_observed() -> bool {
    PUSH_DROP_OBSERVER.get().is_some()
}

/// Reports a push to the observer, when one is installed.
pub(crate) fn report_push_drop(
    zid: ZenohIdProto,
    key_expr: Option<&str>,
    priority: ProtocolPriority,
    reason: PushDropReason,
) {
    let Some(observer) = PUSH_DROP_OBSERVER.get() else {
        return;
    };
    let dropped = PushDrop {
        zid: zid.into(),
        key_expr,
        priority: Priority::try_from(priority).ok(),
        reason,
    };
    // The panic hook has already reported a panic by the time it is caught.
    let _ = catch_unwind(AssertUnwindSafe(|| observer(&dropped)));
}

/// A loss the receive side of a unicast transport detected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct RxLoss {
    /// The peer that sent the frames.
    pub zid: ZenohId,
    /// The priority of the channel the loss is on, or `None` for zenoh's own control priority.
    pub priority: Option<Priority>,
    /// The reliability of the channel the loss is on.
    pub reliability: Reliability,
    /// What was lost.
    pub kind: RxLossKind,
}

/// Installs the process's rx loss observer. It is called on the receive thread that detected
/// the loss, holding that channel's lock, so it must neither block nor panic. It can be
/// installed once per process.
pub fn set_rx_loss_observer(observer: impl Fn(&RxLoss) + Send + Sync + 'static) -> ZResult<()> {
    loss::set_rx_loss_observer(Box::new(move |loss| {
        observer(&RxLoss {
            zid: loss.peer.into(),
            priority: Priority::try_from(loss.priority).ok(),
            reliability: loss.reliability,
            kind: loss.kind,
        });
    }))
}
