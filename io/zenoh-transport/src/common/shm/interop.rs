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
use std::{
    collections::BTreeMap,
    fmt::Debug,
    num::NonZeroUsize,
    sync::{Arc, Mutex, OnceLock},
    time::Instant,
};

use zenoh_buffers::{reader::HasReader, ZBuf, ZSlice, ZSliceKind};
use zenoh_codec::{RCodec, Zenoh080};
use zenoh_core::{zerror, zlock, Wait};
use zenoh_protocol::{
    core::{Priority, ZenohIdProto},
    network::{NetworkBodyMut, NetworkMessageMut, Push, Request, Response},
    zenoh::{
        err::Err,
        ext::ShmType,
        query::{ext::QueryBodyType, Query},
        PushBody, Put, Reply, RequestBody, ResponseBody,
    },
};
use zenoh_result::ZResult;
use zenoh_runtime::ZRuntime;
use zenoh_shm::{
    api::{
        common::types::{ProtocolID, SegmentID},
        protocol_implementations::posix::posix_shm_provider_backend::PosixShmProviderBackend,
        provider::shm_provider::{
            ConstBool, Defragment, GarbageCollect, JustAlloc, ShmProvider, ShmProviderBuilder,
        },
    },
    reader::{PremountedSegment, ShmReader},
    ShmBufInfo, ShmBufInner,
};

use crate::unicast::establishment::ext::shm::{
    handoff::{
        OpenTransaction, RxHandoffChannel, TxHandoffChannel, TxHandoffStorage, TxHandoffTransaction,
    },
    segment::RXAuthSegment,
};

/// How often a discarded-message report may be emitted PER PEER, however many
/// are dropped in between. A failed map is a per-message event on the receive
/// path, so an unconditional log is two defects at once: a sender in this state
/// floods the log, and the flood is what hides a real line.
const SHM_RX_DROP_REPORT_EVERY_MS: u64 = 10_000;

/// What has been dropped for one peer, and when it was last reported.
#[derive(Default)]
struct ShmRxDropState {
    total: u64,
    reported: u64,
    last_report_ms: u64,
}

/// Per-peer discard state.
///
/// KEYED BY PEER on purpose (BER-2172). A single process-wide limiter means a
/// steady failure from one peer takes every report window, so a NEW and
/// different failure from another peer is almost never the line that gets
/// printed -- the limiter blends peers exactly as a shared counter blends the
/// routine with the broken. Keyed this way, each peer's first discard is always
/// reported.
///
/// Unbounded in principle; in practice a process has a handful of peers and
/// each entry is three integers.
static SHM_RX_DROPS: Mutex<BTreeMap<ZenohIdProto, ShmRxDropState>> = Mutex::new(BTreeMap::new());

/// A peer's `(total, reported, last_report_ms)`, for tests only.
#[cfg(test)]
fn shm_rx_drop_state(peer: &ZenohIdProto) -> Option<(u64, u64, u64)> {
    let drops = SHM_RX_DROPS.lock().unwrap_or_else(|e| e.into_inner());
    drops
        .get(peer)
        .map(|s| (s.total, s.reported, s.last_report_ms))
}

/// Is this discard the one that gets reported?
///
/// Pure, so both directions are testable without capturing log output. The
/// FIRST discard from a peer always reports: `last_report_ms` starts at zero,
/// so a peer that fails in the process's first few seconds would otherwise stay
/// silent for the whole window -- and the first one is the most informative,
/// because it is closest to whatever changed.
#[must_use]
fn shm_rx_drop_should_report(total: u64, now_ms: u64, last_ms: u64) -> bool {
    total == 1 || now_ms.saturating_sub(last_ms) >= SHM_RX_DROP_REPORT_EVERY_MS
}

fn shm_rx_now_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    u64::try_from(START.get_or_init(Instant::now).elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Report a message DISCARDED because its shared-memory buffer could not be
/// mapped, and count it against the peer it came from.
///
/// This replaced a `tracing::debug!` that every service silenced, since they all
/// run at `RUST_LOG=info`. The message is dropped either way -- that behaviour is
/// deliberately unchanged, because failing the transport instead would turn a
/// lost payload into a dead link -- but it is no longer SILENT.
///
/// The loss is not symmetric across routes, which is why the text says so: a
/// dropped REQUEST surfaces at the caller as a timeout, while a dropped REPLY
/// does not -- the call completes normally with zero replies, which a reader
/// cannot distinguish from an empty result. A pub/sub route has no timeout at
/// all.
pub fn report_shm_rx_drop(peer: ZenohIdProto, e: &dyn core::fmt::Display) {
    let mut drops = match SHM_RX_DROPS.lock() {
        Ok(drops) => drops,
        // A poisoned lock means another thread panicked while reporting. Keep
        // reporting rather than panicking again on the receive path.
        Err(poisoned) => poisoned.into_inner(),
    };
    let state = drops.entry(peer).or_default();
    state.total += 1;
    let now_ms = shm_rx_now_ms();
    if !shm_rx_drop_should_report(state.total, now_ms, state.last_report_ms) {
        return;
    }
    state.last_report_ms = now_ms;
    let since_last = state.total.saturating_sub(state.reported);
    state.reported = state.total;
    let total = state.total;
    drop(drops);
    tracing::error!(
        %peer,
        total_dropped = total,
        since_last_report = since_last,
        "DISCARDED a message whose shared-memory buffer could not be mapped, and the \
         payload is LOST: {e}. A pub/sub route has no timeout and loses it silently. A \
         DROPPED REQUEST surfaces at the caller as a plain timeout; a dropped REPLY does \
         not -- the call completes normally with zero replies, which a reader cannot \
         distinguish from an empty result. Mounting a peer's pool does NOT lock it \
         (BER-2170), so this is a segment that could not be opened, not a locked-memory \
         limit. A burst shorter than the report window is counted in full but reported \
         only in part"
    );
}

#[derive(Debug)]
struct ProviderInitCfg {
    shm_size: NonZeroUsize,
}

#[derive(Debug)]
pub enum ProviderInitState {
    Initializing(flume::Receiver<Option<Arc<ShmProvider<PosixShmProviderBackend>>>>),
    Ready(Arc<ShmProvider<PosixShmProviderBackend>>),
    Error,
}

enum ProviderInitStateInner {
    Enabled(ProviderInitCfg),
    Initializing(flume::Receiver<Option<Arc<ShmProvider<PosixShmProviderBackend>>>>),
    Ready(Arc<ShmProvider<PosixShmProviderBackend>>),
    Error,
}

impl std::fmt::Debug for ProviderInitStateInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Enabled(cfg) => f.debug_tuple("Enabled").field(cfg).finish(),
            Self::Initializing(_) => f.debug_tuple("Initializing").field(&"..").finish(),
            Self::Ready(provider) => f.debug_tuple("Ready").field(provider).finish(),
            Self::Error => f.debug_tuple("Error").finish(),
        }
    }
}

pub struct LazyShmProvider {
    message_size_threshold: usize,
    state: Mutex<ProviderInitStateInner>,
}

impl std::fmt::Debug for LazyShmProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyShmProvider")
            .field("message_size_threshold", &self.message_size_threshold)
            .field("state", &self.state)
            .finish()
    }
}

impl LazyShmProvider {
    pub fn new(shm_size: NonZeroUsize, message_size_threshold: usize) -> Self {
        let cfg = ProviderInitCfg { shm_size };
        let state = Mutex::new(ProviderInitStateInner::Enabled(cfg));
        Self {
            message_size_threshold,
            state,
        }
    }

    /// Build the provider NOW, synchronously, instead of on first use.
    ///
    /// `ShmInitMode::Init` initialises the SHM subsystem GLOBALS at session
    /// open (`zenoh_shm::init::init`), but the transport's own provider stayed
    /// lazy -- so the segment was still allocated on the first SHM interaction,
    /// mid-session. This is the missing half of "eager".
    ///
    /// Synchronous on purpose, rather than awaiting the `spawn_blocking` path
    /// `try_get_provider` uses: that one hands its result down a `flume`
    /// channel with a single slot, so a caller that waited on its own clone of
    /// the receiver would CONSUME the message and leave the stored state stuck
    /// in `Initializing` for ever. Building under the lock has no such race.
    ///
    /// Idempotent: only the not-yet-started state does any work, so calling it
    /// after the provider is `Ready` (or has already failed) is a no-op and a
    /// later `try_get_provider` sees what this left behind.
    ///
    /// A failure is recorded as `Error` and logged, NOT returned: an eager
    /// build that cannot allocate must leave the transport exactly where a lazy
    /// one would -- falling back to the network path -- rather than failing the
    /// session open on a path that was previously best-effort.
    pub fn init_eagerly(&self) {
        let mut lock = zlock!(self.state);
        let ProviderInitStateInner::Enabled(cfg) = &*lock else {
            return;
        };
        let shm_size = cfg.shm_size.get();
        *lock = match ShmProviderBuilder::default_backend(shm_size).wait() {
            Ok(backend) => ProviderInitStateInner::Ready(Arc::new(backend)),
            Result::Err(err) => {
                tracing::error!("Error creating eager ShmProvider: {err}");
                ProviderInitStateInner::Error
            }
        };
    }

    /// The id of the segment backing this provider's pool, once it exists.
    ///
    /// `None` while the provider has not been built, or after a build that
    /// failed -- in both cases there is no pool for a peer to mount, and
    /// advertising one would send a peer chasing a segment that is not there.
    /// With eager initialisation this is `Some` from session open (BER-2169),
    /// which is what makes advertising it at link establishment possible at all
    /// (BER-2170).
    pub fn data_segment_id(&self) -> Option<SegmentID> {
        match &*zlock!(self.state) {
            ProviderInitStateInner::Ready(provider) => Some(provider.backend().segment_id()),
            ProviderInitStateInner::Enabled(_)
            | ProviderInitStateInner::Initializing(_)
            | ProviderInitStateInner::Error => None,
        }
    }

    pub fn try_get_provider(&self) -> ProviderInitState {
        let mut lock = zlock!(self.state);
        match &mut *lock {
            ProviderInitStateInner::Enabled(cfg) => {
                let shm_size = cfg.shm_size.get();
                let (sender, receiver) = flume::bounded(1);

                ZRuntime::Application.spawn_blocking(move || {
                    // todo: this is supported since 1.76
                    // let _ = sender.send(
                    //     ShmProviderBuilder::default_backend(shm_size)
                    //         .wait()
                    //         .inspect_err(|err| {
                    //             tracing::error!("Error creating lazy ShmProvider: {err}")
                    //         })
                    //         .map(Arc::new)
                    //         .ok(),
                    // );
                    let _ =
                        sender.send(match ShmProviderBuilder::default_backend(shm_size).wait() {
                            Ok(backend) => Some(Arc::new(backend)),
                            Result::Err(err) => {
                                tracing::error!("Error creating lazy ShmProvider: {err}");
                                None
                            }
                        });
                });

                *lock = ProviderInitStateInner::Initializing(receiver.clone());
                ProviderInitState::Initializing(receiver)
            }
            ProviderInitStateInner::Initializing(join_handle) => match join_handle.try_recv() {
                Ok(Some(shm_provider)) => {
                    *lock = ProviderInitStateInner::Ready(shm_provider.clone());
                    ProviderInitState::Ready(shm_provider)
                }
                Ok(None) => {
                    *lock = ProviderInitStateInner::Error;
                    ProviderInitState::Error
                }
                Err(_) => ProviderInitState::Initializing(join_handle.clone()),
            },
            ProviderInitStateInner::Ready(shm_provider) => {
                ProviderInitState::Ready(shm_provider.clone())
            }
            ProviderInitStateInner::Error => ProviderInitState::Error,
        }
    }

    fn wrap_in_place<const ID: u8>(
        &self,
        ext_shm: &mut Option<ShmType<ID>>,
        slice: &mut ZSlice,
        handoff_transaction: &mut OpenTransaction,
    ) {
        if slice.len() >= self.message_size_threshold {
            if let ProviderInitState::Ready(provider) = self.try_get_provider() {
                Self::_wrap_in_place(&provider, ext_shm, slice, handoff_transaction);
            }
        }
    }

    fn _wrap_in_place<const ID: u8>(
        shm_provider: &ShmProvider<PosixShmProviderBackend>,
        ext_shm: &mut Option<ShmType<ID>>,
        slice: &mut ZSlice,
        handoff_transaction: &mut OpenTransaction,
    ) -> bool {
        if let Ok(mut shmbuf) = unsafe {
            shm_provider
            .alloc(slice.len())
            .with_unsafe_policy::<Defragment<GarbageCollect<JustAlloc, JustAlloc, ConstBool<false>>>>()
            .wait()
        } {
            shmbuf.as_mut().copy_from_slice(slice);
            let reference = (&shmbuf).into();
            *slice = shmbuf.into();
            slice.kind = ZSliceKind::ShmPtr;
            *ext_shm = Some(ShmType::new());
            handoff_transaction.on_tx(reference);

            return true;
        }
        false
    }
}

#[derive(Debug)]
pub struct LinkShmHandoffConfig {
    pub rx: RxHandoffChannel,
    pub tx: TxHandoffChannel,
    /// Peer pools this link pre-mounted at establishment (BER-2170).
    ///
    /// Held HERE because this config lives on the link, so the mappings and
    /// their flocks are released exactly when the link goes away. Anywhere with
    /// a longer life would reintroduce the accumulation the lease exists to
    /// prevent, and `TransportShmConfig` -- the obvious alternative -- derives
    /// `Clone` and `Eq`, which a lease cannot satisfy without making a clone
    /// mean a second release.
    pub premounted: Vec<PremountedSegment>,
}

impl LinkShmHandoffConfig {
    pub fn new(
        rx: RxHandoffChannel,
        tx: TxHandoffChannel,
        premounted: Vec<PremountedSegment>,
    ) -> Self {
        Self { rx, tx, premounted }
    }

    pub fn new_disabled() -> Self {
        Self {
            rx: RxHandoffChannel::Disabled,
            tx: TxHandoffChannel::new_disabled(),
            premounted: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransportShmConfig {
    link_partner_segment: Arc<RXAuthSegment>,
}

impl TransportShmConfig {
    pub fn new(link_partner_segment: Arc<RXAuthSegment>) -> Self {
        Self {
            link_partner_segment,
        }
    }
}

impl PartnerShmConfig for TransportShmConfig {
    fn supports_protocol(&self, protocol: ProtocolID) -> bool {
        self.link_partner_segment.protocols().contains(&protocol)
    }
}

#[derive(Clone, Debug)]
pub struct MulticastTransportShmConfig;

impl PartnerShmConfig for MulticastTransportShmConfig {
    fn supports_protocol(&self, _protocol: ProtocolID) -> bool {
        true
    }
}

/// Controls which categories of messages are eligible for the *implicit* SHM
/// optimization on TX, derived from the `transport_optimization.messages` configuration.
///
/// Each flag gates only the implicit optimization for its category — automatically
/// copying a large raw payload into SHM. A payload the application already allocated in
/// SHM is always forwarded as a descriptor (when the partner supports its protocol),
/// regardless of these flags.
#[derive(Clone, Copy, Debug)]
pub struct ShmOptimizationPolicy {
    /// Whether publication (`Put`) payloads may be implicitly copied into SHM.
    pub put: bool,
    /// Whether query (`Request`) payloads may be implicitly copied into SHM.
    pub query: bool,
    /// Whether reply (`Response`: `Reply`/`Err`) payloads may be implicitly copied into SHM.
    pub reply: bool,
}

pub fn map_zmsg_to_partner<ShmCfg: PartnerShmConfig>(
    msg: &mut NetworkMessageMut,
    partner_shm_cfg: &ShmCfg,
    shm_provider: &Option<Arc<LazyShmProvider>>,
    handoff: &TxHandoffStorage,
    policy: ShmOptimizationPolicy,
) -> TxHandoffTransaction {
    // The policy gates only the *implicit* optimization (auto-copying a large raw
    // payload into SHM): a category is granted the provider only when it is enabled.
    // An already-allocated SHM buffer is *always* forwarded as a descriptor (subject to
    // the partner supporting its protocol) since the explicit branch of `map_to_partner`
    // does not use the provider.
    let no_provider: Option<Arc<LazyShmProvider>> = None;
    let open = match &mut msg.body {
        NetworkBodyMut::Push(Push {
            payload, ext_qos, ..
        }) => match payload {
            PushBody::Put(b) => {
                let p = if policy.put {
                    shm_provider
                } else {
                    &no_provider
                };

                let mut transaction = OpenTransaction::new(handoff, ext_qos.get_priority());
                b.map_to_partner(partner_shm_cfg, p, &mut transaction);
                transaction
            }
            PushBody::Del(_) => OpenTransaction::new_disabled(),
        },
        NetworkBodyMut::Request(Request {
            payload, ext_qos, ..
        }) => match payload {
            RequestBody::Query(b) => {
                let p = if policy.query {
                    shm_provider
                } else {
                    &no_provider
                };

                let mut transaction = OpenTransaction::new(handoff, ext_qos.get_priority());
                b.map_to_partner(partner_shm_cfg, p, &mut transaction);
                transaction
            }
        },
        NetworkBodyMut::Response(Response {
            payload, ext_qos, ..
        }) => {
            let p = if policy.reply {
                shm_provider
            } else {
                &no_provider
            };

            match payload {
                ResponseBody::Reply(b) => {
                    let mut transaction = OpenTransaction::new(handoff, ext_qos.get_priority());
                    b.map_to_partner(partner_shm_cfg, p, &mut transaction);
                    transaction
                }
                ResponseBody::Err(b) => {
                    let mut transaction = OpenTransaction::new(handoff, ext_qos.get_priority());
                    b.map_to_partner(partner_shm_cfg, p, &mut transaction);
                    transaction
                }
            }
        }
        NetworkBodyMut::ResponseFinal(_)
        | NetworkBodyMut::Interest(_)
        | NetworkBodyMut::Declare(_)
        | NetworkBodyMut::OAM(_) => OpenTransaction::new_disabled(),
    };
    open.close()
}

pub fn map_zmsg_to_shmbuf(
    msg: NetworkMessageMut,
    shmr: &ShmReader,
    handoff: &RxHandoffChannel,
) -> ZResult<()> {
    match msg.body {
        NetworkBodyMut::Push(Push {
            payload, ext_qos, ..
        }) => match payload {
            PushBody::Put(b) => b.map_to_shmbuf(shmr, handoff, ext_qos.get_priority()),
            PushBody::Del(_) => Ok(()),
        },
        NetworkBodyMut::Request(Request {
            payload, ext_qos, ..
        }) => match payload {
            RequestBody::Query(b) => b.map_to_shmbuf(shmr, handoff, ext_qos.get_priority()),
        },
        NetworkBodyMut::Response(Response {
            payload, ext_qos, ..
        }) => match payload {
            ResponseBody::Err(b) => b.map_to_shmbuf(shmr, handoff, ext_qos.get_priority()),
            ResponseBody::Reply(b) => b.map_to_shmbuf(shmr, handoff, ext_qos.get_priority()),
        },
        NetworkBodyMut::ResponseFinal(_)
        | NetworkBodyMut::Interest(_)
        | NetworkBodyMut::Declare(_)
        | NetworkBodyMut::OAM(_) => Ok(()),
    }
}

pub trait PartnerShmConfig {
    fn supports_protocol(&self, protocol: ProtocolID) -> bool;
}

// Currently, there can be three forms of ZSlice:
// rawbuf - usual non-shm buffer
// shminfo - small SHM info that can be used to mount SHM buffer and get access to it's contents
// shmbuf - mounted SHM buffer
// On RX and TX we need to do the following conversion:
trait MapShm {
    // RX:
    // - shminfo -> shmbuf
    // - rawbuf -> rawbuf (no changes)
    fn map_to_shmbuf(
        &mut self,
        shmr: &ShmReader,
        handoff: &RxHandoffChannel,
        priority: Priority,
    ) -> ZResult<()>;

    // TX:
    // - shmbuf -> shminfo if partner supports shmbuf's SHM protocol
    // - shmbuf -> rawbuf if partner does not support shmbuf's SHM protocol
    // - rawbuf -> rawbuf (no changes)
    fn map_to_partner<ShmCfg: PartnerShmConfig>(
        &mut self,
        partner_shm_cfg: &ShmCfg,
        shm_provider: &Option<Arc<LazyShmProvider>>,
        handoff_transaction: &mut OpenTransaction,
    );
}

fn map_to_partner<const ID: u8, ShmCfg: PartnerShmConfig>(
    zbuf: &mut ZBuf,
    ext_shm: &mut Option<ShmType<ID>>,
    partner_shm_cfg: &ShmCfg,
    shm_provider: &Option<Arc<LazyShmProvider>>,
    handoff_transaction: &mut OpenTransaction,
) {
    for zs in zbuf.zslices_mut() {
        match zs.downcast_ref::<ShmBufInner>() {
            None => {
                if let Some(shm_provider) = shm_provider {
                    // Implicit SHM optimization: try to convert to SHM buffer
                    shm_provider.wrap_in_place(ext_shm, zs, handoff_transaction);
                }
            }
            Some(shmb) => {
                if partner_shm_cfg.supports_protocol(shmb.protocol()) {
                    handoff_transaction.on_tx(shmb.into());

                    zs.kind = ZSliceKind::ShmPtr;
                    *ext_shm = Some(ShmType::new());
                }
            }
        }
    }
}

fn map_to_shmbuf<const ID: u8>(
    zbuf: &mut ZBuf,
    ext_shm: &mut Option<ShmType<ID>>,
    shmr: &ShmReader,
    handoff: &RxHandoffChannel,
    priority: Priority,
) -> ZResult<()> {
    let mut result = Ok(());
    if ext_shm.is_some() {
        *ext_shm = None;
        for zs in zbuf.zslices_mut() {
            if zs.kind == ZSliceKind::ShmPtr {
                let new_result = map_zslice_to_shmbuf(zs, shmr);
                // Handle RX handoff
                handoff.on_rx(priority);
                if let Err(e) = new_result {
                    result = Err(e);
                }
            }
        }
    }
    result
}

// Impl - Put
impl MapShm for Put {
    fn map_to_partner<ShmCfg: PartnerShmConfig>(
        &mut self,
        partner_shm_cfg: &ShmCfg,
        shm_provider: &Option<Arc<LazyShmProvider>>,
        handoff_transaction: &mut OpenTransaction,
    ) {
        let Self {
            payload, ext_shm, ..
        } = self;
        map_to_partner(
            payload,
            ext_shm,
            partner_shm_cfg,
            shm_provider,
            handoff_transaction,
        );
    }

    fn map_to_shmbuf(
        &mut self,
        shmr: &ShmReader,
        handoff: &RxHandoffChannel,
        priority: Priority,
    ) -> ZResult<()> {
        let Self {
            payload, ext_shm, ..
        } = self;
        map_to_shmbuf(payload, ext_shm, shmr, handoff, priority)
    }
}

// Impl - Query
impl MapShm for Query {
    fn map_to_partner<ShmCfg: PartnerShmConfig>(
        &mut self,
        partner_shm_cfg: &ShmCfg,
        shm_provider: &Option<Arc<LazyShmProvider>>,
        handoff_transaction: &mut OpenTransaction,
    ) {
        if let Self {
            ext_body: Some(QueryBodyType {
                payload, ext_shm, ..
            }),
            ..
        } = self
        {
            map_to_partner(
                payload,
                ext_shm,
                partner_shm_cfg,
                shm_provider,
                handoff_transaction,
            );
        }
    }

    fn map_to_shmbuf(
        &mut self,
        shmr: &ShmReader,
        handoff: &RxHandoffChannel,
        priority: Priority,
    ) -> ZResult<()> {
        if let Self {
            ext_body: Some(QueryBodyType {
                payload, ext_shm, ..
            }),
            ..
        } = self
        {
            map_to_shmbuf(payload, ext_shm, shmr, handoff, priority)?;
        }
        Ok(())
    }
}

// Impl - Reply
impl MapShm for Reply {
    fn map_to_partner<ShmCfg: PartnerShmConfig>(
        &mut self,
        partner_shm_cfg: &ShmCfg,
        shm_provider: &Option<Arc<LazyShmProvider>>,
        handoff_transaction: &mut OpenTransaction,
    ) {
        if let PushBody::Put(Put {
            payload, ext_shm, ..
        }) = &mut self.payload
        {
            map_to_partner(
                payload,
                ext_shm,
                partner_shm_cfg,
                shm_provider,
                handoff_transaction,
            );
        }
    }

    fn map_to_shmbuf(
        &mut self,
        shmr: &ShmReader,
        handoff: &RxHandoffChannel,
        priority: Priority,
    ) -> ZResult<()> {
        if let PushBody::Put(Put {
            payload, ext_shm, ..
        }) = &mut self.payload
        {
            map_to_shmbuf(payload, ext_shm, shmr, handoff, priority)?;
        }
        Ok(())
    }
}

// Impl - Err
impl MapShm for Err {
    fn map_to_partner<ShmCfg: PartnerShmConfig>(
        &mut self,
        partner_shm_cfg: &ShmCfg,
        shm_provider: &Option<Arc<LazyShmProvider>>,
        handoff_transaction: &mut OpenTransaction,
    ) {
        let Self {
            payload, ext_shm, ..
        } = self;
        map_to_partner(
            payload,
            ext_shm,
            partner_shm_cfg,
            shm_provider,
            handoff_transaction,
        );
    }

    fn map_to_shmbuf(
        &mut self,
        shmr: &ShmReader,
        handoff: &RxHandoffChannel,
        priority: Priority,
    ) -> ZResult<()> {
        let Self {
            payload, ext_shm, ..
        } = self;
        map_to_shmbuf(payload, ext_shm, shmr, handoff, priority)
    }
}

#[cold]
#[inline(never)]
pub fn map_zslice_to_shmbuf(zslice: &mut ZSlice, shmr: &ShmReader) -> ZResult<()> {
    let codec = Zenoh080::new();
    let mut reader = zslice.reader();

    // Deserialize the shminfo
    let shmbinfo: ShmBufInfo = codec.read(&mut reader).map_err(|e| zerror!("{:?}", e))?;

    // Mount shmbuf
    let smb = shmr.read_shmbuf(shmbinfo)?;

    // Replace the content of the slice
    *zslice = smb.into();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **ONE PEER'S STEADY FAILURE MUST NOT SILENCE ANOTHER PEER'S FIRST
    /// REPORT** (BER-2172).
    ///
    /// With a single process-wide limiter, a peer failing continuously took
    /// every window, so a NEW failure from a DIFFERENT peer was almost never
    /// the line that got printed -- the limiter blended peers the way a shared
    /// counter blends the routine with the broken. Asserted on the per-peer
    /// state rather than on log output, so it holds without capturing logs.
    #[test]
    fn a_steady_failure_from_one_peer_does_not_silence_another() {
        let noisy = ZenohIdProto::rand();
        let quiet = ZenohIdProto::rand();

        // The noisy peer discards twice in quick succession. The FIRST is
        // reported, the second is inside the window and is not.
        report_shm_rx_drop(noisy, &"first");
        let (total, reported, _) = shm_rx_drop_state(&noisy).expect("noisy peer not tracked");
        assert_eq!(
            (total, reported),
            (1, 1),
            "a peer's first discard must report"
        );

        report_shm_rx_drop(noisy, &"second");
        let (total, reported, _) = shm_rx_drop_state(&noisy).expect("noisy peer not tracked");
        assert_eq!(
            (total, reported),
            (2, 1),
            "a second discard inside the window must be counted but not reported"
        );

        // THE POINT: a different peer's first discard is reported even though
        // the window for the noisy one is still open.
        report_shm_rx_drop(quiet, &"first from the other peer");
        let (total, reported, _) = shm_rx_drop_state(&quiet).expect("quiet peer not tracked");
        assert_eq!(
            (total, reported),
            (1, 1),
            "another peer's first discard must report regardless of the noisy peer's window"
        );

        // And the peers' counts stay separate.
        assert_eq!(shm_rx_drop_state(&noisy).map(|s| s.0), Some(2));
    }

    /// **A DISCARDED MESSAGE IS REPORTED, AND THE RATE LIMIT CANNOT SILENCE THE
    /// FIRST ONE.**
    ///
    /// Two-sided, because a predicate that always reported would pass a
    /// one-sided test while reintroducing the flood the limit exists to prevent.
    #[test]
    fn a_dropped_shm_message_reports_once_per_window_and_always_the_first() {
        // THE FIRST DISCARD, in a process that has only just started, so the
        // window has NOT elapsed. Reported anyway: it is the one closest to
        // whatever changed, and the old code's silence here is the defect.
        assert!(
            shm_rx_drop_should_report(1, 5, 0),
            "the first discard must report even inside the window"
        );

        // THE QUIET DIRECTION: a later discard inside the window must NOT
        // report, or a malformed sender floods the log per message and the
        // flood is what hides a real line.
        assert!(
            !shm_rx_drop_should_report(2, SHM_RX_DROP_REPORT_EVERY_MS - 1, 0),
            "a discard inside the window must stay quiet"
        );
        assert!(
            !shm_rx_drop_should_report(9_999, 12_000, 11_999),
            "a high total does not by itself earn a report"
        );

        // AND IT RECOVERS: once the window has elapsed the next discard reports,
        // so a sustained failure is visible rather than reported once and
        // forgotten.
        assert!(
            shm_rx_drop_should_report(2, SHM_RX_DROP_REPORT_EVERY_MS, 0),
            "exactly at the window the next discard reports"
        );
        assert!(
            shm_rx_drop_should_report(500, 40_000, 12_000),
            "a sustained failure keeps reporting"
        );
    }

    /// **EAGER INIT MUST LEAVE NO BUILD FOR THE FIRST MESSAGE TO WAIT ON.**
    ///
    /// Asserted as "the pending state is unreachable afterwards" rather than as
    /// "the provider is Ready", because a host whose `RLIMIT_MEMLOCK` is below
    /// the pool cannot allocate at all -- and on such a host `Ready` is simply
    /// not available. `Error` is a correct outcome: it falls back to the network
    /// path, which is what a failed lazy build did too. What must NOT survive is
    /// `Initializing`, the state that means a message is waiting on a background
    /// build -- that wait is the latency `ShmInitMode::Init` promises to remove.
    #[test]
    fn init_eagerly_leaves_no_lazy_build_for_the_first_message() {
        let provider = LazyShmProvider::new(NonZeroUsize::new(1024 * 1024).unwrap(), 0);

        // THE CONTROL: untouched, it starts in the state that makes the first
        // message pay for the build. Without this the test could pass against a
        // provider that was never lazy to begin with.
        assert!(
            matches!(&*zlock!(provider.state), ProviderInitStateInner::Enabled(_)),
            "a fresh provider must start lazy, or this test proves nothing"
        );

        provider.init_eagerly();

        assert!(
            !matches!(
                &*zlock!(provider.state),
                ProviderInitStateInner::Enabled(_) | ProviderInitStateInner::Initializing(_)
            ),
            "eager init must settle the provider, leaving neither a lazy build nor a pending one"
        );
        assert!(
            !matches!(
                provider.try_get_provider(),
                ProviderInitState::Initializing(_)
            ),
            "the first message must not find a background build still running"
        );

        // IDEMPOTENT: the runtime may reach this more than once, and a second
        // call must not restart a build or discard a provider already in use.
        provider.init_eagerly();
        assert!(
            !matches!(
                provider.try_get_provider(),
                ProviderInitState::Initializing(_)
            ),
            "a second eager init must not reopen the lazy path"
        );
    }
}
