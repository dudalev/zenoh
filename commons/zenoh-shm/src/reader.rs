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

use std::{collections::BTreeMap, num::NonZeroUsize, ops::Deref, sync::Arc};

use zenoh_core::{bail, zerror, zlock, zread, zwrite};
use zenoh_result::ZResult;

use crate::{
    api::{
        client::shm_segment::ShmSegment,
        client_storage::ShmClientStorage,
        common::types::{ProtocolID, PtrInSegment, SegmentID},
        provider::chunk::ChunkDescriptor,
    },
    metadata::subscription::GLOBAL_METADATA_SUBSCRIPTION,
    statics::try_live_read,
    watchdog::confirmator::GLOBAL_CONFIRMATOR,
    ShmBufInfo, ShmBufInner,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ShmReader {
    client_storage: Arc<ShmClientStorage>,
}

impl Deref for ShmReader {
    type Target = ShmClientStorage;

    fn deref(&self) -> &Self::Target {
        &self.client_storage
    }
}

impl ShmReader {
    pub fn new(client_storage: Arc<ShmClientStorage>) -> Self {
        Self { client_storage }
    }

    /// Mount a peer's segment NOW, before any message refers to it.
    ///
    /// `ensure_data_segment` is reached only from `read_shmbuf`, i.e. from a
    /// message that already carries a chunk descriptor -- so a receiver cannot
    /// learn a publisher's segment id, and therefore cannot map its pool, until
    /// the first buffer arrives. Any failure to map it there is discovered at
    /// that moment, and the message is discarded (BER-2167).
    ///
    /// This lets the establishment handshake mount the peer's pool while both
    /// peers are known, so a failure is REPORTED against a known peer rather
    /// than surfacing later as a dropped message. It is not refused: the link
    /// stays up without shared memory from that peer.
    ///
    /// Mounting does NOT lock the segment -- only its owner does (BER-2170) --
    /// so this is not a locked-memory cost. What it does cost is a mapping and
    /// a shared flock held for the life of this process, because the reader's
    /// segment map is insert-only.
    ///
    /// Idempotent: `ensure_data_segment` returns an already-mounted segment
    /// from its map.
    ///
    /// Takes only `(protocol_id, segment)` because that is all the mount needs;
    /// the chunk id in a descriptor selects a buffer WITHIN a segment and is not
    /// required to attach one.
    pub fn premount_segment(
        &self,
        protocol_id: ProtocolID,
        segment: SegmentID,
    ) -> ZResult<PremountedSegment> {
        self.ensure_data_segment(
            protocol_id,
            &ChunkDescriptor {
                segment,
                // Not read by the mount: `ensure_data_segment` keys on
                // `(protocol_id, segment)` and attaches by `segment` alone.
                chunk: 0,
                len: NonZeroUsize::new(1).unwrap(),
            },
        )?;
        let id = get_global_id(protocol_id, segment);
        *zlock!(self.client_storage.premounts).entry(id).or_insert(0) += 1;
        Ok(PremountedSegment {
            storage: self.client_storage.clone(),
            id,
        })
    }

    pub fn read_shmbuf(&self, info: ShmBufInfo) -> ZResult<Arc<ShmBufInner>> {
        // Read does not increment the reference count as it is assumed
        // that the sender of this buffer has incremented it for us.

        // A receive thread can still be mapping buffers while the process exits,
        // by which point these statics may be FINALIZED. `.read()` PANICS there,
        // which turns a deliberate non-zero exit into a double fault whose
        // backtrace points here instead of at the real reason for the exit.
        // Failing the map is the honest outcome: the message cannot be read, and
        // the caller already handles that.
        let metadata = try_live_read!(GLOBAL_METADATA_SUBSCRIPTION)
            .map_err(|e| zerror!("SHM metadata subscription unavailable: {e}"))?
            .link(&info.metadata)?;
        // attach to the watchdog before doing other things
        let confirmed_metadata = try_live_read!(GLOBAL_CONFIRMATOR)
            .map_err(|e| zerror!("SHM confirmator unavailable: {e}"))?
            .add(metadata);

        // retrieve data descriptor from metadata
        let data_descriptor = confirmed_metadata.owned.header().data_descriptor();

        let segment = self.ensure_data_segment(
            confirmed_metadata
                .owned
                .header()
                .protocol
                .load(std::sync::atomic::Ordering::Relaxed),
            &data_descriptor,
        )?;
        let buf = segment.map(data_descriptor.chunk)?;
        let shmb = Arc::new(ShmBufInner {
            metadata: confirmed_metadata,
            buf: PtrInSegment::new(buf, Arc::new(segment)),
            info: info.clone(),
        });

        // Validate buffer
        match shmb.is_valid() {
            true => Ok(shmb),
            false => bail!("Buffer is invalidated"),
        }
    }

    fn ensure_data_segment(
        &self,
        protocol_id: ProtocolID,
        descriptor: &ChunkDescriptor,
    ) -> ZResult<Arc<dyn ShmSegment>> {
        let id = get_global_id(protocol_id, descriptor.segment);

        // fastest path: try to get access to already mounted SHM segment
        // read lock allows concurrent execution of multiple requests
        let r_guard = zread!(self.segments);
        if let Some(val) = r_guard.get(&id) {
            return Ok(val.clone());
        }
        // fastest path failed: need to mount a new segment

        // drop read lock because we're gonna obtain write lock further
        drop(r_guard);

        // find appropriate client
        let client = self
            .clients
            .get_clients()
            .get(&protocol_id)
            .ok_or_else(|| zerror!("Unsupported SHM protocol: {}", protocol_id))?;

        // obtain write lock...
        let mut w_guard = zwrite!(self.segments);

        // many concurrent threads may be racing for mounting this particular segment, so we must check again if the segment exists
        match w_guard.entry(id) {
            // (rare case) segment already mounted
            std::collections::btree_map::Entry::Occupied(occupied) => Ok(occupied.get().clone()),

            // (common case) mount a new segment and add it to the map
            std::collections::btree_map::Entry::Vacant(vacant) => {
                let new_segment = client.attach(descriptor.segment)?;
                Ok(vacant.insert(new_segment).clone())
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct ClientStorage<Inner>
where
    Inner: Sized,
{
    clients: BTreeMap<ProtocolID, Inner>,
}

impl<Inner: Sized> ClientStorage<Inner> {
    pub(crate) fn new(clients: BTreeMap<ProtocolID, Inner>) -> Self {
        Self { clients }
    }

    pub(crate) fn get_clients(&self) -> &BTreeMap<ProtocolID, Inner> {
        &self.clients
    }
}

/// # Safety
/// Only immutable access to internal container is allowed,
/// so we are Send if the contained type is Send
unsafe impl<Inner: Send> Send for ClientStorage<Inner> {}

/// # Safety
/// Only immutable access to internal container is allowed,
/// so we are Sync if the contained type is Sync
unsafe impl<Inner: Sync> Sync for ClientStorage<Inner> {}

/// A pre-mounted peer segment, held for as long as the link that asked for it.
///
/// Dropping it releases the mapping, and the shared flock that pins the
/// segment's file, once no other link holds the same one (BER-2170). Without
/// this a process keeps a mapping per peer it has ever handshaked with, and a
/// restarted peer's 16 MiB pool stays in /dev/shm until every process that met
/// it exits.
///
/// Releasing cannot invalidate a buffer in flight: `ShmBufInner` holds its own
/// `Arc` on the segment, so a live buffer keeps it alive whatever the map says,
/// and a later message simply mounts it again.
#[zenoh_macros::unstable_doc]
#[derive(Debug)]
pub struct PremountedSegment {
    storage: Arc<ShmClientStorage>,
    id: GlobalDataSegmentId,
}

impl Drop for PremountedSegment {
    fn drop(&mut self) {
        // The two locks are taken in this order ONLY here and in
        // `premount_segment`, and `ensure_data_segment` never takes
        // `premounts`, so there is no path that takes them the other way round.
        let mut premounts = zlock!(self.storage.premounts);
        match premounts.get_mut(&self.id) {
            Some(remaining) if *remaining > 1 => *remaining -= 1,
            Some(_) => {
                premounts.remove(&self.id);
                zwrite!(self.storage.segments).remove(&self.id);
            }
            // Not tracked: nothing to release. Reachable only if a lease
            // outlived its own bookkeeping, which no current path allows.
            None => {}
        }
    }
}

pub(crate) type GlobalDataSegmentId = u64;
fn get_global_id(protocol: ProtocolID, segment: SegmentID) -> GlobalDataSegmentId {
    static_assertions::assert_eq_size!(GlobalDataSegmentId, (ProtocolID, SegmentID));
    ((protocol as GlobalDataSegmentId) << 32) | (segment as GlobalDataSegmentId)
}
