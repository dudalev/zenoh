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

use std::{
    collections::VecDeque,
    ops::Deref,
    sync::{
        atomic::{AtomicU32, Ordering::SeqCst},
        Arc,
    },
    time::{Duration, Instant},
};

use zenoh_buffers::{
    reader::{DidntRead, Reader},
    writer::{DidntWrite, Writer},
};
use zenoh_codec::{RCodec, WCodec, Zenoh080};
use zenoh_core::{bail, zerror, zlock};
use zenoh_result::ZResult;
use zenoh_shm::{
    api::common::types::ProtocolID, posix_shm::struct_in_shm::StructInSHM, version::SHM_VERSION,
};

/*************************************/
/*             Segment               */
/*************************************/
pub(crate) type AuthSegmentID = u32;
pub(crate) type AuthChallenge = u64;
pub(crate) type ShmCounterID = u16;

#[derive(Debug)]
#[repr(C)]
struct ShmTransportMetadata {
    id_count: u64,
    challenge: u64,
    version: u64,
    protocols: [ProtocolID; 256],
    // Allows to service 351 priority-enabled peers
    // TODO: Support growing counters array based on segment memory overcommit to control memory usage
    shm_counters: [AtomicU32; 762 + 2048],
}

impl ShmTransportMetadata {
    fn validate(&self, expected_challenge: AuthChallenge, s: &str) -> bool {
        if self.challenge != expected_challenge {
            tracing::debug!(
                "{} Challenge mismatch: expected: {}, found in shm: {}.",
                s,
                expected_challenge,
                self.challenge
            );
            return false;
        }

        if self.version != SHM_VERSION {
            tracing::warn!(
                "{} Version mismatch: ours: {}, theirs: {}.",
                s,
                SHM_VERSION,
                self.version
            );
            return false;
        }

        if self.id_count as usize > self.protocols.len() {
            tracing::warn!(
                "{} Protocol count mismatch: ours: {}, theirs: {}.",
                s,
                self.protocols.len(),
                self.id_count
            );
            return false;
        }

        true
    }

    fn protocols(&self) -> &[ProtocolID] {
        &self.protocols[..self.id_count as usize]
    }

    fn counter(&self, id: ShmCounterID) -> &AtomicU32 {
        &self.shm_counters[id as usize]
    }

    pub fn validate_counter_index(&self, id: ShmCounterID) -> bool {
        (id as usize) < self.shm_counters.len()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ShmTransportMetadataSegment {
    data: StructInSHM<AuthSegmentID, ShmTransportMetadata>,
}

impl ShmTransportMetadataSegment {
    fn create(challenge: AuthChallenge, shm_protocols: &[ProtocolID]) -> ZResult<Self> {
        let mut data: StructInSHM<AuthSegmentID, ShmTransportMetadata> = StructInSHM::create()?;

        if data.protocols.len() < shm_protocols.len() {
            bail!(
                "Too many protocols: {}. Max is {}",
                shm_protocols.len(),
                data.protocols.len()
            );
        }

        data.id_count = shm_protocols.len() as u64;
        data.challenge = challenge;
        data.version = SHM_VERSION;
        data.protocols[..shm_protocols.len()].copy_from_slice(shm_protocols);

        // We intentionally do not reset the counters here, as they are expected to be reset when leased.
        // This allows to rely on memory overcommit mechanics and save some physical memory pages for most setups
        // that don't really utilize all the counters
        // data.shm_counters
        //     .iter()
        //     .for_each(|counter| counter.store(0, std::sync::atomic::Ordering::Relaxed));

        Ok(Self { data })
    }

    fn open(id: AuthSegmentID) -> ZResult<Self> {
        let data = StructInSHM::open(id)?;
        Ok(Self { data })
    }
}

/// How recently a counter must have been released for its reuse to be worth
/// REPORTING (BER-2173).
///
/// A TX counter is decremented by the REMOTE peer, through shared memory, using
/// the index we gave it at establishment. So when our link closes and the lease
/// is released, the peer may still be decrementing that index until its own
/// teardown catches up. Reissuing it at once -- which `push_front` did, making
/// reuse LIFO and therefore immediate -- let a departing peer corrupt the
/// counter of a link that had just reset it to zero.
///
/// FIFO reuse is what fixes that, and it is not a marginal improvement: there
/// are 2810 counters per segment, so an index now returns only after every
/// other free index has been handed out. Reuse within a teardown skew requires
/// thousands of intervening leases in a few seconds.
///
/// This threshold is therefore a REPORTING one, not a gate. Gating would mean
/// refusing a lease, which turns a narrow race into a link that cannot use
/// shared memory at all -- a worse failure, and a visible one. So the lease is
/// always granted and the implausible case is made loud instead of silent. A
/// true guarantee needs a generation beside each counter, which changes the
/// shared segment layout and so the peer protocol.
const COUNTER_REUSE_REPORT_WINDOW: Duration = Duration::from_secs(5);

pub struct TXAuthSegment {
    segment: ShmTransportMetadataSegment,
    /// Free counter indices with the instant each was released, oldest first.
    ///
    /// `None` for an index that has never been leased, so a fresh segment hands
    /// out all of its counters without waiting.
    available_shm_counters: Arc<std::sync::Mutex<VecDeque<(ShmCounterID, Option<Instant>)>>>,
}

impl std::fmt::Debug for TXAuthSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TXAuthSegment")
            .field("segment", &self.segment)
            .field(
                "available_shm_counters",
                &zlock!(self.available_shm_counters).len(),
            )
            .finish()
    }
}

impl TXAuthSegment {
    pub fn create(challenge: AuthChallenge, shm_protocols: &[ProtocolID]) -> ZResult<Self> {
        let segment = ShmTransportMetadataSegment::create(challenge, shm_protocols)?;
        let available_shm_counters = Arc::new(std::sync::Mutex::new(
            (0..segment.data.shm_counters.len())
                .map(|index| (index as ShmCounterID, None))
                .collect(),
        ));

        Ok(Self {
            segment,
            available_shm_counters,
        })
    }

    /// How many handoff counters this segment holds.
    ///
    /// Exists so a test can walk the whole pool and assert that a released
    /// index returns to the BACK of the queue (BER-2173).
    #[cfg(test)]
    pub fn counter_capacity(&self) -> usize {
        self.segment.data.shm_counters.len()
    }

    pub fn validate(&self, expected_challenge: AuthChallenge, s: &str) -> bool {
        self.segment.data.validate(expected_challenge, s)
    }

    pub fn segment_id(&self) -> AuthSegmentID {
        self.segment.data.id()
    }
}

#[derive(Debug)]
pub struct ShmTXCounterLease {
    segment: Arc<TXAuthSegment>,
    counter_index: ShmCounterID,
}

impl ShmTXCounterLease {
    pub fn new(segment: Arc<TXAuthSegment>) -> ZResult<Self> {
        // The queue is oldest-first, so the front is the index that has been out
        // of use longest -- which is the one least likely to still be touched by
        // a departed peer.
        let taken = zlock!(segment.available_shm_counters).pop_front();
        let Some((counter_index, released_at)) = taken else {
            return Err(zerror!("No available SHM counters").into());
        };

        if let Some(at) = released_at {
            let since = at.elapsed();
            if since < COUNTER_REUSE_REPORT_WINDOW {
                tracing::warn!(
                    counter_index,
                    since_ms = since.as_millis(),
                    "reusing an SHM handoff counter released very recently, after every other \
                     counter in the segment: the peer that held it may still be decrementing \
                     it through shared memory, which would corrupt this link's count \
                     (BER-2173)"
                );
            }
        }

        // Reset counter to 0 when lease is created
        segment.segment.data.counter(counter_index).store(0, SeqCst);
        Ok(Self {
            segment,
            counter_index,
        })
    }

    pub fn id(&self) -> ShmCounterID {
        self.counter_index
    }

    pub fn add(&self, val: u32) {
        self.segment
            .segment
            .data
            .counter(self.counter_index)
            .fetch_add(val, SeqCst);
    }

    pub fn sub(&self, val: u32) {
        self.segment
            .segment
            .data
            .counter(self.counter_index)
            .fetch_sub(val, SeqCst);
    }

    pub fn counter(&self) -> u32 {
        self.segment
            .segment
            .data
            .counter(self.counter_index)
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Drop for ShmTXCounterLease {
    fn drop(&mut self) {
        // push_BACK, so reuse is FIFO: an index goes to the end of the queue and
        // every other free index is handed out before it comes round again.
        // `push_front` made reuse immediate, which is the worst possible order
        // for a counter a departing peer may still be touching (BER-2173).
        zlock!(self.segment.available_shm_counters)
            .push_back((self.counter_index, Some(Instant::now())));
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct RXAuthSegment {
    segment: ShmTransportMetadataSegment,
}

impl RXAuthSegment {
    pub fn open(id: AuthSegmentID) -> ZResult<Self> {
        let segment = ShmTransportMetadataSegment::open(id)?;
        Ok(Self { segment })
    }

    pub fn challenge(&self) -> AuthChallenge {
        self.segment.data.challenge
    }

    pub fn protocols(&self) -> &[ProtocolID] {
        self.segment.data.protocols()
    }
}

// Codec
impl<W> WCodec<&RXAuthSegment, &mut W> for Zenoh080
where
    W: Writer,
{
    type Output = Result<(), DidntWrite>;

    fn write(self, writer: &mut W, x: &RXAuthSegment) -> Self::Output {
        self.write(writer, &x.segment.data)
    }
}

impl<R> RCodec<RXAuthSegment, &mut R> for Zenoh080
where
    R: Reader,
{
    type Error = DidntRead;

    fn read(self, reader: &mut R) -> Result<RXAuthSegment, Self::Error> {
        let segment_data = self.read(&mut *reader)?;
        Ok(RXAuthSegment {
            segment: ShmTransportMetadataSegment { data: segment_data },
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ShmRXCounterLease {
    segment: Arc<RXAuthSegment>,
    counter_index: ShmCounterID,
}

impl ShmRXCounterLease {
    pub fn new(segment: Arc<RXAuthSegment>, counter_index: ShmCounterID) -> ZResult<Self> {
        segment
            .segment
            .data
            .validate_counter_index(counter_index)
            .then_some(Self {
                segment,
                counter_index,
            })
            .ok_or_else(|| zerror!("Invalid counter index: {}", counter_index).into())
    }

    pub fn counter_decrease(&self) {
        self.segment
            .segment
            .data
            .counter(self.counter_index)
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn segment(&self) -> &RXAuthSegment {
        &self.segment
    }
}

impl<W> WCodec<&ShmRXCounterLease, &mut W> for Zenoh080
where
    W: Writer,
{
    type Output = Result<(), DidntWrite>;

    fn write(self, writer: &mut W, x: &ShmRXCounterLease) -> Self::Output {
        self.write(&mut *writer, x.segment.deref())?;
        self.write(writer, &x.counter_index)
    }
}

impl<R> RCodec<ShmRXCounterLease, &mut R> for Zenoh080
where
    R: Reader,
{
    type Error = DidntRead;

    fn read(self, reader: &mut R) -> Result<ShmRXCounterLease, Self::Error> {
        let segment_data = self.read(&mut *reader)?;
        let counter_index = self.read(reader)?;
        Ok(ShmRXCounterLease {
            segment: Arc::new(RXAuthSegment {
                segment: ShmTransportMetadataSegment { data: segment_data },
            }),
            counter_index,
        })
    }
}

#[cfg(test)]
mod counter_reuse_tests {
    use zenoh_shm::api::protocol_implementations::posix::protocol_id::POSIX_PROTOCOL_ID;

    use super::{ShmTXCounterLease, TXAuthSegment};

    fn segment() -> std::sync::Arc<TXAuthSegment> {
        std::sync::Arc::new(
            TXAuthSegment::create(0x1234_5678, &[POSIX_PROTOCOL_ID]).expect("cannot create"),
        )
    }

    /// **A RELEASED COUNTER IS NOT HANDED STRAIGHT BACK OUT** (BER-2173).
    ///
    /// A TX counter is decremented by the REMOTE peer through shared memory,
    /// using the index we gave it at establishment. `push_front` made reuse
    /// LIFO, so the very next link got the index a departing peer might still
    /// be touching -- and the new lease resets it to zero, so the corruption is
    /// silent and unbounded.
    #[test]
    fn a_released_counter_is_not_reissued_immediately() {
        let segment = segment();

        let first = ShmTXCounterLease::new(segment.clone()).expect("cannot lease");
        let first_index = first.id();
        drop(first);

        let second = ShmTXCounterLease::new(segment.clone()).expect("cannot lease again");
        assert_ne!(
            second.id(),
            first_index,
            "a counter released a moment ago must not be the next one handed out: the peer \
             that held it may still be decrementing it"
        );
    }

    /// THE CONTROL: the released index is RETURNED to the pool, not leaked.
    ///
    /// Without this, a change that simply dropped released counters on the floor
    /// would satisfy the test above while slowly exhausting the segment -- a
    /// quieter failure than the race it replaces.
    #[test]
    fn a_released_counter_comes_back_within_one_cycle() {
        let segment = segment();
        let total = segment.counter_capacity();

        let first = ShmTXCounterLease::new(segment.clone()).expect("cannot lease");
        let first_index = first.id();
        drop(first);

        // Walk the whole pool. The released index must appear, and because reuse
        // is FIFO it must be the LAST one -- every other free index is handed out
        // before it comes round.
        let mut leases = Vec::with_capacity(total);
        for _ in 0..total {
            leases.push(ShmTXCounterLease::new(segment.clone()).expect("pool exhausted early"));
        }

        assert_eq!(
            leases.last().map(ShmTXCounterLease::id),
            Some(first_index),
            "the released counter must return to the BACK of the queue, and so be the last \
             of a full cycle"
        );
    }
}
