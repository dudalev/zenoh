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
    fmt,
    ops::Add,
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU8, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::{Duration, Instant},
};

use crossbeam_utils::CachePadded;
use ringbuffer_spsc::{RingBuffer, RingBufferReader, RingBufferWriter};
use zenoh_buffers::{
    reader::{HasReader, Reader},
    writer::HasWriter,
    ZBuf,
};
use zenoh_codec::{transport::batch::BatchError, WCodec, Zenoh080};
use zenoh_config::{QueueAllocConf, QueueAllocMode, QueueSizeConf};
use zenoh_core::zlock;
use zenoh_protocol::{
    core::Priority,
    network::{NetworkMessageExt, NetworkMessageRef},
    transport::{
        fragment,
        fragment::FragmentHeader,
        frame::{self, FrameHeader},
        AtomicBatchSize, BatchSize, TransportMessage,
    },
};
use zenoh_sync::{event, Notifier, WaitDeadlineError, Waiter};

use super::{
    batch::{Encode, WBatch},
    priority::{TransportChannelTx, TransportPriorityTx},
};
use crate::common::batch::BatchConfig;

// Batches are moved all over the pipeline and are quite big (56B+), so they are boxed to optimize
// the moves. They are always reused, so there is no allocation performance penalty.
type BoxedWBatch = Box<WBatch>;

// Twice the queue cap, so that the ring has room beside a full queue for ephemeral batches,
// which are not counted against the cap (BER-2554).
const RBLEN: usize = 2 * QueueSizeConf::MAX;

// Inner structure to reuse serialization batches
struct StageInRefill {
    n_ref_r: Waiter,
    s_ref_r: RingBufferReader<BoxedWBatch, RBLEN>,
    batch_config: (usize, BatchConfig),
    batch_allocs: usize,
}

#[derive(Debug)]
pub(crate) struct TransportClosed;
impl fmt::Display for TransportClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "transport closed")
    }
}
impl std::error::Error for TransportClosed {}

impl StageInRefill {
    fn pull(&mut self) -> Option<BoxedWBatch> {
        match self.s_ref_r.pull() {
            Some(b) => Some(b),
            None if self.batch_allocs < self.batch_config.0 => {
                self.batch_allocs += 1;
                Some(Box::new(WBatch::new(self.batch_config.1)))
            }
            None => None,
        }
    }

    fn wait(&self) -> bool {
        self.n_ref_r.wait().is_ok()
    }

    fn wait_deadline(&self, instant: Instant) -> Result<bool, TransportClosed> {
        match self.n_ref_r.wait_deadline(instant) {
            Ok(()) => Ok(true),
            Err(WaitDeadlineError::Deadline) => Ok(false),
            Err(WaitDeadlineError::WaitError) => Err(TransportClosed),
        }
    }
}

lazy_static::lazy_static! {
   static ref LOCAL_EPOCH: Instant = Instant::now();
}

type AtomicMicroSeconds = AtomicU32;
type MicroSeconds = u32;

struct AtomicBackoff {
    active: CachePadded<AtomicBool>,
    bytes: CachePadded<AtomicBatchSize>,
    first_write: CachePadded<AtomicMicroSeconds>,
}

// Inner structure to link the initial stage with the final stage of the pipeline
struct StageInOut {
    n_out_w: Notifier,
    s_out_w: RingBufferWriter<BoxedWBatch, RBLEN>,
    atomic_backoff: Arc<AtomicBackoff>,
}

impl StageInOut {
    #[inline]
    fn notify(&self, bytes: BatchSize) {
        self.atomic_backoff.bytes.store(bytes, Ordering::Relaxed);
        if !self.atomic_backoff.active.load(Ordering::Relaxed) {
            let _ = self.n_out_w.notify();
        }
    }

    #[inline]
    fn move_batch(&mut self, batch: BoxedWBatch) {
        assert!(
            self.s_out_w.push(batch).is_none(),
            "a batch found the out ring full"
        );
        self.atomic_backoff.bytes.store(0, Ordering::Relaxed);
        let _ = self.n_out_w.notify();
    }
}

struct Current {
    batch: Option<BoxedWBatch>,
    status: Arc<TransmissionPipelineStatus>,
    prioflag: u8,
}

impl Current {
    fn notify_pending(&mut self) {
        self.status
            .pending
            .fetch_or(self.prioflag, Ordering::Relaxed);
    }

    fn reset_pending(&mut self) {
        self.status
            .pending
            .fetch_and(!self.prioflag, Ordering::Relaxed);
    }
}

// Inner structure containing mutexes for current serialization batch and SNs
struct StageInMutex {
    current: Arc<Mutex<Current>>,
    priority: TransportPriorityTx,
}

impl StageInMutex {
    #[inline]
    fn channel(&self, is_reliable: bool) -> MutexGuard<'_, TransportChannelTx> {
        if is_reliable {
            zlock!(self.priority.reliable)
        } else {
            zlock!(self.priority.best_effort)
        }
    }
}

#[derive(Debug)]
struct WaitTime {
    wait_time: Duration,
    max_wait_time: Option<Duration>,
}

impl WaitTime {
    fn new(wait_time: Duration, max_wait_time: Option<Duration>) -> Self {
        Self {
            wait_time,
            max_wait_time,
        }
    }

    fn advance(&mut self, instant: &mut Instant) {
        // grow wait_time exponentially
        self.wait_time = self.wait_time.saturating_mul(2);

        // check for waiting limits
        match &mut self.max_wait_time {
            // if we have reached the waiting limit, we do not increase wait instant
            Some(max_wait_time) if *max_wait_time == Duration::ZERO => {
                tracing::trace!("Backoff increase limit reached")
            }
            // if the leftover of waiting time is less than next iteration, we select leftover
            Some(max_wait_time) if *max_wait_time <= self.wait_time => {
                *instant += *max_wait_time;
                *max_wait_time = Duration::ZERO;
            }
            // if the leftover of waiting time is bigger than next iteration, select next iteration
            Some(max_wait_time) => {
                *instant += self.wait_time;
                *max_wait_time -= self.wait_time;
            }
            // just select next iteration without checking the upper limit
            None => {}
        }
    }

    fn wait_time(&self) -> Duration {
        self.wait_time
    }
}

#[derive(Clone)]
enum DeadlineSetting {
    Immediate,
    Finite(Instant),
}

struct LazyDeadline {
    deadline: Option<DeadlineSetting>,
    wait_time: WaitTime,
}

impl LazyDeadline {
    fn new(wait_time: WaitTime) -> Self {
        Self {
            deadline: None,
            wait_time,
        }
    }

    fn advance(&mut self) {
        match self.deadline().to_owned() {
            DeadlineSetting::Immediate => {}
            DeadlineSetting::Finite(mut instant) => {
                self.wait_time.advance(&mut instant);
                self.deadline = Some(DeadlineSetting::Finite(instant));
            }
        }
    }

    #[inline]
    fn deadline(&mut self) -> &mut DeadlineSetting {
        self.deadline
            .get_or_insert_with(|| match self.wait_time.wait_time() {
                Duration::ZERO => DeadlineSetting::Immediate,
                nonzero_wait_time => DeadlineSetting::Finite(Instant::now().add(nonzero_wait_time)),
            })
    }
}

struct Deadline {
    lazy_deadline: LazyDeadline,
}

impl Deadline {
    fn new(wait_time: Duration, max_wait_time: Option<Duration>) -> Self {
        Self {
            lazy_deadline: LazyDeadline::new(WaitTime::new(wait_time, max_wait_time)),
        }
    }

    #[inline]
    fn wait(&mut self, s_ref: &StageInRefill) -> Result<bool, TransportClosed> {
        match self.lazy_deadline.deadline() {
            DeadlineSetting::Immediate => Ok(false),
            DeadlineSetting::Finite(instant) => s_ref.wait_deadline(*instant),
        }
    }

    fn on_next_fragment(&mut self) {
        self.lazy_deadline.advance();
    }
}

// What became of a message given to the initial stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pushed {
    Queued,
    // No batch came free before the deadline.
    NoBatch,
    // The message does not fit this pipeline's batches even as fragments (BER-2554).
    Unfragmentable,
}

// This is the initial stage of the pipeline where messages are serliazed on
struct StageIn {
    s_ref: StageInRefill,
    s_out: StageInOut,
    mutex: StageInMutex,
    fragbuf: ZBuf,
    batching: bool,
    // used for stop fragment
    batch_config: BatchConfig,
}

impl StageIn {
    fn push_network_message(
        &mut self,
        msg: NetworkMessageRef,
        priority: Priority,
        deadline: &mut Deadline,
    ) -> Result<Pushed, TransportClosed> {
        // Lock the current serialization batch.
        let mut c_guard = zlock!(self.mutex.current);
        c_guard.notify_pending();

        macro_rules! zgetbatch_rets {
            ($($restore_sn:stmt)?) => {
                loop {
                    match c_guard.batch.take() {
                        Some(batch) => break batch,
                        None => match self.s_ref.pull() {
                            Some(mut batch) => {
                                batch.clear();
                                self.s_out.atomic_backoff.first_write.store(
                                    LOCAL_EPOCH.elapsed().as_micros() as MicroSeconds,
                                    Ordering::Relaxed,
                                );
                                break batch;
                            }
                            None => {
                                // Wait for an available batch until deadline
                                if !deadline.wait(&self.s_ref)? {
                                    // Still no available batch.
                                    // Restore the sequence number and drop the message
                                    $($restore_sn)?
                                    tracing::trace!(
                                        "Zenoh message dropped because it's over the deadline {:?}: {:?}",
                                        deadline.lazy_deadline.wait_time, msg
                                    );
                                    return Ok(Pushed::NoBatch);
                                }
                            }
                        },
                    }
                }
            };
        }

        macro_rules! zretok {
            ($batch:expr, $msg:expr) => {{
                if !self.batching || $msg.is_express() {
                    // Move out existing batch
                    self.s_out.move_batch($batch);
                    return Ok(Pushed::Queued);
                } else {
                    let bytes = $batch.len();
                    c_guard.batch = Some($batch);
                    drop(c_guard);
                    self.s_out.notify(bytes);
                    return Ok(Pushed::Queued);
                }
            }};
        }

        // Get the current serialization batch.
        let mut batch = zgetbatch_rets!();
        // Attempt the serialization on the current batch
        let e = match batch.encode(msg) {
            Ok(_) => zretok!(batch, msg),
            Err(e) => e,
        };

        // Lock the channel. We are the only one that will be writing on it.
        let mut tch = self.mutex.channel(msg.is_reliable());

        // A message dropped on this channel since its last frame leaves one SN unused, so that
        // the receiver sees a gap (BER-2554). A message that is not sent either restores
        // `restore_sn` and re-arms the skip it took.
        let restore_sn = tch.sn.now();
        let skipped = c_guard
            .status
            .take_skip(c_guard.prioflag, msg.is_reliable());
        if skipped {
            tch.sn.get();
        }
        macro_rules! restore_sn {
            () => {{
                tch.sn.set(restore_sn).unwrap();
                if skipped {
                    c_guard.status.arm_skip(c_guard.prioflag, msg.is_reliable());
                }
            }};
        }

        // Retrieve the next SN
        let sn = tch.sn.get();

        // The Frame
        let frame = FrameHeader {
            reliability: msg.reliability,
            sn,
            ext_qos: frame::ext::QoSType::new(priority),
        };

        if let BatchError::NewFrame = e {
            // Attempt a serialization with a new frame
            if batch.encode((msg, &frame)).is_ok() {
                zretok!(batch, msg);
            }
        }

        if !batch.is_empty() {
            // Move out existing batch
            self.s_out.move_batch(batch);
            batch = zgetbatch_rets!(restore_sn!());
        }

        // Attempt a second serialization on fully empty batch
        if batch.encode((msg, &frame)).is_ok() {
            zretok!(batch, msg);
        }

        // The second serialization attempt has failed. This means that the message is
        // too large for the current batch size: we need to fragment.
        // Reinsert the current batch for fragmentation.
        c_guard.batch = Some(batch);

        // Take the expandable buffer and serialize the totality of the message
        self.fragbuf.clear();

        let mut writer = self.fragbuf.writer();
        let codec = Zenoh080::new();
        codec.write(&mut writer, msg).unwrap();

        // Fragment the whole message
        let mut fragment = FragmentHeader {
            reliability: frame.reliability,
            more: true,
            sn,
            ext_qos: frame.ext_qos,
            ext_first: Some(fragment::ext::First::new()),
            ext_drop: None,
        };
        let mut reader = self.fragbuf.reader();
        while reader.can_read() {
            // Get the current serialization batch
            batch = zgetbatch_rets!({
                // If no fragment has been sent, the sequence number is just reset
                if fragment.ext_first.is_some() {
                    restore_sn!()
                // Otherwise, an ephemeral batch is created to send the stop fragment, which
                // takes the SN reserved for the next fragment
                } else {
                    let mut batch = Box::new(WBatch::new_ephemeral(self.batch_config));
                    self.fragbuf.clear();
                    fragment.ext_drop = Some(fragment::ext::Drop::new());
                    // A stop fragment has no payload, and a fragment with none does not
                    // encode. Its SN is then given back, so that a gap at the receiver means
                    // a message was lost, and not one that was then sent again in full
                    // (BER-2554).
                    if batch
                        .encode((&mut self.fragbuf.reader(), &mut fragment))
                        .is_ok()
                    {
                        self.s_out.move_batch(batch);
                    } else {
                        tch.sn.set(fragment.sn).unwrap();
                    }
                }
            });

            // Serialize the message fragment
            match batch.encode((&mut reader, &mut fragment)) {
                Ok(_) => {
                    // Reserve the next fragment's SN, if one follows: an SN reserved after the
                    // last fragment would go unused and read as a gap (BER-2554).
                    if reader.can_read() {
                        fragment.sn = tch.sn.get();
                    }
                    fragment.ext_first = None;
                    // Move the serialization batch into the OUT pipeline
                    self.s_out.move_batch(batch);
                }
                Err(_) => {
                    // With no fragment sent, the SNs taken are given back. With fragments
                    // sent, only the SN reserved for the next one is: going back below the
                    // sent ones would have the receiver drop the next frames as old. The
                    // drop itself leaves an SN unused at the next frame (BER-2554).
                    if fragment.ext_first.is_some() {
                        restore_sn!();
                    } else {
                        tch.sn.set(fragment.sn).unwrap();
                    }
                    // Reinsert the batch
                    c_guard.batch = Some(batch);
                    self.fragbuf.clear();
                    tracing::warn!(
                        "Zenoh message dropped because it can not be fragmented: {:?}",
                        msg
                    );
                    return Ok(Pushed::Unfragmentable);
                }
            }

            // adopt deadline for the next fragment
            deadline.on_next_fragment();
        }

        // Clean the fragbuf
        self.fragbuf.clear();

        Ok(Pushed::Queued)
    }

    #[inline]
    fn push_transport_message(&mut self, msg: TransportMessage) -> bool {
        // Lock the current serialization batch.
        let mut c_guard = zlock!(self.mutex.current);
        c_guard.notify_pending();

        macro_rules! zgetbatch_rets {
            () => {
                loop {
                    match c_guard.batch.take() {
                        Some(batch) => break batch,
                        None => match self.s_ref.pull() {
                            Some(mut batch) => {
                                batch.clear();
                                self.s_out.atomic_backoff.first_write.store(
                                    LOCAL_EPOCH.elapsed().as_micros() as MicroSeconds,
                                    Ordering::Relaxed,
                                );
                                break batch;
                            }
                            None => {
                                if !self.s_ref.wait() {
                                    return false;
                                }
                            }
                        },
                    }
                }
            };
        }

        macro_rules! zretok {
            ($batch:expr) => {{
                if !self.batching {
                    // Move out existing batch
                    self.s_out.move_batch($batch);
                    return true;
                } else {
                    let bytes = $batch.len();
                    c_guard.batch = Some($batch);
                    drop(c_guard);
                    self.s_out.notify(bytes);
                    return true;
                }
            }};
        }

        // Get the current serialization batch.
        let mut batch = zgetbatch_rets!();
        // Attempt the serialization on the current batch
        match batch.encode(&msg) {
            Ok(_) => zretok!(batch),
            Err(_) => {
                if !batch.is_empty() {
                    self.s_out.move_batch(batch);
                    batch = zgetbatch_rets!();
                }
            }
        };

        // The first serialization attempt has failed. This means that the current
        // batch is full. Therefore, we move the current batch to stage out.
        batch.encode(&msg).is_ok()
    }
}

// The result of the pull operation
enum Pull {
    Some(BoxedWBatch),
    None,
    Backoff(MicroSeconds),
}

// Inner structure to keep track and signal backoff operations
#[derive(Clone)]
struct Backoff {
    threshold: MicroSeconds,
    last_bytes: BatchSize,
    atomic: Arc<AtomicBackoff>,
    // active: bool,
}

impl Backoff {
    fn new(threshold: Duration, atomic: Arc<AtomicBackoff>) -> Self {
        Self {
            threshold: threshold.as_micros() as MicroSeconds,
            last_bytes: 0,
            atomic,
            // active: false,
        }
    }
}

// Inner structure to link the final stage with the initial stage of the pipeline
struct StageOutIn {
    s_out_r: RingBufferReader<BoxedWBatch, RBLEN>,
    current: Arc<Mutex<Current>>,
    backoff: Backoff,
}

impl StageOutIn {
    #[inline]
    fn try_pull(&mut self) -> Pull {
        if let Some(batch) = self.s_out_r.pull() {
            self.backoff.atomic.active.store(false, Ordering::Relaxed);
            return Pull::Some(batch);
        }

        self.try_pull_deep()
    }

    fn try_pull_deep(&mut self) -> Pull {
        // Verify first backoff is not active
        let mut pull = !self.backoff.atomic.active.load(Ordering::Relaxed);

        // If backoff is active, verify the current number of bytes is equal to the old number
        // of bytes seen in the previous backoff iteration
        if !pull {
            let new_bytes = self.backoff.atomic.bytes.load(Ordering::Relaxed);
            let old_bytes = self.backoff.last_bytes;
            self.backoff.last_bytes = new_bytes;

            pull = new_bytes == old_bytes;
        }

        // Verify that we have not been doing backoff for too long
        let mut backoff = 0;
        if !pull {
            let diff = (LOCAL_EPOCH.elapsed().as_micros() as MicroSeconds)
                .saturating_sub(self.backoff.atomic.first_write.load(Ordering::Relaxed));

            if diff >= self.backoff.threshold {
                pull = true;
            } else {
                backoff = self.backoff.threshold - diff;
            }
        }

        if pull {
            // It seems no new bytes have been written on the batch, try to pull
            if let Ok(mut g) = self.current.try_lock() {
                self.backoff.atomic.active.store(false, Ordering::Relaxed);

                // First try to pull from stage OUT to make sure we are not in the case
                // where new_bytes == old_bytes are because of two identical serializations
                if let Some(batch) = self.s_out_r.pull() {
                    return Pull::Some(batch);
                }

                // An incomplete (non-empty) batch may be available in the state IN pipeline.
                g.reset_pending();
                match g.batch.take() {
                    Some(batch) => {
                        return Pull::Some(batch);
                    }
                    None => {
                        return Pull::None;
                    }
                }
            }
        }

        // Activate backoff
        self.backoff.atomic.active.store(true, Ordering::Relaxed);

        // Do backoff
        Pull::Backoff(backoff)
    }
}

struct StageOutRefill {
    n_ref_w: Notifier,
    s_ref_w: RingBufferWriter<BoxedWBatch, RBLEN>,
}

impl StageOutRefill {
    fn refill(&mut self, batch: BoxedWBatch) {
        assert!(self.s_ref_w.push(batch).is_none());
        let _ = self.n_ref_w.notify();
    }
}

struct StageOut {
    s_in: StageOutIn,
    s_ref: StageOutRefill,
    n_out_r: Waiter,
}

impl StageOut {
    #[inline]
    fn try_pull(&mut self) -> Pull {
        self.s_in.try_pull()
    }

    #[inline]
    fn refill(&mut self, batch: BoxedWBatch) {
        self.s_ref.refill(batch);
    }

    fn drain(&mut self, guard: &mut MutexGuard<'_, Current>) -> Vec<BoxedWBatch> {
        let mut batches = vec![];
        // Empty the ring buffer
        while let Some(batch) = self.s_in.s_out_r.pull() {
            batches.push(batch);
        }
        // Take the current batch
        if let Some(batch) = guard.batch.take() {
            batches.push(batch);
        }
        batches
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransmissionPipelineConf {
    pub(crate) batch: BatchConfig,
    pub(crate) queue_size: [usize; Priority::NUM],
    pub(crate) wait_before_drop: Duration,
    pub(crate) max_wait_before_drop_fragments: Duration,
    pub(crate) wait_before_close: Duration,
    pub(crate) batching_enabled: bool,
    pub(crate) batching_time_limit: Duration,
    pub(crate) queue_alloc: QueueAllocConf,
}

// A 2-stage transmission pipeline
pub(crate) struct TransmissionPipeline;
impl TransmissionPipeline {
    // A MPSC pipeline
    pub(crate) fn make(
        config: TransmissionPipelineConf,
        priority: &[TransportPriorityTx],
        link_supports_priority: bool,
    ) -> (TransmissionPipelineProducer, TransmissionPipelineConsumer) {
        let status = Arc::new(TransmissionPipelineStatus {
            disabled: AtomicBool::new(false),
            congested: AtomicU8::new(0),
            pending: AtomicU8::new(0),
            skip_reliable: AtomicU8::new(0),
            skip_best_effort: AtomicU8::new(0),
            waits: Waits {
                wait_before_drop: config.wait_before_drop,
                max_wait_before_drop_fragments: config.max_wait_before_drop_fragments,
                wait_before_close: config.wait_before_close,
            },
        });

        let mut stage_in = vec![];
        let mut stage_out = vec![];

        let default_queue_size = [config.queue_size[Priority::DEFAULT as usize]];
        let size_iter = if priority.len() == 1 {
            default_queue_size.iter()
        } else {
            config.queue_size.iter()
        };

        // Create the channel for notifying that new batches are in the out ring buffer
        // This is a MPSC channel
        let (n_out_w, n_out_r) = event::new();

        for (prio, num) in size_iter.enumerate() {
            assert!(*num != 0 && *num <= QueueSizeConf::MAX);

            // Create the refill ring buffer
            // This is a SPSC ring buffer
            let (mut s_ref_w, s_ref_r) = RingBuffer::<BoxedWBatch, RBLEN>::init();
            let mut batch_allocs = 0;
            if *config.queue_alloc.mode() == QueueAllocMode::Init {
                // Fill the refill ring buffer with batches
                for _ in 0..*num {
                    let batch = Box::new(WBatch::new(config.batch));
                    batch_allocs += 1;
                    assert!(s_ref_w.push(batch).is_none());
                }
            }
            // Create the channel for notifying that new batches are in the refill ring buffer
            // This is a SPSC channel
            let (n_ref_w, n_ref_r) = event::new();
            // If link supports priority, use per priority channel to notify for new batches
            let (n_out_w, n_out_r) = if link_supports_priority {
                event::new()
            } else {
                (n_out_w.clone(), n_out_r.clone())
            };

            // Create the refill ring buffer
            // This is a SPSC ring buffer
            let (s_out_w, s_out_r) = RingBuffer::<BoxedWBatch, RBLEN>::init();
            let current = Arc::new(Mutex::new(Current {
                batch: None,
                status: status.clone(),
                prioflag: 1 << (prio as u8),
            }));
            let bytes = Arc::new(AtomicBackoff {
                active: CachePadded::new(AtomicBool::new(false)),
                bytes: CachePadded::new(AtomicBatchSize::new(0)),
                first_write: CachePadded::new(AtomicMicroSeconds::new(
                    LOCAL_EPOCH.elapsed().as_micros() as MicroSeconds,
                )),
            });

            stage_in.push(Mutex::new(StageIn {
                s_ref: StageInRefill {
                    n_ref_r,
                    s_ref_r,
                    batch_config: (*num, config.batch),
                    batch_allocs,
                },
                s_out: StageInOut {
                    n_out_w: n_out_w.clone(),
                    s_out_w,
                    atomic_backoff: bytes.clone(),
                },
                mutex: StageInMutex {
                    current: current.clone(),
                    priority: priority[prio].clone(),
                },
                fragbuf: ZBuf::empty(),
                batching: config.batching_enabled,
                batch_config: config.batch,
            }));

            // The stage out for this priority
            stage_out.push(StageOut {
                s_in: StageOutIn {
                    s_out_r,
                    current,
                    backoff: Backoff::new(config.batching_time_limit, bytes),
                },
                s_ref: StageOutRefill { n_ref_w, s_ref_w },
                n_out_r,
            });
        }

        let producer = TransmissionPipelineProducer {
            stage_in: stage_in.into_boxed_slice().into(),
            status: status.clone(),
        };
        let consumer = TransmissionPipelineConsumer {
            stage_out: stage_out.into_boxed_slice(),
            n_out_r: (!link_supports_priority).then_some(n_out_r),
            status,
        };

        (producer, consumer)
    }
}

struct TransmissionPipelineStatus {
    // The whole pipeline is enabled or disabled
    disabled: AtomicBool,
    // Bitflags to indicate the given priority queue is congested
    congested: AtomicU8,
    // Bitflags to indicate the given priority queue has messages waiting to be sent
    pending: AtomicU8,
    // Bitflags, per priority queue, to indicate a message was dropped on that queue's reliable
    // or best-effort channel and the next frame there is to leave one SN unused (BER-2554).
    skip_reliable: AtomicU8,
    skip_best_effort: AtomicU8,
    // wait parameters
    // Note: this is placed here to optimize TransmissionPipelineProducer memory layout and improve performance
    waits: Waits,
}

impl TransmissionPipelineStatus {
    fn set_disabled(&self, status: bool) {
        self.disabled.store(status, Ordering::Relaxed);
    }

    fn is_disabled(&self) -> bool {
        self.disabled.load(Ordering::Relaxed)
    }

    fn set_congested(&self, priority: Priority, status: bool) {
        let prioflag = 1 << priority as u8;
        if status {
            self.congested.fetch_or(prioflag, Ordering::Relaxed);
        } else {
            self.congested.fetch_and(!prioflag, Ordering::Relaxed);
        }
    }

    fn is_congested(&self, priority: Priority) -> bool {
        let prioflag = 1 << priority as u8;
        self.congested.load(Ordering::Relaxed) & prioflag != 0
    }

    fn get_pending(&self) -> Option<Priority> {
        let pending = self.pending.load(Ordering::Relaxed);
        let prio = pending.trailing_zeros();
        // Don't use try_from directly because it unfortunately returns a costly error
        if prio as usize >= Priority::NUM {
            return None;
        }
        Some(Priority::try_from(prio as u8).unwrap())
    }

    fn skip_flags(&self, is_reliable: bool) -> &AtomicU8 {
        if is_reliable {
            &self.skip_reliable
        } else {
            &self.skip_best_effort
        }
    }

    fn arm_skip(&self, prioflag: u8, is_reliable: bool) {
        self.skip_flags(is_reliable)
            .fetch_or(prioflag, Ordering::Relaxed);
    }

    // The load keeps a read-modify-write off the path of a frame that has nothing to skip.
    fn take_skip(&self, prioflag: u8, is_reliable: bool) -> bool {
        let flags = self.skip_flags(is_reliable);
        flags.load(Ordering::Relaxed) & prioflag != 0
            && flags.fetch_and(!prioflag, Ordering::Relaxed) & prioflag != 0
    }
}

#[derive(Clone)]
struct Waits {
    wait_before_drop: Duration,
    max_wait_before_drop_fragments: Duration,

    wait_before_close: Duration,
}

#[derive(Clone)]
pub(crate) struct TransmissionPipelineProducer {
    // Each priority queue has its own Mutex
    stage_in: Arc<[Mutex<StageIn>]>,
    status: Arc<TransmissionPipelineStatus>,
}

impl TransmissionPipelineProducer {
    #[inline]
    pub(crate) fn push_network_message(
        &self,
        msg: NetworkMessageRef,
    ) -> Result<bool, TransportClosed> {
        // Fail fast if the pipeline has already been disabled (e.g. because a previous
        // non droppable message could not be pushed and the transport is being closed).
        // This prevents threads from blocking up to `wait_before_close` on a transport
        // that is already known to be unresponsive.
        if self.status.is_disabled() {
            return Err(TransportClosed);
        }
        let (idx, priority) = self.queue(msg);
        // Every drop below leaves one SN unused at the next frame on its channel (BER-2554).
        let prioflag = 1 << (idx as u8);

        // If message is droppable, compute a deadline after which the sample could be dropped
        let (wait_time, max_wait_time) = if msg.is_droppable() {
            // Checked if we are blocked on the priority queue and we drop directly the message
            if self.status.is_congested(priority) {
                self.status.arm_skip(prioflag, msg.is_reliable());
                return Ok(false);
            }
            (
                self.status.waits.wait_before_drop,
                Some(self.status.waits.max_wait_before_drop_fragments),
            )
        } else {
            (self.status.waits.wait_before_close, None)
        };
        let mut deadline = Deadline::new(wait_time, max_wait_time);
        // Lock the channel. We are the only one that will be writing on it.
        let mut queue = zlock!(self.stage_in[idx]);
        // Check again for congestion in case it happens when blocking on the mutex.
        if msg.is_droppable() && self.status.is_congested(priority) {
            self.status.arm_skip(prioflag, msg.is_reliable());
            return Ok(false);
        }
        let mut pushed = queue.push_network_message(msg, priority, &mut deadline)?;
        // If the message cannot be sent for want of a batch, mark the pipeline as congested.
        if pushed == Pushed::NoBatch {
            self.status.set_congested(priority, true);
            // During the time between deadline wakeup and setting the congested flag,
            // all batches could have been refilled (especially if there is a single one),
            // so try again with the same already expired deadline.
            pushed = queue.push_network_message(msg, priority, &mut deadline)?;
            // If the message is sent in the end, reset the status.
            // Setting the status to `true` is only done with the stage_in mutex acquired,
            // so it is not possible that further messages see the congestion flag set
            // after this point.
            if pushed == Pushed::Queued {
                self.status.set_congested(priority, false);
            }
            // There is one edge case that is fortunately supported: if the message that
            // has been pushed again is fragmented, we might have some batches actually
            // refilled, but still end with dropping the message, not resetting the
            // congested flag in that case. However, if some batches were available,
            // that means that they would have still been pushed, so we can expect them to
            // be refilled, and they will eventually unset the congested flag.
        }
        let sent = pushed == Pushed::Queued;
        if !sent {
            self.status.arm_skip(prioflag, msg.is_reliable());
        }
        Ok(sent)
    }

    // If the queue is not QoS, it means that we only have one priority with index 0.
    #[inline]
    fn queue(&self, msg: NetworkMessageRef) -> (usize, Priority) {
        if self.stage_in.len() > 1 {
            let priority = msg.priority();
            (priority as usize, priority)
        } else {
            (0, Priority::DEFAULT)
        }
    }

    /// Leaves one SN unused at the next frame on `msg`'s channel, for a message dropped before
    /// it reached the pipeline, so that the receiver sees a gap (BER-2554).
    #[cfg(feature = "unstable")]
    pub(crate) fn report_drop(&self, msg: NetworkMessageRef) {
        let (idx, _) = self.queue(msg);
        self.status.arm_skip(1 << (idx as u8), msg.is_reliable());
    }

    #[inline]
    pub(crate) fn push_transport_message(&self, msg: TransportMessage, priority: Priority) -> bool {
        // If the queue is not QoS, it means that we only have one priority with index 0.
        let priority = if self.stage_in.len() > 1 {
            priority as usize
        } else {
            0
        };
        // Lock the channel. We are the only one that will be writing on it.
        let mut queue = zlock!(self.stage_in[priority]);
        queue.push_transport_message(msg)
    }

    /// Marks the pipeline as disabled without acquiring the `stage_in` locks.
    ///
    /// Contrary to [`Self::disable`], this method can safely be called while another
    /// thread is blocked pushing a message on this pipeline (such a thread holds the
    /// `stage_in` lock until its deadline expires). New network message pushes fail
    /// fast with [`TransportClosed`]. This does not cancel an in-flight link write;
    /// the caller must arrange teardown to stop a stalled consumer task.
    pub(crate) fn mark_disabled(&self) {
        self.status.set_disabled(true);
    }

    pub(crate) fn disable(&self) {
        self.status.set_disabled(true);

        // Acquire all the locks, in_guard first, out_guard later
        // Use the same locking order as in drain to avoid deadlocks
        let mut in_guards: Vec<MutexGuard<'_, StageIn>> =
            self.stage_in.iter().map(|x| zlock!(x)).collect();

        // Unblock waiting pullers
        for ig in in_guards.iter_mut() {
            ig.s_out.notify(BatchSize::MAX);
        }
    }
}

pub(crate) struct TransmissionPipelineConsumer {
    // A single Mutex for all the priority queues
    stage_out: Box<[StageOut]>,
    n_out_r: Option<Waiter>,
    status: Arc<TransmissionPipelineStatus>,
}

pub(crate) trait PipelineConsumer {
    #[allow(private_interfaces)]
    fn status(&self) -> &TransmissionPipelineStatus;
    // Refuses every later push, for a link that can no longer write what is pushed (BER-2554).
    fn disable(&self) {
        self.status().set_disabled(true);
    }
    fn stage_pull(&mut self) -> Result<(BoxedWBatch, Priority), Option<MicroSeconds>>;
    fn n_out_r(&self) -> &Waiter;
    async fn pull(&mut self) -> Option<(BoxedWBatch, Priority)> {
        while !self.status().is_disabled() {
            let backoff = match self.stage_pull() {
                Ok(res) => return Some(res),
                Err(b) => b,
            };
            // In case of writing many small messages, `recv_async()` will most likely return immediately.
            // While trying to pull from the queue, the stage_in `lock()` will most likely taken, leading to
            // a spinning behaviour while attempting to take the lock. Yield the current task to avoid
            // spinning the current task indefinitely.
            tokio::task::yield_now().await;

            // Wait for the backoff to expire or for a new message
            let backoff_wait = match backoff {
                Some(b) => {
                    tokio::time::timeout(
                        Duration::from_micros(b as u64),
                        self.n_out_r().wait_async(),
                    )
                    .await
                }
                None => Ok(self.n_out_r().wait_async().await),
            };
            match backoff_wait {
                Ok(Ok(())) => {
                    // We have received a notification from the channel that some bytes are available, retry to pull.
                }
                Ok(Err(_channel_error)) => {
                    // The channel is closed, we can't be notified anymore. Break the loop and return None.
                    break;
                }
                Err(_timeout) => {
                    // The backoff timeout expired. Be aware that tokio timeout may not sleep for short duration since
                    // it has time resolution of 1ms: https://docs.rs/tokio/latest/tokio/time/fn.sleep.html
                }
            }
        }
        None
    }
    fn refill(&mut self, batch: BoxedWBatch, priority: Priority);
    fn drain(&mut self) -> Vec<(BoxedWBatch, Priority)>;
}

impl PipelineConsumer for TransmissionPipelineConsumer {
    #[allow(private_interfaces)]
    fn status(&self) -> &TransmissionPipelineStatus {
        &self.status
    }

    fn stage_pull(&mut self) -> Result<(BoxedWBatch, Priority), Option<MicroSeconds>> {
        while let Some(prio) = self.status.get_pending() {
            let queue = &mut self.stage_out[prio as usize];
            match queue.try_pull() {
                Pull::Some(batch) => return Ok((batch, prio)),
                Pull::Backoff(deadline) => return Err(Some(deadline)),
                Pull::None => {}
            }
        }
        Err(None)
    }

    fn n_out_r(&self) -> &Waiter {
        self.n_out_r.as_ref().unwrap()
    }

    fn refill(&mut self, batch: BoxedWBatch, priority: Priority) {
        if !batch.is_ephemeral() {
            self.stage_out[priority as usize].refill(batch);
            self.status.set_congested(priority, false);
        }
    }

    fn drain(&mut self) -> Vec<(BoxedWBatch, Priority)> {
        // Drain the remaining batches
        let mut batches = vec![];

        // Acquire all the locks, in_guard first, out_guard later
        // Use the same locking order as in disable to avoid deadlocks
        let locks = self
            .stage_out
            .iter()
            .map(|x| x.s_in.current.clone())
            .collect::<Vec<_>>();
        let mut currents: Vec<_> = locks.iter().map(|x| zlock!(x)).collect::<Vec<_>>();

        for (prio, s_out) in self.stage_out.iter_mut().enumerate() {
            let mut bs = s_out.drain(&mut currents[prio]);
            for b in bs.drain(..) {
                batches.push((b, Priority::try_from(prio as u8).unwrap()));
            }
        }

        batches
    }
}

impl TransmissionPipelineConsumer {
    pub(crate) fn split(self) -> Vec<SplitTransmissionPipelineConsumer> {
        assert!(self.n_out_r.is_none());
        self.stage_out
            .into_vec()
            .into_iter()
            .enumerate()
            .map(|(prio, stage_out)| SplitTransmissionPipelineConsumer {
                priority: Priority::try_from(prio as u8).unwrap(),
                stage_out,
                status: self.status.clone(),
            })
            .collect()
    }
}

pub(crate) struct SplitTransmissionPipelineConsumer {
    priority: Priority,
    stage_out: StageOut,
    status: Arc<TransmissionPipelineStatus>,
}

impl SplitTransmissionPipelineConsumer {
    pub(crate) fn priority(&self) -> Priority {
        self.priority
    }
}

impl PipelineConsumer for SplitTransmissionPipelineConsumer {
    #[allow(private_interfaces)]
    fn status(&self) -> &TransmissionPipelineStatus {
        &self.status
    }

    fn stage_pull(&mut self) -> Result<(BoxedWBatch, Priority), Option<MicroSeconds>> {
        match self.stage_out.try_pull() {
            Pull::Some(batch) => Ok((batch, self.priority)),
            Pull::Backoff(deadline) => Err(Some(deadline)),
            Pull::None => Err(None),
        }
    }

    fn n_out_r(&self) -> &Waiter {
        &self.stage_out.n_out_r
    }

    fn refill(&mut self, batch: BoxedWBatch, priority: Priority) {
        if !batch.is_ephemeral() {
            debug_assert_eq!(self.priority, priority);
            self.stage_out.refill(batch);
            self.status.set_congested(priority, false);
        }
    }

    fn drain(&mut self) -> Vec<(BoxedWBatch, Priority)> {
        let current = self.stage_out.s_in.current.clone();
        let batches = self.stage_out.drain(&mut current.lock().unwrap());
        batches.into_iter().map(|b| (b, self.priority)).collect()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        convert::TryFrom,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    };

    use tokio::{task, time::timeout};
    use zenoh_buffers::reader::{DidntRead, HasReader};
    use zenoh_codec::{RCodec, Zenoh080};
    use zenoh_config::{QueueAllocConf, QueueAllocMode};
    use zenoh_protocol::{
        core::{Bits, CongestionControl, Priority, Reliability},
        network::{ext, NetworkBody, NetworkMessage, Push},
        transport::{BatchSize, Fragment, Frame, TransportBody, TransportSn},
    };
    use zenoh_result::ZResult;

    use super::*;

    const SLEEP: Duration = Duration::from_millis(100);
    const TIMEOUT: Duration = Duration::from_secs(60);

    const CONFIG_STREAMED: TransmissionPipelineConf = TransmissionPipelineConf {
        batch: BatchConfig {
            mtu: BatchSize::MAX,
            is_streamed: true,
            #[cfg(feature = "transport_compression")]
            is_compression: true,
        },
        queue_size: [1; Priority::NUM],
        batching_enabled: true,
        wait_before_drop: Duration::from_millis(1),
        max_wait_before_drop_fragments: Duration::from_millis(1024),
        wait_before_close: Duration::from_secs(5),
        batching_time_limit: Duration::from_micros(1),
        queue_alloc: QueueAllocConf {
            mode: QueueAllocMode::Init,
        },
    };

    const CONFIG_NOT_STREAMED: TransmissionPipelineConf = TransmissionPipelineConf {
        batch: BatchConfig {
            mtu: BatchSize::MAX,
            is_streamed: false,
            #[cfg(feature = "transport_compression")]
            is_compression: false,
        },
        queue_size: [1; Priority::NUM],
        batching_enabled: true,
        wait_before_drop: Duration::from_millis(1),
        max_wait_before_drop_fragments: Duration::from_millis(1024),
        wait_before_close: Duration::from_secs(5),
        batching_time_limit: Duration::from_micros(1),
        queue_alloc: QueueAllocConf {
            mode: QueueAllocMode::Init,
        },
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tx_pipeline_flow() -> ZResult<()> {
        fn schedule(queue: TransmissionPipelineProducer, num_msg: usize, payload_size: usize) {
            // Send reliable messages
            let key = "test".into();

            let message = NetworkMessage::from(Push {
                wire_expr: key,
                ext_qos: ext::QoSType::new(Priority::Control, CongestionControl::Block, false),
                ..Push::from(vec![0_u8; payload_size])
            });

            println!(
                "Pipeline Flow [>>>]: Sending {num_msg} messages with payload size of {payload_size} bytes"
            );
            for i in 0..num_msg {
                println!(
                    "Pipeline Flow [>>>]: Pushed {} msgs ({payload_size} bytes)",
                    i + 1
                );
                queue.push_network_message(message.as_ref()).unwrap();
            }
        }

        async fn consume(mut queue: impl PipelineConsumer, num_msg: usize) {
            let mut batches: usize = 0;
            let mut bytes: usize = 0;
            let mut msgs: usize = 0;
            let mut fragments: usize = 0;

            while msgs != num_msg {
                let (batch, priority) = queue.pull().await.unwrap();
                batches += 1;
                bytes += batch.len() as usize;
                // Create a ZBuf for deserialization starting from the batch
                let bytes = batch.as_slice();
                // Deserialize the messages
                let mut reader = bytes.reader();
                let codec = Zenoh080::new();

                loop {
                    let res: Result<TransportMessage, DidntRead> = codec.read(&mut reader);
                    match res {
                        Ok(msg) => {
                            match msg.body {
                                TransportBody::Frame(Frame { payload, .. }) => {
                                    msgs += payload.len()
                                }
                                TransportBody::Fragment(Fragment { more, .. }) => {
                                    fragments += 1;
                                    if !more {
                                        msgs += 1;
                                    }
                                }
                                _ => {
                                    msgs += 1;
                                }
                            }
                            println!("Pipeline Flow [<<<]: Pulled {} msgs", msgs + 1);
                        }
                        Err(_) => break,
                    }
                }
                println!("Pipeline Flow [+++]: Refill {} msgs", msgs + 1);
                // Reinsert the batch
                queue.refill(batch, priority);
            }

            println!(
                "Pipeline Flow [<<<]: Received {msgs} messages, {bytes} bytes, {batches} batches, {fragments} fragments"
            );
        }

        // Pipeline priorities
        let tct = TransportPriorityTx::make(Bits::from(TransportSn::MAX))?;
        let priorities = vec![tct];

        // Total amount of bytes to send in each test
        let bytes: usize = 100_000_000;
        let max_msgs: usize = 1_000;
        // Payload size of the messages
        let payload_sizes = [8, 64, 512, 4_096, 8_192, 32_768, 262_144, 2_097_152];

        for link_supports_priority in [false, true] {
            for ps in payload_sizes.iter() {
                if u64::try_from(*ps).is_err() {
                    break;
                }

                // Compute the number of messages to send
                let num_msg = max_msgs.min(bytes / ps);

                let (producer, consumer) = TransmissionPipeline::make(
                    CONFIG_NOT_STREAMED,
                    priorities.as_slice(),
                    link_supports_priority,
                );

                let t_c = if link_supports_priority {
                    consumer
                        .split()
                        .into_iter()
                        .map(|c| task::spawn(consume(c, num_msg)))
                        .collect()
                } else {
                    vec![task::spawn(consume(consumer, num_msg))]
                };

                let c_ps = *ps;
                let t_s = task::spawn_blocking(move || {
                    schedule(producer, num_msg, c_ps);
                });

                let res = tokio::time::timeout(
                    TIMEOUT,
                    futures::future::join_all(std::iter::once(t_s).chain(t_c)),
                )
                .await;
                assert!(res.is_ok());
            }
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tx_pipeline_blocking() -> ZResult<()> {
        fn schedule(queue: TransmissionPipelineProducer, counter: Arc<AtomicUsize>, id: usize) {
            // Make sure to put only one message per batch: set the payload size
            // to half of the batch in such a way the serialized zenoh message
            // will be larger then half of the batch size (header + payload).
            let payload_size = (CONFIG_STREAMED.batch.mtu / 2) as usize;

            // Send reliable messages
            let key = "test".into();

            let message = NetworkMessage::from(Push {
                wire_expr: key,
                ext_qos: ext::QoSType::new(Priority::Control, CongestionControl::Block, false),
                ..Push::from(vec![0_u8; payload_size])
            });

            // The last push should block since there shouldn't any more batches
            // available for serialization.
            let num_msg = 1 + CONFIG_STREAMED.queue_size[0];
            for i in 0..num_msg {
                println!(
                    "Pipeline Blocking [>>>]: ({id}) Scheduling message #{i} with payload size of {payload_size} bytes"
                );
                queue.push_network_message(message.as_ref()).unwrap();
                let c = counter.fetch_add(1, Ordering::AcqRel);
                println!(
                    "Pipeline Blocking [>>>]: ({}) Scheduled message #{} (tot {}) with payload size of {} bytes",
                    id, i, c + 1,
                    payload_size
                );
            }
        }

        // Pipeline
        let tct = TransportPriorityTx::make(Bits::from(TransportSn::MAX))?;
        let priorities = vec![tct];
        for link_supports_priority in [false, true] {
            let (producer, mut consumer) = TransmissionPipeline::make(
                CONFIG_NOT_STREAMED,
                priorities.as_slice(),
                link_supports_priority,
            );

            let counter = Arc::new(AtomicUsize::new(0));

            let c_producer = producer.clone();
            let c_counter = counter.clone();
            let h1 = task::spawn_blocking(move || {
                schedule(c_producer, c_counter, 1);
            });

            let c_counter = counter.clone();
            let h2 = task::spawn_blocking(move || {
                schedule(producer, c_counter, 2);
            });

            // Wait to have sent enough messages and to have blocked
            println!(
                "Pipeline Blocking [---]: waiting to have {} messages being scheduled",
                CONFIG_STREAMED.queue_size[Priority::MAX as usize]
            );
            let check = async {
                while counter.load(Ordering::Acquire)
                    < CONFIG_STREAMED.queue_size[Priority::MAX as usize]
                {
                    tokio::time::sleep(SLEEP).await;
                }
            };

            timeout(TIMEOUT, check).await?;

            // Drain the queue (but don't drop it to avoid dropping the messages)
            let _consumers = if link_supports_priority {
                let status = consumer.status.clone();
                let splits = timeout(
                    TIMEOUT,
                    futures::future::try_join_all(consumer.split().into_iter().map(|mut c| {
                        task::spawn_blocking(move || {
                            println!("Pipeline Blocking [---]: draining the queue");
                            let _ = c.drain();
                            c
                        })
                    })),
                )
                .await??;
                TransmissionPipelineConsumer {
                    stage_out: splits
                        .into_iter()
                        .map(|s| s.stage_out)
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                    n_out_r: None,
                    status,
                }
            } else {
                timeout(
                    TIMEOUT,
                    task::spawn_blocking(move || {
                        println!("Pipeline Blocking [---]: draining the queue");
                        let _ = consumer.drain();
                        consumer
                    }),
                )
                .await??
            };

            // Make sure that the tasks scheduling have been unblocked
            println!("Pipeline Blocking [---]: waiting for schedule (1) to be unblocked");
            timeout(TIMEOUT, h1).await??;
            println!("Pipeline Blocking [---]: waiting for schedule (2) to be unblocked");
            timeout(TIMEOUT, h2).await??;
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore]
    async fn tx_pipeline_thr() {
        // Queue
        let tct = TransportPriorityTx::make(Bits::from(TransportSn::MAX)).unwrap();
        let priorities = vec![tct];
        let (producer, mut consumer) =
            TransmissionPipeline::make(CONFIG_STREAMED, priorities.as_slice(), false);
        let count = Arc::new(AtomicUsize::new(0));
        let size = Arc::new(AtomicUsize::new(0));

        let c_size = size.clone();
        task::spawn_blocking(move || {
            loop {
                let payload_sizes: [usize; 16] = [
                    8, 16, 32, 64, 128, 256, 512, 1_024, 2_048, 4_096, 8_192, 16_384, 32_768,
                    65_536, 262_144, 1_048_576,
                ];
                for size in payload_sizes.iter() {
                    c_size.store(*size, Ordering::Release);

                    // Send reliable messages
                    let key = "pipeline/thr".into();

                    let message = NetworkMessage::from(Push {
                        wire_expr: key,
                        ext_qos: ext::QoSType::new(
                            Priority::Control,
                            CongestionControl::Block,
                            false,
                        ),
                        ..Push::from(vec![0_u8; *size])
                    });

                    let duration = Duration::from_millis(5_500);
                    let start = Instant::now();
                    while start.elapsed() < duration {
                        producer.push_network_message(message.as_ref()).unwrap();
                    }
                }
            }
        });

        let c_count = count.clone();
        task::spawn(async move {
            loop {
                let (batch, priority) = consumer.pull().await.unwrap();
                c_count.fetch_add(batch.len() as usize, Ordering::AcqRel);
                consumer.refill(batch, priority);
            }
        });

        let mut prev_size: usize = usize::MAX;
        loop {
            let received = count.swap(0, Ordering::AcqRel);
            let current: usize = size.load(Ordering::Acquire);
            if current == prev_size {
                let thr = (8.0 * received as f64) / 1_000_000_000.0;
                println!("{} bytes: {:.6} Gbps", current, 2.0 * thr);
            }
            prev_size = current;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tx_pipeline_closed() -> ZResult<()> {
        // Pipeline
        let tct = TransportPriorityTx::make(Bits::from(TransportSn::MAX))?;
        let priorities = vec![tct];
        for link_supports_priority in [false, true] {
            let (producer, consumer) = TransmissionPipeline::make(
                CONFIG_NOT_STREAMED,
                priorities.as_slice(),
                link_supports_priority,
            );
            // Drop consumer to close the pipeline
            if link_supports_priority {
                for split in consumer.split() {
                    drop(split);
                }
            } else {
                drop(consumer);
            }

            let message = NetworkMessage::from(Push {
                wire_expr: "test".into(),
                ext_qos: ext::QoSType::new(Priority::Control, CongestionControl::Block, true),
                ..Push::from(vec![42u8])
            });
            // First message should not be rejected as the is one batch available in the queue
            assert!(producer.push_network_message(message.as_ref()).is_ok());
            // Second message should be rejected
            assert!(producer.push_network_message(message.as_ref()).is_err());
        }

        Ok(())
    }

    /// A droppable push of `payload_size` bytes on `priority` and `reliability`.
    fn droppable(
        priority: Priority,
        reliability: Reliability,
        express: bool,
        payload_size: usize,
    ) -> NetworkMessage {
        NetworkMessage {
            body: NetworkBody::Push(Push {
                wire_expr: "test".into(),
                ext_qos: ext::QoSType::new(priority, CongestionControl::Drop, express),
                ..Push::from(vec![0_u8; payload_size])
            }),
            reliability,
        }
    }

    fn push(producer: &TransmissionPipelineProducer, msg: &NetworkMessage) -> bool {
        producer.push_network_message(msg.as_ref()).unwrap()
    }

    /// The SN of the first frame or fragment in `batch`.
    fn first_sn(batch: &WBatch) -> TransportSn {
        let mut reader = batch.as_slice().reader();
        let msg: TransportMessage = Zenoh080::new().read(&mut reader).unwrap();
        match msg.body {
            TransportBody::Frame(Frame { sn, .. })
            | TransportBody::Fragment(Fragment { sn, .. }) => sn,
            body => panic!("expected a frame or a fragment, got {body:?}"),
        }
    }

    /// Pushes `msg`, which must be express, and returns the SN its frame took.
    async fn send(
        producer: &TransmissionPipelineProducer,
        consumer: &mut TransmissionPipelineConsumer,
        msg: &NetworkMessage,
    ) -> TransportSn {
        assert!(push(producer, msg), "the push was dropped");
        let (batch, priority) = timeout(TIMEOUT, consumer.pull()).await.unwrap().unwrap();
        let sn = first_sn(&batch);
        consumer.refill(batch, priority);
        sn
    }

    /// One SN channel per priority, as a transport with QoS has.
    fn qos_priorities() -> ZResult<Vec<TransportPriorityTx>> {
        (0..Priority::NUM)
            .map(|_| TransportPriorityTx::make(Bits::from(TransportSn::MAX)))
            .collect()
    }

    /// A congestion drop leaves exactly one SN unused at the next frame on its channel, and
    /// nothing is skipped without a drop (BER-2554).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tx_pipeline_drop_leaves_one_sn_unused() -> ZResult<()> {
        let (producer, mut consumer) =
            TransmissionPipeline::make(CONFIG_NOT_STREAMED, &qos_priorities()?, false);
        let msg = droppable(Priority::DEFAULT, Reliability::Reliable, true, 8);

        let first = send(&producer, &mut consumer, &msg).await;
        assert_eq!(send(&producer, &mut consumer, &msg).await, first + 1);

        // The queue's one batch is held, so a push is dropped at its deadline.
        assert!(push(&producer, &msg));
        let (held, priority) = timeout(TIMEOUT, consumer.pull()).await?.unwrap();
        assert_eq!(first_sn(&held), first + 2);
        assert!(
            !push(&producer, &msg),
            "a push with no batch free must be dropped"
        );
        consumer.refill(held, priority);
        assert_eq!(send(&producer, &mut consumer, &msg).await, first + 4);
        assert_eq!(send(&producer, &mut consumer, &msg).await, first + 5);

        // A drop on the congested fast path, which takes no lock, leaves one SN unused too.
        producer.status.set_congested(Priority::DEFAULT, true);
        assert!(!push(&producer, &msg));
        assert!(!push(&producer, &msg));
        producer.status.set_congested(Priority::DEFAULT, false);
        assert_eq!(
            send(&producer, &mut consumer, &msg).await,
            first + 7,
            "the drops before one frame leave one SN unused, not one each"
        );

        Ok(())
    }

    /// A drop leaves an SN unused on its own priority and reliability only (BER-2554).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tx_pipeline_drop_skips_on_its_own_channel_only() -> ZResult<()> {
        let (producer, mut consumer) =
            TransmissionPipeline::make(CONFIG_NOT_STREAMED, &qos_priorities()?, false);
        let dropped = droppable(Priority::Data, Reliability::BestEffort, true, 8);
        let same_priority = droppable(Priority::Data, Reliability::Reliable, true, 8);
        let same_reliability = droppable(Priority::DataHigh, Reliability::BestEffort, true, 8);

        let channels = [&dropped, &same_priority, &same_reliability];
        let mut sns = vec![];
        for msg in channels {
            sns.push(send(&producer, &mut consumer, msg).await);
        }

        producer.status.set_congested(Priority::Data, true);
        assert!(!push(&producer, &dropped));
        producer.status.set_congested(Priority::Data, false);

        assert_eq!(
            send(&producer, &mut consumer, &same_priority).await,
            sns[1] + 1
        );
        assert_eq!(
            send(&producer, &mut consumer, &same_reliability).await,
            sns[2] + 1
        );
        assert_eq!(send(&producer, &mut consumer, &dropped).await, sns[0] + 2);

        Ok(())
    }

    /// A message that takes the skip and is then dropped itself gives back the SNs it took and
    /// re-arms the skip, so the next frame leaves the SN unused instead (BER-2554).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tx_pipeline_restored_sn_rearms_the_skip() -> ZResult<()> {
        let tct = TransportPriorityTx::make(Bits::from(TransportSn::MAX))?;
        let (producer, mut consumer) =
            TransmissionPipeline::make(CONFIG_NOT_STREAMED, std::slice::from_ref(&tct), false);
        let half = (CONFIG_NOT_STREAMED.batch.mtu / 2) as usize;

        // A batched message holds the queue's one batch.
        let batched = droppable(Priority::DEFAULT, Reliability::Reliable, false, half);
        assert!(push(&producer, &batched));
        producer.status.arm_skip(1, true);

        // The next message does not fit beside it: the batch is moved out, the message takes
        // the skip and an SN, and finds no batch free.
        let mut deadline = Deadline::new(Duration::from_millis(1), None);
        let sent = zlock!(producer.stage_in[0]).push_network_message(
            batched.as_ref(),
            Priority::DEFAULT,
            &mut deadline,
        )?;
        assert_eq!(sent, Pushed::NoBatch);
        assert_eq!(
            zlock!(tct.reliable).sn.now(),
            1,
            "the SNs taken are given back"
        );
        assert!(
            producer.status.take_skip(1, true),
            "the skip taken is re-armed"
        );
        producer.status.arm_skip(1, true);

        let (batch, priority) = timeout(TIMEOUT, consumer.pull()).await?.unwrap();
        assert_eq!(first_sn(&batch), 0);
        consumer.refill(batch, priority);
        let express = droppable(Priority::DEFAULT, Reliability::Reliable, true, 8);
        assert_eq!(send(&producer, &mut consumer, &express).await, 2);

        Ok(())
    }

    /// A pipeline of `queue` batches of 512 bytes per priority, with QoS.
    fn small_batches(
        queue: usize,
    ) -> ZResult<(TransmissionPipelineProducer, TransmissionPipelineConsumer)> {
        let config = TransmissionPipelineConf {
            batch: BatchConfig {
                mtu: 512,
                ..CONFIG_NOT_STREAMED.batch
            },
            queue_size: [queue; Priority::NUM],
            ..CONFIG_NOT_STREAMED
        };
        Ok(TransmissionPipeline::make(
            config,
            &qos_priorities()?,
            false,
        ))
    }

    /// Pulls `n` batches, refills them, and returns the SN of the first frame or fragment of
    /// each.
    async fn pull_sns(consumer: &mut TransmissionPipelineConsumer, n: usize) -> Vec<TransportSn> {
        let mut sns = vec![];
        for _ in 0..n {
            let (batch, priority) = timeout(TIMEOUT, consumer.pull()).await.unwrap().unwrap();
            sns.push(first_sn(&batch));
            consumer.refill(batch, priority);
        }
        sns
    }

    /// A fragmented message sent in full takes one SN per fragment and no more, so the frame
    /// after it follows its last fragment with no gap (BER-2554).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tx_pipeline_fragmented_message_leaves_no_gap() -> ZResult<()> {
        let (producer, mut consumer) = small_batches(4)?;
        let small = droppable(Priority::DEFAULT, Reliability::Reliable, true, 8);
        let large = droppable(Priority::DEFAULT, Reliability::Reliable, true, 1024);

        assert_eq!(send(&producer, &mut consumer, &small).await, 0);
        assert!(push(&producer, &large));
        assert_eq!(pull_sns(&mut consumer, 3).await, [1, 2, 3]);
        assert_eq!(send(&producer, &mut consumer, &small).await, 4);

        Ok(())
    }

    /// A fragment with no payload, which is what a stop fragment is, does not encode, so the
    /// pipeline never has a stop fragment to send (BER-2554).
    #[test]
    fn tx_pipeline_stop_fragment_does_not_encode() {
        let mut batch = WBatch::new_ephemeral(CONFIG_NOT_STREAMED.batch);
        let empty = ZBuf::empty();
        let mut stop = FragmentHeader {
            reliability: Reliability::Reliable,
            more: true,
            sn: 1,
            ext_qos: frame::ext::QoSType::new(Priority::DEFAULT),
            ext_first: None,
            ext_drop: Some(fragment::ext::Drop::new()),
        };
        assert!(batch.encode((&mut empty.reader(), &mut stop)).is_err());
        assert!(batch.is_empty());
    }

    /// A fragmented message abandoned part way, and then sent again in full, starts again at
    /// the SN after its last fragment sent: no stop fragment is queued, and the SN reserved
    /// for it is given back, so the receiver sees no gap for a message it receives (BER-2554).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tx_pipeline_abandoned_fragments_leave_no_gap() -> ZResult<()> {
        let (producer, mut consumer) = small_batches(4)?;
        let idx = Priority::DEFAULT as usize;
        let attempt = |deadline: &mut Deadline, msg: &NetworkMessage| {
            zlock!(producer.stage_in[idx])
                .push_network_message(msg.as_ref(), Priority::DEFAULT, deadline)
                .unwrap()
        };

        // Three of the four batches are held, so the first attempt sends one fragment and
        // finds no batch for the second.
        let small = droppable(Priority::DEFAULT, Reliability::Reliable, true, 8);
        let mut held = vec![];
        for sn in 0..3 {
            assert!(push(&producer, &small));
            let (batch, priority) = timeout(TIMEOUT, consumer.pull()).await?.unwrap();
            assert_eq!(first_sn(&batch), sn);
            held.push((batch, priority));
        }
        let large = droppable(Priority::DEFAULT, Reliability::Reliable, false, 1024);
        let mut deadline = Deadline::new(Duration::from_millis(1), Some(Duration::from_millis(16)));
        assert_eq!(attempt(&mut deadline, &large), Pushed::NoBatch);
        let (first, priority) = timeout(TIMEOUT, consumer.pull()).await?.unwrap();
        assert_eq!(first_sn(&first), 3);
        held.push((first, priority));
        for (batch, priority) in held {
            consumer.refill(batch, priority);
        }

        let mut deadline = Deadline::new(Duration::from_millis(1), Some(Duration::from_millis(16)));
        assert_eq!(attempt(&mut deadline, &large), Pushed::Queued);
        assert_eq!(pull_sns(&mut consumer, 3).await, [4, 5, 6]);
        assert_eq!(send(&producer, &mut consumer, &small).await, 7);

        Ok(())
    }

    /// A queue of the largest size, full of one abandoned message's fragments, still queues
    /// the next message: an abandoned message takes no ring slot beside its queue's batches
    /// (BER-2554).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tx_pipeline_full_queue_keeps_every_batch() -> ZResult<()> {
        let (producer, mut consumer) = small_batches(QueueSizeConf::MAX)?;
        let small = droppable(Priority::DEFAULT, Reliability::Reliable, true, 8);

        // One batch is held, so the message is abandoned when its fragments fill the others.
        assert!(push(&producer, &small));
        let (held, priority) = timeout(TIMEOUT, consumer.pull()).await?.unwrap();
        assert_eq!(first_sn(&held), 0);
        let large = droppable(Priority::DEFAULT, Reliability::Reliable, false, 40_000);
        assert!(!push(&producer, &large));
        consumer.refill(held, priority);
        assert!(push(&producer, &small));

        let mut expected: Vec<TransportSn> = (1..QueueSizeConf::MAX as TransportSn).collect();
        // The abandoned message leaves one SN unused at the next frame.
        expected.push(QueueSizeConf::MAX as TransportSn + 1);
        assert_eq!(pull_sns(&mut consumer, QueueSizeConf::MAX).await, expected);

        Ok(())
    }

    /// A message that does not fit the batches even as fragments is reported dropped, gives
    /// back its SN and leaves one unused at the next frame, without marking the queue
    /// congested (BER-2554).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tx_pipeline_unfragmentable_message_is_dropped() -> ZResult<()> {
        let config = TransmissionPipelineConf {
            batch: BatchConfig {
                mtu: 3,
                ..CONFIG_NOT_STREAMED.batch
            },
            ..CONFIG_NOT_STREAMED
        };
        let tct = TransportPriorityTx::make(Bits::from(TransportSn::MAX))?;
        let (producer, _consumer) =
            TransmissionPipeline::make(config, std::slice::from_ref(&tct), false);
        let msg = droppable(Priority::DEFAULT, Reliability::Reliable, true, 8);

        assert!(!push(&producer, &msg));
        assert!(!producer.status.is_congested(Priority::DEFAULT));
        assert_eq!(zlock!(tct.reliable).sn.now(), 0);
        assert!(producer.status.take_skip(1, true));

        Ok(())
    }
}
