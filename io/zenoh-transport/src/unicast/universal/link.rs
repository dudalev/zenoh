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
#[cfg(all(
    feature = "uring",
    target_os = "linux",
    any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64",
        target_arch = "loongarch64",
        target_arch = "powerpc64"
    )
))]
use std::fmt::Debug;
use std::{
    future::poll_fn,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, OnceLock,
    },
    task::Poll,
    time::{Duration, Instant},
};

use futures::{future::select_all, task::AtomicWaker};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use zenoh_buffers::ZSlice;
#[cfg(all(
    feature = "uring",
    target_os = "linux",
    any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64",
        target_arch = "loongarch64",
        target_arch = "powerpc64"
    )
))]
use zenoh_buffers::{
    buffer::Buffer,
    reader::{BacktrackableReader, HasReader},
};
use zenoh_link::Link;
use zenoh_protocol::{
    core::Priority,
    transport::{KeepAlive, TransportMessage},
};
use zenoh_result::{bail, zerror, ZResult};
use zenoh_sync::RecyclingObjectPool;
#[cfg(feature = "unstable")]
use zenoh_sync::{event, Notifier, Waiter};
use zenoh_task::TaskController;
#[cfg(all(
    feature = "uring",
    target_os = "linux",
    any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64",
        target_arch = "loongarch64",
        target_arch = "powerpc64"
    )
))]
use zenoh_uring::api::reader::{
    fragmented_batch::{DefragmentationState, FragmentedBatch},
    rx_buffer::RxBuffer,
};

use super::transport::TransportUnicastUniversal;
use crate::{
    common::{
        batch::{BatchConfig, RBatch, WBatch},
        loss::{self, LocalClose, TxLossKind},
        pipeline::{
            PipelineConsumer, TransmissionPipeline, TransmissionPipelineConf,
            TransmissionPipelineConsumer, TransmissionPipelineProducer,
        },
        priority::TransportPriorityTx,
    },
    unicast::link::{TransportLinkUnicast, TransportLinkUnicastRx, TransportLinkUnicastTx},
};

#[derive(Clone)]
pub(super) struct TransportLinkUnicastUniversal {
    // The underlying link
    pub(super) link: TransportLinkUnicast,
    // The transmission pipeline
    pub(super) pipeline: TransmissionPipelineProducer,
    // The task handling substruct
    task_controller: TaskController,
    #[cfg(feature = "unstable")]
    // Notifier for a BlockFirst message to be ready to be sent
    // (after the previous one has been sent)
    pub block_first_notifiers: [Notifier; Priority::NUM],
    #[cfg(feature = "unstable")]
    // Waiter for a BlockFirst message to be ready to be sent
    pub block_first_waiters: [Waiter; Priority::NUM],
    #[cfg(feature = "stats")]
    pub(super) stats: zenoh_stats::LinkStats,
}

impl TransportLinkUnicastUniversal {
    pub(super) fn new(
        transport: &TransportUnicastUniversal,
        link: TransportLinkUnicast,
        priority_tx: &[TransportPriorityTx],
    ) -> (Self, TransmissionPipelineConsumer) {
        assert!(!priority_tx.is_empty());

        let config = TransmissionPipelineConf {
            batch: BatchConfig {
                mtu: link.config.batch.mtu,
                is_streamed: link.link.is_streamed(),
                #[cfg(feature = "transport_compression")]
                is_compression: link.config.batch.is_compression,
            },
            queue_size: transport.manager.config.queue_size,
            wait_before_drop: transport.manager.config.wait_before_drop,
            max_wait_before_drop_fragments: transport.manager.config.max_wait_before_drop_fragments,
            wait_before_close: transport.manager.config.wait_before_close,
            batching_enabled: transport.manager.config.batching,
            batching_time_limit: transport.manager.config.queue_backoff,
            queue_alloc: transport.manager.config.queue_alloc,
        };

        // The pipeline
        let (producer, consumer) =
            TransmissionPipeline::make(config, priority_tx, link.link.supports_priorities());

        // Use the complete src and dest locators including parameters
        #[cfg(feature = "stats")]
        let link_unicast = link.link();
        #[cfg(feature = "stats")]
        let stats = transport
            .stats
            .link_stats(&link_unicast.src, &link_unicast.dst);

        #[cfg(feature = "unstable")]
        let mut block_first_notifiers = Vec::new();
        #[cfg(feature = "unstable")]
        let mut block_first_waiters = Vec::new();
        #[cfg(feature = "unstable")]
        for _ in 0..Priority::NUM {
            let (notifier, waiter) = event::new();
            // notify to be make the BlockFirst "slot" available
            notifier.notify().unwrap();
            block_first_notifiers.push(notifier);
            block_first_waiters.push(waiter);
        }

        let result = Self {
            link,
            pipeline: producer,
            task_controller: TaskController::default(),
            #[cfg(feature = "unstable")]
            block_first_notifiers: block_first_notifiers.try_into().ok().unwrap(),
            #[cfg(feature = "unstable")]
            block_first_waiters: block_first_waiters.try_into().ok().unwrap(),
            #[cfg(feature = "stats")]
            stats,
        };

        (result, consumer)
    }

    pub(super) fn start_tx(
        &mut self,
        transport: TransportUnicastUniversal,
        consumer: TransmissionPipelineConsumer,
        keep_alive: Duration,
    ) {
        // Spawn the TX task
        let mut tx = self.link.tx();
        #[cfg(feature = "stats")]
        let stats = self.stats.clone();
        let ct = self.task_controller.get_cancellation_token();
        let task = async move {
            let res = tx_task(
                consumer,
                &mut tx,
                &transport,
                keep_alive,
                ct,
                #[cfg(feature = "stats")]
                stats,
            )
            .await;

            if let Err(e) = res {
                tracing::debug!("TX task failed: {}", e);
                // Spawn a task to avoid a deadlock waiting for this same task
                // to finish in the close() joining its handle
                // TODO(yuyuan): do more study to check which ZRuntime should be used or refine the
                // termination
                zenoh_runtime::ZRuntime::Net
                    .spawn(async move { transport.del_link(tx.inner.link()).await });
            }
        };
        self.task_controller
            .spawn_with_rt(zenoh_runtime::ZRuntime::TX, task);
    }

    pub(super) fn start_rx(&mut self, transport: TransportUnicastUniversal, lease: Duration) {
        let priorities = self.link.config.priorities.clone();
        let reliability = self.link.config.reliability;
        let mut rx = self.link.rx();
        let cancellation_token = self.task_controller.get_cancellation_token();
        #[cfg(feature = "stats")]
        let stats = self.stats.clone();
        let task = async move {
            // Start the consume task
            let res = rx_task(
                &mut rx,
                transport.clone(),
                lease,
                transport.manager.config.link_rx_buffer_size,
                cancellation_token,
                #[cfg(feature = "stats")]
                stats,
            )
            .await;

            // TODO(yuyuan): improve this callback
            if let Err(e) = res {
                // process error if task was not cancelled
                tracing::debug!("RX task failed: {}", e);
                let local_close = local_close_of(&e);

                // Spawn a task to avoid a deadlock waiting for this same task
                // to finish in the close() joining its handle
                // WARN: Must be spawned on RX

                zenoh_runtime::ZRuntime::RX.spawn(async move {
                    transport
                        .del_link_closed_by(
                            Link::new_unicast(&rx.link, priorities, reliability),
                            local_close,
                        )
                        .await
                });

                // // WARN: This ZRuntime blocks
                // zenoh_runtime::ZRuntime::Net
                //     .spawn(async move { transport.del_link((&rx.link).into()).await });

                // // WARN: This cloud block
                // transport.del_link((&rx.link).into()).await;
            }
        };
        // WARN: If this is on ZRuntime::TX, a deadlock would occur.
        self.task_controller
            .spawn_with_rt(zenoh_runtime::ZRuntime::RX, task);
    }

    pub(super) async fn close(self) -> ZResult<()> {
        tracing::trace!("{}: closing", self.link);
        self.task_controller.terminate_all_async().await;
        self.pipeline.disable();

        self.link.close(None).await
    }
}

/*************************************/
/*              TASKS                */
/*************************************/
async fn tx_task(
    pipeline: TransmissionPipelineConsumer,
    link: &mut TransportLinkUnicastTx,
    transport: &TransportUnicastUniversal,
    keep_alive: Duration,
    cancellation_token: CancellationToken,
    #[cfg(feature = "stats")] stats: zenoh_stats::LinkStats,
) -> ZResult<()> {
    let keep_alive_tracker = TimeoutTracker::new(keep_alive);
    if link.inner.link.supports_priorities() {
        let (res, _, _) = select_all(pipeline.split().into_iter().map(|pipeline| {
            let mut link = link.clone();
            let cancellation_token = cancellation_token.clone();
            let keep_alive_tracker = keep_alive_tracker.clone();
            let transport = transport.clone();
            #[cfg(feature = "stats")]
            let stats = stats.clone();
            zenoh_runtime::ZRuntime::TX.spawn(async move {
                write_loop(
                    Some(pipeline.priority()),
                    pipeline,
                    &mut link,
                    &transport,
                    keep_alive_tracker,
                    cancellation_token,
                    #[cfg(feature = "stats")]
                    stats,
                )
                .await
            })
        }))
        .await;
        res.unwrap()?;
    } else {
        write_loop(
            None,
            pipeline,
            link,
            transport,
            keep_alive_tracker,
            cancellation_token,
            #[cfg(feature = "stats")]
            stats,
        )
        .await?;
    }
    Ok(())
}

async fn write_loop(
    write_priority: Option<Priority>,
    mut pipeline: impl PipelineConsumer,
    link: &mut TransportLinkUnicastTx,
    transport: &TransportUnicastUniversal,
    keep_alive_tracker: TimeoutTracker,
    cancellation_token: CancellationToken,
    #[cfg(feature = "stats")] stats: zenoh_stats::LinkStats,
) -> ZResult<()> {
    // The priority of the batch being written if it holds network messages, which are lost if
    // its write fails or is cancelled (BER-2554).
    let mut writing = None;
    // Whether a write, of a batch or of a keep-alive, is in progress (BER-2554).
    let mut mid_write = false;
    let task = async {
        loop {
            tokio::select! {
                pull = pipeline.pull() => {
                    let Some((mut batch, priority)) = pull else {
                        // The queue has been disabled: break the tx loop, drain the queue, and exit
                        break
                    };
                    debug_assert!(write_priority.is_none() || write_priority == Some(priority));
                    writing = batch.holds_network_message().then_some(priority);
                    mid_write = true;
                    link.send_batch(&mut batch, write_priority).await?;
                    mid_write = false;
                    writing = None;
                    // inform the latest message tracker that a message has been sent
                    keep_alive_tracker.reset();

                    #[cfg(feature = "stats")]
                    {
                        stats.inc_bytes(zenoh_stats::Tx, batch.len() as u64);
                        stats.inc_transport_message(zenoh_stats::Tx, batch.stats.t_msgs as u64);
                    }

                    // Reinsert the batch into the queue
                    pipeline.refill(batch, priority);
                },
                _ = keep_alive_tracker.wait_if(write_priority.unwrap_or(Priority::Control) == Priority::Control) => {
                    // A timeout occurred, no control/data messages have been sent during
                    // the keep_alive period, we need to send a KeepAlive message
                    let message: TransportMessage = KeepAlive.into();

                    mid_write = true;
                    #[allow(unused_variables)] // Used when stats feature is enabled
                    let n = link.send(&message, Some(Priority::Control)).await?;
                    mid_write = false;

                    #[cfg(feature = "stats")]
                    {
                        stats.inc_bytes(zenoh_stats::Tx, n as u64);
                        stats.inc_transport_message(zenoh_stats::Tx, 1);
                    }
                }
            }
        }
        ZResult::Ok(())
    };
    let outcome = cancellation_token.run_until_cancelled(task).await;
    if let Some(Err(e)) = outcome {
        // Nothing more is pushed to a link that cannot write, and what it held is lost.
        pipeline.disable();
        let unsent = writing.into_iter().chain(unsent(pipeline.drain()));
        report_unsent(transport, link, Unwritten::WriteFailed, unsent);
        return Err(e);
    }
    if mid_write {
        // A cut write can leave the stream inside a batch, whose rest the reader would take
        // from the bytes written next: nothing more is written on the link, and the batch the
        // close cut short is lost with the rest (BER-2554).
        pipeline.disable();
        let unsent = writing.into_iter().chain(unsent(pipeline.drain()));
        report_unsent(transport, link, Unwritten::NotRead, unsent);
        bail!("{link}: write cut short by the close");
    }

    // Drain the transmission pipeline and write remaining bytes on the wire
    let mut batches = pipeline.drain().into_iter();
    while let Some((mut b, priority)) = batches.next() {
        let flushed = tokio::time::timeout(
            keep_alive_tracker.timeout(),
            link.send_batch(&mut b, write_priority),
        )
        .await;
        let unwritten = match flushed {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some((Unwritten::WriteFailed, e)),
            Err(_) => Some((
                Unwritten::NotRead,
                zerror!(
                    "{link}: flush failed after {} ms",
                    keep_alive_tracker.timeout().as_millis()
                )
                .into(),
            )),
        };
        if let Some((unwritten, e)) = unwritten {
            let unsent = unsent(std::iter::once((b, priority)).chain(batches));
            report_unsent(transport, link, unwritten, unsent);
            return Err(e);
        }

        #[cfg(feature = "stats")]
        {
            stats.inc_bytes(zenoh_stats::Tx, b.len() as u64);
            stats.inc_transport_message(zenoh_stats::Tx, b.stats.t_msgs as u64);
        }
    }

    Ok(())
}

/// The priorities of the batches among `batches` that hold network messages, which a batch
/// holding only a `Close` does not.
fn unsent(
    batches: impl IntoIterator<Item = (Box<WBatch>, Priority)>,
) -> impl Iterator<Item = Priority> {
    batches
        .into_iter()
        .filter(|(batch, _)| batch.holds_network_message())
        .map(|(_, priority)| priority)
}

/// Why a link did not write batches it held.
#[derive(Clone, Copy)]
enum Unwritten {
    /// A write failed: the connection was closed or reset at the peer's end.
    WriteFailed,
    /// The link closed at this end while its peer was not reading: a write at close found no
    /// room before the keep-alive timeout, or was cut short by the close.
    NotRead,
}

/// Reports, per priority, the batches a link holds and will not write (BER-2554). A reader
/// that is alive loses them when its connection was open and not reading, or when it stays
/// reachable on another link of the transport.
fn report_unsent(
    transport: &TransportUnicastUniversal,
    link: &TransportLinkUnicastTx,
    unwritten: Unwritten,
    priorities: impl IntoIterator<Item = Priority>,
) {
    let mut batches = [0_usize; Priority::NUM];
    for priority in priorities {
        batches[priority as usize] += 1;
    }
    if batches.iter().all(|batches| *batches == 0) {
        return;
    }
    let reader_alive = match unwritten {
        Unwritten::NotRead => true,
        Unwritten::WriteFailed => transport.has_link_other_than(&link.inner.link),
    };
    for (priority, batches) in batches.into_iter().enumerate() {
        if batches > 0 {
            let priority = Priority::try_from(priority as u8).unwrap();
            let kind = if reader_alive {
                TxLossKind::ClosedQueued { batches }
            } else {
                TxLossKind::ResetQueued { batches }
            };
            loss::report_tx(transport.config.zid, priority, kind);
        }
    }
}

/// The error a link's RX task ends with when nothing arrived from the peer for its lease. The
/// link is removed as one that expired, which reports a local close if it was the transport's
/// last (BER-2627).
#[derive(Debug)]
struct LeaseExpired {
    link: String,
    lease: Duration,
}

impl std::fmt::Display for LeaseExpired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: expired after {} milliseconds",
            self.link,
            self.lease.as_millis()
        )
    }
}

impl std::error::Error for LeaseExpired {}

/// What the end of a link's RX task with `e` closes locally.
fn local_close_of(e: &zenoh_result::Error) -> Option<LocalClose> {
    e.downcast_ref::<LeaseExpired>()
        .map(|_| LocalClose::LeaseExpired)
}

async fn rx_task(
    link: &mut TransportLinkUnicastRx,
    transport: TransportUnicastUniversal,
    lease: Duration,
    rx_buffer_size: usize,
    cancellation_token: CancellationToken,
    #[cfg(feature = "stats")] stats: zenoh_stats::LinkStats,
) -> ZResult<()> {
    #[cfg(all(
        feature = "uring",
        target_os = "linux",
        any(
            target_arch = "x86_64",
            target_arch = "aarch64",
            target_arch = "riscv64",
            target_arch = "loongarch64",
            target_arch = "powerpc64"
        )
    ))]
    if transport.manager.state.uring.is_some() && link.link.get_fd().is_ok() {
        return rx_task_uring(
            link,
            transport.clone(),
            lease,
            rx_buffer_size,
            #[cfg(feature = "stats")]
            stats,
            cancellation_token,
        )
        .await;
    }

    cancellation_token
        .run_until_cancelled(rx_task_non_uring(
            link,
            transport.clone(),
            lease,
            rx_buffer_size,
            cancellation_token.clone(),
            #[cfg(feature = "stats")]
            stats,
        ))
        .await
        .unwrap_or(Ok(()))
}

async fn rx_task_non_uring(
    link: &mut TransportLinkUnicastRx,
    transport: TransportUnicastUniversal,
    lease: Duration,
    rx_buffer_size: usize,
    cancellation_token: CancellationToken,
    #[cfg(feature = "stats")] stats: zenoh_stats::LinkStats,
) -> ZResult<()> {
    // The pool of buffers
    let mtu = link.config.batch.mtu as usize;
    let mut n = rx_buffer_size / mtu;
    if n == 0 {
        tracing::debug!("RX configured buffer of {rx_buffer_size} bytes is too small for {link} that has an MTU of {mtu} bytes. Defaulting to {mtu} bytes for RX buffer.");
        n = 1;
    }
    let pool = RecyclingObjectPool::new(n, move || vec![0_u8; mtu].into_boxed_slice());

    let lease_tracker = TimeoutTracker::new(lease);
    if link.link.supports_priorities() {
        let (res, _, _) = select_all((Priority::MAX as u8..=Priority::MIN as u8).map(|prio| {
            let mut link = link.clone();
            let transport = transport.clone();
            let cancellation_token = cancellation_token.clone();
            let lease_tracker = lease_tracker.clone();
            #[cfg(feature = "stats")]
            let stats = stats.clone();
            let pool = pool.clone();
            zenoh_runtime::ZRuntime::RX.spawn(async move {
                cancellation_token
                    .run_until_cancelled(read_loop(
                        Some(Priority::try_from(prio).unwrap()),
                        &mut link,
                        transport,
                        lease_tracker,
                        #[cfg(feature = "stats")]
                        stats,
                        &pool,
                    ))
                    .await
            })
        }))
        .await;
        res.unwrap().transpose()?;
        Ok(())
    } else {
        read_loop(
            None,
            link,
            transport,
            lease_tracker,
            #[cfg(feature = "stats")]
            stats,
            &pool,
        )
        .await
    }
}

async fn read_loop<F: Fn() -> Box<[u8]>>(
    priority: Option<Priority>,
    link: &mut TransportLinkUnicastRx,
    transport: TransportUnicastUniversal,
    lease_tracker: TimeoutTracker,
    #[cfg(feature = "stats")] stats: zenoh_stats::LinkStats,
    pool: &RecyclingObjectPool<Box<[u8]>, F>,
) -> ZResult<()> {
    async fn read<F: Fn() -> Box<[u8]>>(
        link: &mut TransportLinkUnicastRx,
        priority: Option<Priority>,
        pool: &RecyclingObjectPool<Box<[u8]>, F>,
    ) -> ZResult<RBatch<ZSlice>> {
        let batch = link
            .recv_batch(|| pool.try_take().unwrap_or_else(|| pool.alloc()), priority)
            .await?;
        Ok(batch)
    }

    loop {
        // What has arrived is read before the lease is checked: a receive task that a callback
        // held past the lease finds the peer's bytes, or its close, waiting, and the lease runs
        // again from them (BER-2627).
        tokio::select! {
            biased;
            batch = read(link, priority, pool) => {
                let batch = batch?;
                lease_tracker.reset();
                #[cfg(feature = "stats")]
                {
                    let header_bytes = if link.link.is_streamed() { 2 } else { 0 };
                    stats.inc_bytes(zenoh_stats::Rx, header_bytes + batch.len() as u64);
                }
                transport.read_messages(batch, link, #[cfg(feature = "stats")] &stats)?;
            }
            _ = lease_tracker.wait_if(priority.unwrap_or(Priority::Control) == Priority::Control) => {
                return Err(LeaseExpired {
                    link: link.to_string(),
                    lease: lease_tracker.timeout(),
                }
                .into());
            }
        }
    }
}

struct TimeoutTrackerInner {
    timeout: Duration,
    waker: AtomicWaker,
    has_timed_out: AtomicBool,
    latest_reset: Mutex<Instant>,
    task: OnceLock<JoinHandle<()>>,
}

#[derive(Clone)]
struct TimeoutTracker(Arc<TimeoutTrackerInner>);

impl TimeoutTracker {
    fn new(timeout: Duration) -> TimeoutTracker {
        let now = Instant::now();
        let inner = Arc::new(TimeoutTrackerInner {
            timeout,
            waker: AtomicWaker::new(),
            has_timed_out: AtomicBool::new(false),
            latest_reset: Mutex::new(now),
            task: OnceLock::new(),
        });
        let tracker = Arc::downgrade(&inner);
        let task = tokio::spawn(async move {
            let mut latest_reset = now;
            loop {
                tokio::time::sleep_until((latest_reset + timeout).into()).await;
                let prev = latest_reset;
                let Some(tracker) = tracker.upgrade() else {
                    break;
                };
                latest_reset = *tracker.latest_reset.lock().unwrap();
                if latest_reset <= prev {
                    latest_reset = Instant::now();
                    tracker.has_timed_out.store(true, Ordering::Release);
                    tracker.waker.wake();
                }
            }
        });
        inner.task.set(task).unwrap();
        Self(inner)
    }

    fn timeout(&self) -> Duration {
        self.0.timeout
    }

    fn reset(&self) {
        *self.0.latest_reset.lock().unwrap() = Instant::now();
        // A timeout seen before this reset is not one (BER-2627).
        self.0.has_timed_out.store(false, Ordering::Release);
    }

    async fn wait_if(&self, predicate: bool) {
        poll_fn(|cx| {
            if !predicate {
                return Poll::Pending;
            }
            self.0.waker.register(cx.waker());
            if self.0.has_timed_out.load(Ordering::Acquire) {
                self.0.has_timed_out.store(false, Ordering::Release);
                return Poll::Ready(());
            }
            Poll::Pending
        })
        .await
    }
}

impl Drop for TimeoutTracker {
    fn drop(&mut self) {
        self.0.task.get().unwrap().abort();
    }
}

#[cfg(all(
    feature = "uring",
    target_os = "linux",
    any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64",
        target_arch = "loongarch64",
        target_arch = "powerpc64"
    )
))]
async fn rx_task_uring(
    link: &mut TransportLinkUnicastRx,
    transport: TransportUnicastUniversal,
    lease: Duration,
    rx_buffer_size: usize,
    #[cfg(feature = "stats")] stats: zenoh_stats::LinkStats,
    cancellation_token: CancellationToken,
) -> ZResult<()> {
    // The pool of buffers
    let mtu = link.config.batch.mtu as usize;
    let mut n = rx_buffer_size / mtu;
    if n == 0 {
        tracing::debug!("RX configured buffer of {rx_buffer_size} bytes is too small for {link} that has an MTU of {mtu} bytes. Defaulting to {mtu} bytes for RX buffer.");
        n = 1;
    }

    let pool = RecyclingObjectPool::new(n, move || vec![0_u8; mtu].into_boxed_slice());

    let c_link = link.clone();

    let batch_config = link.config.batch;

    let r = transport
        .manager
        .state
        .uring
        .as_ref()
        .expect("uring is Some: checked by caller")
        .reader
        .clone();

    let lease_tracker = TimeoutTracker::new(lease);

    fn read_batch<TBuffer: BacktrackableReader + Buffer + Debug>(
        transport: &TransportUnicastUniversal,
        link: &TransportLinkUnicastRx,
        batch: RBatch<TBuffer>,
        #[cfg(feature = "stats")] stats: &zenoh_stats::LinkStats,
    ) -> ZResult<()> {
        #[cfg(feature = "stats")]
        {
            let header_bytes = 2;
            stats.inc_bytes(zenoh_stats::Rx, header_bytes + batch.len() as u64);
        }
        transport.read_messages(
            batch,
            link,
            #[cfg(feature = "stats")]
            stats,
        )
    }

    let mut uring_read_task = {
        match link.link.is_streamed() {
            true => {
                let lt = lease_tracker.clone();
                let ring_cb = move |data: FragmentedBatch| {
                    lt.reset();
                    match data.defragment()? {
                        DefragmentationState::Single(slice) => {
                            let mut batch = RBatch::new(batch_config, slice);
                            batch.initialize_uring(|| {
                                pool.try_take().unwrap_or_else(|| pool.alloc())
                            })?;
                            read_batch(
                                &transport,
                                &c_link,
                                batch,
                                #[cfg(feature = "stats")]
                                &stats,
                            )
                        }
                        DefragmentationState::Fragmented(buf) => {
                            let mut batch = RBatch::new(batch_config, buf.reader());
                            match batch.initialize_uring(|| {
                                pool.try_take().unwrap_or_else(|| pool.alloc())
                            })? {
                                Some(decompressed_batch) => read_batch(
                                    &transport,
                                    &c_link,
                                    decompressed_batch,
                                    #[cfg(feature = "stats")]
                                    &stats,
                                ),
                                None => read_batch(
                                    &transport,
                                    &c_link,
                                    batch,
                                    #[cfg(feature = "stats")]
                                    &stats,
                                ),
                            }
                        }
                    }
                };

                r.setup_fragmented_read(link.link.get_fd()?, ring_cb)
                    .await?
            }
            false => {
                let lt = lease_tracker.clone();
                let ring_cb = move |data: Arc<RxBuffer>| {
                    lt.reset();
                    let slice: ZSlice = data.into();
                    let mut batch = RBatch::new(batch_config, slice);
                    batch.initialize_uring(|| pool.try_take().unwrap_or_else(|| pool.alloc()))?;

                    read_batch(
                        &transport,
                        &c_link,
                        batch,
                        #[cfg(feature = "stats")]
                        &stats,
                    )
                };

                r.setup_read(link.link.get_fd()?, ring_cb).await?
            }
        }
    };

    let result = tokio::select! {
        biased;
        e = uring_read_task.read_error() => {
            tracing::debug!("Uring RX task stopped by uring task error event");
            Err(e)
        },
        finished = r.wait_finished() => {
            tracing::debug!("Uring RX task stopped by uring finished event: {:?}", finished);
            finished
        }
        _ = cancellation_token.cancelled() => {
            tracing::debug!("Uring RX task stopped by cancellation event");
            Ok(())
        }
        _ = lease_tracker.wait_if(true) => {
            tracing::debug!("Uring RX task stopped by lease timeout");
            Err(LeaseExpired {
                link: link.to_string(),
                lease,
            }
            .into())
        }
    };

    uring_read_task.stop().await;

    result
}

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };

    use async_trait::async_trait;
    use zenoh_codec::{RCodec, Zenoh080};
    use zenoh_link::{LinkAuthId, LinkUnicast, LinkUnicastTrait};
    use zenoh_protocol::{
        core::{Bits, Locator, Priority, WhatAmI, ZenohIdProto},
        transport::{init::ext::PatchType, BatchSize, TransportBody, TransportMessage},
    };
    use zenoh_result::ZResult;

    #[cfg(feature = "auth_usrpwd")]
    use crate::unicast::establishment::ext::auth::UsrPwdId;
    #[cfg(feature = "shared-memory")]
    use crate::unicast::link::TransportLinkShmHandoff;
    use crate::{
        common::{batch::BatchConfig, loss::recorder},
        unicast::{
            link::{
                LinkUnicastWithOpenAck, TransportLinkUnicast, TransportLinkUnicastConfig,
                TransportLinkUnicastDirection,
            },
            TransportConfigUnicast, TransportManagerBuilderUnicast, TransportUnicast,
        },
        DummyTransportEventHandler, TransportManager,
    };

    const TIMEOUT: Duration = Duration::from_secs(60);
    const MTU: BatchSize = 1024;

    /// A streamed link whose every write is held, as a socket whose peer reads nothing holds
    /// it, and which records the writes cut short and the writes begun after one.
    struct HeldLink {
        src: Locator,
        dst: Locator,
        state: Mutex<HeldState>,
    }

    #[derive(Default)]
    struct HeldState {
        holding: Option<Vec<u8>>,
        cut: Vec<Vec<u8>>,
        begun_after_cut: usize,
    }

    impl HeldLink {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                src: Locator::new("tcp", "127.0.0.1:1", "").unwrap(),
                dst: Locator::new("tcp", "127.0.0.1:2", "").unwrap(),
                state: Mutex::default(),
            })
        }

        fn holding(&self) -> Option<TransportBody> {
            self.state.lock().unwrap().holding.as_deref().map(body)
        }

        fn cut(&self) -> Vec<TransportBody> {
            self.state
                .lock()
                .unwrap()
                .cut
                .iter()
                .map(|b| body(b))
                .collect()
        }

        fn begun_after_cut(&self) -> usize {
            self.state.lock().unwrap().begun_after_cut
        }
    }

    /// The transport message a batch the link wrote begins with.
    fn body(batch: &[u8]) -> TransportBody {
        let mut reader = &batch[2..];
        let message: TransportMessage = Zenoh080::new().read(&mut reader).unwrap();
        message.body
    }

    /// Records the held write as cut short, unless it completes.
    struct Holding<'a>(&'a HeldLink);

    impl Drop for Holding<'_> {
        fn drop(&mut self) {
            let mut state = self.0.state.lock().unwrap();
            if let Some(bytes) = state.holding.take() {
                state.cut.push(bytes);
            }
        }
    }

    #[async_trait]
    impl LinkUnicastTrait for HeldLink {
        fn get_mtu(&self) -> BatchSize {
            MTU
        }
        fn get_src(&self) -> &Locator {
            &self.src
        }
        fn get_dst(&self) -> &Locator {
            &self.dst
        }
        fn is_reliable(&self) -> bool {
            true
        }
        fn is_streamed(&self) -> bool {
            true
        }
        fn get_interface_names(&self) -> Vec<String> {
            vec![]
        }
        fn get_auth_id(&self) -> &LinkAuthId {
            &LinkAuthId::Tcp
        }
        async fn write(&self, buffer: &[u8], priority: Option<Priority>) -> ZResult<usize> {
            self.write_all(buffer, priority).await?;
            Ok(buffer.len())
        }
        async fn write_all(&self, buffer: &[u8], _priority: Option<Priority>) -> ZResult<()> {
            {
                let mut state = self.state.lock().unwrap();
                if !state.cut.is_empty() {
                    state.begun_after_cut += 1;
                }
                state.holding = Some(buffer.to_vec());
            }
            let _holding = Holding(self);
            pending().await
        }
        async fn read(&self, _buffer: &mut [u8], _priority: Option<Priority>) -> ZResult<usize> {
            pending().await
        }
        async fn read_exact(&self, _buffer: &mut [u8], _priority: Option<Priority>) -> ZResult<()> {
            pending().await
        }
        async fn close(&self) -> ZResult<()> {
            Ok(())
        }
        #[cfg(all(feature = "uring", target_os = "linux"))]
        fn get_fd(&self) -> ZResult<std::os::fd::RawFd> {
            zenoh_result::bail!("no file descriptor")
        }
    }

    /// A transport to peer `n` over `link`, from a manager whose `lease` sets how long the link
    /// may stay idle before it writes a keep-alive. The peer's lease is long, so the link's
    /// silence does not expire it.
    async fn transport_over(
        n: u8,
        link: Arc<HeldLink>,
        lease: Duration,
    ) -> (TransportManager, TransportUnicast) {
        let manager = TransportManager::builder()
            .whatami(WhatAmI::Peer)
            .zid(ZenohIdProto::try_from([0x4c, n, 1]).unwrap())
            .unicast(TransportManagerBuilderUnicast::default().lease(lease))
            .build_test(Arc::new(DummyTransportEventHandler))
            .unwrap();
        let config = TransportConfigUnicast {
            zid: ZenohIdProto::try_from([0x4c, n, 2]).unwrap(),
            whatami: WhatAmI::Peer,
            region_name: None,
            bound: None,
            sn_resolution: Bits::U32,
            tx_initial_sn: 0,
            is_qos: false,
            #[cfg(feature = "transport_multilink")]
            multilink: None,
            #[cfg(feature = "shared-memory")]
            shm: None,
            is_lowlatency: false,
            #[cfg(feature = "auth_usrpwd")]
            auth_id: UsrPwdId(None),
            patch: PatchType::CURRENT,
        };
        let link = TransportLinkUnicast::new(
            LinkUnicast::from(link as Arc<dyn LinkUnicastTrait>),
            TransportLinkUnicastConfig {
                direction: TransportLinkUnicastDirection::Outbound,
                batch: BatchConfig {
                    mtu: MTU,
                    is_streamed: true,
                    #[cfg(feature = "transport_compression")]
                    is_compression: false,
                },
                priorities: None,
                reliability: None,
            },
            #[cfg(feature = "shared-memory")]
            TransportLinkShmHandoff::disabled(),
        );
        let transport = manager
            .init_transport_unicast(
                config,
                LinkUnicastWithOpenAck::new(link, None, None),
                0,
                TIMEOUT,
            )
            .await
            .unwrap();
        (manager, transport)
    }

    async fn wait_until(what: &str, condition: impl Fn() -> bool) {
        let start = Instant::now();
        while !condition() {
            assert!(start.elapsed() < TIMEOUT, "timed out waiting until {what}");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// A close whose own Close the link has begun to write, with nothing else queued, cuts that
    /// write and reports nothing lost: the batch held no message. The close pushes the Close
    /// and then ends the link's tasks, so each trial counts only if the write of the Close began
    /// in between (BER-2628).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_cut_write_of_the_close_alone_reports_no_loss() {
        recorder::install_tx();
        for n in 0..40 {
            let link = HeldLink::new();
            let (manager, transport) = transport_over(n, link.clone(), TIMEOUT).await;
            let peer = transport.get_zid().unwrap();
            transport.close().await.unwrap();
            let counts = matches!(link.cut()[..], [TransportBody::Close(_)]);
            manager.close().await;
            if counts {
                assert_eq!(recorder::tx_losses_to(peer), []);
                return;
            }
        }
        panic!("no close cut the write of its Close");
    }

    /// A keep-alive whose write a close cuts short ends the link's writes as a batch's does:
    /// the link begins no other write, not even of the Close (BER-2628).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_cut_keep_alive_ends_the_link_writes() {
        recorder::install_tx();
        let link = HeldLink::new();
        let (manager, transport) =
            transport_over(100, link.clone(), Duration::from_millis(400)).await;
        let peer = transport.get_zid().unwrap();
        wait_until("the link writes a keep-alive", || {
            matches!(link.holding(), Some(TransportBody::KeepAlive(_)))
        })
        .await;
        transport.close().await.unwrap();
        assert!(matches!(link.cut()[..], [TransportBody::KeepAlive(_)]));
        assert_eq!(link.begun_after_cut(), 0);
        assert_eq!(recorder::tx_losses_to(peer), []);
        manager.close().await;
    }
}
