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
use std::fmt::Debug;

use zenoh_buffers::{buffer::Buffer, reader::BacktrackableReader};
use zenoh_codec::transport::frame::FrameReader;
use zenoh_core::zlock;
use zenoh_link::Link;
use zenoh_protocol::{
    core::{Priority, Reliability, ZenohIdProto},
    network::{NetworkMessage, NetworkMessageMut},
    transport::{Close, Fragment, KeepAlive, TransportBody, TransportMessage, TransportSn},
};
use zenoh_result::{bail, zerror, ZResult};

use super::transport::TransportUnicastUniversal;
use crate::{
    common::{
        batch::{Decode, RBatch},
        loss::{self, RxLossKind},
        priority::TransportChannelRx,
    },
    unicast::{link::TransportLinkUnicastRx, transport_unicast_inner::TransportUnicastTrait},
    TransportPeerEventHandler,
};

/*************************************/
/*            TRANSPORT RX           */
/*************************************/
impl TransportUnicastUniversal {
    fn trigger_callback(
        &self,
        callback: &dyn TransportPeerEventHandler,
        #[allow(unused_mut)] // shared-memory feature requires mut
        mut msg: NetworkMessageMut,
        #[cfg(feature = "stats")] stats: &zenoh_stats::LinkStats,
        #[cfg(feature = "shared-memory")] link: &TransportLinkUnicastRx,
    ) -> ZResult<()> {
        // A closing manager, or a process that has begun to exit, delivers nothing and maps
        // nothing (BER-2230, BER-2456).
        if self.manager.is_closed() {
            self.manager.count_undelivered();
            return Ok(());
        }
        #[cfg(feature = "stats")]
        stats.inc_network_message(
            zenoh_stats::Rx,
            zenoh_protocol::network::NetworkMessageExt::as_ref(&msg),
        );
        #[cfg(feature = "shared-memory")]
        if let Some(shm_context) = &self.shm_context {
            use crate::common::shm::interop::{map_zmsg_to_shmbuf, report_shm_rx_drop, ShmRx};
            match map_zmsg_to_shmbuf(msg.as_mut(), &shm_context.shm_reader, &link.shm) {
                Ok(ShmRx::Deliver) => {}
                Ok(ShmRx::ExitBegun) => {
                    self.manager.count_undelivered();
                    return Ok(());
                }
                Err(e) => {
                    report_shm_rx_drop(self.config.zid, &e);
                    return Ok(());
                }
            }
        }

        callback.handle_message(msg)
    }

    fn handle_close(&self, link: &Link, _reason: u8, session: bool) -> ZResult<()> {
        // Delete and clean up
        let c_transport = self.clone();
        let c_link = link.clone();
        // Spawn a task to avoid a deadlock waiting for this same task
        // to finish in the link close() joining the rx handle
        zenoh_runtime::ZRuntime::Net.spawn(async move {
            if session {
                let _ = c_transport.delete().await;
            } else {
                let _ = c_transport.del_link(c_link).await;
            }
        });

        Ok(())
    }

    fn handle_frame<R: BacktrackableReader>(
        &self,
        frame: FrameReader<R>,
        #[cfg(feature = "stats")] stats: &zenoh_stats::LinkStats,
        #[cfg(feature = "shared-memory")] link: &TransportLinkUnicastRx,
    ) -> ZResult<()> {
        let priority = frame.ext_qos.priority();
        let c = if self.is_qos() {
            &self.priority_rx[priority as usize]
        } else if priority == Priority::DEFAULT {
            &self.priority_rx[0]
        } else {
            bail!(
                "Transport: {}. Unknown priority: {:?}.",
                self.config.zid,
                priority
            );
        };

        let mut guard = match frame.reliability {
            Reliability::Reliable => zlock!(c.reliable),
            Reliability::BestEffort => zlock!(c.best_effort),
        };

        if !verify_sn(
            self.config.zid,
            "Frame",
            frame.sn,
            priority,
            frame.reliability,
            &mut guard,
        )? {
            // Drop invalid message and continue
            return Ok(());
        }
        if let Some(callback) = self.callback.get() {
            for mut msg in frame {
                self.trigger_callback(
                    callback.as_ref(),
                    msg.as_mut(),
                    #[cfg(feature = "stats")]
                    stats,
                    #[cfg(feature = "shared-memory")]
                    link,
                )?;
            }
        } else {
            tracing::debug!(
                "Transport: {}. No callback available, dropping messages",
                self.config.zid,
            );
        }

        Ok(())
    }

    fn handle_fragment(
        &self,
        fragment: Fragment,
        #[cfg(feature = "stats")] stats: &zenoh_stats::LinkStats,
        #[cfg(feature = "shared-memory")] link: &TransportLinkUnicastRx,
    ) -> ZResult<()> {
        let priority = fragment.ext_qos.priority();
        let c = if self.is_qos() {
            &self.priority_rx[priority as usize]
        } else if priority == Priority::DEFAULT {
            &self.priority_rx[0]
        } else {
            bail!(
                "Transport: {}. Unknown priority: {:?}.",
                self.config.zid,
                priority
            );
        };

        let mut guard = match fragment.reliability {
            Reliability::Reliable => zlock!(c.reliable),
            Reliability::BestEffort => zlock!(c.best_effort),
        };

        let Some(mut msg) = receive_fragment(
            self.config.zid,
            self.config.patch.has_fragmentation_markers(),
            &mut guard,
            fragment,
        )?
        else {
            return Ok(());
        };
        if let Some(callback) = self.callback.get() {
            return self.trigger_callback(
                callback.as_ref(),
                msg.as_mut(),
                #[cfg(feature = "stats")]
                stats,
                #[cfg(feature = "shared-memory")]
                link,
            );
        }
        tracing::debug!(
            "Transport: {}. No callback available, dropping messages: {:?}",
            self.config.zid,
            msg
        );

        Ok(())
    }

    pub(super) fn read_messages<TBuffer: BacktrackableReader + Buffer + Debug>(
        &self,
        mut batch: RBatch<TBuffer>,
        link: &TransportLinkUnicastRx,
        #[cfg(feature = "stats")] stats: &zenoh_stats::LinkStats,
    ) -> ZResult<()> {
        while !batch.is_empty() {
            if let Ok(frame) = batch.decode() {
                tracing::trace!("Received: {:?}", frame);
                #[cfg(feature = "stats")]
                {
                    stats.inc_transport_message(zenoh_stats::Rx, 1);
                }
                self.handle_frame(
                    frame,
                    #[cfg(feature = "stats")]
                    stats,
                    #[cfg(feature = "shared-memory")]
                    link,
                )?;
                continue;
            }
            let msg: TransportMessage = batch
                .decode()
                .map_err(|_| zerror!("{}: decoding error", link.link))?;

            tracing::trace!("Received: {:?}", msg);

            #[cfg(feature = "stats")]
            {
                stats.inc_transport_message(zenoh_stats::Rx, 1);
            }

            match msg.body {
                TransportBody::Frame(_) => unreachable!(),
                TransportBody::Fragment(fragment) => self.handle_fragment(
                    fragment,
                    #[cfg(feature = "stats")]
                    stats,
                    #[cfg(feature = "shared-memory")]
                    link,
                )?,
                TransportBody::Close(Close { reason, session }) => {
                    let l = Link::new_unicast(
                        &link.link,
                        link.config.priorities.clone(),
                        link.config.reliability,
                    );
                    self.handle_close(&l, reason, session)?
                }
                TransportBody::KeepAlive(KeepAlive { .. }) => {}
                _ => {
                    tracing::debug!(
                        "Transport: {}. Message handling not implemented: {:?}",
                        self.config.zid,
                        msg
                    );
                }
            }
        }

        // Process the received message

        Ok(())
    }
}

/// Accepts `sn` if it follows the last SN accepted on `channel`, and reports to the rx loss
/// observer the SNs it skips (BER-2554).
fn verify_sn(
    peer: ZenohIdProto,
    message_type: &str,
    sn: TransportSn,
    priority: Priority,
    reliability: Reliability,
    channel: &mut TransportChannelRx,
) -> ZResult<bool> {
    let gap = channel.sn.gap(sn)?;
    if !channel.sn.roll(sn)? {
        tracing::trace!(
            "Transport: {}. {} with invalid SN dropped: {}. Expected: {}.",
            peer,
            message_type,
            sn,
            channel.sn.next()
        );
        return Ok(false);
    }
    if gap > 1 {
        loss::report(peer, priority, reliability, RxLossKind::SnGap { gap });
    }

    Ok(true)
}

/// Feeds a fragment into its channel's reassembly and returns the message it completes. A
/// fragment that ends a reassembly without a message is reported to the rx loss observer
/// (BER-2554).
fn receive_fragment(
    peer: ZenohIdProto,
    has_fragmentation_markers: bool,
    channel: &mut TransportChannelRx,
    fragment: Fragment,
) -> ZResult<Option<NetworkMessage>> {
    let Fragment {
        reliability,
        more,
        sn,
        ext_qos: qos,
        ext_first,
        ext_drop,
        payload,
    } = fragment;
    let priority = qos.priority();
    let report = |kind| loss::report(peer, priority, reliability, kind);

    if !verify_sn(peer, "Fragment", sn, priority, reliability, channel)? {
        // Drop invalid message and continue
        return Ok(None);
    }
    if has_fragmentation_markers {
        if ext_first.is_some() {
            channel.defrag.clear();
        } else if channel.defrag.is_empty() {
            tracing::trace!(
                "Transport: {}. First fragment received without start marker.",
                peer,
            );
            report(RxLossKind::FragmentWithoutStart);
            return Ok(None);
        }
        if ext_drop.is_some() {
            channel.defrag.clear();
            report(RxLossKind::FragmentDropped);
            return Ok(None);
        }
    }
    if channel.defrag.is_empty() {
        let _ = channel.defrag.sync(sn);
    }
    if let Err(e) = channel.defrag.push(sn, payload) {
        // Defrag errors don't close transport
        tracing::trace!("{}", e);
        report(RxLossKind::Defragmentation);
        return Ok(None);
    }
    if !more {
        let msg = channel.defrag.defragment();
        if msg.is_none() {
            tracing::trace!("Transport: {}. Defragmentation error.", peer);
            report(RxLossKind::Defragmentation);
        }
        return Ok(msg);
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use zenoh_buffers::{
        reader::{HasReader, Reader},
        writer::HasWriter,
        ZSlice,
    };
    use zenoh_codec::{RCodec, WCodec, Zenoh080};
    use zenoh_config::{QueueAllocConf, QueueAllocMode};
    use zenoh_protocol::{
        core::{Bits, CongestionControl, Priority, Reliability, ZenohIdProto},
        network::{ext, NetworkBody, NetworkMessage, NetworkMessageExt, Push},
        transport::{fragment, BatchSize, Fragment, Frame, TransportBody, TransportSn},
    };

    use super::{receive_fragment, verify_sn, TransportMessage};
    use crate::common::{
        batch::{BatchConfig, WBatch},
        loss::{recorder, RxLoss, RxLossKind},
        pipeline::{PipelineConsumer, TransmissionPipeline, TransmissionPipelineConf},
        priority::{TransportChannelRx, TransportPriorityTx},
    };

    const TIMEOUT: Duration = Duration::from_secs(60);
    const PRIORITY: Priority = Priority::Data;
    const RELIABILITY: Reliability = Reliability::Reliable;

    fn channel(defrag_buff_size: usize) -> TransportChannelRx {
        TransportChannelRx::make(RELIABILITY, Bits::U32, defrag_buff_size).unwrap()
    }

    fn accept(peer: ZenohIdProto, channel: &mut TransportChannelRx, sn: TransportSn) -> bool {
        verify_sn(peer, "Frame", sn, PRIORITY, RELIABILITY, channel).unwrap()
    }

    fn kinds(peer: ZenohIdProto) -> Vec<RxLossKind> {
        recorder::losses_from(peer)
            .into_iter()
            .map(|loss| loss.kind)
            .collect()
    }

    fn push(payload_size: usize, express: bool) -> NetworkMessage {
        NetworkMessage {
            body: NetworkBody::Push(Push {
                wire_expr: "test".into(),
                ext_qos: ext::QoSType::new(PRIORITY, CongestionControl::Drop, express),
                ..Push::from(vec![7_u8; payload_size])
            }),
            reliability: RELIABILITY,
        }
    }

    fn fragment(sn: TransportSn, first: bool, more: bool, payload: ZSlice) -> Fragment {
        Fragment {
            reliability: RELIABILITY,
            more,
            sn,
            payload,
            ext_qos: fragment::ext::QoSType::new(PRIORITY),
            ext_first: first.then(fragment::ext::First::new),
            ext_drop: None,
        }
    }

    /// `msg` serialized as it is fragmented, cut in two.
    fn halves(msg: &NetworkMessage) -> (ZSlice, ZSlice) {
        let mut bytes: Vec<u8> = vec![];
        let mut writer = bytes.writer();
        Zenoh080::new().write(&mut writer, msg.as_ref()).unwrap();
        let (a, b) = bytes.split_at(bytes.len() / 2);
        (a.to_vec().into(), b.to_vec().into())
    }

    /// A frame accepted two SNs after the previous one reports a gap of 2; consecutive SNs
    /// report nothing (BER-2554).
    #[test]
    fn an_sn_gap_is_reported_with_its_size() {
        recorder::install();
        let peer = ZenohIdProto::rand();
        let mut channel = channel(1024);
        channel.sync(10).unwrap();

        assert!(accept(peer, &mut channel, 10));
        assert!(accept(peer, &mut channel, 11));
        assert_eq!(kinds(peer), []);

        assert!(accept(peer, &mut channel, 13));
        assert_eq!(
            recorder::losses_from(peer),
            [RxLoss {
                peer,
                priority: PRIORITY,
                reliability: RELIABILITY,
                kind: RxLossKind::SnGap { gap: 2 },
            }]
        );
        assert!(accept(peer, &mut channel, 14));
        assert_eq!(kinds(peer).len(), 1);
    }

    /// The first frame after the transport syncs its SNs follows them with a gap of 1, also
    /// when the peer's initial SN is 0 (BER-2554).
    #[test]
    fn the_first_frame_after_sync_reports_nothing() {
        recorder::install();
        let mask = crate::common::seq_num::get_mask(Bits::U32);
        for initial in [0, 1, 1_000, mask] {
            let peer = ZenohIdProto::rand();
            let mut channel = channel(1024);
            channel.sync(initial).unwrap();
            assert!(accept(peer, &mut channel, initial));
            assert_eq!(kinds(peer), [], "initial SN {initial}");
        }
    }

    /// A panic in the rx loss observer stays in the observer: the frame is still accepted, and
    /// the channel's lock is not poisoned (BER-2554).
    #[test]
    fn a_panicking_observer_does_not_unwind() {
        recorder::install();
        let peer = ZenohIdProto::rand();
        recorder::panic_for(peer);
        let channel = std::sync::Mutex::new(channel(1024));
        channel.lock().unwrap().sync(10).unwrap();

        assert!(accept(peer, &mut channel.lock().unwrap(), 10));
        assert!(accept(peer, &mut channel.lock().unwrap(), 12));
        assert!(!channel.is_poisoned());
        assert!(accept(peer, &mut channel.lock().unwrap(), 13));
        assert_eq!(kinds(peer), [RxLossKind::SnGap { gap: 2 }]);
    }

    /// A repeated or old SN is dropped as before, and reported as no loss.
    #[test]
    fn a_repeated_or_old_sn_is_dropped_unreported() {
        recorder::install();
        let peer = ZenohIdProto::rand();
        let mut channel = channel(1024);
        channel.sync(10).unwrap();

        assert!(accept(peer, &mut channel, 10));
        assert!(accept(peer, &mut channel, 11));
        assert!(!accept(peer, &mut channel, 11));
        assert!(!accept(peer, &mut channel, 9));
        assert_eq!(kinds(peer), []);
    }

    /// A message reassembled from its fragments reports nothing.
    #[test]
    fn a_reassembled_message_reports_nothing() {
        recorder::install();
        let peer = ZenohIdProto::rand();
        let mut channel = channel(1024);
        channel.sync(10).unwrap();
        let msg = push(64, false);
        let (a, b) = halves(&msg);

        let first = receive_fragment(peer, true, &mut channel, fragment(10, true, true, a));
        assert!(first.unwrap().is_none());
        let last = receive_fragment(peer, true, &mut channel, fragment(11, false, false, b));
        assert_eq!(last.unwrap(), Some(msg));
        assert_eq!(kinds(peer), []);
    }

    /// A stop fragment discards the message being reassembled and reports `FragmentDropped`
    /// (BER-2554).
    #[test]
    fn a_stop_fragment_is_reported() {
        recorder::install();
        let peer = ZenohIdProto::rand();
        let mut channel = channel(1024);
        channel.sync(10).unwrap();
        let (a, b) = halves(&push(64, false));

        let first = receive_fragment(peer, true, &mut channel, fragment(10, true, true, a));
        assert!(first.unwrap().is_none());
        let stop = Fragment {
            ext_drop: Some(fragment::ext::Drop::new()),
            ..fragment(11, false, false, b)
        };
        assert!(receive_fragment(peer, true, &mut channel, stop)
            .unwrap()
            .is_none());
        assert_eq!(kinds(peer), [RxLossKind::FragmentDropped]);
    }

    /// A fragment that arrives with no message being reassembled reports
    /// `FragmentWithoutStart` (BER-2554).
    #[test]
    fn a_fragment_without_its_start_is_reported() {
        recorder::install();
        let peer = ZenohIdProto::rand();
        let mut channel = channel(1024);
        channel.sync(10).unwrap();
        let (_, b) = halves(&push(64, false));

        let msg = receive_fragment(peer, true, &mut channel, fragment(10, false, false, b));
        assert!(msg.unwrap().is_none());
        assert_eq!(kinds(peer), [RxLossKind::FragmentWithoutStart]);
    }

    /// A message larger than the defragmentation buffer, and bytes that do not decode, report
    /// `Defragmentation` (BER-2554).
    #[test]
    fn a_failed_reassembly_is_reported() {
        recorder::install();
        let (a, _) = halves(&push(64, false));

        let peer = ZenohIdProto::rand();
        let mut small = channel(a.len() - 1);
        small.sync(10).unwrap();
        let msg = receive_fragment(peer, true, &mut small, fragment(10, true, true, a));
        assert!(msg.unwrap().is_none());
        assert_eq!(kinds(peer), [RxLossKind::Defragmentation]);

        let peer = ZenohIdProto::rand();
        let mut channel = channel(1024);
        channel.sync(10).unwrap();
        let garbage: ZSlice = vec![0xff_u8; 16].into();
        let msg = receive_fragment(peer, true, &mut channel, fragment(10, true, false, garbage));
        assert!(msg.unwrap().is_none());
        assert_eq!(kinds(peer), [RxLossKind::Defragmentation]);
    }

    /// A sender that runs out of batches part way through a fragmented message abandons it,
    /// and the receiver reports the loss as a gap at the sender's next frame (BER-2554). The
    /// stop fragment carries no payload, and a fragment with none does not encode, so the
    /// sender queues none.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_fragment_dropped_mid_message_is_reported() {
        recorder::install();
        let mtu: BatchSize = 512;
        let config = TransmissionPipelineConf {
            batch: BatchConfig {
                mtu,
                is_streamed: false,
                #[cfg(feature = "transport_compression")]
                is_compression: false,
            },
            queue_size: [1; Priority::NUM],
            batching_enabled: true,
            wait_before_drop: Duration::from_millis(1),
            max_wait_before_drop_fragments: Duration::from_millis(16),
            wait_before_close: Duration::from_secs(5),
            batching_time_limit: Duration::from_micros(1),
            queue_alloc: QueueAllocConf {
                mode: QueueAllocMode::Init,
            },
        };
        let priorities = (0..Priority::NUM)
            .map(|_| TransportPriorityTx::make(Bits::U32).unwrap())
            .collect::<Vec<_>>();
        let (producer, mut consumer) = TransmissionPipeline::make(config, &priorities, false);

        // The queue's one batch carries the first fragment and is not given back, so the
        // second fragment finds no batch free.
        let large = push(4 * mtu as usize, false);
        assert!(!producer.push_network_message(large.as_ref()).unwrap());
        let (batch, priority) = tokio::time::timeout(TIMEOUT, consumer.pull())
            .await
            .unwrap()
            .unwrap();
        let mut batches = vec![decode(&batch)];
        consumer.refill(batch, priority);
        assert_eq!(batches[0].len(), 1, "the first fragment");
        let small = push(8, true);
        assert!(producer.push_network_message(small.as_ref()).unwrap());
        let (batch, _) = tokio::time::timeout(TIMEOUT, consumer.pull())
            .await
            .unwrap()
            .unwrap();
        batches.push(decode(&batch));

        let peer = ZenohIdProto::rand();
        let mut channel = channel(1 << 20);
        channel.sync(0).unwrap();
        let mut delivered = 0;
        for msg in batches.into_iter().flatten() {
            match msg.body {
                TransportBody::Fragment(f) => {
                    if receive_fragment(peer, true, &mut channel, f)
                        .unwrap()
                        .is_some()
                    {
                        delivered += 1;
                    }
                }
                TransportBody::Frame(Frame { sn, payload, .. }) => {
                    if accept(peer, &mut channel, sn) {
                        delivered += payload.len();
                    }
                }
                body => panic!("unexpected {body:?}"),
            }
        }
        assert_eq!(delivered, 1, "only the small message arrives");
        assert_eq!(kinds(peer), [RxLossKind::SnGap { gap: 2 }]);
    }

    fn decode(batch: &WBatch) -> Vec<TransportMessage> {
        let mut reader = batch.as_slice().reader();
        let mut msgs = vec![];
        while reader.can_read() {
            msgs.push(Zenoh080::new().read(&mut reader).unwrap());
        }
        msgs
    }
}
