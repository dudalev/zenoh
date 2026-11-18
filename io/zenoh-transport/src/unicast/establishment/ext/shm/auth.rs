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
    ops::Deref,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use rand::{Rng, SeedableRng};
use zenoh_buffers::{
    reader::{DidntRead, HasReader, Reader},
    writer::{DidntWrite, HasWriter, Writer},
};
use zenoh_codec::{RCodec, WCodec, Zenoh080};
use zenoh_core::bail;
use zenoh_crypto::PseudoRng;
use zenoh_protocol::{
    core::Reliability,
    transport::{init, open},
};
use zenoh_result::{zerror, Error as ZError, ZResult};
use zenoh_shm::{
    api::common::types::{ProtocolID, SegmentID},
    reader::ShmReader,
};

use crate::{
    common::shm::interop::{LinkShmHandoffConfig, TransportShmConfig},
    unicast::establishment::{
        ext::shm::{
            handoff::{HandoffCounterIds, RxHandoffChannel, TxHandoffChannel},
            segment::{
                AuthChallenge, AuthSegmentID, RXAuthSegment, ShmTXCounterLease, TXAuthSegment,
            },
        },
        AcceptFsm, OpenFsm,
    },
};

/*************************************/
/*          Authenticator            */
/*************************************/
pub struct AuthUnicast {
    segment: Arc<TXAuthSegment>,
}

impl Deref for AuthUnicast {
    type Target = TXAuthSegment;

    fn deref(&self) -> &Self::Target {
        &self.segment
    }
}

impl AuthUnicast {
    pub fn new(shm_protocols: &[ProtocolID]) -> ZResult<Self> {
        // Create a challenge for session establishment
        let mut prng = PseudoRng::from_entropy();
        let nonce = prng.gen();

        // allocate SHM segment with challenge
        let segment = Arc::new(TXAuthSegment::create(nonce, shm_protocols)?);

        Ok(Self { segment })
    }

    pub fn validate(&self, expected_challenge: AuthChallenge, s: &str) -> bool {
        self.segment.validate(expected_challenge, s)
    }

    pub fn segment_id(&self) -> AuthSegmentID {
        self.segment.segment_id()
    }

    pub fn lease_tx_counter(&self) -> ZResult<ShmTXCounterLease> {
        ShmTXCounterLease::new(self.segment.clone())
    }
}

/*************************************/
/*             InitSyn               */
/*************************************/
/// ```text
///  7 6 5 4 3 2 1 0
/// +-+-+-+-+-+-+-+-+
/// ~  Segment id   ~
/// +---------------+
/// ```
pub(crate) struct InitSyn {
    pub(crate) alice_segment: AuthSegmentID,
    /// The id of Alice's TRANSPORT POOL, so Bob can mount it at establishment
    /// instead of on the first buffer that references it (BER-2170).
    ///
    /// `Option` for two independent reasons, and both really happen: Alice may
    /// have no provider (transport optimisation off, or an eager build that
    /// could not allocate), and Alice may be running a peer that predates this
    /// field and therefore sends nothing.
    pub(crate) alice_data_segment: Option<SegmentID>,
}

/// Marks a present data-segment id in the trailing bytes of an SHM
/// establishment message.
///
/// A tag rather than bare bytes, so a LATER field can be added unambiguously:
/// today "absent" is indistinguishable from end-of-buffer, and without a tag
/// the next field added would be read as this one by an intermediate build.
const EXT_DATA_SEGMENT_TAG: u8 = 1;

/// Write a trailing, optional data-segment id.
///
/// Trailing on purpose: a peer that predates the field reads the fields it
/// knows and returns, without checking that the buffer is exhausted, so these
/// bytes are ignored rather than fatal. That is what makes this safe to deploy
/// to one host at a time.
fn write_data_segment<W>(codec: Zenoh080, writer: &mut W, id: Option<SegmentID>) -> Result<(), DidntWrite>
where
    W: Writer,
{
    if let Some(id) = id {
        codec.write(&mut *writer, EXT_DATA_SEGMENT_TAG)?;
        codec.write(&mut *writer, id)?;
    }
    Ok(())
}

/// Read a trailing, optional data-segment id.
///
/// Every failure is `None`, never an error: an absent field means the peer
/// predates it, and an UNKNOWN tag means the peer is newer than us. Refusing
/// the handshake in either case would turn a forward-compatible field into a
/// fleet-wide outage during a rolling deploy.
fn read_data_segment<R>(codec: Zenoh080, reader: &mut R) -> Option<SegmentID>
where
    R: Reader,
{
    let tag: u8 = codec.read(&mut *reader).ok()?;
    if tag != EXT_DATA_SEGMENT_TAG {
        return None;
    }
    codec.read(&mut *reader).ok()
}

// Codec
impl<W> WCodec<&InitSyn, &mut W> for Zenoh080
where
    W: Writer,
{
    type Output = Result<(), DidntWrite>;

    fn write(self, writer: &mut W, x: &InitSyn) -> Self::Output {
        self.write(&mut *writer, &x.alice_segment)?;
        write_data_segment(self, writer, x.alice_data_segment)?;
        Ok(())
    }
}

impl<R> RCodec<InitSyn, &mut R> for Zenoh080
where
    R: Reader,
{
    type Error = DidntRead;

    fn read(self, reader: &mut R) -> Result<InitSyn, Self::Error> {
        let alice_segment = self.read(&mut *reader)?;
        let alice_data_segment = read_data_segment(self, reader);
        Ok(InitSyn {
            alice_segment,
            alice_data_segment,
        })
    }
}

#[cfg(test)]
mod data_segment_wire_tests {
    use zenoh_buffers::{reader::HasReader, writer::HasWriter};
    use zenoh_codec::{RCodec, WCodec, Zenoh080};

    use super::{read_data_segment, write_data_segment, InitSyn, EXT_DATA_SEGMENT_TAG};

    /// **A ROLLING DEPLOY MUST WORK IN BOTH DIRECTIONS.**
    ///
    /// This field is appended to a message that peers already exchange, so one
    /// host is upgraded before the other and BOTH mixed pairings occur. The
    /// failure mode if either direction is wrong is not a lost optimisation --
    /// it is a handshake that does not complete, i.e. two services that cannot
    /// talk at all.
    ///
    /// Asserted against BYTES rather than by round-tripping the new code with
    /// itself, which is the mistake that would make this vacuous: a
    /// self-consistent codec passes a round-trip test while being incompatible
    /// with every deployed peer.
    #[test]
    fn the_data_segment_field_is_compatible_with_a_peer_that_does_not_know_it() {
        let codec = Zenoh080::new();

        // What an OLD peer writes: the auth segment id and nothing else.
        let mut old_bytes = vec![];
        {
            let mut w = old_bytes.writer();
            codec.write(&mut w, &7u32).unwrap();
        }

        // A NEW reader on an OLD message must see "no pool advertised", NOT an
        // error. An error here would refuse every peer that has not been
        // upgraded yet.
        let decoded: InitSyn = codec.read(&mut old_bytes.reader()).unwrap();
        assert_eq!(decoded.alice_segment, 7);
        assert_eq!(
            decoded.alice_data_segment, None,
            "an old peer advertises no pool, and that must decode cleanly"
        );

        // What a NEW peer writes when it HAS a pool.
        let mut new_bytes = vec![];
        {
            let mut w = new_bytes.writer();
            codec
                .write(
                    &mut w,
                    &InitSyn {
                        alice_segment: 7,
                        alice_data_segment: Some(42),
                    },
                )
                .unwrap();
        }

        // An OLD reader is modelled exactly as the old code behaved: read the
        // one field it knows and stop, WITHOUT requiring the buffer to be
        // exhausted. The appended bytes must therefore be harmless.
        let mut r = new_bytes.reader();
        let old_read: u32 = codec.read(&mut r).unwrap();
        assert_eq!(
            old_read, 7,
            "an old peer must still read the auth segment id from a new message"
        );

        // And a new reader gets the pool back.
        let round: InitSyn = codec.read(&mut new_bytes.reader()).unwrap();
        assert_eq!(round.alice_data_segment, Some(42));

        // A NEW peer with NO pool must be byte-identical to an old peer, so it
        // cannot be told apart on the wire.
        let mut none_bytes = vec![];
        {
            let mut w = none_bytes.writer();
            codec
                .write(
                    &mut w,
                    &InitSyn {
                        alice_segment: 7,
                        alice_data_segment: None,
                    },
                )
                .unwrap();
        }
        assert_eq!(
            none_bytes, old_bytes,
            "advertising nothing must put nothing on the wire"
        );
    }

    /// **AN UNKNOWN TAG IS "NO POOL", NEVER A FAILURE.**
    ///
    /// The quiet direction of forward compatibility: a peer NEWER than us may
    /// append a field we do not know. Reading it as a pool id would mount a
    /// segment that does not exist, and failing would refuse the link.
    #[test]
    fn an_unknown_trailing_tag_decodes_as_no_pool() {
        let codec = Zenoh080::new();
        let mut bytes = vec![];
        {
            let mut w = bytes.writer();
            codec.write(&mut w, EXT_DATA_SEGMENT_TAG + 9).unwrap();
            codec.write(&mut w, &1234u32).unwrap();
        }
        assert_eq!(
            read_data_segment(codec, &mut bytes.reader()),
            None,
            "a tag from a newer peer must read as no pool"
        );

        // The control: the KNOWN tag does yield the id, so the assertion above
        // is not passing merely because nothing is ever read.
        let mut good = vec![];
        {
            let mut w = good.writer();
            write_data_segment(codec, &mut w, Some(1234)).unwrap();
        }
        assert_eq!(read_data_segment(codec, &mut good.reader()), Some(1234));
    }
}

/*************************************/
/*             InitAck               */
/*************************************/
/// ```text
///  7 6 5 4 3 2 1 0
/// +-+-+-+-+-+-+-+-+
/// ~   challenge   ~
/// +---------------+
/// ~  Segment id   ~
/// +---------------+
/// ```
struct InitAck {
    alice_challenge: u64,
    bob_segment: AuthSegmentID,
    /// Bob's transport pool, so Alice can mount it at establishment too
    /// (BER-2170). Optional for the same two reasons as Alice's.
    bob_data_segment: Option<SegmentID>,
}

impl<W> WCodec<&InitAck, &mut W> for Zenoh080
where
    W: Writer,
{
    type Output = Result<(), DidntWrite>;

    fn write(self, writer: &mut W, x: &InitAck) -> Self::Output {
        self.write(&mut *writer, x.alice_challenge)?;
        self.write(&mut *writer, &x.bob_segment)?;
        write_data_segment(self, writer, x.bob_data_segment)?;
        Ok(())
    }
}

impl<R> RCodec<InitAck, &mut R> for Zenoh080
where
    R: Reader,
{
    type Error = DidntRead;

    fn read(self, reader: &mut R) -> Result<InitAck, Self::Error> {
        let alice_challenge: u64 = self.read(&mut *reader)?;
        let bob_segment = self.read(&mut *reader)?;
        let bob_data_segment = read_data_segment(self, reader);
        Ok(InitAck {
            alice_challenge,
            bob_segment,
            bob_data_segment,
        })
    }
}

/*************************************/
/*             OpenSyn               */
/*************************************/
/// ```text
///  7 6 5 4 3 2 1 0
/// +-+-+-+-+-+-+-+-+
/// ~   challenge   ~
/// +---------------+
/// ~  Counter Id   ~
/// +---------------+
/// ```
struct OpenSyn {
    bob_challenge: u64,
    alice_counters: HandoffCounterIds,
}

impl<W> WCodec<&OpenSyn, &mut W> for Zenoh080
where
    W: Writer,
{
    type Output = Result<(), DidntWrite>;

    fn write(self, writer: &mut W, x: &OpenSyn) -> Self::Output {
        self.write(&mut *writer, x.bob_challenge)?;
        self.write(&mut *writer, &x.alice_counters)?;
        Ok(())
    }
}

impl<R> RCodec<OpenSyn, &mut R> for Zenoh080
where
    R: Reader,
{
    type Error = DidntRead;

    fn read(self, reader: &mut R) -> Result<OpenSyn, Self::Error> {
        let bob_challenge: u64 = self.read(&mut *reader)?;
        let alice_counters = self.read(&mut *reader)?;
        Ok(OpenSyn {
            bob_challenge,
            alice_counters,
        })
    }
}

/*************************************/
/*             OpenAck               */
/*************************************/
/// ```text
///  7 6 5 4 3 2 1 0
/// +-+-+-+-+-+-+-+-+
/// ~  Counter Id   ~
/// +---------------+
/// ```
struct OpenAck {
    bob_counters: HandoffCounterIds,
}

impl<W> WCodec<&OpenAck, &mut W> for Zenoh080
where
    W: Writer,
{
    type Output = Result<(), DidntWrite>;

    fn write(self, writer: &mut W, x: &OpenAck) -> Self::Output {
        self.write(&mut *writer, &x.bob_counters)?;
        Ok(())
    }
}

impl<R> RCodec<OpenAck, &mut R> for Zenoh080
where
    R: Reader,
{
    type Error = DidntRead;

    fn read(self, reader: &mut R) -> Result<OpenAck, Self::Error> {
        let bob_counters = self.read(&mut *reader)?;
        Ok(OpenAck { bob_counters })
    }
}

// Extension Fsm
pub(crate) struct ShmFsm<'a> {
    inner: &'a AuthUnicast,
    /// Our transport pool's segment id, advertised so the peer can mount it at
    /// establishment (BER-2170). `None` when we have no pool to offer.
    our_data_segment: Option<SegmentID>,
    /// The reader the peer's pool is mounted into. `None` leaves the peer's
    /// pool to be mounted on the first buffer, which is the old behaviour.
    reader: Option<ShmReader>,
    // Set when the acceptor's InitAck carried an SHM extension, whatever became of it.
    acceptor_offered_shm: AtomicBool,
    tx_lease: std::sync::OnceLock<TxHandoffChannel>,
    rx_segment: std::sync::OnceLock<Arc<RXAuthSegment>>,
    rx_lease: std::sync::OnceLock<RxHandoffChannel>,
}

impl<'a> ShmFsm<'a> {
    pub(crate) const fn new(inner: &'a AuthUnicast) -> Self {
        Self {
            inner,
            our_data_segment: None,
            reader: None,
            acceptor_offered_shm: AtomicBool::new(false),
            tx_lease: std::sync::OnceLock::new(),
            rx_segment: std::sync::OnceLock::new(),
            rx_lease: std::sync::OnceLock::new(),
        }
    }

    /// Advertise our transport pool, and mount the peer's, during establishment.
    ///
    /// Separate from `new` so the many auth tests, which exercise the challenge
    /// and not the pool, need no change and simply keep the old behaviour.
    pub(crate) fn with_data_segment_exchange(
        mut self,
        our_data_segment: Option<SegmentID>,
        reader: ShmReader,
    ) -> Self {
        self.our_data_segment = our_data_segment;
        self.reader = Some(reader);
        self
    }

    /// Mount the peer's transport pool now, while both peers are known.
    ///
    /// A failure is REPORTED and then tolerated. It must not fail the
    /// handshake: the peer is reachable and its auth segment opened, so the
    /// link is usable -- just without shared memory from this peer, which is
    /// exactly where a failed mount left us before, only silently and one
    /// message later (BER-2167).
    fn premount_peer_pool(&self, peer: Option<SegmentID>, s: &str) {
        let (Some(segment), Some(reader)) = (peer, self.reader.as_ref()) else {
            return;
        };
        for protocol in reader.supported_protocols() {
            if let Err(e) = reader.premount_segment(protocol, segment) {
                tracing::error!(
                    segment,
                    protocol,
                    "{} could not mount the peer's shared-memory pool at establishment: {}. \
                     Shared memory from this peer will be DROPPED rather than received, one \
                     message at a time (BER-2167). The usual cause is this process's \
                     RLIMIT_MEMLOCK",
                    s,
                    e
                );
            }
        }
    }

    pub fn shm_init_result(self) -> (Option<TransportShmConfig>, LinkShmHandoffConfig) {
        // Opening the peer's auth segment is only the first negotiation step.
        // Enable SHM only after both directions of the handshake have completed.
        match (
            self.rx_segment.into_inner(),
            self.rx_lease.into_inner(),
            self.tx_lease.into_inner(),
        ) {
            (Some(segment), Some(rx), Some(tx)) => (
                Some(TransportShmConfig::new(segment)),
                LinkShmHandoffConfig::new(rx, tx),
            ),
            _ => (None, LinkShmHandoffConfig::new_disabled()),
        }
    }
}

/*************************************/
/*              OPEN                 */
/*************************************/
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct StateOpen {}

impl StateOpen {
    pub(crate) const fn new() -> Self {
        Self {}
    }

    #[cfg(test)]
    pub(crate) fn rand() -> Self {
        Self {}
    }
}

#[async_trait]
impl<'a> OpenFsm for &'a ShmFsm<'a> {
    type Error = ZError;

    type SendInitSynIn = &'a StateOpen;
    type SendInitSynOut = Option<init::ext::Shm>;
    async fn send_init_syn(
        self,
        _state: Self::SendInitSynIn,
    ) -> Result<Self::SendInitSynOut, Self::Error> {
        const S: &str = "Shm extension - Send InitSyn.";

        // Send Alice's SHM Segment ID to Bob
        let init_syn = InitSyn {
            alice_segment: self.inner.segment_id(),
            alice_data_segment: self.our_data_segment,
        };

        let codec = Zenoh080::new();
        let mut buff = vec![];
        let mut writer = buff.writer();
        codec
            .write(&mut writer, &init_syn)
            .map_err(|_| zerror!("{} Encoding error", S))?;

        Ok(Some(init::ext::Shm::new(buff.into())))
    }

    type RecvInitAckIn = Option<init::ext::Shm>;
    type RecvInitAckOut = ();
    async fn recv_init_ack(
        self,
        mut input: Self::RecvInitAckIn,
    ) -> Result<Self::RecvInitAckOut, Self::Error> {
        const S: &str = "Shm extension - Recv InitAck.";

        // Record the offer before any check can decline it.
        self.acceptor_offered_shm
            .store(input.is_some(), Ordering::Relaxed);

        let Some(ext) = input.take() else {
            return Ok(());
        };

        // Decode the extension
        let codec = Zenoh080::new();
        let mut reader = ext.value.reader();
        let Ok(init_ack): Result<InitAck, _> = codec.read(&mut reader) else {
            tracing::trace!("{} Decoding error.", S);
            return Ok(());
        };

        // Verify that Bob has correctly read Alice challenge
        if !self.inner.validate(init_ack.alice_challenge, S) {
            return Ok(());
        }

        // Read Bob's SHM Segment
        let bob_segment = match RXAuthSegment::open(init_ack.bob_segment) {
            Ok(buff) => buff,
            Err(e) => {
                tracing::trace!("{} {}", S, e);
                return Ok(());
            }
        };

        // Store Bob's SHM Segment for later use
        self.rx_segment
            .set(Arc::new(bob_segment))
            .map_err(|_| zerror!("{} State machine error", S))?;

        // Bob's challenge checked out, so mount his pool now rather than on the
        // first buffer that references it (BER-2170).
        self.premount_peer_pool(init_ack.bob_data_segment, S);

        Ok(())
    }

    type SendOpenSynIn = Reliability;
    type SendOpenSynOut = Option<open::ext::Shm>;
    async fn send_open_syn(
        self,
        input: Self::SendOpenSynIn,
    ) -> Result<Self::SendOpenSynOut, Self::Error> {
        const S: &str = "Shm extension - Send OpenSyn.";

        // Link is multipriority
        let reliability = input;

        // take RX segment from input
        let Some(rx_segment) = self.rx_segment.get() else {
            return Ok(None);
        };

        // Allocate TX counter for this session
        let tx_handoff = TxHandoffChannel::new_tx(reliability, self.inner)?;

        let open_syn = OpenSyn {
            bob_challenge: rx_segment.challenge(),
            alice_counters: tx_handoff.ids(),
        };

        // Encode the extension
        let codec = Zenoh080::new();
        let mut buff = vec![];
        let mut writer = buff.writer();
        codec
            .write(&mut writer, &open_syn)
            .map_err(|_| zerror!("{} Encoding error", S))?;

        self.tx_lease
            .set(tx_handoff)
            .map_err(|_| zerror!("{} State machine error", S))?;

        Ok(Some(open::ext::Shm::new(buff.into())))
    }

    type RecvOpenAckIn = Option<open::ext::Shm>;
    type RecvOpenAckOut = ();
    async fn recv_open_ack(
        self,
        mut input: Self::RecvOpenAckIn,
    ) -> Result<Self::RecvOpenAckOut, Self::Error> {
        const S: &str = "Shm extension - Recv OpenAck.";

        let Some(ext) = input.take() else {
            return Ok(());
        };

        // An acceptor that offered no SHM in its InitAck did not ask for SHM. An unpatched
        // acceptor still puts an extension in every OpenAck, so ignore it and run without SHM.
        if !self.acceptor_offered_shm.load(Ordering::Relaxed) {
            tracing::trace!("{} Ignoring an SHM acknowledgement: no SHM offer.", S);
            return Ok(());
        }

        // After an offer, an SHM OpenAck commits the acceptor to SHM. If we cannot honor it,
        // reject establishment instead of silently enabling SHM on one side.
        let segment = self
            .rx_segment
            .get()
            .ok_or_else(|| zerror!("{} Unexpected SHM acknowledgement", S))?;
        if self.tx_lease.get().is_none() {
            bail!("{} SHM acknowledgement without a local proposal", S);
        }

        // Decode the extension
        let codec = Zenoh080::new();
        let mut reader = ext.value.reader();
        let open_ack: OpenAck = codec
            .read(&mut reader)
            .map_err(|_| zerror!("{} Decoding error", S))?;

        let rx_handoff = RxHandoffChannel::new_rx(segment, open_ack.bob_counters)
            .map_err(|e| zerror!("{} Handoff channel creation error: {}", S, e))?;

        self.rx_lease
            .set(rx_handoff)
            .map_err(|_| zerror!("{} State machine error", S))?;

        Ok(())
    }
}

/*************************************/
/*            ACCEPT                 */
/*************************************/

pub(crate) type StateAccept = StateOpen;

// Codec
impl<W> WCodec<&StateAccept, &mut W> for Zenoh080
where
    W: Writer,
{
    type Output = Result<(), DidntWrite>;

    fn write(self, _writer: &mut W, _x: &StateAccept) -> Self::Output {
        Ok(())
    }
}

impl<R> RCodec<StateAccept, &mut R> for Zenoh080
where
    R: Reader,
{
    type Error = DidntRead;

    fn read(self, _reader: &mut R) -> Result<StateAccept, Self::Error> {
        Ok(StateAccept {})
    }
}

#[async_trait]
impl<'a> AcceptFsm for &'a ShmFsm<'a> {
    type Error = ZError;

    type RecvInitSynIn = Option<init::ext::Shm>;
    type RecvInitSynOut = ();
    async fn recv_init_syn(
        self,
        input: Self::RecvInitSynIn,
    ) -> Result<Self::RecvInitSynOut, Self::Error> {
        const S: &str = "Shm extension - Recv InitSyn.";

        let Some(ext) = input.as_ref() else {
            return Ok(());
        };

        // Decode the extension
        let codec = Zenoh080::new();
        let mut reader = ext.value.reader();
        let Ok(init_syn): Result<InitSyn, _> = codec.read(&mut reader) else {
            tracing::trace!("{} Decoding error.", S);
            return Ok(());
        };

        // Read Alice's SHM Segment
        let alice_segment = match RXAuthSegment::open(init_syn.alice_segment) {
            Ok(buff) => buff,
            Err(e) => {
                tracing::trace!("{} {}", S, e);
                return Ok(());
            }
        };

        self.rx_segment
            .set(Arc::new(alice_segment))
            .map_err(|_| zerror!("{} State machine error", S))?;

        // Alice's auth segment has opened, so Alice is real: mount her pool now
        // rather than on the first buffer that references it (BER-2170).
        self.premount_peer_pool(init_syn.alice_data_segment, S);

        Ok(())
    }

    type SendInitAckIn = ();
    type SendInitAckOut = Option<init::ext::Shm>;
    async fn send_init_ack(
        self,
        _input: Self::SendInitAckIn,
    ) -> Result<Self::SendInitAckOut, Self::Error> {
        const S: &str = "Shm extension - Send InitAck.";

        let Some(alice_segment) = self.rx_segment.get() else {
            return Ok(None);
        };

        let init_ack = InitAck {
            alice_challenge: alice_segment.challenge(),
            bob_segment: self.inner.segment_id(),
            bob_data_segment: self.our_data_segment,
        };

        let codec = Zenoh080::new();
        let mut buff = vec![];
        let mut writer = buff.writer();
        codec
            .write(&mut writer, &init_ack)
            .map_err(|_| zerror!("{} Encoding error", S))?;

        Ok(Some(init::ext::Shm::new(buff.into())))
    }

    type RecvOpenSynIn = Option<open::ext::Shm>;
    type RecvOpenSynOut = ();
    async fn recv_open_syn(
        self,
        input: Self::RecvOpenSynIn,
    ) -> Result<Self::RecvOpenSynOut, Self::Error> {
        const S: &str = "Shm extension - Recv OpenSyn.";

        let Some(ext) = input else {
            return Ok(());
        };

        let Some(segment) = self.rx_segment.get() else {
            return Ok(());
        };

        // Decode the extension
        let codec = Zenoh080::new();
        let mut reader = ext.value.reader();
        let Ok(open_syn): Result<OpenSyn, _> = codec.read(&mut reader) else {
            tracing::trace!("{} Decoding error.", S);
            bail!("");
        };

        // Alice sent an SHM proposal and an unpatched Alice has already enabled SHM from the
        // segment it mapped, without waiting for our OpenAck. If we declined here, we would run
        // plain while she sends SHM descriptors. Refuse the link instead.
        if !self.inner.validate(open_syn.bob_challenge, S) {
            bail!("{} Challenge mismatch: refusing the SHM proposal", S);
        }

        // Allocate RX counter for this session
        let rx_handoff = RxHandoffChannel::new_rx(segment, open_syn.alice_counters)
            .map_err(|e| zerror!("{} Handoff channel creation error: {}", S, e))?;

        self.rx_lease
            .set(rx_handoff)
            .map_err(|_| zerror!("{} State machine error", S))?;

        Ok(())
    }

    type SendOpenAckIn = Reliability;
    type SendOpenAckOut = Option<open::ext::Shm>;
    async fn send_open_ack(
        self,
        input: Self::SendOpenAckIn,
    ) -> Result<Self::SendOpenAckOut, Self::Error> {
        const S: &str = "Shm extension - Send OpenAck.";

        // A missing or rejected SHM OpenSyn means the peer did not negotiate
        // SHM. Do not advertise success or allocate outgoing handoff counters.
        if self.rx_lease.get().is_none() {
            return Ok(None);
        }

        // Link is multipriority
        let reliability = input;

        // Allocate TX counter for this session
        let tx_handoff = TxHandoffChannel::new_tx(reliability, self.inner)?;

        let open_ack = OpenAck {
            bob_counters: tx_handoff.ids(),
        };

        let codec = Zenoh080::new();
        let mut buff = vec![];
        let mut writer = buff.writer();
        codec
            .write(&mut writer, &open_ack)
            .map_err(|_| zerror!("{} Encoding error", S))?;

        self.tx_lease
            .set(tx_handoff)
            .map_err(|_| zerror!("{} State machine error", S))?;

        Ok(Some(open::ext::Shm::new(buff.into())))
    }
}

#[cfg(test)]
#[path = "auth_tests.rs"]
mod tests;
