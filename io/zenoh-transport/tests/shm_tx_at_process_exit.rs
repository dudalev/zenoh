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
//! A send while process exit finalizes zenoh-shm's statics goes out as an inline copy instead
//! of panicking (BER-2486).
//!
//! On its way out a payload of at least `message_size_threshold` that is not in shared memory
//! already is copied into the transport's own pool (`map_zmsg_to_partner`), which allocates.
//! static_init finalizes its statics in the reverse order of their registration, and an
//! allocation reads the metadata storage and the confirmator before the validator, so exit
//! finalizes the validator first. An allocation in between met it in `ShmProvider::wrap`,
//! which read it with the panicking `.read()`. The allocation now fails there, and a failed
//! allocation leaves the payload as it is, so the message is sent as the copy it was.
//!
//! The test runs itself again as a child. The child registers the storage and the
//! confirmator, then `WINDOW`, a static of this test's, then the validator, and exits. Exit
//! then finalizes the validator, then `WINDOW`, whose drop has another thread map a message
//! for sending: the window between the validator's finalization and the others'.
#![cfg(all(unix, feature = "shared-memory"))]

use std::{
    io::Read,
    num::NonZeroUsize,
    process::{Command, Stdio},
    sync::{
        mpsc::{channel, Receiver, Sender},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use static_init::{dynamic, Phase};
use zenoh_buffers::{buffer::SplitBuffer, ZBuf, ZSliceKind};
use zenoh_protocol::{
    core::{CongestionControl, Priority},
    network::{push::ext::QoSType, NetworkBody, NetworkMessage, Push},
    zenoh::{PushBody, Put},
};
use zenoh_shm::{
    api::common::types::ProtocolID,
    metadata::storage::GLOBAL_METADATA_STORAGE,
    watchdog::{confirmator::GLOBAL_CONFIRMATOR, validator::GLOBAL_VALIDATOR},
};
use zenoh_transport::{
    common::shm::{
        interop::{map_zmsg_to_partner, LazyShmProvider, PartnerShmConfig, ShmOptimizationPolicy},
        ProviderInitState,
    },
    unicast::establishment::ext::shm::handoff::TxHandoffStorage,
};

/// Set in the child.
const CHILD: &str = "ZENOH_TEST_SHM_TX_EXIT_CHILD";
const TEST_NAME: &str = "a_send_while_exit_finalizes_the_validator_goes_out_as_a_copy";
/// The child's line with what a send did to its payload while live.
const LIVE: &str = "CHILD LIVE";
/// The child's line with the statics' phases once `WINDOW` is being finalized.
const PHASES: &str = "CHILD PHASES";
/// The child's line with what the send in exit did to its payload.
const MAPPED: &str = "CHILD MAPPED";
const TIMEOUT: Duration = Duration::from_secs(60);
const POOL_SIZE: usize = 1024 * 1024;
const MESSAGE_SIZE_THRESHOLD: usize = 3072;
/// Above the threshold, so a send copies the payload into shared memory.
const PAYLOAD_SIZE: usize = 4096;
const PAYLOAD_BYTE: u8 = 0x5a;
const ALL_ON: ShmOptimizationPolicy = ShmOptimizationPolicy {
    put: true,
    query: true,
    reply: true,
};

/// How `WINDOW`'s drop tells the sending thread to go, and hears what became of its payload.
static CONTROL: Mutex<Option<(Sender<()>, Receiver<String>)>> = Mutex::new(None);

/// Registered between the confirmator and the validator, so finalized between them.
struct Window;

impl Drop for Window {
    fn drop(&mut self) {
        let phase = |accessible: Result<(), Phase>| match accessible {
            Ok(()) => "live".to_string(),
            Err(phase) if phase.intersects(Phase::FINALIZED) => "finalized".to_string(),
            Err(phase) => format!("{phase:?}"),
        };
        println!(
            "{PHASES} validator={} storage={} confirmator={}",
            phase(GLOBAL_VALIDATOR.try_read().map(drop).map_err(|e| e.phase)),
            phase(
                GLOBAL_METADATA_STORAGE
                    .try_read()
                    .map(drop)
                    .map_err(|e| e.phase)
            ),
            phase(GLOBAL_CONFIRMATOR.try_read().map(drop).map_err(|e| e.phase)),
        );
        let control = CONTROL.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some((go, done)) = control {
            go.send(()).unwrap();
            match done.recv_timeout(Duration::from_secs(10)) {
                Ok(mapped) => println!("{MAPPED} {mapped}"),
                Err(e) => println!("{MAPPED} no answer: {e}"),
            }
        }
    }
}

#[dynamic(lazy, drop)]
static mut WINDOW: Window = Window;

/// A partner that supports every shared-memory protocol.
struct AllProtocols;

impl PartnerShmConfig for AllProtocols {
    fn supports_protocol(&self, _protocol: ProtocolID) -> bool {
        true
    }
}

/// Maps a large inline payload for sending, as the send path does, and says what became of
/// it: copied into shared memory, or left as it is, and whether it is still the payload.
fn send_mapping(provider: &Option<Arc<LazyShmProvider>>) -> String {
    let mut msg = NetworkMessage::from(Push {
        wire_expr: "test".into(),
        ext_qos: QoSType::new(Priority::DEFAULT, CongestionControl::Block, false),
        ..Push::from(Put {
            payload: ZBuf::from(vec![PAYLOAD_BYTE; PAYLOAD_SIZE]),
            ..Put::default()
        })
    });
    {
        let handoff = TxHandoffStorage::new_disabled();
        let mut m = msg.as_mut();
        let _transaction = map_zmsg_to_partner(&mut m, &AllProtocols, provider, &handoff, ALL_ON);
    }
    let NetworkBody::Push(Push {
        payload: PushBody::Put(Put {
            payload, ext_shm, ..
        }),
        ..
    }) = &msg.body
    else {
        unreachable!("a Put was sent")
    };
    let shm = payload.zslices().next().unwrap().kind == ZSliceKind::ShmPtr;
    let bytes = payload.contiguous();
    let intact = bytes.len() == PAYLOAD_SIZE && bytes.iter().all(|b| *b == PAYLOAD_BYTE);
    format!("shm={shm} ext_shm={} intact={intact}", ext_shm.is_some())
}

/// **A SEND WHILE EXIT FINALIZES THE VALIDATOR GOES OUT AS A COPY.**
#[test]
fn a_send_while_exit_finalizes_the_validator_goes_out_as_a_copy() {
    if std::env::var_os(CHILD).is_some() {
        child();
    }
    parent();
}

/// Registers the statics in the order that puts `WINDOW` between the confirmator and the
/// validator, and exits with a thread ready to send.
fn child() -> ! {
    // The pool is built now, as `init` mode does, and its segment registers the cleanup
    // registry first.
    let provider = Arc::new(LazyShmProvider::new(
        NonZeroUsize::new(POOL_SIZE).unwrap(),
        MESSAGE_SIZE_THRESHOLD,
    ));
    provider.init_eagerly();
    assert!(matches!(
        provider.try_get_provider(),
        ProviderInitState::Ready(_)
    ));
    GLOBAL_METADATA_STORAGE.init();
    GLOBAL_CONFIRMATOR.init();
    WINDOW.init();
    GLOBAL_VALIDATOR.init();
    let provider = Some(provider);

    // THE CONTROL: while live, a send copies the payload into shared memory.
    println!("{LIVE} {}", send_mapping(&provider));

    let (go, go_rx) = channel();
    let (done_tx, done) = channel();
    *CONTROL.lock().unwrap() = Some((go, done));
    std::thread::spawn(move || {
        go_rx.recv().unwrap();
        done_tx.send(send_mapping(&provider)).unwrap();
        // The provider stays, so that nothing it owns is released during exit.
        loop {
            std::thread::park();
        }
    });
    std::process::exit(0)
}

fn parent() {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST_NAME, "--nocapture", "--test-threads=1"])
        .env(CHILD, "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout_pipe = child.stdout.take().unwrap();
    let mut stderr_pipe = child.stderr.take().unwrap();
    let stdout_reader = std::thread::spawn(move || {
        let mut out = String::new();
        stdout_pipe.read_to_string(&mut out).unwrap();
        out
    });
    let stderr_reader = std::thread::spawn(move || {
        let mut err = String::new();
        stderr_pipe.read_to_string(&mut err).unwrap();
        err
    });
    let deadline = Instant::now() + TIMEOUT;
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("the child did not exit within {TIMEOUT:?}");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let stdout = stdout_reader.join().unwrap();
    let stderr = stderr_reader.join().unwrap();
    let log = format!("child stdout:\n{stdout}\nchild stderr:\n{stderr}");
    let line = |prefix: &str| stdout.lines().position(|line| line.contains(prefix));

    assert!(
        status.success(),
        "the child must exit as it chose to: {status}\n{log}"
    );
    assert!(
        stdout.contains(&format!("{LIVE} shm=true ext_shm=true intact=true")),
        "while live a send must copy its payload into shared memory, or the copy below says \
         nothing\n{log}"
    );
    let phases = line(&format!(
        "{PHASES} validator=finalized storage=live confirmator=live"
    ));
    assert!(
        phases.is_some(),
        "the send must run while the validator is finalized and the storage and the \
         confirmator are live, or its allocation never reached `wrap`\n{log}"
    );
    assert!(
        !stderr.contains("panicked"),
        "a send while exit finalizes the validator must not panic\n{log}"
    );
    let mapped = line(&format!("{MAPPED} shm=false ext_shm=false intact=true"));
    assert!(
        matches!((phases, mapped), (Some(p), Some(m)) if p < m),
        "the send in exit must leave its payload as it is, to go out as a copy\n{log}"
    );
}
