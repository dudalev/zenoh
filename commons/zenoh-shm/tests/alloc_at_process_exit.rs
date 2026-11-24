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
//! An allocation while process exit finalizes this crate's statics fails, and gives its
//! chunk back, instead of panicking (BER-2486).
//!
//! static_init finalizes its statics in the reverse order of their registration, and an
//! allocation reads the metadata storage and the confirmator before the validator, so exit
//! finalizes the validator first. An allocation between the validator's finalization and
//! theirs got as far as `ShmProvider::wrap`, which read the validator with the panicking
//! `.read()`.
//!
//! The test runs itself again as a child, which registers the storage and the confirmator,
//! then `WINDOW`, a static of this test's, then the validator, and exits. Exit then finalizes
//! the validator, then `WINDOW`, whose drop has another thread allocate: an allocation that
//! meets the validator finalized and the storage and the confirmator live.
#![cfg(all(unix, feature = "test"))]

use std::{
    io::Read,
    process::{Command, Stdio},
    sync::{
        mpsc::{channel, Receiver, Sender},
        Mutex,
    },
    time::{Duration, Instant},
};

use static_init::{dynamic, Phase};
use zenoh_core::Wait;
use zenoh_shm::{
    api::{
        protocol_implementations::posix::posix_shm_provider_backend_binary_heap::PosixShmProviderBackendBinaryHeap,
        provider::{shm_provider::ShmProviderBuilder, types::ZLayoutAllocError},
    },
    metadata::storage::GLOBAL_METADATA_STORAGE,
    watchdog::{confirmator::GLOBAL_CONFIRMATOR, validator::GLOBAL_VALIDATOR},
};

/// Set in the child.
const CHILD: &str = "ZENOH_TEST_ALLOC_EXIT_CHILD";
const TEST_NAME: &str = "an_allocation_while_exit_finalizes_the_validator_fails";
/// The child's line with the result of an allocation while live.
const LIVE: &str = "CHILD LIVE";
/// The child's line with the statics' phases once `WINDOW` is being finalized.
const PHASES: &str = "CHILD PHASES";
/// The child's line with the result of the allocation in exit.
const ALLOCATED: &str = "CHILD ALLOCATED";
const TIMEOUT: Duration = Duration::from_secs(60);

/// What the allocation in exit reports: its result, and the provider's available bytes
/// before and after it.
type Allocated = (Result<(), ZLayoutAllocError>, usize, usize);

/// How `WINDOW`'s drop tells the allocating thread to go, and hears its result.
static CONTROL: Mutex<Option<(Sender<()>, Receiver<Allocated>)>> = Mutex::new(None);

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
                Ok((result, before, after)) => {
                    println!("{ALLOCATED} {result:?} available {before} -> {after}")
                }
                Err(e) => println!("{ALLOCATED} no answer: {e}"),
            }
        }
    }
}

#[dynamic(lazy, drop)]
static mut WINDOW: Window = Window;

/// **AN ALLOCATION WHILE EXIT FINALIZES THE VALIDATOR FAILS**, and gives its chunk back.
#[test]
fn an_allocation_while_exit_finalizes_the_validator_fails() {
    if std::env::var_os(CHILD).is_some() {
        child();
    }
    parent();
}

/// Registers the statics in the order that puts `WINDOW` between the confirmator and the
/// validator, and exits with a thread ready to allocate.
fn child() -> ! {
    // The provider's segment registers the cleanup registry first. This backend counts its
    // available bytes, which the default one does not.
    let backend = PosixShmProviderBackendBinaryHeap::builder(65536)
        .wait()
        .unwrap();
    let provider = ShmProviderBuilder::backend(backend).wait();
    GLOBAL_METADATA_STORAGE.init();
    GLOBAL_CONFIRMATOR.init();
    WINDOW.init();
    GLOBAL_VALIDATOR.init();

    // THE CONTROL: while live, an allocation succeeds.
    let live = provider.alloc(64).wait().map(drop);
    println!("{LIVE} {live:?}");

    let (go, go_rx) = channel();
    let (done_tx, done) = channel();
    *CONTROL.lock().unwrap() = Some((go, done));
    std::thread::spawn(move || {
        go_rx.recv().unwrap();
        let before = provider.available();
        let result = provider.alloc(64).wait().map(drop);
        let after = provider.available();
        done_tx.send((result, before, after)).unwrap();
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

    assert!(
        status.success(),
        "the child must exit as it chose to: {status}\n{log}"
    );
    assert!(
        stdout.contains(&format!("{LIVE} Ok(())")),
        "while live an allocation must succeed\n{log}"
    );
    let phases = stdout.lines().position(|line| {
        line.contains(&format!(
            "{PHASES} validator=finalized storage=live confirmator=live"
        ))
    });
    assert!(
        phases.is_some(),
        "the allocation must run while the validator is finalized and the storage and the \
         confirmator are live, or it never reached `wrap`\n{log}"
    );
    let allocated = stdout
        .lines()
        .position(|line| line.contains(ALLOCATED))
        .map(|i| (i, stdout.lines().nth(i).unwrap().to_string()));
    let Some((at, allocated)) = allocated else {
        panic!("the allocation in exit must report\n{log}");
    };
    assert!(at > phases.unwrap(), "{log}");
    assert!(
        !stderr.contains("panicked"),
        "an allocation while exit finalizes the validator must fail, not panic\n{log}"
    );
    let words: Vec<&str> = allocated.split_whitespace().collect();
    assert_eq!(
        &words[..4],
        &["CHILD", "ALLOCATED", "Err(Alloc(Other))", "available"],
        "the allocation must fail\n{log}"
    );
    let (before, after): (usize, usize) = (words[4].parse().unwrap(), words[6].parse().unwrap());
    assert!(
        before > 0,
        "the provider must count its available bytes\n{log}"
    );
    assert_eq!(
        before, after,
        "the failed allocation must give its chunk back: {allocated}\n{log}"
    );
}
