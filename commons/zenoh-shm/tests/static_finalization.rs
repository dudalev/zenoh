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

//! A `#[dynamic(lazy, drop)]` static is FINALIZED while other threads still run,
//! and reading one then is a PANIC.
//!
//! `static_init` registers a destructor for every `#[dynamic(lazy, drop)]`
//! static. At process exit those destructors run, and the static enters the
//! `FINALIZED` phase. The generated `.read()` accessor is
//! `init_then_try_read_lock(..).unwrap()` (`generic_lazy.rs`), so a read after
//! finalization does not fail -- it panics.
//!
//! Nothing stops zenoh's own receive and publish threads before that point.
//! They are owned by the runtime, not by these statics, so a thread mapping or
//! allocating a buffer during exit reaches a finalized global. Measured on giedi
//! 2026-11-17 and 2026-11-18: three occurrences, each one second after a
//! deliberate non-zero exit, `thread 'rx-1' panicked ... AccessError { phase:
//! INITIALIZED | REGISTERED | FINALIZED | INITIALIZED_AND_REGISTERED }`. The
//! exit's own error message was good; the panic landed after it and pointed into
//! this crate instead, so the real cause read as a footnote.
//!
//! `Segment::drop` in `posix_shm/segment.rs` already guards this, with the
//! reasoning in its own comment -- *"this drop might be executed inside static
//! drop callstack, so statics might not be available here"*. It was the only
//! site that did.
//!
//! THIS IS A SOURCE SCAN, and deliberately so. The condition it rules out is a
//! PANIC during process teardown, which no behavioural test in this crate can
//! reach: finalization happens once, after the harness has finished. What can be
//! checked is that no production site holds the panicking accessor, which is the
//! property that keeps the bug from coming back by a route nobody is watching.

use std::{fs, path::Path};

/// Every `#[dynamic(lazy, drop)]` static in this crate. Each one finalizes
/// independently, so each is a separate way to panic.
const FINALIZABLE_STATICS: &[&str] = &[
    "GLOBAL_VALIDATOR",
    "GLOBAL_CONFIRMATOR",
    "GLOBAL_METADATA_STORAGE",
    "GLOBAL_METADATA_SUBSCRIPTION",
    "GLOBAL_CLIENT_STORAGE",
    "CLEANUP",
];

fn rust_sources(dir: &Path, out: &mut Vec<(String, String)>) {
    let entries = fs::read_dir(dir).expect("the crate's src directory must be readable");
    for entry in entries {
        let path = entry.expect("a readable directory entry").path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            let text = fs::read_to_string(&path).expect("a readable .rs file");
            out.push((path.display().to_string(), text));
        }
    }
}

/// No production site may use the PANICKING accessor on a finalizable static.
///
/// Two-sided: the scan must also find the statics at all, or a rename would make
/// it vacuous and it would pass by looking at nothing.
#[test]
fn no_production_site_reads_a_finalizable_static_with_the_panicking_accessor() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_sources(&src, &mut files);
    assert!(
        !files.is_empty(),
        "no .rs files found under {src:?} -- fix the walk"
    );

    let mut mentions = 0usize;
    let mut offenders = Vec::new();

    for (path, text) in &files {
        for (lineno, line) in text.lines().enumerate() {
            let code = line.split("//").next().unwrap_or(line);
            for name in FINALIZABLE_STATICS {
                if !code.contains(name) {
                    continue;
                }
                mentions += 1;
                // `.init()` is the eager-init path and cannot be finalized yet;
                // `.try_read()` is the whole point of this test.
                if code.contains(&format!("{name}.read()")) {
                    offenders.push(format!("{path}:{} -- {}", lineno + 1, code.trim()));
                }
            }
        }
    }

    // NON-VACUITY: the statics must actually appear, or the scan proves nothing.
    assert!(
        mentions >= FINALIZABLE_STATICS.len(),
        "found only {mentions} mentions of {} statics -- the names have moved and this \
         scan is looking at nothing",
        FINALIZABLE_STATICS.len()
    );

    assert!(
        offenders.is_empty(),
        "these sites would PANIC if the static is already FINALIZED at process \
         exit -- use `.try_read()` and degrade, as `Segment::drop` does:\n  {}",
        offenders.join("\n  ")
    );
}

/// THE CONTROL for the test above: it can actually see a panicking accessor.
///
/// Without this, a scan whose matcher silently stopped working would report a
/// clean bill of health, which is the failure mode the scan exists to prevent.
#[test]
fn the_scan_detects_a_panicking_accessor_when_one_is_present() {
    let sample = "        GLOBAL_VALIDATOR.read().remove(self.descriptor.clone());";
    let code = sample.split("//").next().unwrap_or(sample);
    assert!(
        code.contains("GLOBAL_VALIDATOR.read()"),
        "the matcher must recognise the exact shape this test bans"
    );

    // And it must NOT fire on the accepted forms.
    for ok in [
        "        if let Ok(validator) = GLOBAL_VALIDATOR.try_read() {",
        "    GLOBAL_VALIDATOR.init();",
    ] {
        let code = ok.split("//").next().unwrap_or(ok);
        assert!(
            !code.contains("GLOBAL_VALIDATOR.read()"),
            "the matcher must accept `try_read` and `init`: {ok}"
        );
    }
}
