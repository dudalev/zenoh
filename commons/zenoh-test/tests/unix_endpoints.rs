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
//! Every unix socket and named pipe a test binds is in a `zenoh_test::TestDir`, outside the
//! source tree (BER-2480).
//!
//! An endpoint whose address is a relative path binds in the test's working directory,
//! which is its package's directory, so a test that ends before it removes the file leaves
//! it in the source tree, where git does not list it. zenoh-transport's `endpoints.rs`
//! checks the tree while its sockets are bound, but only the tests that run can check
//! anything, and most unix-socket and unixpipe tests are ignored or need a feature that is
//! off. So this scans their source: no test writes such an endpoint itself; each takes it
//! from a `TestDir`.
//!
//! It scans every `.rs` file under a directory named `tests`. A unit test in a crate's own
//! sources is not scanned; none binds a unix socket or pipe.

use std::{
    fs,
    path::{Path, PathBuf},
};

/// The protocols whose address is a path in the filesystem.
const PROTOCOLS: &[&str] = &["unixsock-stream", "unixpipe"];

/// The protocol of an endpoint `line` writes itself: a string literal that starts with the
/// protocol and its slash, as `format!("unixsock-stream/{f1}")` or `"unixpipe/name"` do.
fn writes_its_own(line: &str) -> Option<&'static str> {
    PROTOCOLS
        .iter()
        .copied()
        .find(|protocol| line.contains(&format!("\"{protocol}/")))
}

/// Whether `line` takes such an endpoint from a `TestDir`.
fn takes_from_a_test_dir(line: &str) -> bool {
    PROTOCOLS
        .iter()
        .any(|protocol| line.contains(&format!(".endpoint(\"{protocol}\"")))
}

/// The `.rs` files under every directory named `tests` below `dir`, skipping `.git` and
/// every `target` directory and not following symlinks.
fn test_sources(dir: &Path, in_tests: bool, out: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(dir).unwrap_or_else(|e| panic!("could not read {dir:?}: {e}"));
    for entry in entries {
        let entry = entry.unwrap();
        let file_type = entry.file_type().unwrap();
        let name = entry.file_name();
        if file_type.is_dir() {
            if name != ".git" && name != "target" {
                test_sources(&entry.path(), in_tests || name == "tests", out);
            }
        } else if in_tests && entry.path().extension().is_some_and(|e| e == "rs") {
            out.push(entry.path());
        }
    }
}

/// **NO TEST WRITES A UNIX-SOCKET OR UNIXPIPE ENDPOINT OF ITS OWN.**
///
/// Two-sided: the scan must also find the endpoints taken from a `TestDir`, or a walk that
/// stopped reaching the tests would pass by looking at nothing.
#[test]
fn no_test_writes_a_unix_endpoint_of_its_own() {
    // This crate is `commons/zenoh-test`.
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    // This file quotes the shapes it bans, in its control below.
    let this = root.join(file!()).canonicalize().unwrap();
    let mut files = Vec::new();
    test_sources(&root, false, &mut files);
    files.retain(|file| *file != this);

    let mut from_a_test_dir = 0_usize;
    let mut offenders = Vec::new();
    for file in &files {
        let text = fs::read_to_string(file).unwrap();
        for (i, line) in text.lines().enumerate() {
            if let Some(protocol) = writes_its_own(line) {
                offenders.push(format!(
                    "{}:{} ({protocol}): {}",
                    file.display(),
                    i + 1,
                    line.trim()
                ));
            }
            if takes_from_a_test_dir(line) {
                from_a_test_dir += 1;
            }
        }
    }

    assert!(
        from_a_test_dir > 0,
        "no endpoint taken from a TestDir in {} test files under {root:?} -- the walk no \
         longer reaches the tests, fix it",
        files.len()
    );
    assert!(
        offenders.is_empty(),
        "these tests write a unix-socket or unixpipe endpoint themselves, so a relative \
         path binds in the source tree; take it from a `zenoh_test::TestDir` with \
         `dir.endpoint(protocol, name)`:\n  {}",
        offenders.join("\n  ")
    );
}

/// THE CONTROL for the scan above: it flags the shapes it bans, and accepts a `TestDir`'s.
#[test]
fn the_scan_tells_an_endpoint_of_its_own_from_a_test_dirs() {
    for own in [
        r#"    let endpoint: EndPoint = format!("unixsock-stream/{f1}").parse().unwrap();"#,
        r#"        "unixpipe/endpoint_unixpipe".parse().unwrap(),"#,
        r#"    let endpoint: EndPoint = "unixsock-stream//tmp/a.sock".parse().unwrap();"#,
    ] {
        assert!(writes_its_own(own).is_some(), "must be flagged: {own}");
        assert!(
            !takes_from_a_test_dir(own),
            "must not count as a TestDir's: {own}"
        );
    }
    for test_dir in [
        r#"        dir.endpoint("unixsock-stream", "zenoh-test-unix-socket-0.sock"),"#,
        r#"    let endpoint: EndPoint = dir.endpoint("unixpipe", "openclose_unixpipe_only");"#,
    ] {
        assert!(
            writes_its_own(test_dir).is_none(),
            "must be accepted: {test_dir}"
        );
        assert!(
            takes_from_a_test_dir(test_dir),
            "must count as a TestDir's: {test_dir}"
        );
    }
}
