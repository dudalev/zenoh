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
//! A `TestDir` is outside the source tree, and goes when it drops, also when its test panics
//! (BER-2480).

use std::path::{Path, PathBuf};

use zenoh_test::TestDir;

/// The fork's root: this crate is `commons/zenoh-test`.
fn source_tree() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

/// Puts what a test binds in `dir`: a file, and on unix a socket.
fn bind_in(dir: &TestDir) {
    std::fs::write(dir.path().join("a.lock"), b"").unwrap();
    #[cfg(unix)]
    {
        let socket = dir.path().join("a.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        // Closing a listener leaves its file; the directory has to take it.
        drop(listener);
        assert!(socket.exists());
    }
}

/// **A TESTDIR IS OUTSIDE THE SOURCE TREE, AND GOES WHEN IT DROPS.**
#[test]
fn a_test_dir_is_outside_the_tree_and_goes_when_it_drops() {
    let dir = TestDir::new();
    let path = dir.path().canonicalize().unwrap();
    assert!(path.is_dir());
    assert!(
        !path.starts_with(source_tree()),
        "{path:?} is in the source tree"
    );
    assert!(path.starts_with(std::env::temp_dir().canonicalize().unwrap()));

    let endpoint = dir.endpoint("unixsock-stream", "a.sock");
    assert_eq!(
        Path::new(endpoint.address().as_str()),
        dir.path().join("a.sock"),
        "the endpoint names the file in the directory"
    );

    bind_in(&dir);
    drop(dir);
    assert!(!path.exists(), "{path:?} outlived its TestDir");
}

/// **AND WHEN ITS TEST PANICS.**
#[test]
fn a_test_dir_goes_when_its_test_panics() {
    let mut path = None;
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let dir = TestDir::new();
        path = Some(dir.path().to_path_buf());
        bind_in(&dir);
        panic!("the test fails with its TestDir in scope");
    }));
    assert!(panicked.is_err());
    let path = path.unwrap();
    assert!(!path.exists(), "{path:?} outlived the panic");
}

/// Two at once are two directories.
#[test]
fn each_test_dir_is_its_own() {
    let (a, b) = (TestDir::new(), TestDir::new());
    assert_ne!(a.path(), b.path());
}
