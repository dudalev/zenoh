//
// LOCAL PATCH, not part of upstream zenoh. Regression test for the session expression id
// rollover (eclipse-zenoh/zenoh issue 2860). Carried with the patch to `api/session.rs`.
//

//! The control that isolates the cause of #2860: `Session::put` declares nothing.
//!
//! `expr_id_rollover.rs` drives the same traffic through a publisher DECLARED once per
//! message, and before the fix that fails, because each declaration spends an expression id
//! and the `u16` counter eventually re-issues one a live declaration still holds.
//!
//! Here the per-message publisher is replaced by a plain `Session::put` on the key. Nothing
//! is declared, so no id is spent, and the counter cannot turn however long the run goes.
//!
//! **This test must pass on every zenoh, patched or not.** That is its whole value: it is
//! the negative half of the experiment. If it ever fails, whatever is wrong is NOT id reuse
//! -- so do not "fix" it by touching `next_free_expr_id`; find out what started declaring.

// The lint set of the project that carries this fork (its root Cargo.toml,
// `[workspace.lints]`), applied here as crate attributes rather than through the workspace.
// Every file under tests/ is its own crate root, so this binds EXACTLY the code we wrote.
// Opting the upstream crates in would mean 2,451 findings in code we do not own -- about
// 2,000 of them missing doc comments on zenoh's public API -- and every one of those edits
// would conflict on the next rebase onto a newer upstream tag.
#![deny(warnings)]
#![deny(missing_docs)]
#![deny(clippy::all)]
#![deny(clippy::too_many_lines)]
#![deny(clippy::equatable_if_let)]
#![deny(clippy::cognitive_complexity)]
#![deny(clippy::unnested_or_patterns)]

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use zenoh_core::ztimeout;
use zenoh_test::TestSessions;

const TIMEOUT: Duration = Duration::from_secs(60);

/// Just past one full turn of the `u16` expression id counter -- the same figure
/// `expr_id_rollover.rs` uses, so the two runs differ only in how the beat is published.
const BEATS: usize = 65_600;
/// Samples put on the kept publisher by each probe.
const PROBES: usize = 20;

const KEPT_SUB: &str = "test/put_control/sub/**";
const KEPT_KEY: &str = "test/put_control/kept";
const BEAT_KEY: &str = "test/put_control/beat";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_put_spends_no_expr_id() {
    zenoh::init_log_from_env_or("error");
    let mut test_context = TestSessions::new();
    let (observer, service) = ztimeout!(test_context.open_pairs_client());

    // The far side says what arrived and on which key: every payload is the key it was put on.
    let kept_ok = Arc::new(AtomicUsize::new(0));
    let wrong_key = Arc::new(AtomicUsize::new(0));
    let _observing = {
        let (kept_ok, wrong_key) = (kept_ok.clone(), wrong_key.clone());
        ztimeout!(observer
            .declare_subscriber("test/put_control/**")
            .callback(move |sample| {
                let key = sample.key_expr().as_str().to_owned();
                let payload = String::from_utf8_lossy(&sample.payload().to_bytes()).into_owned();
                if payload != key {
                    wrong_key.fetch_add(1, Ordering::SeqCst);
                } else if key == KEPT_KEY {
                    kept_ok.fetch_add(1, Ordering::SeqCst);
                }
            }))
        .unwrap()
    };

    // The kept declarations go first, so they hold the lowest expression ids.
    let _kept_sub = ztimeout!(service.declare_subscriber(KEPT_SUB).callback(|_| {})).unwrap();
    let kept_pub = ztimeout!(service.declare_publisher(KEPT_KEY)).unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Non-vacuity control: the kept publisher delivers before any beats. Without it a
    // failure below could not be told from sessions that never connected.
    for _ in 0..PROBES {
        ztimeout!(kept_pub.put(KEPT_KEY)).unwrap();
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        kept_ok.load(Ordering::SeqCst),
        PROBES,
        "the kept publisher must deliver before the beats"
    );

    // The beat, declaring nothing. This is the one line that differs from
    // `expr_id_rollover.rs`, where it is declare_publisher / put / undeclare.
    for _ in 0..BEATS {
        ztimeout!(service.put(BEAT_KEY, BEAT_KEY)).unwrap();
    }

    for _ in 0..PROBES {
        ztimeout!(kept_pub.put(KEPT_KEY)).unwrap();
    }
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert_eq!(
        wrong_key.load(Ordering::SeqCst),
        0,
        "a sample was delivered on a key other than the one it was put on, although nothing \
         in this test declares a key expression per message"
    );
    assert_eq!(
        kept_ok.load(Ordering::SeqCst),
        2 * PROBES,
        "the kept publisher stopped delivering after {BEATS} undeclared puts, which spend no \
         expression id -- so the cause is not the id counter"
    );

    test_context.close().await;
}
