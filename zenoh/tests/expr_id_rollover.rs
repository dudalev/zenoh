//
// LOCAL PATCH, not part of upstream zenoh. Regression test for the session expression id
// rollover (eclipse-zenoh/zenoh issue 2860). Carried with the patch to `api/session.rs`.
//

//! A session's expression ids are a `u16`. A session that declares and undeclares more than
//! 65,535 key expressions turns the counter round, and it must not then re-issue an id that one
//! of its live declarations still holds (#2860).
//!
//! The shape is a service that keeps a subscriber and a publisher, and declares one more
//! publisher per message. Before the fix the per-message publisher eventually drew the kept
//! publisher's id: that one message went out on the kept publisher's key, and undeclaring the
//! per-message publisher then deleted the kept publisher's mapping, so every later `put` on it
//! returned `Ok` and was dropped inside the session.

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

/// Just past one full turn of the `u16` expression id counter.
const CHURN: usize = 65_600;
/// Samples put on the kept publisher by each probe.
const PROBES: usize = 20;

const KEPT_SUB: &str = "test/expr_id_rollover/sub/**";
const KEPT_KEY: &str = "test/expr_id_rollover/kept";
const CHURN_KEY: &str = "test/expr_id_rollover/churn";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_expr_id_rollover_does_not_reissue_live_ids() {
    zenoh::init_log_from_env_or("error");
    let mut test_context = TestSessions::new();
    let (observer, service) = ztimeout!(test_context.open_pairs_client());

    // The far side says what arrived and on which key: every payload is the key it was put on.
    let kept_ok = Arc::new(AtomicUsize::new(0));
    let wrong_key = Arc::new(AtomicUsize::new(0));
    let _observing = {
        let (kept_ok, wrong_key) = (kept_ok.clone(), wrong_key.clone());
        ztimeout!(observer
            .declare_subscriber("test/expr_id_rollover/**")
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

    // The kept declarations are made first, so they hold the lowest expression ids: the
    // subscriber's non-wild prefix, then the publisher's key.
    let _kept_sub = ztimeout!(service.declare_subscriber(KEPT_SUB).callback(|_| {})).unwrap();
    let kept_pub = ztimeout!(service.declare_publisher(KEPT_KEY)).unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Control: before any churn the kept publisher delivers. Without it, a failure below could
    // not be told from sessions that never connected.
    for _ in 0..PROBES {
        ztimeout!(kept_pub.put(KEPT_KEY)).unwrap();
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        kept_ok.load(Ordering::SeqCst),
        PROBES,
        "the kept publisher must deliver before the churn"
    );

    for _ in 0..CHURN {
        let churn = ztimeout!(service.declare_publisher(CHURN_KEY)).unwrap();
        ztimeout!(churn.put(CHURN_KEY)).unwrap();
        ztimeout!(churn.undeclare()).unwrap();
    }

    for _ in 0..PROBES {
        ztimeout!(kept_pub.put(KEPT_KEY)).unwrap();
    }
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert_eq!(
        wrong_key.load(Ordering::SeqCst),
        0,
        "a sample was delivered on a key other than the one it was put on"
    );
    assert_eq!(
        kept_ok.load(Ordering::SeqCst),
        2 * PROBES,
        "the kept publisher stopped delivering after the expression id counter wrapped"
    );

    test_context.close().await;
}
