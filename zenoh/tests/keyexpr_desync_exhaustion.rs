//
// LOCAL PATCH, not part of upstream zenoh. Regression test for the session expression id
// rollover (eclipse-zenoh/zenoh issue 2860). Carried with the patch to `api/session.rs`.
//

//! What happens when EVERY expression id is held by a live declaration.
//!
//! `expr_id_rollover.rs` turns the counter round while only a handful of declarations are
//! live, so `next_free_expr_id` always finds a free id a little further on. This test fills
//! the space instead: it declares more live publishers than there are ids and never
//! undeclares them, which is the one path in the patch where `next_free_expr_id` returns
//! `None` and the caller must send the key expression in full rather than declare it.
//!
//! That branch had no test when the patch was measured, and an untested branch in a fix is
//! how the fix regresses: the obvious wrong thing for a future edit to do is to fall back to
//! the counter's raw value when no id is free, which puts the defect straight back.
//!
//! Whether it is TWO-SIDED is a reading, not a measurement, and the difference matters.
//! After the fix every sample arrives on its own key whether or not an id was available for
//! it, and that half is measured. The other half -- that this test FAILS before the fix --
//! is not: both recorded pre-fix runs die at `face.rs:268` on the router-side defect below
//! before a single probe is sent, so neither shows a misroute. By reading it would fail,
//! because declarations 65,536 onward re-draw ids 1 upward while their holders are live and
//! the third probe's publisher holds one of those ids. Establishing it needs the one
//! combination never run: the UNPATCHED session against the now-patched router.
//!
//! # IGNORED, and why: it currently fails inside the ROUTER, before it can assert anything
//!
//! Filling the id space trips a SECOND, unrelated defect. `Face::get_next_local_id`
//! (`net/routing/dispatcher/face.rs:265`) picks the router's id for a face by scanning from 1
//! and incrementing, with no bound: once every id is mapped it runs past `u16::MAX`, which
//! panics in a debug build and in a release build wraps to 0 -- `EMPTY_EXPR_ID`, the reserved
//! scope. Its linear rescan is also why this test takes minutes rather than seconds.
//!
//! Measured 2026-11-13 on BOTH the patched session and vanilla 1.10.2: the same three panic
//! sites (`face.rs:268`, then `:567` and `:581` on the control lock the first panic poisoned)
//! and the same exit 101. So it is pre-existing and this file's subject is not the cause.
//!
//! Tracked as BER-1902. **Remove the `#[ignore]` as part of fixing that**: this test is the
//! gate for it, and it will then also cover the branch it was written for.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use zenoh_core::ztimeout;
use zenoh_test::TestSessions;

const TIMEOUT: Duration = Duration::from_secs(120);

/// More live declarations than there are expression ids. The counter starts at 1 and 0 is
/// reserved, so the space is 1..=65535 and this overruns it by 65 declarations that can
/// never be given an id of their own.
const LIVE: usize = 65_600;
/// Samples put on each probed publisher.
const PROBES: usize = 10;

const PREFIX: &str = "test/exhaustion/live";

/// The key of the `i`th live declaration, spread over two levels.
///
/// The shape of the key space turned out NOT to be what costs here: measured 2026-11-13,
/// 65,600 declarations ran at ~200% CPU for minutes both as flat siblings and across two
/// levels of at most 256 children. What did matter is WHEN the wildcard subscription is
/// declared -- see the test body. Two levels is kept because it is also the more realistic
/// shape, not because it was the fix.
fn live_key(i: usize) -> String {
    format!("{PREFIX}/{}/{}", i / 256, i % 256)
}

#[ignore = "fails in the router on a separate pre-existing defect, BER-1902; see this file's \
            header. Run with --include-ignored once that is fixed."]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_declarations_past_the_whole_id_space_still_route() {
    zenoh::init_log_from_env_or("error");
    let mut test_context = TestSessions::new();
    let (observer, service) = ztimeout!(test_context.open_pairs_client());

    // THE ORDER HERE IS LOAD-BEARING, for speed rather than for correctness.
    //
    // Declaring the observer's wildcard subscription FIRST makes every one of the 65,600
    // declarations below recompute its matches against that subscription, which is
    // quadratic: measured at ~200% CPU for minutes without finishing. Declared after the
    // fill it matches the existing resources once.
    //
    // It does not weaken the test. The lasting damage from a reused id is that the older
    // declaration's mapping is deleted, and that is what the probes below read -- after
    // both the declarations and the subscription are in place.

    // Fill the id space, keeping every declaration alive. Nothing is undeclared, so no id
    // is ever returned to the pool.
    let mut live = Vec::with_capacity(LIVE);
    for i in 0..LIVE {
        live.push(ztimeout!(service.declare_publisher(live_key(i))).unwrap());
    }

    let delivered = Arc::new(AtomicUsize::new(0));
    let wrong_key = Arc::new(AtomicUsize::new(0));
    let detail = Arc::new(Mutex::new(Vec::<String>::new()));
    let _observing = {
        let (delivered, wrong_key, detail) = (delivered.clone(), wrong_key.clone(), detail.clone());
        ztimeout!(observer
            .declare_subscriber("test/exhaustion/**")
            .callback(move |sample| {
                let key = sample.key_expr().as_str().to_owned();
                let payload = String::from_utf8_lossy(&sample.payload().to_bytes()).into_owned();
                delivered.fetch_add(1, Ordering::SeqCst);
                if payload != key {
                    if wrong_key.fetch_add(1, Ordering::SeqCst) < 8 {
                        detail
                            .lock()
                            .unwrap()
                            .push(format!("arrived on {key} but was put on {payload}"));
                    }
                }
            }))
        .unwrap()
    };

    // One more, declared with the space already full: this is the `None` branch.
    let past_the_end = format!("{PREFIX}/past/the_end");
    let overflow_pub = ztimeout!(service.declare_publisher(past_the_end.clone())).unwrap();

    // Four probes, chosen to span the space. With LIVE = 65,600 declarations and ids running
    // 1..=65,535, index i gets id i+1 while there is one to give:
    //   live[0]        -- the first declaration, lowest id (1)
    //   live[LIVE-100] -- index 65,500, id 65,501: near the top, and it DID get an id
    //   live[LIVE-1]   -- index 65,599, past the end: it cannot have had an id at all
    //   overflow_pub   -- declared after the space was already full, i.e. the `None` branch
    //
    // The second is deliberately not the LAST id: that is index 65,534, and this probes a
    // holder just below the boundary. An earlier version of this comment called it "the last
    // one to get an id", which is wrong by 34 declarations.
    let probes: Vec<(String, &zenoh::pubsub::Publisher<'_>)> = vec![
        (live_key(0), &live[0]),
        (live_key(LIVE - 100), &live[LIVE - 100]),
        (live_key(LIVE - 1), &live[LIVE - 1]),
        (past_the_end.clone(), &overflow_pub),
    ];
    for (key, publisher) in &probes {
        for _ in 0..PROBES {
            ztimeout!(publisher.put(key.as_str())).unwrap();
        }
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Non-vacuity first: a run that delivered nothing would satisfy the misroute assertion
    // below while proving nothing at all.
    assert_eq!(
        delivered.load(Ordering::SeqCst),
        probes.len() * PROBES,
        "every probe must arrive: {} of {} did, so the id space being full has cost delivery \
         even where nothing was misrouted",
        delivered.load(Ordering::SeqCst),
        probes.len() * PROBES
    );
    let wrong = wrong_key.load(Ordering::SeqCst);
    assert_eq!(
        wrong,
        0,
        "{wrong} sample(s) arrived on a key other than the one they were put on with {LIVE} \
         live declarations and only 65535 ids: an id held by a live declaration was given to \
         another one.\n  {}",
        detail.lock().unwrap().join("\n  ")
    );

    test_context.close().await;
}
