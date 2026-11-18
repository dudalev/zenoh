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
#![cfg(feature = "test")]
use std::{num::NonZeroUsize, slice};

use zenoh_shm::{posix_shm::segment::Segment, shm::SegmentID};

pub mod common;
use common::validate_memory;

fn validate_segment<ID: SegmentID>(
    created_segment: &Segment<ID>,
    opened_segment: &Segment<ID>,
    expected_elem_count: NonZeroUsize,
) where
    rand::distributions::Standard: rand::distributions::Distribution<ID>,
{
    assert!(created_segment.len() >= expected_elem_count);
    assert!(opened_segment.len() >= expected_elem_count);
    assert!(opened_segment.len() == created_segment.len());

    let ptr1 = created_segment.as_ptr();
    let ptr2 = opened_segment.as_ptr();

    let slice1 = unsafe { slice::from_raw_parts_mut(ptr1, created_segment.len().get()) };
    let slice2 = unsafe { slice::from_raw_parts(ptr2, opened_segment.len().get()) };

    validate_memory(slice1, slice2);
}

fn test_segment<ID: SegmentID>()
where
    rand::distributions::Standard: rand::distributions::Distribution<ID>,
{
    let elem_count = 900.try_into().unwrap();

    let created_segment: Segment<ID> =
        Segment::create(elem_count).expect("error creating new segment");

    let opened_segment_instance_1 =
        Segment::open(created_segment.id()).expect("error opening existing segment!");

    validate_segment(&created_segment, &opened_segment_instance_1, elem_count);

    let opened_segment_instance_2 =
        Segment::open(created_segment.id()).expect("error opening existing segment!");

    validate_segment(&created_segment, &opened_segment_instance_1, elem_count);
    validate_segment(&created_segment, &opened_segment_instance_2, elem_count);

    drop(opened_segment_instance_1);
    validate_segment(&created_segment, &opened_segment_instance_2, elem_count);
}

/// UNSIGNED ///

#[test]
fn segment_u8_id() {
    test_segment::<u8>()
}

#[test]
fn segment_u16_id() {
    test_segment::<u16>()
}

#[test]
fn segment_u32_id() {
    test_segment::<u32>()
}

#[test]
fn segment_u64_id() {
    test_segment::<u64>()
}

// TODO: this is not yet supported (produces too long shm name for Mac),
// but we don't really need this
//#[test]
//fn segment_u128_id() {
//    test_segment::<u128>()
//}

/// Behaviour checks ///

#[test]
fn segment_open() {
    let created_segment: Segment<u8> =
        Segment::create(900.try_into().unwrap()).expect("error creating new segment");

    let _opened_segment =
        Segment::open(created_segment.id()).expect("error opening existing segment!");
}

#[test]
fn segment_open_error() {
    let id = {
        let created_segment: Segment<u8> =
            Segment::create(900.try_into().unwrap()).expect("error creating new segment");
        created_segment.id()
    };

    let _opened_segment = Segment::open(id).expect_err("must fail: opened not existing segment!");
}

/// Reads this process's locked-memory total, in kB, as the kernel accounts it.
#[cfg(target_os = "linux")]
fn vmlck_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .expect("cannot read /proc/self/status")
        .lines()
        .find_map(|l| l.strip_prefix("VmLck:"))
        .and_then(|v| v.split_whitespace().next())
        .and_then(|v| v.parse().ok())
        .expect("no VmLck line in /proc/self/status")
}

/// **OPENING SOMEONE ELSE'S SEGMENT MUST NOT CHARGE THIS PROCESS'S
/// `RLIMIT_MEMLOCK`, WHILE CREATING ONE STILL MUST** (BER-2170).
///
/// `RLIMIT_MEMLOCK` is per process and `VmLck` sums the locked ranges, so a
/// reader that locked every publisher's pool was charged once per publisher for
/// memory it does not own -- and a limit sized for one pool became a limit for
/// one publisher. Past that the open fails, the message carrying the buffer is
/// discarded, and the sender sees a timeout or nothing (BER-2167).
///
/// The pages do not need the second lock: both mappings are `MAP_SHARED` over
/// the same tmpfs object, so the creator's lock has already made those physical
/// pages unswappable.
///
/// Linux-only because it reads `/proc`. The property is the same on every unix,
/// and `unix.rs` is shared, so the Linux assertion covers the change.
#[cfg(target_os = "linux")]
#[test]
fn opening_a_foreign_segment_does_not_charge_memlock_but_creating_one_does() {
    // Large enough that a lock is unmistakable at kB granularity.
    let elem_count = (4 * 1024 * 1024_usize).try_into().unwrap();

    let before = vmlck_kb();
    let created: Segment<u8> = Segment::create(elem_count).expect("error creating new segment");
    let after_create = vmlck_kb();

    // THE CONTROL, and it is the half that makes the other half mean something:
    // the OWNER still locks. Without this a build where mlock never happened at
    // all would satisfy the assertion below.
    assert!(
        after_create > before,
        "the creator must lock its own pool: VmLck {before} -> {after_create} kB"
    );

    let opened = Segment::open(created.id()).expect("error opening existing segment!");
    let after_open = vmlck_kb();

    // Asserted as "NOT PROPORTIONAL TO THE SEGMENT", not as exact equality.
    // Measured, the open path still adds one page: it touches shm-backed
    // bookkeeping of its own (`register_cleanup`), which this process owns and
    // therefore locks correctly. That is a small fixed cost and does not grow
    // with the number of publishers, so it is not the defect.
    //
    // The defect was a charge the SIZE OF THE POOL, once per publisher. The
    // bound below is far above the fixed cost and far below one pool, so
    // restoring the lock on the open path fails this by a wide margin rather
    // than by a page.
    let create_growth = after_create - before;
    let open_growth = after_open - after_create;
    assert!(
        open_growth * 16 < create_growth,
        "opening a segment must not be charged in proportion to its size: create grew VmLck by \
         {create_growth} kB and open by {open_growth} kB -- a reader charged a pool per publisher \
         runs out of budget by fan-in alone"
    );

    drop(opened);
    drop(created);
}
