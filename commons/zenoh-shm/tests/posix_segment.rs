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

/// `Locked:` for the mapping containing `addr`, in kB, as the kernel reports it
/// per VMA in `/proc/self/smaps`.
///
/// PER MAPPING on purpose. The obvious instrument is `VmLck` from
/// `/proc/self/status`, and it is the wrong one: it is a PROCESS-WIDE total, so
/// the sibling tests in this binary -- which create and drop their own locked
/// segments concurrently -- move it between any two readings. A test built on it
/// is not measuring its own subject.
#[cfg(target_os = "linux")]
fn locked_kb_at(addr: *const u8) -> u64 {
    let addr = addr as u64;
    let smaps = std::fs::read_to_string("/proc/self/smaps").expect("cannot read /proc/self/smaps");
    let mut inside = false;
    for line in smaps.lines() {
        let first = line.split_whitespace().next().unwrap_or_default();
        // A mapping header starts with "<lo>-<hi>"; every other line is a field
        // of the mapping above it.
        if let Some((lo, hi)) = first.split_once('-') {
            if let (Ok(lo), Ok(hi)) = (u64::from_str_radix(lo, 16), u64::from_str_radix(hi, 16)) {
                inside = (lo..hi).contains(&addr);
                continue;
            }
        }
        if inside {
            if let Some(rest) = line.strip_prefix("Locked:") {
                return rest
                    .split_whitespace()
                    .next()
                    .and_then(|v| v.parse().ok())
                    .expect("malformed Locked: line");
            }
        }
    }
    panic!("no mapping in /proc/self/smaps contains {addr:#x}");
}

/// **OPENING SOMEONE ELSE'S SEGMENT MUST NOT LOCK IT, WHILE CREATING ONE
/// MUST** (BER-2170).
///
/// `RLIMIT_MEMLOCK` is per process, so a reader that locked every publisher's
/// pool was charged once per publisher for memory it does not own -- and a
/// limit sized for one pool became a limit for one publisher. Past that the
/// open fails, the message carrying the buffer is discarded, and the sender
/// sees a timeout or nothing (BER-2167).
///
/// The pages do not need the second lock: both mappings are `MAP_SHARED` over
/// the same tmpfs object, so the creator's lock has already made those physical
/// pages unswappable.
///
/// Linux-only because it reads `/proc`. The property is the same on every unix,
/// and `unix.rs` is shared, so the Linux assertion covers the change.
#[cfg(target_os = "linux")]
#[test]
fn opening_a_foreign_segment_does_not_lock_it_but_creating_one_does() {
    let elem_count = (4 * 1024 * 1024_usize).try_into().unwrap();
    let created: Segment<u8> = Segment::create(elem_count).expect("error creating new segment");
    let opened = Segment::open(created.id()).expect("error opening existing segment!");

    let created_locked = locked_kb_at(created.as_ptr());
    let opened_locked = locked_kb_at(opened.as_ptr());

    // THE CONTROL, and the half that makes the other half mean anything: the
    // OWNER still locks. Without it, a build where mlock never happened at all
    // would satisfy the assertion below.
    assert!(
        created_locked >= 4096,
        "the creator must lock its own pool: its mapping reports {created_locked} kB locked"
    );

    assert_eq!(
        opened_locked, 0,
        "opening a segment must not lock it (its mapping reports {opened_locked} kB locked): a \
         reader charged a pool per publisher runs out of budget by fan-in alone"
    );

    drop(opened);
    drop(created);
}
