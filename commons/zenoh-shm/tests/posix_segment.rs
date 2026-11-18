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

/// `(Size, Locked)` in kB for the mapping containing `addr`, as the kernel
/// reports them per VMA in `/proc/self/smaps`.
///
/// PER MAPPING on purpose. The obvious instrument is `VmLck` from
/// `/proc/self/status`, and it is the wrong one: it is a PROCESS-WIDE total, so
/// the sibling tests in this binary move it between any two readings.
///
/// `Size` comes back as well as `Locked` so a test can prove it is looking at
/// the right VMA. Without it, an assertion that `Locked` is zero would also
/// pass against a reader that found some unrelated mapping, which is the one
/// way this measurement can go blind.
#[cfg(target_os = "linux")]
fn mapping_size_and_locked_kb(addr: *const u8) -> (u64, u64) {
    let addr = addr as u64;
    let smaps = std::fs::read_to_string("/proc/self/smaps").expect("cannot read /proc/self/smaps");
    let mut inside = false;
    let mut size = None;
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
        if !inside {
            continue;
        }
        let field = |p: &str| {
            line.strip_prefix(p)
                .and_then(|r| r.split_whitespace().next())
                .and_then(|v| v.parse::<u64>().ok())
        };
        if let Some(v) = field("Size:") {
            size = Some(v);
        }
        if let Some(locked) = field("Locked:") {
            return (size.expect("Size: precedes Locked: in smaps"), locked);
        }
    }
    panic!("no mapping in /proc/self/smaps contains {addr:#x}");
}

/// **NO SHM MAPPING IS LOCKED, ON EITHER PATH** (operator ruling 2026-11-18,
/// BER-2170).
///
/// This test previously asserted the OPPOSITE for `create`: that the owner
/// locks its own pool. It is INVERTED rather than deleted, because the old
/// assertion recorded the old intent and the change is deliberate -- upstream
/// `mlock`ed every mapping, and that one call produced every locked-memory
/// failure this fork has chased: fan-in exhausting `RLIMIT_MEMLOCK`, messages
/// discarded in silence, a `systemd --user` limit that cannot be raised from a
/// unit, and a band where session open panicked instead of degrading.
///
/// The residual is a possible page fault on first touch under real memory
/// pressure, which is a latency event rather than a correctness one.
#[cfg(target_os = "linux")]
#[test]
fn no_shm_mapping_is_locked_whether_created_or_opened() {
    let elem_count = (4 * 1024 * 1024_usize).try_into().unwrap();
    let created: Segment<u8> = Segment::create(elem_count).expect("error creating new segment");
    let opened = Segment::open(created.id()).expect("error opening existing segment!");

    let (created_size, created_locked) = mapping_size_and_locked_kb(created.as_ptr());
    let (opened_size, opened_locked) = mapping_size_and_locked_kb(opened.as_ptr());

    // THE CONTROL: both readings must have found a mapping of about the right
    // size. Without it, "Locked is 0" would also hold for a reader that landed
    // on some unrelated VMA, and the test would be blind rather than passing.
    assert!(
        created_size >= 4096,
        "the created mapping should be about 4 MiB, not {created_size} kB -- wrong VMA"
    );
    assert!(
        opened_size >= 4096,
        "the opened mapping should be about 4 MiB, not {opened_size} kB -- wrong VMA"
    );

    assert_eq!(
        created_locked, 0,
        "creating a segment must not lock it ({created_locked} kB locked)"
    );
    assert_eq!(
        opened_locked, 0,
        "opening a segment must not lock it ({opened_locked} kB locked)"
    );

    drop(opened);
    drop(created);
}
