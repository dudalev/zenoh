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
#![cfg(feature = "test")]
use std::{num::NonZeroUsize, sync::Arc};

use zenoh_shm::{
    api::{
        client_storage::ShmClientStorage,
        protocol_implementations::posix::protocol_id::POSIX_PROTOCOL_ID,
    },
    posix_shm::segment::Segment,
    reader::ShmReader,
};

fn reader() -> ShmReader {
    ShmReader::new(Arc::new(
        ShmClientStorage::builder()
            .with_default_client_set()
            .build(),
    ))
}

fn segment_count(reader: &ShmReader) -> usize {
    reader.mounted_segment_count()
}

/// **A PRE-MOUNT IS RELEASED WHEN ITS LEASE DROPS** (BER-2170).
///
/// The reader's segment map is insert-only by upstream design, which is correct
/// for a segment mounted because a buffer referenced it: another buffer may
/// reference it at any moment. A PRE-MOUNT is taken on behalf of one link
/// before any buffer exists, so without a release a process accumulates a
/// mapping and a shared flock for every peer it has ever handshaked with -- and
/// a restarted peer's pool stays pinned in /dev/shm until every process that
/// met it exits.
#[test]
fn a_dropped_premount_lease_releases_the_segment() {
    let reader = reader();
    let owned: Segment<u32> =
        Segment::create(NonZeroUsize::new(4096).unwrap()).expect("cannot create segment");

    let before = segment_count(&reader);
    let lease = reader
        .premount_segment(POSIX_PROTOCOL_ID, owned.id())
        .expect("cannot pre-mount");
    assert_eq!(
        segment_count(&reader),
        before + 1,
        "pre-mounting must mount the segment, or this test proves nothing"
    );

    drop(lease);
    assert_eq!(
        segment_count(&reader),
        before,
        "dropping the lease must release the mapping and its flock"
    );
}

/// **A SEGMENT SHARED BY TWO LINKS SURVIVES UNTIL THE LAST LEASE GOES.**
///
/// The quiet direction. Releasing on the FIRST drop would unmount a pool another
/// live link is still using, which no test asserting only the release above
/// would notice.
#[test]
fn a_shared_premount_survives_until_the_last_lease_drops() {
    let reader = reader();
    let owned: Segment<u32> =
        Segment::create(NonZeroUsize::new(4096).unwrap()).expect("cannot create segment");

    let before = segment_count(&reader);
    let first = reader
        .premount_segment(POSIX_PROTOCOL_ID, owned.id())
        .expect("cannot pre-mount");
    let second = reader
        .premount_segment(POSIX_PROTOCOL_ID, owned.id())
        .expect("cannot pre-mount twice");

    // Mounted ONCE despite two leases: the map is keyed by segment.
    assert_eq!(segment_count(&reader), before + 1);

    drop(first);
    assert_eq!(
        segment_count(&reader),
        before + 1,
        "one link closing must not unmount a pool another link still holds"
    );

    drop(second);
    assert_eq!(
        segment_count(&reader),
        before,
        "the last lease out releases it"
    );
}
