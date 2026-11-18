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

pub(crate) use platform::cleanup_orphaned_segments;

#[cfg(not(target_os = "linux"))]
mod platform {
    pub(crate) fn cleanup_orphaned_segments() {}
}

#[cfg(target_os = "linux")]
mod platform {
    use std::fs;

    use zenoh_result::ZResult;

    use crate::shm;

    pub(crate) fn cleanup_orphaned_segments() {
        if let Err(_e) = cleanup_orphaned_segments_inner() {
            // TODO: we cannot use our logger inside of atexit() callstack!
            //tracing::error!("Error performing orphaned SHM segments cleanup: {e}")
        }
    }

    /// How old a segment file must be before the sweep will consider unlinking
    /// it (BER-2173).
    ///
    /// THE RACE THIS CLOSES. A creator does `shm_open(O_CREAT|O_EXCL)` and only
    /// THEN takes its shared advisory lock. In between, the file exists and is
    /// unlocked, so a sweep running in another process takes the exclusive lock,
    /// concludes the segment is orphaned, and unlinks a live segment. Every peer
    /// that later tries to open it gets ENOENT, and the owner never finds out.
    ///
    /// WHY AN AGE IS THE RIGHT INSTRUMENT, rather than a tuned threshold. A
    /// genuine orphan is left behind by a process that has DIED, so it is as old
    /// as that death and only grows older; delaying its removal costs nothing,
    /// because the next sweep takes it and a sweep runs in every process that
    /// starts. A live creation, by contrast, is microseconds old at the
    /// dangerous moment. The two populations are separated by many orders of
    /// magnitude, so this is not a value balanced between competing failures --
    /// anything far above the creator's window and far below "a process
    /// lifetime" is correct, and generous is strictly safer.
    ///
    /// WHY IT MATTERS MORE NOW. Under `ShmInitMode::Init` both the sweep and
    /// segment creation happen at session open, and a deploy restarts many
    /// services at once -- so the sweeps and creations of ~20 processes now land
    /// in the same few seconds, which is exactly the interleaving this race
    /// needs.
    const MIN_ORPHAN_AGE: std::time::Duration = std::time::Duration::from_secs(60);

    /// Has this file existed long enough that no live creator can still be
    /// between its `shm_open` and its lock?
    ///
    /// Fails CLOSED: if the age cannot be determined the file is left alone.
    /// Unlinking a segment we know nothing about is the damaging direction, and
    /// a skipped orphan is retried by the next sweep.
    fn old_enough_to_be_an_orphan(entry: &fs::DirEntry) -> bool {
        entry
            .metadata()
            .and_then(|m| m.modified())
            .map(|modified| modified.elapsed().is_ok_and(|age| age >= MIN_ORPHAN_AGE))
            .unwrap_or(false)
    }

    #[cfg(test)]
    mod tests {
        use std::num::NonZeroUsize;

        use super::*;
        use crate::posix_shm::segment::Segment;

        /// **A SEGMENT THAT WAS JUST CREATED IS NOT AN ORPHAN** (BER-2173).
        ///
        /// The sweep unlinks anything it can take the exclusive lock on, and a
        /// creator is briefly unlocked between its `shm_open` and its own lock.
        /// This is the guard that stops a live segment being removed in that
        /// window.
        #[test]
        fn a_freshly_created_segment_is_not_swept() {
            let segment: Segment<u32> =
                Segment::create(NonZeroUsize::new(4096).unwrap()).expect("cannot create");
            let name = format!("{}.zenoh", segment.id());

            let entry = fs::read_dir("/dev/shm")
                .expect("cannot read /dev/shm")
                .filter_map(Result::ok)
                .find(|e| e.file_name().to_string_lossy() == name)
                .expect("the segment we just created is not in /dev/shm");

            assert!(
                !old_enough_to_be_an_orphan(&entry),
                "a segment created microseconds ago must not be eligible for sweeping"
            );
        }

        /// THE CONTROL: the guard must not refuse everything, or it would
        /// disable orphan cleanup altogether and leak every dead process's
        /// segments -- a quieter failure than the race it replaces.
        ///
        /// Asserted by backdating the file rather than by waiting, so the test
        /// is deterministic and costs no wall time.
        #[test]
        fn a_segment_older_than_the_minimum_age_is_eligible() {
            let segment: Segment<u32> =
                Segment::create(NonZeroUsize::new(4096).unwrap()).expect("cannot create");
            let path = std::path::PathBuf::from(format!("/dev/shm/{}.zenoh", segment.id()));

            let backdated = std::time::SystemTime::now()
                - (MIN_ORPHAN_AGE + std::time::Duration::from_secs(60));
            let file = fs::File::options()
                .write(true)
                .open(&path)
                .expect("cannot open the segment file");
            file.set_modified(backdated).expect("cannot backdate");

            let entry = fs::read_dir("/dev/shm")
                .expect("cannot read /dev/shm")
                .filter_map(Result::ok)
                .find(|e| e.path() == path)
                .expect("segment not found");

            assert!(
                old_enough_to_be_an_orphan(&entry),
                "an old segment must remain eligible, or orphan cleanup stops working"
            );
        }
    }

    fn cleanup_orphaned_segments_inner() -> ZResult<()> {
        #[cfg(shm_external_lockfile)]
        let shm_files = fs::read_dir(std::env::temp_dir())?;
        #[cfg(not(shm_external_lockfile))]
        let shm_files = fs::read_dir("/dev/shm")?;

        for segment_file in shm_files.filter_map(Result::ok).filter(|f| {
            if let Some(ext) = f.path().extension() {
                return ext == "zenoh" && old_enough_to_be_an_orphan(f);
            }
            false
        }) {
            if let Some(Some(id_str)) = segment_file
                .path()
                .file_stem()
                .map(|os_str| os_str.to_str())
            {
                if let Ok(id) = id_str.parse::<u64>() {
                    shm::Segment::ensure_not_persistent(id);
                }
            }
        }

        Ok(())
    }
}
