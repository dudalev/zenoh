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

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use thread_priority::ThreadBuilder;
#[cfg(unix)]
use thread_priority::{
    set_current_thread_priority, RealtimeThreadSchedulePolicy, ThreadPriority, ThreadPriorityValue,
    ThreadSchedulePolicy::Realtime,
};

#[derive(Debug, PartialEq, Eq)]
pub enum TaskWakeReason {
    Timeout,
    Kick,
}

#[derive(Debug)]
pub struct PeriodicTask {
    running: Arc<AtomicBool>,
    send: std::sync::mpsc::Sender<()>,
}

impl Drop for PeriodicTask {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        self.kick();
    }
}

impl PeriodicTask {
    pub fn kicker(&self) -> std::sync::mpsc::Sender<()> {
        self.send.clone()
    }

    pub fn kick(&self) {
        let _ = self.send.send(());
    }

/// Is a real-time scheduling request CERTAIN to be refused?
///
/// True only when `RLIMIT_RTPRIO`'s soft limit reads as exactly zero, which is
/// what a `systemd --user` service inherits unless the user MANAGER was given
/// more -- a unit cannot raise it itself (BER-2159 proved the same ceiling for
/// `RLIMIT_MEMLOCK`).
///
/// Anything else, including "could not tell", returns false so the request is
/// attempted as upstream does. This must never turn an available priority into
/// a skipped one.
#[cfg(unix)]
fn realtime_priority_is_refused() -> bool {
    #[cfg(target_os = "linux")]
    {
        let Ok(limits) = std::fs::read_to_string("/proc/self/limits") else {
            return false;
        };
        limits
            .lines()
            .find(|l| l.starts_with("Max realtime priority"))
            .and_then(|l| l.split_whitespace().nth(3))
            .is_some_and(|soft| soft == "0")
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

/// Say once, per process, that the watchdogs run at normal priority.
///
/// Once rather than per thread, because the condition is a property of the
/// PROCESS's limits and repeating it per thread taught readers to skip it.
#[cfg(unix)]
fn report_unavailable_realtime_priority() {
    static SAID: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    if SAID.set(()).is_err() {
        return;
    }
    tracing::warn!(
        "RLIMIT_RTPRIO is 0, so the SHM watchdog threads run at NORMAL scheduling priority \
         and no real-time request is made. Under a heavily congested system a watchdog may \
         then miss its interval, which can invalidate SHM buffers and surface as SHM \
         timeouts. Raising it needs LimitRTPRIO on the user MANAGER -- a unit cannot raise \
         it for itself (BER-2160)"
    );
}

    pub fn new<F>(name: String, interval: Duration, mut f: F) -> Self
    where
        F: FnMut(TaskWakeReason) + Send + 'static,
    {
        let (send, recv) = std::sync::mpsc::channel::<()>();

        let running = Arc::new(AtomicBool::new(true));

        let c_running = running.clone();

        // DO NOT ASK FOR REAL-TIME PRIORITY WE DEMONSTRABLY CANNOT HAVE
        // (BER-2160). Requesting it under `RLIMIT_RTPRIO = 0` is a guaranteed
        // EPERM, and the fallback path below then logs a four-line WARN per
        // thread -- 2 to 4 per process, measured at 157 lines a day across two
        // hosts once shared memory engaged everywhere. A warning that cannot
        // ever be acted on by the process emitting it is how a real line becomes
        // invisible.
        //
        // NOT A SILENCING: the condition is still reported, once per process and
        // naming what is lost, by `report_unavailable_realtime_priority` below.
        // What is removed is the futile syscall and the repetition.
        //
        // FAIL-SAFE TOWARDS UPSTREAM BEHAVIOUR: only a limit read as EXACTLY
        // zero skips the request. If the limit cannot be determined -- a non-Linux
        // unix, an unreadable /proc -- the request is attempted exactly as before,
        // so this can only remove a request that was certain to fail.
        #[cfg(unix)]
        let builder = {
            let base = ThreadBuilder::default().name(name);
            if Self::realtime_priority_is_refused() {
                Self::report_unavailable_realtime_priority();
                base
            } else {
                base.policy(Realtime(RealtimeThreadSchedulePolicy::Fifo))
                    .priority(ThreadPriority::Min)
            }
        };

        // TODO: deal with windows realtime scheduling
        #[cfg(windows)]
        let builder = ThreadBuilder::default().name(name);

        let _ = builder.spawn(move |result| {
            if let Err(e) = result {
                let mut err = format!(
                    "{:?}: error setting scheduling priority for thread: {:?}, will run with ",
                    std::thread::current().name(),
                    e
                );
                #[cfg(windows)]
                {
                    err.push_str("the default one. ");
                }
                #[cfg(unix)]
                {
                    for priority in (ThreadPriorityValue::MIN..ThreadPriorityValue::MAX).rev() {
                        if let Ok(p) = priority.try_into() {
                            if set_current_thread_priority(ThreadPriority::Crossplatform(p)).is_ok()
                            {
                                err.push_str(&format!("priority {priority}. "));
                                break;
                            }
                        }
                    }
                }
                err.push_str("This is not an hard error and it can be safely ignored under normal operating conditions. \
                Though the SHM subsystem may experience some timeouts in case of an heavy congested system where this watchdog thread may not be scheduled at the required frequency.");
                tracing::warn!("{}", err);
            }

            //TODO: need mlock here!

            while c_running.load(Ordering::Relaxed) {
                let cycle_start = std::time::Instant::now();

                f(TaskWakeReason::Timeout);

                // sleep for next iteration
                let elapsed = cycle_start.elapsed();
                if elapsed < interval {
                    let mut sleep_interval = interval - elapsed;
                    while recv.recv_timeout (sleep_interval).is_ok() {
                        f(TaskWakeReason::Kick);
                        let elapsed = cycle_start.elapsed();
                        if elapsed < interval {
                            sleep_interval = interval - elapsed;
                        } else {
                            break;
                        }
                    }
                } else {
                    let err = format!("{:?}: timer overrun", std::thread::current().name());
                    #[cfg(not(feature = "test"))]
                    tracing::error!("{err}");
                    #[cfg(feature = "test")]
                    panic!("{err}");
                }
            }
        });

        Self { running, send }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod realtime_priority_tests {
    /// **THE SKIP TRIGGERS ONLY ON A LIMIT OF EXACTLY ZERO** (BER-2160).
    ///
    /// Parsing `/proc/self/limits` is where this can go silently wrong, and both
    /// directions are damaging in different ways: reading a usable limit as zero
    /// would abandon real-time scheduling on a host that had granted it, and
    /// reading zero as usable brings back the per-thread EPERM warning this
    /// removes.
    ///
    /// The column matters -- `Max realtime priority` has SOFT at index 3 of the
    /// whitespace split, because the name itself is three words.
    #[test]
    fn only_a_zero_soft_limit_counts_as_refused() {
        let soft_of = |line: &str| {
            line.split_whitespace().nth(3).map(|s| s == "0")
        };

        // The real shape, as the kernel prints it.
        assert_eq!(
            soft_of("Max realtime priority     0                    0                    "),
            Some(true),
            "a zero soft limit is a refusal"
        );
        assert_eq!(
            soft_of("Max realtime priority     50                   50                   "),
            Some(false),
            "a granted priority must NOT be read as refused -- that would abandon real-time \
             scheduling on a host that allows it"
        );
        assert_eq!(
            soft_of("Max realtime priority     unlimited            unlimited            "),
            Some(false),
            "unlimited is not zero"
        );
    }

    /// The live limit parses, whatever it happens to be.
    ///
    /// A non-vacuity control: the test above works on literals, so it would pass
    /// even if the real file had no such line and the predicate always returned
    /// false. This one proves the line exists and is found.
    #[test]
    fn the_live_limits_file_carries_a_realtime_priority_line() {
        let limits = std::fs::read_to_string("/proc/self/limits").expect("read limits");
        let line = limits
            .lines()
            .find(|l| l.starts_with("Max realtime priority"))
            .expect("the kernel must print a realtime priority limit");
        assert!(
            line.split_whitespace().nth(3).is_some(),
            "and the soft column must be present: {line}"
        );
    }
}
