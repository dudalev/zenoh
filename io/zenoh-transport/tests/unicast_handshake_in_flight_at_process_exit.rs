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
//! A handshake already in flight when its process begins to exit is refused cleanly
//! (BER-2479): no panic, nothing logged at ERROR, and no Close frame to the healthy peer.
//!
//! Each test runs itself again as a child, in the shape of a FATAL exit. A proxy in this
//! process holds one of the child's handshakes half way, the child calls
//! `std::process::exit` with it held, and `linger` keeps the child in exit after static_init
//! has finalized its statics. Only then is the handshake released. A link with
//! shared-memory handoff registers with the transport's handoff reactor, a static, so the
//! handshake meets it finalized -- or, when it is the child's first such link, never
//! initialized, where initializing it now panics as well.
#![cfg(all(
    unix,
    feature = "shared-memory",
    feature = "test",
    feature = "transport_tcp"
))]

use std::{
    any::Any,
    io::{BufRead, BufReader, Read, Write},
    net::SocketAddr,
    process::{Child, ChildStdin, Command, ExitStatus, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread::JoinHandle,
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{tcp::OwnedReadHalf, TcpListener, TcpStream},
    sync::Notify,
};
use tracing::Instrument;
use zenoh_core::ztimeout;
use zenoh_link::Link;
use zenoh_protocol::{
    core::{EndPoint, WhatAmI, ZenohIdProto},
    network::NetworkMessageMut,
};
use zenoh_result::ZResult;
use zenoh_transport::{
    multicast::TransportMulticast, unicast::TransportUnicast, TransportEventHandler,
    TransportManager, TransportMulticastEventHandler, TransportPeer, TransportPeerEventHandler,
};

const TIMEOUT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(10);
/// Set in the child, to the name of the test it runs.
const CHILD: &str = "ZENOH_TEST_HANDSHAKE_EXIT_CHILD";
/// The endpoint a dialling child opens while live.
const DIRECT: &str = "ZENOH_TEST_HANDSHAKE_EXIT_DIRECT";
/// The endpoint a dialling child opens through the proxy.
const HELD: &str = "ZENOH_TEST_HANDSHAKE_EXIT_HELD";
/// How long the child stays in exit after static_init has finalized its statics.
const LINGER: Duration = Duration::from_secs(3);
/// Starts every line of the child's log, which it writes to its stdout.
const LOG: &str = "CHILD LOG";
/// The child's first log line, an ERROR, so that its log is known to reach this process.
const LOGGING: &str = "the child's log reaches the parent";
/// The child's line naming the endpoint it listens on.
const LISTENING: &str = "CHILD LISTENING";
/// A dialling child's line with the result of its dial while live.
const CONTROL: &str = "CHILD CONTROL";
/// The child's line just before it exits.
const EXITING: &str = "CHILD EXITING";
/// The child's line once static_init has finalized its statics, from `linger`.
const FINALIZED: &str = "CHILD FINALIZED";
/// A dialling child's line with the result of its held dial.
const OPENED: &str = "CHILD OPENED";
/// What this process writes to the child's stdin when the child is to exit.
const EXIT: &str = "EXIT";
/// The refusal, from `unicast/establishment/ext/shm/handoff.rs`, and its two causes.
const REFUSED: &str = "SHM handoff reactor unavailable";
const REFUSED_FINALIZED: &str = "process exit has finalized it";
const REFUSED_BEGUN: &str = "process exit has begun";
/// The span this process dials in, with a `test` field naming its test.
const DIALLER: &str = "dialler";
/// Logged at ERROR in that span, so that what it logs is known to reach the test.
const PROBE: &str = "the dialler's log reaches the test";

const ACCEPT: &str = "an_accept_in_flight_at_exit_is_refused_cleanly";
const FIRST_ACCEPT: &str = "a_first_accept_in_flight_at_exit_is_refused_cleanly";
const OPEN: &str = "an_open_in_flight_at_exit_is_refused_cleanly";

/// Runs after static_init has finalized its statics, which it does at priority 0, so the
/// child stays in exit with them finalized while its held handshake is released.
#[static_init::destructor(1)]
extern "C" fn linger() {
    if std::env::var_os(CHILD).is_some() {
        println!("{FINALIZED}");
        std::thread::sleep(LINGER);
    }
}

struct Peer;

impl TransportPeerEventHandler for Peer {
    fn handle_message(&self, _message: NetworkMessageMut) -> ZResult<()> {
        Ok(())
    }
    fn new_link(&self, _link: Link) {}
    fn del_link(&self, _link: Link) {}
    fn closed(&self) {}
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Counts the transports a manager admits.
struct Handler(Arc<AtomicUsize>);

impl TransportEventHandler for Handler {
    fn new_unicast(
        &self,
        _peer: TransportPeer,
        _transport: TransportUnicast,
    ) -> ZResult<Arc<dyn TransportPeerEventHandler>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(Peer))
    }

    fn new_multicast(
        &self,
        _transport: TransportMulticast,
    ) -> ZResult<Arc<dyn TransportMulticastEventHandler>> {
        panic!("not used")
    }
}

/// A manager with shared memory on, in `lazy` mode, counting what it admits.
fn manager(byte: u8, admitted: Arc<AtomicUsize>) -> TransportManager {
    let mut shm = zenoh_config::ShmConf::default();
    shm.set_enabled(true).unwrap();
    shm.set_mode(zenoh_config::ShmInitMode::Lazy).unwrap();
    TransportManager::builder()
        .whatami(WhatAmI::Peer)
        .zid(ZenohIdProto::try_from([byte]).unwrap())
        .shm(shm)
        .build_test(Arc::new(Handler(admitted)))
        .unwrap()
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}

/// Logs WARN and above, and the handshakes' DEBUG, to stdout, one line per event.
fn init_child_log() {
    zenoh_util::init_log_with_callback(
        |meta| {
            !meta.is_span()
                && (*meta.level() <= tracing::Level::WARN
                    || (*meta.level() <= tracing::Level::DEBUG
                        && meta
                            .target()
                            .starts_with("zenoh_transport::unicast::establishment")))
        },
        |record| {
            println!(
                "{LOG} {} {}: {}",
                record.level,
                record.target,
                record.message.as_deref().unwrap_or_default()
            )
        },
    );
}

/// Every WARN and ERROR event of this process, each with the fields of its spans.
static LOGGED: Mutex<Vec<zenoh_util::LogRecord>> = Mutex::new(Vec::new());

/// Logs WARN and above to stderr and into [`LOGGED`].
fn init_parent_log() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        zenoh_util::init_log_with_callback(
            |meta| {
                if meta.is_span() {
                    meta.name() == DIALLER
                } else {
                    *meta.level() <= tracing::Level::WARN
                }
            },
            |record| {
                eprintln!(
                    "{} {}: {}",
                    record.level,
                    record.target,
                    record.message.as_deref().unwrap_or_default()
                );
                LOGGED
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(record);
            },
        );
    });
}

/// The ERROR events logged in the span of the dialler whose `test` field is `test`.
fn dialler_errors(test: u8) -> Vec<String> {
    LOGGED
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter(|record| record.level == tracing::Level::ERROR)
        .filter(|record| {
            record
                .attributes
                .iter()
                .any(|(name, value)| *name == "test" && *value == test.to_string())
        })
        .map(|record| record.message.clone().unwrap_or_default())
        .collect()
}

async fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while !done() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(POLL).await;
    }
}

/// Which batch of a handshake [`HeldHandshake`] holds back.
#[derive(Clone, Copy)]
enum Hold {
    /// The dialler's second batch, its OpenSyn: the acceptor waits in `recv_open_syn`.
    OpenSyn,
    /// The acceptor's second batch, its OpenAck: the dialler waits in `recv_open_ack`.
    OpenAck,
}

/// A TCP proxy in front of an acceptor that holds one handshake half way, until `release`.
struct HeldHandshake {
    addr: SocketAddr,
    holding: Arc<AtomicBool>,
    release: Arc<Notify>,
}

impl HeldHandshake {
    async fn start(acceptor: SocketAddr, hold: Hold) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let holding = Arc::new(AtomicBool::new(false));
        let release = Arc::new(Notify::new());
        let (c_holding, c_release) = (holding.clone(), release.clone());
        tokio::spawn(async move {
            let (dialler, _) = listener.accept().await.unwrap();
            let acceptor = TcpStream::connect(acceptor).await.unwrap();
            let (from_dialler, to_dialler) = dialler.into_split();
            let (from_acceptor, to_acceptor) = acceptor.into_split();
            let ((mut from, mut to), (mut free_from, mut free_to)) = match hold {
                Hold::OpenSyn => ((from_dialler, to_acceptor), (from_acceptor, to_dialler)),
                Hold::OpenAck => ((from_acceptor, to_dialler), (from_dialler, to_acceptor)),
            };
            tokio::spawn(async move {
                let _ = tokio::io::copy(&mut free_from, &mut free_to).await;
            });
            let first = read_batch(&mut from).await;
            to.write_all(&first).await.unwrap();
            let second = read_batch(&mut from).await;
            c_holding.store(true, Ordering::SeqCst);
            c_release.notified().await;
            // The far side may have gone by now.
            if to.write_all(&second).await.is_ok() {
                let _ = tokio::io::copy(&mut from, &mut to).await;
            }
        });
        Self {
            addr,
            holding,
            release,
        }
    }

    fn endpoint(&self) -> EndPoint {
        format!("tcp/{}", self.addr).parse().unwrap()
    }

    fn holding(&self) -> bool {
        self.holding.load(Ordering::SeqCst)
    }

    fn release(&self) {
        self.release.notify_one();
    }
}

/// One batch of a streamed link, with its two-byte little-endian length prefix.
async fn read_batch(from: &mut OwnedReadHalf) -> Vec<u8> {
    let mut len = [0_u8; 2];
    from.read_exact(&mut len).await.unwrap();
    let mut batch = vec![0_u8; u16::from_le_bytes(len) as usize];
    from.read_exact(&mut batch).await.unwrap();
    [len.to_vec(), batch].concat()
}

/// The address a `tcp/` endpoint names.
fn socket_addr(endpoint: &EndPoint) -> SocketAddr {
    endpoint.address().as_str().parse().unwrap()
}

/// Whether this process is the child of `test`.
fn is_child(test: &str) -> bool {
    std::env::var(CHILD).is_ok_and(|child| child == test)
}

/// Waits for this process to write [`EXIT`] to the child's stdin, or to go.
async fn wait_for_exit() {
    let line = tokio::task::spawn_blocking(|| {
        let mut line = String::new();
        std::io::stdin().lock().read_line(&mut line).unwrap();
        line
    })
    .await
    .unwrap();
    if line.trim() != EXIT {
        println!("{EXITING}: the parent closed its end of stdin");
        std::process::exit(2);
    }
}

/// Listens, and exits with its listener open once this process says so.
fn accepting_child() -> ! {
    init_child_log();
    tracing::error!("{LOGGING}");
    runtime().block_on(async {
        let admitted = Arc::new(AtomicUsize::new(0));
        let listener = manager(0xc1, admitted.clone());
        let locator = ztimeout!(listener.add_listener("tcp/127.0.0.1:0".parse().unwrap())).unwrap();
        println!("{LISTENING} {locator}");
        wait_for_exit().await;
        println!(
            "{EXITING} having admitted {}",
            admitted.load(Ordering::SeqCst)
        );
        std::process::exit(0)
    });
    unreachable!("the child exits from inside its runtime")
}

/// Dials while live, then dials through the proxy and exits once this process says so.
fn opening_child() -> ! {
    init_child_log();
    tracing::error!("{LOGGING}");
    let direct: EndPoint = std::env::var(DIRECT).unwrap().parse().unwrap();
    let held: EndPoint = std::env::var(HELD).unwrap().parse().unwrap();
    runtime().block_on(async {
        let first = manager(0xc3, Arc::new(AtomicUsize::new(0)));
        let live = ztimeout!(first.open_transport_unicast(direct)).map(|t| t.is_shm());
        println!("{CONTROL} {live:?}");
        let second = manager(0xe3, Arc::new(AtomicUsize::new(0)));
        tokio::spawn(async move {
            let opened = second
                .open_transport_unicast(held)
                .await
                .map(|t| t.is_shm());
            println!("{OPENED} {opened:?}");
        });
        wait_for_exit().await;
        println!("{EXITING}");
        std::process::exit(0)
    });
    unreachable!("the child exits from inside its runtime")
}

/// The child, and what it has said so far. Killed if the test gives up on it.
struct Spawned {
    child: Child,
    stdin: ChildStdin,
    stdout: Arc<Mutex<Vec<String>>>,
    stdout_reader: Option<JoinHandle<()>>,
    stderr_reader: Option<JoinHandle<String>>,
}

impl Spawned {
    fn new(test: &str, env: &[(&str, String)]) -> Self {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", test, "--nocapture", "--test-threads=1"])
            .env(CHILD, test)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (name, value) in env {
            command.env(name, value);
        }
        let mut child = command.spawn().unwrap();
        let stdin = child.stdin.take().unwrap();
        // Read the child's two streams as they come, so that neither pipe fills up.
        let stdout = Arc::new(Mutex::new(Vec::new()));
        let stdout_reader = std::thread::spawn({
            let pipe = child.stdout.take().unwrap();
            let stdout = stdout.clone();
            move || {
                for line in BufReader::new(pipe).lines() {
                    stdout.lock().unwrap().push(line.unwrap());
                }
            }
        });
        let stderr_reader = std::thread::spawn({
            let mut pipe = child.stderr.take().unwrap();
            move || {
                let mut stderr = String::new();
                pipe.read_to_string(&mut stderr).unwrap();
                stderr
            }
        });
        Self {
            child,
            stdin,
            stdout,
            stdout_reader: Some(stdout_reader),
            stderr_reader: Some(stderr_reader),
        }
    }

    /// The index and the rest of the first stdout line that starts with `prefix`.
    fn line(&self, prefix: &str) -> Option<(usize, String)> {
        self.stdout
            .lock()
            .unwrap()
            .iter()
            .enumerate()
            .find_map(|(i, line)| line.strip_prefix(prefix).map(|rest| (i, rest.to_string())))
    }

    async fn wait_for_line(&self, prefix: &str) -> (usize, String) {
        let mut found = None;
        wait_until(&format!("the child to say {prefix}"), || {
            found = self.line(prefix);
            found.is_some()
        })
        .await;
        found.unwrap()
    }

    /// Tells the child to exit.
    fn exit(&mut self) {
        writeln!(self.stdin, "{EXIT}").unwrap();
        self.stdin.flush().unwrap();
    }

    fn is_alive(&mut self) -> bool {
        self.child.try_wait().unwrap().is_none()
    }

    /// Waits for the child, and returns its status, its stdout lines and its stderr.
    fn finish(&mut self) -> (ExitStatus, Vec<String>, String) {
        let status = self.child.wait().unwrap();
        self.stdout_reader.take().unwrap().join().unwrap();
        let stderr = self.stderr_reader.take().unwrap().join().unwrap();
        (status, self.stdout.lock().unwrap().clone(), stderr)
    }
}

impl Drop for Spawned {
    fn drop(&mut self) {
        // Reaped already on the way through `finish`; this is for a test that panicked.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// What the child said, for a failure message.
fn transcript(stdout: &[String], stderr: &str) -> String {
    format!(
        "child stdout:\n{}\nchild stderr:\n{stderr}",
        stdout.join("\n")
    )
}

/// The index of the first line that starts with `prefix`.
fn index_of(lines: &[String], prefix: &str) -> Option<usize> {
    lines.iter().position(|line| line.starts_with(prefix))
}

/// Whether `line` is a log line of the child's at ERROR. Not `starts_with`: the first shares
/// its line with libtest's `test <name> ... `.
fn is_error(line: &str) -> bool {
    line.contains(&format!("{LOG} ERROR "))
}

/// The child's log lines at ERROR after it began to exit.
fn errors_in_exit(lines: &[String]) -> Vec<String> {
    let from = index_of(lines, EXITING).unwrap_or(lines.len());
    lines[from..]
        .iter()
        .filter(|line| is_error(line))
        .cloned()
        .collect()
}

/// The checks both kinds of child share: it exited as it chose to, its log reached this
/// process, it did not panic, and it logged no ERROR in exit.
fn assert_exited_cleanly(status: ExitStatus, stdout: &[String], stderr: &str) {
    let log = transcript(stdout, stderr);
    assert!(
        status.success(),
        "the child must exit as it chose to: {status}\n{log}"
    );
    assert!(
        stdout
            .iter()
            .any(|line| is_error(line) && line.contains(LOGGING)),
        "the child's log must reach this process, or the absences below prove nothing\n{log}"
    );
    assert!(
        !stderr.contains("panicked"),
        "a handshake in flight at exit must be refused, not panic on a finalized static\n{log}"
    );
    let errors = errors_in_exit(stdout);
    assert!(
        errors.is_empty(),
        "a process in exit must log no ERROR for a handshake it refuses: {errors:?}\n{log}"
    );
}

/// Dials the child through a proxy that holds the OpenSyn, and releases it once the child
/// is in exit with its statics finalized. With `control`, dials the child directly first,
/// while it is live, which initializes its handoff reactor.
fn held_accept(test: &str, id: u8, control: bool) {
    init_parent_log();
    let rt = runtime();
    let mut child = Spawned::new(test, &[]);
    let (live, opened, alive_after, first) = rt.block_on(async {
        let (_, endpoint) = child.wait_for_line(LISTENING).await;
        let endpoint: EndPoint = endpoint.trim().parse().unwrap();

        // THE CONTROL: while the child is live, a dial is admitted, with shared memory.
        let (live, first) = if control {
            let first = manager(0xa0 + id, Arc::new(AtomicUsize::new(0)));
            let live = ztimeout!(first.open_transport_unicast(endpoint.clone()))
                .map(|transport| transport.is_shm());
            (Some(live), Some(first))
        } else {
            (None, None)
        };

        let proxy = HeldHandshake::start(socket_addr(&endpoint), Hold::OpenSyn).await;
        let proxy_endpoint = proxy.endpoint();
        let dialling = tokio::spawn(
            async move {
                tracing::error!("{PROBE}");
                let dialler = manager(0xb0 + id, Arc::new(AtomicUsize::new(0)));
                let opened = dialler
                    .open_transport_unicast(proxy_endpoint)
                    .await
                    .map(|transport| transport.is_shm());
                dialler.close().await;
                opened
            }
            .instrument(tracing::error_span!(DIALLER, test = id)),
        );
        wait_until("the child to wait for the OpenSyn", || proxy.holding()).await;

        child.exit();
        child.wait_for_line(FINALIZED).await;
        proxy.release();
        let opened = ztimeout!(dialling).unwrap();
        // The dial met the child in exit only if it is still there.
        let alive_after = child.is_alive();
        (live, opened, alive_after, first)
    });

    let (status, stdout, stderr) = child.finish();
    if let Some(first) = first {
        rt.block_on(async { ztimeout!(first.close()) });
    }
    let log = transcript(&stdout, &stderr);
    assert_exited_cleanly(status, &stdout, &stderr);
    if let Some(live) = live {
        assert!(
            matches!(live, Ok(Ok(true))),
            "while the child is live a dial must be admitted, with shared memory: {live:?}\n{log}"
        );
    }
    let admitted = format!("{EXITING} having admitted {}", u8::from(control));
    assert!(
        stdout.iter().any(|line| *line == admitted),
        "the child must have admitted exactly the control before it exited, {admitted:?}\n{log}"
    );
    assert!(
        alive_after,
        "the child must still be in exit after the dial, or the dial did not meet it there\n{log}"
    );
    assert!(
        opened.is_err(),
        "a process in exit must admit no transport: {opened:?}\n{log}"
    );

    // The refusal is the handoff's, in exit: the reactor finalized when the control
    // initialized it, never initialized when the held handshake was the child's first.
    let cause = if control {
        REFUSED_FINALIZED
    } else {
        REFUSED_BEGUN
    };
    let finalized = index_of(&stdout, FINALIZED);
    let refused = stdout
        .iter()
        .position(|line| line.starts_with(LOG) && line.contains(REFUSED) && line.contains(cause));
    assert!(
        matches!((finalized, refused), (Some(f), Some(r)) if f < r),
        "the child must refuse the handshake in exit because {cause}, after its statics are \
         finalized: line {refused:?}, finalized at line {finalized:?}\n{log}"
    );

    // The healthy dialler is told nothing at ERROR: the refusal sends no Close frame.
    let logged = dialler_errors(id);
    assert!(
        logged.iter().any(|message| message == PROBE),
        "the dialler's log must reach this test, or the absence below proves nothing: \
         {logged:?}"
    );
    assert!(
        logged.iter().all(|message| message == PROBE),
        "the dialler is healthy and must log no ERROR when the child refuses it: {logged:?}"
    );
}

/// Accepts the child's dials, the second through a proxy that holds the OpenAck, and
/// releases it once the child is in exit with its statics finalized.
fn held_open() {
    init_parent_log();
    let rt = runtime();
    let (listener, mut child) = rt.block_on(async {
        let listener = manager(0xd3, Arc::new(AtomicUsize::new(0)));
        let locator = ztimeout!(listener.add_listener("tcp/127.0.0.1:0".parse().unwrap())).unwrap();
        let direct: EndPoint = locator.to_string().parse().unwrap();
        let proxy = HeldHandshake::start(socket_addr(&direct), Hold::OpenAck).await;
        let mut child = Spawned::new(
            OPEN,
            &[
                (DIRECT, direct.to_string()),
                (HELD, proxy.endpoint().to_string()),
            ],
        );
        child.wait_for_line(CONTROL).await;
        wait_until("the child to wait for the OpenAck", || proxy.holding()).await;

        child.exit();
        child.wait_for_line(FINALIZED).await;
        proxy.release();
        (listener, child)
    });

    let (status, stdout, stderr) = child.finish();
    rt.block_on(async { ztimeout!(listener.close()) });
    let log = transcript(&stdout, &stderr);
    assert_exited_cleanly(status, &stdout, &stderr);
    let live = stdout
        .iter()
        .find_map(|line| line.strip_prefix(CONTROL))
        .map(str::trim);
    assert_eq!(
        live,
        Some("Ok(Ok(true))"),
        "while the child is live its dial must be admitted, with shared memory\n{log}"
    );

    // The child's own line, written while it was in exit with its statics finalized.
    let finalized = index_of(&stdout, FINALIZED);
    let opened = index_of(&stdout, OPENED);
    assert!(
        matches!((finalized, opened), (Some(f), Some(o)) if f < o),
        "the held dial must end while the child is in exit with its statics finalized: line \
         {opened:?}, finalized at line {finalized:?}\n{log}"
    );
    let opened = &stdout[opened.unwrap()];
    assert!(
        opened.contains("Err(") && opened.contains(REFUSED) && opened.contains(REFUSED_FINALIZED),
        "the held dial must be refused because {REFUSED_FINALIZED}: {opened}\n{log}"
    );
}

/// **AN ACCEPT IN FLIGHT AT EXIT IS REFUSED CLEANLY**, once exit has finalized the handoff
/// reactor that a live accept initialized.
#[test]
fn an_accept_in_flight_at_exit_is_refused_cleanly() {
    if is_child(ACCEPT) {
        accepting_child();
    }
    held_accept(ACCEPT, 1, true);
}

/// **AND SO IS THE FIRST**, whose reactor was never initialized: static_init refuses to
/// initialize a static once it has finalized the rest.
#[test]
fn a_first_accept_in_flight_at_exit_is_refused_cleanly() {
    if is_child(FIRST_ACCEPT) {
        accepting_child();
    }
    held_accept(FIRST_ACCEPT, 2, false);
}

/// **AN OPEN IN FLIGHT AT EXIT IS REFUSED CLEANLY**, once exit has finalized the handoff
/// reactor that a live open initialized.
#[test]
fn an_open_in_flight_at_exit_is_refused_cleanly() {
    if is_child(OPEN) {
        opening_child();
    }
    held_open();
}
