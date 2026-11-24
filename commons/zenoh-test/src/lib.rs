//
// Copyright (c) 2025 ZettaScale Technology
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

//! Shared test utilities for the Zenoh workspace.
//!
//! This crate provides `TestSessions`, with its `sessions` feature, a helper that manages the
//! lifecycle of Zenoh sessions used in integration tests:
//!
//! * Dynamic port allocation (`tcp/127.0.0.1:0`) to avoid port collisions during
//!   parallel test execution.
//! * Automatic locator resolution after a listener binds to port `0`.
//! * Deterministic teardown — connectors are closed before listeners.
//!
//! # Quick start
//!
//! ```rust,ignore
//! use zenoh_test::TestSessions;
//!
//! let mut test_sessions = TestSessions::new();
//! let (listener, connector) = test_sessions.open_pairs().await;
//! // … run assertions …
//! test_sessions.close().await;
//! ```
//!
//! # Scouting
//!
//! A test that scouts does so in a [`ScoutingGroup`] of its own, so that it finds the
//! sessions and routers it starts and no zenoh the host runs. A test whose subject needs a
//! router runs the zenohd built from this workspace, through `Zenohd`, with the `sessions`
//! feature.
//!
//! # Files a test binds
//!
//! A unix socket or named pipe a test binds goes in a [`TestDir`] of its own, outside the
//! source tree.

#[cfg(feature = "sessions")]
use std::{
    io::{BufRead, BufReader, Read},
    process::{Child, Command, Stdio},
    sync::{atomic::AtomicU16, Arc, Mutex},
    thread::JoinHandle,
    time::Instant,
};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, UdpSocket},
    ops::DerefMut,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

#[cfg(feature = "internal")]
use zenoh::internal::runtime::{Runtime, RuntimeBuilder};
#[cfg(feature = "sessions")]
use zenoh::{Session, Wait};
#[cfg(feature = "sessions")]
use zenoh_config::{WhatAmI, ZenohId};
#[cfg(feature = "sessions")]
use zenoh_core::ztimeout;
use zenoh_link::EndPoint;
#[cfg(feature = "sessions")]
use zenoh_protocol::core::EndPoints;
use zenoh_protocol::core::Parameters;

/// Default timeout applied to async operations via [`ztimeout!`](zenoh_core::ztimeout).
pub const TIMEOUT: Duration = Duration::from_secs(60);

/// How often a wait below checks again.
#[cfg(feature = "sessions")]
const POLL: Duration = Duration::from_millis(10);

/// Binds to a random TCP port on loopback and returns the assigned port number.
///
/// The socket is dropped (and the port released) before the caller can use it,
/// so there is a tiny theoretical race.  In practice this is negligible on
/// localhost because tests bind immediately after calling this function.
pub fn get_free_tcp_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Binds to a random UDP port on loopback and returns the assigned port number.
///
/// The socket is dropped (and the port released) before the caller can use it,
/// so there is a tiny theoretical race. In practice this is negligible on
/// localhost because tests bind immediately after calling this function.
pub fn get_free_udp_port() -> u16 {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    socket.local_addr().unwrap().port()
}

/// A directory of a test's own for the files it binds, unix sockets and named pipes, in the
/// system's temporary directory and so outside the source tree. It is removed when dropped,
/// and so also when the test fails or panics (BER-2480).
///
/// An endpoint at a relative path binds in the test's working directory, which is its
/// package's directory.
pub struct TestDir(PathBuf);

impl TestDir {
    pub fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        loop {
            let dir = std::env::temp_dir().join(format!(
                "zenoh-test-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            match std::fs::create_dir(&dir) {
                Ok(()) => return Self(dir),
                // Left by an earlier process that had this one's id.
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => panic!("could not create the test directory {}: {e}", dir.display()),
            }
        }
    }

    pub fn path(&self) -> &Path {
        &self.0
    }

    /// An endpoint of `protocol` whose address is the file `name` in this directory, as in
    /// `dir.endpoint("unixsock-stream", "a.sock")`.
    pub fn endpoint(&self, protocol: &str, name: &str) -> EndPoint {
        let path = self.0.join(name);
        let path = path
            .to_str()
            .expect("the temporary directory's path is UTF-8");
        format!("{protocol}/{path}").parse().unwrap()
    }
}

impl Default for TestDir {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_dir_all(&self.0) {
            eprintln!(
                "could not remove the test directory {}: {e}",
                self.0.display()
            );
        }
    }
}

/// The unix sockets and named pipes under `dir`, not following symlinks and skipping `.git`
/// and every `target` directory.
///
/// Every test binds its own in a [`TestDir`], so one found under a package's directory was
/// bound there by a test, in the source tree (BER-2480).
#[cfg(unix)]
pub fn sockets_under(dir: &Path) -> Vec<PathBuf> {
    use std::os::unix::fs::FileTypeExt;

    let mut found = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let entries = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("could not read {}: {e}", dir.display()));
        for entry in entries {
            let entry = entry.unwrap();
            let file_type = entry.file_type().unwrap();
            if file_type.is_dir() {
                if entry.file_name() != ".git" && entry.file_name() != "target" {
                    pending.push(entry.path());
                }
            } else if file_type.is_socket() || file_type.is_fifo() {
                found.push(entry.path());
            }
        }
    }
    found.sort();
    found
}

/// The interface the tests' multicast uses, by name or by address: loopback, unless
/// `ZENOH_TEST_MULTICAST_IFACE` names another.
///
/// Left to choose, a udp link joins its group on the first non-loopback multicast
/// interface, so the host's network would decide the test (BER-2458).
pub fn multicast_iface() -> String {
    std::env::var("ZENOH_TEST_MULTICAST_IFACE").unwrap_or_else(|_| "127.0.0.1".to_string())
}

/// `endpoint`, joined on [`multicast_iface`] unless it names an interface itself.
pub fn on_multicast_iface(endpoint: &str) -> String {
    with_iface(endpoint, &multicast_iface())
}

/// `endpoint`, joined on `iface` unless it names an interface itself.
fn with_iface(endpoint: &str, iface: &str) -> String {
    let parsed: EndPoint = endpoint.parse().unwrap();
    if parsed.config().get("iface").is_some() {
        return endpoint.to_string();
    }
    let mut config = Parameters::from(parsed.config().as_str());
    config.insert("iface", iface);
    EndPoint::new(
        parsed.protocol(),
        parsed.address(),
        parsed.metadata(),
        config.as_str(),
    )
    .unwrap()
    .to_string()
}

/// A multicast scouting group a test has to itself: `224.0.0.224` on a port no other live
/// group has, joined on [`multicast_iface`].
///
/// A session that scouts in it finds the sessions and routers the test puts in it, and no
/// zenoh the host runs in the default group, `224.0.0.224:7446` (BER-2469). Scouting stays
/// on. Keep the group alive for as long as its sessions are.
pub struct ScoutingGroup {
    address: SocketAddr,
    interface: String,
    /// Holds the group's port on loopback, so that no other group is given it while this
    /// one lives. A scouting socket binds the group's own address, so this does not stop
    /// it.
    _port: UdpSocket,
}

impl Default for ScoutingGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl ScoutingGroup {
    pub fn new() -> Self {
        let port = UdpSocket::bind("127.0.0.1:0").unwrap();
        let address = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(224, 0, 0, 224)),
            port.local_addr().unwrap().port(),
        );
        Self {
            address,
            interface: multicast_iface(),
            _port: port,
        }
    }

    /// The group's multicast address.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// The interface the group is joined on.
    pub fn interface(&self) -> &str {
        &self.interface
    }

    /// Turns multicast scouting on in `config`, in this group.
    pub fn apply(&self, config: &mut zenoh_config::Config) {
        config.scouting.multicast.set_enabled(Some(true)).unwrap();
        config
            .scouting
            .multicast
            .set_address(Some(self.address))
            .unwrap();
        config
            .scouting
            .multicast
            .set_interface(Some(self.interface.clone()))
            .unwrap();
    }

    /// A default config, scouting in this group.
    pub fn config<C>(&self) -> C
    where
        C: Default + DerefMut<Target = zenoh_config::Config>,
    {
        let mut config = C::default();
        self.apply(&mut config);
        config
    }

    /// The arguments that put a zenohd in this group.
    pub fn zenohd_args(&self) -> [String; 4] {
        [
            "--cfg".to_string(),
            format!("scouting/multicast/address:\"{}\"", self.address),
            "--cfg".to_string(),
            format!("scouting/multicast/interface:\"{}\"", self.interface),
        ]
    }
}

/// A zenohd in a [`ScoutingGroup`], started for one test and stopped with it.
///
/// `binary` must be the zenohd built from this workspace: an integration test of the
/// `zenohd` package is given it as `env!("CARGO_BIN_EXE_zenohd")`, and cargo builds it for
/// that run. A zenohd from anywhere else, such as the one installed on the host, would test
/// a different router (BER-2469).
#[cfg(feature = "sessions")]
pub struct Zenohd {
    child: Mutex<Child>,
    zid: ZenohId,
    log: Arc<Mutex<String>>,
    readers: Vec<JoinHandle<()>>,
}

#[cfg(feature = "sessions")]
impl Zenohd {
    /// Starts `binary` in `group`, listening on a loopback port the OS picks, with no
    /// plugins.
    pub fn start(binary: &str, group: &ScoutingGroup) -> Self {
        static STARTED: AtomicU16 = AtomicU16::new(0);
        let zid: ZenohId = format!(
            "{:x}{:04x}{:04x}",
            std::process::id(),
            group.address().port(),
            STARTED.fetch_add(1, Ordering::Relaxed)
        )
        .parse()
        .unwrap();
        let mut child = Command::new(binary)
            .args(["--id", &zid.to_string(), "-l", "tcp/127.0.0.1:0"])
            .args(group.zenohd_args())
            .args(["--cfg", "plugins_loading/enabled:false"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("cannot start {binary}: {e}"));
        let log = Arc::new(Mutex::new(String::new()));
        let readers = [
            child
                .stdout
                .take()
                .map(|pipe| Box::new(pipe) as Box<dyn Read + Send>),
            child
                .stderr
                .take()
                .map(|pipe| Box::new(pipe) as Box<dyn Read + Send>),
        ]
        .into_iter()
        .flatten()
        .map(|pipe| {
            let log = log.clone();
            std::thread::spawn(move || {
                for line in BufReader::new(pipe).lines().map_while(Result::ok) {
                    let mut log = log.lock().unwrap_or_else(|e| e.into_inner());
                    log.push_str(&line);
                    log.push('\n');
                }
            })
        })
        .collect();
        Self {
            child: Mutex::new(child),
            zid,
            log,
            readers,
        }
    }

    pub fn zid(&self) -> ZenohId {
        self.zid
    }

    /// What the zenohd has logged so far.
    pub fn log(&self) -> String {
        self.log.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Waits until `session` has a transport to this zenohd. When a session opens, it may
    /// not have found its router yet.
    pub fn wait_linked(&self, session: &Session) {
        let deadline = Instant::now() + TIMEOUT;
        while !session
            .info()
            .routers_zid()
            .wait()
            .any(|zid| zid == self.zid)
        {
            let exited = self
                .child
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .try_wait()
                .unwrap();
            assert!(
                exited.is_none(),
                "zenohd {} exited ({exited:?}) before {} linked to it; its log:\n{}",
                self.zid,
                session.zid(),
                self.log()
            );
            assert!(
                Instant::now() < deadline,
                "{} did not link to zenohd {} within {TIMEOUT:?}; its log:\n{}",
                session.zid(),
                self.zid,
                self.log()
            );
            std::thread::sleep(POLL);
        }
    }
}

#[cfg(feature = "sessions")]
impl Drop for Zenohd {
    fn drop(&mut self) {
        let child = self.child.get_mut().unwrap_or_else(|e| e.into_inner());
        let _ = child.kill();
        let _ = child.wait();
        for reader in self.readers.drain(..) {
            let _ = reader.join();
        }
        if std::thread::panicking() {
            eprintln!("zenohd {} log:\n{}", self.zid, self.log());
        }
    }
}

/// Waits until `a` and `b` each have a transport to the other: two sessions that find each
/// other by scouting may not have done so when they open.
#[cfg(feature = "sessions")]
pub async fn wait_for_peers(a: &Session, b: &Session) {
    ztimeout!(async {
        while !(is_linked(a, &b.zid()).await && is_linked(b, &a.zid()).await) {
            tokio::time::sleep(POLL).await;
        }
    });
}

#[cfg(feature = "sessions")]
async fn is_linked(session: &Session, peer: &ZenohId) -> bool {
    session.info().peers_zid().await.any(|zid| &zid == peer)
}

/// Returns the first TCP [`EndPoint`] exposed by `session`.
///
/// This is a convenience wrapper around [`get_locators_from_session`] that
/// filters for the `tcp/` protocol — handy when a listener was opened with
/// `tcp/127.0.0.1:0` and you need the resolved address for a connector.
#[cfg(feature = "sessions")]
pub async fn get_tcp_locator(session: &Session) -> EndPoint {
    get_locators_from_session(session)
        .await
        .into_iter()
        .find(|ep| ep.to_string().starts_with("tcp/"))
        .expect("Expected a TCP listener endpoint from session")
}

/// Queries `session` for its current locators (async version).
#[cfg(feature = "sessions")]
pub async fn get_locators_from_session(session: &Session) -> Vec<EndPoint> {
    session
        .info()
        .locators()
        .await
        .into_iter()
        .map(|l| l.to_endpoint())
        .collect()
}

/// Queries `session` for its current locators (blocking version).
#[cfg(feature = "sessions")]
pub fn get_locators_from_session_sync(session: &Session) -> Vec<EndPoint> {
    session
        .info()
        .locators()
        .wait()
        .into_iter()
        .map(|l| l.to_endpoint())
        .collect()
}

/// Closes two sessions in order, printing progress to stdout.
///
/// This is a standalone helper kept for backward compatibility with tests that
/// manage sessions outside of [`TestSessions`].  Prefer
/// [`TestSessions::close`] for new code.
#[cfg(feature = "sessions")]
pub async fn close_session(peer01: Session, peer02: Session) {
    println!("[  ][01d] Closing peer01 session");
    ztimeout!(peer01.close()).unwrap();
    println!("[  ][02d] Closing peer02 session");
    ztimeout!(peer02.close()).unwrap();
}

/// Manages session lifecycle for integration tests.
///
/// `TestSessions` keeps track of every listener and connector it opens so that
/// [`close`](Self::close) can tear them down in the right order (connectors
/// first, then listeners).
///
/// # Locator tracking
///
/// Each call to [`open_listener`](Self::open_listener) or
/// [`open_listener_with_cfg`](Self::open_listener_with_cfg) **replaces** the
/// internally stored locators with the ones from the newly opened listener.
/// If you need locators from a previously opened listener, call
/// [`get_locators_from_session`] directly.
#[cfg(feature = "sessions")]
pub struct TestSessions {
    locators: Vec<EndPoint>,
    listener_sessions: Vec<Session>,
    connector_sessions: Vec<Session>,
}

#[cfg(feature = "sessions")]
impl Default for TestSessions {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "sessions")]
impl TestSessions {
    /// Creates an empty `TestSessions` with no sessions and no stored locators.
    pub fn new() -> Self {
        TestSessions {
            locators: vec![],
            listener_sessions: vec![],
            connector_sessions: vec![],
        }
    }

    /// Builds a listener [`Config`](zenoh_config::Config) that listens on
    /// `link_num` copies of `endpoint` (typically `"tcp/127.0.0.1:0"`).
    ///
    /// Multicast scouting is disabled and `max_links` is set to `link_num`.
    pub fn get_listener_config(&self, endpoint: &str, link_num: usize) -> zenoh_config::Config {
        let endpoints: Vec<EndPoint> = (0..link_num).map(|_| endpoint.parse().unwrap()).collect();

        let mut config = zenoh_config::Config::default();
        config.scouting.multicast.set_enabled(Some(false)).unwrap();
        config.listen.endpoints.set(endpoints).unwrap();
        config.transport.unicast.set_max_links(link_num).unwrap();

        config
    }

    /// Builds a connector [`Config`](zenoh_config::Config) that connects to
    /// the given `locators`.
    ///
    /// Multicast scouting is disabled and `max_links` matches the number of
    /// locators.
    pub fn get_connector_config_with_endpoint(
        &self,
        locators: Vec<EndPoint>,
    ) -> zenoh_config::Config {
        println!("Connecting to {:?}", locators);
        let endpoint_groups: Vec<EndPoints> = locators.into_iter().map(Into::into).collect();
        let mut config = zenoh_config::Config::default();
        config.scouting.multicast.set_enabled(Some(false)).unwrap();
        config
            .transport
            .unicast
            .set_max_links(endpoint_groups.len())
            .unwrap();
        config.connect.endpoints.set(endpoint_groups).unwrap();

        config
    }

    /// Builds a connector config using the internally stored locators
    /// (set by the most recent [`open_listener`](Self::open_listener) call).
    pub fn get_connector_config(&self) -> zenoh_config::Config {
        self.get_connector_config_with_endpoint(self.locators.clone())
    }

    /// Returns a clone of the internally stored locators.
    pub fn locators(&self) -> Vec<EndPoint> {
        self.locators.clone()
    }

    /// Opens a listener session with the given config, stores it, and updates
    /// the internal locators.
    pub async fn open_listener_with_cfg(&mut self, config: zenoh_config::Config) -> Session {
        let session = ztimeout!(zenoh::open(config)).unwrap();
        let locators = get_locators_from_session(&session).await;

        println!("Listening to {:?}", locators);
        self.listener_sessions.push(session.clone());
        self.locators = locators;

        session
    }

    /// Opens a listener session with the given config (blocking version).
    pub fn open_listener_with_cfg_sync(&mut self, config: zenoh_config::Config) -> Session {
        let session = zenoh::open(config).wait().unwrap();
        let locators = get_locators_from_session_sync(&session);

        println!("Listening to {:?}", locators);
        self.listener_sessions.push(session.clone());
        self.locators = locators;

        session
    }

    /// Opens a TCP listener on a random port (`tcp/127.0.0.1:0`).
    pub async fn open_listener(&mut self) -> Session {
        let config = self.get_listener_config("tcp/127.0.0.1:0", 1);
        self.open_listener_with_cfg(config).await
    }

    /// Opens a connector session with the given config and tracks it for
    /// later teardown.
    pub async fn open_connector_with_cfg(&mut self, config: zenoh_config::Config) -> Session {
        let session = ztimeout!(zenoh::open(config)).unwrap();
        self.connector_sessions.push(session.clone());

        session
    }

    /// Opens a connector session with the given config (blocking version).
    pub fn open_connector_with_cfg_sync(&mut self, config: zenoh_config::Config) -> Session {
        let session = zenoh::open(config).wait().unwrap();
        self.connector_sessions.push(session.clone());

        session
    }

    /// Opens a connector that connects to the internally stored locators.
    pub async fn open_connector(&mut self) -> Session {
        let config = self.get_connector_config();
        self.open_connector_with_cfg(config).await
    }

    /// Opens a listener + connector pair on a random TCP port.
    ///
    /// Returns `(listener_session, connector_session)`.
    pub async fn open_pairs(&mut self) -> (Session, Session) {
        let listener_session = self.open_listener().await;
        let connector_session = self.open_connector().await;
        (listener_session, connector_session)
    }

    /// Opens a listener + client-mode connector pair on a random TCP port.
    ///
    /// Returns `(listener_session, client_session)`.
    pub async fn open_pairs_client(&mut self) -> (Session, Session) {
        let listener_session = self.open_listener().await;

        let mut config = self.get_connector_config();
        config.set_mode(Some(WhatAmI::Client)).unwrap();
        let connector_session = self.open_connector_with_cfg(config).await;

        (listener_session, connector_session)
    }

    /// Opens two sessions joined via multicast on `endpoint` (e.g.
    /// `"udp/224.0.0.1:0"`), on [`multicast_iface`] unless `endpoint` names an interface.
    ///
    /// The first session binds to port `0`; the second session then binds to
    /// the resolved port, on the same interface, so both share the same multicast group.
    pub async fn open_pairs_multicast(&mut self, endpoint: &str) -> (Session, Session) {
        let endpoint = on_multicast_iface(endpoint);
        let iface = endpoint
            .parse::<EndPoint>()
            .unwrap()
            .config()
            .get("iface")
            .unwrap()
            .to_string();
        let config = self.get_listener_config(&endpoint, 1);
        let session01 = ztimeout!(zenoh::open(config.clone())).unwrap();
        self.listener_sessions.push(session01.clone());
        let locator = session01
            .info()
            .locators()
            .await
            .into_iter()
            .find(|l| l.protocol().as_str() == "udp")
            .expect("Expected at least one UDP locator")
            .to_string();
        let locator = with_iface(&locator, &iface);
        println!("Connecting to {:?}", locator);
        let config = self.get_listener_config(&locator, 1);
        let session02 = ztimeout!(zenoh::open(config)).unwrap();
        self.listener_sessions.push(session02.clone());

        (session01, session02)
    }

    /// Opens a listener + connector pair at the **runtime** level.
    ///
    /// Useful for tests that need direct access to [`Runtime`] instead of
    /// [`Session`].
    #[cfg(feature = "internal")]
    pub async fn open_pairs_runtime(&mut self) -> (Runtime, Runtime) {
        let config = self.get_listener_config("tcp/127.0.0.1:0", 1);
        let mut listener_runtime = RuntimeBuilder::new(config.into()).build().await.unwrap();
        listener_runtime.start().await.unwrap();

        let locators = listener_runtime
            .get_locators_noloopback()
            .into_iter()
            .map(|l| l.to_endpoint())
            .collect();

        let config = self.get_connector_config_with_endpoint(locators);
        let mut connector_runtime = RuntimeBuilder::new(config.into()).build().await.unwrap();
        connector_runtime.start().await.unwrap();

        (listener_runtime, connector_runtime)
    }

    /// Closes all tracked sessions: connectors first, then listeners.
    pub async fn close(&mut self) {
        for session in self.connector_sessions.drain(..) {
            println!("Closing connector session");
            ztimeout!(session.close()).unwrap();
        }

        for session in self.listener_sessions.drain(..) {
            println!("Closing listener session");
            ztimeout!(session.close()).unwrap();
        }
    }

    /// Closes all tracked sessions (blocking version).
    pub fn close_sync(&mut self) {
        for session in self.connector_sessions.drain(..) {
            println!("Closing connector session");
            session.close().wait().unwrap();
        }

        for session in self.listener_sessions.drain(..) {
            println!("Closing listener session");
            session.close().wait().unwrap();
        }
    }
}
