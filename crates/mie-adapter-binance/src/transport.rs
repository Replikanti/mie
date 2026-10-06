//! The I/O seams of live capture: WebSocket, HTTP and clock.
//!
//! The connection loops ([`crate::live`]) see only these traits, so tests
//! drive them with scripted fakes and a fake clock, and never touch the
//! network. The real implementations are blocking: [`TungsteniteConnector`]
//! and [`UreqHttp`] over rustls (ring provider, the operating system's root
//! store), and [`SystemClock`].

use std::io::ErrorKind;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

/// The outcome of one read on a WebSocket connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadOutcome {
    /// A data frame (text or binary): its exact payload bytes.
    Frame(Vec<u8>),
    /// A control frame (ping, pong). Proves liveness; never persisted.
    Control,
    /// Nothing arrived within the connection's read timeout.
    Timeout,
    /// The peer closed the connection.
    Closed(String),
    /// The connection failed.
    Error(String),
}

/// One open WebSocket connection.
pub trait WsConnection: Send {
    /// Reads the next frame, waiting at most the connection's read timeout.
    fn read(&mut self) -> ReadOutcome;

    /// Sends a ping frame.
    ///
    /// # Errors
    ///
    /// A description of the failure when the frame cannot be sent.
    fn ping(&mut self) -> Result<(), String>;

    /// Closes the connection, best effort.
    fn close(&mut self);
}

/// Opens WebSocket connections.
pub trait WsConnector: Send + Sync {
    /// Connects to `url` and completes the WebSocket handshake.
    ///
    /// # Errors
    ///
    /// A description of the failure.
    fn connect(&self, url: &str) -> Result<Box<dyn WsConnection>, String>;
}

/// A blocking HTTP GET.
pub trait HttpGet: Send + Sync {
    /// Fetches `url` and returns the status code and the exact body bytes.
    /// Non-2xx statuses are results, not errors.
    ///
    /// # Errors
    ///
    /// A description of a transport failure (DNS, connect, TLS, timeout).
    fn get(&self, url: &str) -> Result<(u16, Vec<u8>), String>;
}

/// Time for capture metadata and scheduling, never for ordering
/// (ADR-028 D1).
pub trait Clock: Send + Sync {
    /// Wall-clock time in nanoseconds since the Unix epoch (UTC).
    fn now_utc_ns(&self) -> i64;

    /// Monotonic nanoseconds since an arbitrary origin.
    fn monotonic_ns(&self) -> u64;

    /// Blocks the calling thread for `duration`.
    fn sleep(&self, duration: Duration);
}

/// The system clock.
#[derive(Debug, Clone, Copy)]
pub struct SystemClock {
    origin: Instant,
}

impl SystemClock {
    /// A clock whose monotonic origin is now.
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn now_utc_ns(&self) -> i64 {
        // Before 1970 or after 2262 cannot happen on a running capture host.
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
    }

    fn monotonic_ns(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

/// Timeouts of the real connectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetTimeouts {
    /// TCP connect, TLS and WebSocket handshake, and HTTP request timeout.
    pub connect: Duration,
    /// Read timeout of an open WebSocket connection: how often a reading
    /// thread wakes up to check pings, liveness and shutdown.
    pub read: Duration,
}

impl Default for NetTimeouts {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(10),
            read: Duration::from_secs(1),
        }
    }
}

/// The operating system's root certificates as DER.
fn native_root_ders() -> Result<Vec<Vec<u8>>, String> {
    let result = rustls_native_certs::load_native_certs();
    if result.certs.is_empty() {
        return Err(format!(
            "no root certificates in the operating system store: {:?}",
            result.errors
        ));
    }
    Ok(result.certs.iter().map(|c| c.as_ref().to_vec()).collect())
}

fn ring_provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// WebSocket client over rustls ([`tungstenite`]).
#[derive(Debug, Clone)]
pub struct TungsteniteConnector {
    timeouts: NetTimeouts,
    tls: Arc<rustls::ClientConfig>,
}

impl TungsteniteConnector {
    /// A connector trusting the operating system's root certificates.
    ///
    /// # Errors
    ///
    /// A description of the failure when no root certificate can be loaded.
    pub fn new(timeouts: NetTimeouts) -> Result<Self, String> {
        let mut roots = rustls::RootCertStore::empty();
        for der in native_root_ders()? {
            // Unparsable system certificates are skipped, as browsers do.
            let _ = roots.add(der.into());
        }
        let tls = rustls::ClientConfig::builder_with_provider(ring_provider())
            .with_safe_default_protocol_versions()
            .map_err(|e| format!("TLS configuration: {e}"))?
            .with_root_certificates(roots)
            .with_no_client_auth();
        Ok(Self {
            timeouts,
            tls: Arc::new(tls),
        })
    }
}

impl WsConnector for TungsteniteConnector {
    fn connect(&self, url: &str) -> Result<Box<dyn WsConnection>, String> {
        let uri: tungstenite::http::Uri = url.parse().map_err(|e| format!("url {url}: {e}"))?;
        let host = uri.host().ok_or_else(|| format!("url {url} has no host"))?;
        let port = uri.port_u16().unwrap_or(if uri.scheme_str() == Some("ws") {
            80
        } else {
            443
        });
        let addrs = (host, port)
            .to_socket_addrs()
            .map_err(|e| format!("resolve {host}: {e}"))?;
        let mut last_error = format!("{host} resolved to no address");
        let mut tcp = None;
        for addr in addrs {
            match TcpStream::connect_timeout(&addr, self.timeouts.connect) {
                Ok(stream) => {
                    tcp = Some(stream);
                    break;
                }
                Err(e) => last_error = format!("connect {addr}: {e}"),
            }
        }
        let tcp = tcp.ok_or(last_error)?;
        let io = |e: std::io::Error| format!("socket setup: {e}");
        tcp.set_nodelay(true).map_err(io)?;
        tcp.set_read_timeout(Some(self.timeouts.connect))
            .map_err(io)?;
        tcp.set_write_timeout(Some(self.timeouts.connect))
            .map_err(io)?;
        let connector = tungstenite::Connector::Rustls(Arc::clone(&self.tls));
        let (socket, _response) =
            tungstenite::client_tls_with_config(url, tcp, None, Some(connector))
                .map_err(|e| format!("handshake {url}: {e}"))?;
        let mut connection = TungsteniteConnection { socket };
        connection
            .tcp()
            .set_read_timeout(Some(self.timeouts.read))
            .map_err(io)?;
        Ok(Box::new(connection))
    }
}

struct TungsteniteConnection {
    socket: WebSocket<MaybeTlsStream<TcpStream>>,
}

impl TungsteniteConnection {
    fn tcp(&mut self) -> &mut TcpStream {
        match self.socket.get_mut() {
            MaybeTlsStream::Plain(tcp) => tcp,
            MaybeTlsStream::Rustls(tls) => &mut tls.sock,
            // `MaybeTlsStream` is non-exhaustive; no other TLS backend is
            // compiled in.
            _ => unreachable!("only plain and rustls streams are built"),
        }
    }
}

impl WsConnection for TungsteniteConnection {
    fn read(&mut self) -> ReadOutcome {
        match self.socket.read() {
            Ok(Message::Text(text)) => ReadOutcome::Frame(text.as_bytes().to_vec()),
            Ok(Message::Binary(bytes)) => ReadOutcome::Frame(bytes.to_vec()),
            Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_)) => ReadOutcome::Control,
            Ok(Message::Close(frame)) => ReadOutcome::Closed(match frame {
                Some(frame) => format!("close frame {} {}", frame.code, frame.reason),
                None => "close frame".to_owned(),
            }),
            Err(tungstenite::Error::Io(e))
                if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) =>
            {
                ReadOutcome::Timeout
            }
            Err(tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed) => {
                ReadOutcome::Closed("connection closed".to_owned())
            }
            Err(e) => ReadOutcome::Error(e.to_string()),
        }
    }

    fn ping(&mut self) -> Result<(), String> {
        self.socket
            .send(Message::Ping(Vec::new().into()))
            .map_err(|e| format!("ping: {e}"))
    }

    fn close(&mut self) {
        let _ = self.socket.close(None);
        let _ = self.socket.flush();
    }
}

/// HTTP client over rustls ([`ureq`]).
#[derive(Debug, Clone)]
pub struct UreqHttp {
    agent: ureq::Agent,
}

impl UreqHttp {
    /// A client trusting the operating system's root certificates.
    ///
    /// # Errors
    ///
    /// A description of the failure when no root certificate can be loaded.
    pub fn new(timeouts: NetTimeouts) -> Result<Self, String> {
        let roots: Vec<_> = native_root_ders()?
            .iter()
            .map(|der| ureq::tls::Certificate::from_der(der).to_owned())
            .collect();
        let tls = ureq::tls::TlsConfig::builder()
            .provider(ureq::tls::TlsProvider::Rustls)
            .unversioned_rustls_crypto_provider(ring_provider())
            .root_certs(ureq::tls::RootCerts::new_with_certs(&roots))
            .build();
        let agent = ureq::Agent::config_builder()
            .tls_config(tls)
            .http_status_as_error(false)
            .timeout_global(Some(timeouts.connect))
            .build()
            .new_agent();
        Ok(Self { agent })
    }
}

impl HttpGet for UreqHttp {
    fn get(&self, url: &str) -> Result<(u16, Vec<u8>), String> {
        let mut response = self
            .agent
            .get(url)
            .call()
            .map_err(|e| format!("GET {url}: {e}"))?;
        let status = response.status().as_u16();
        let body = response
            .body_mut()
            .read_to_vec()
            .map_err(|e| format!("GET {url} body: {e}"))?;
        Ok((status, body))
    }
}
