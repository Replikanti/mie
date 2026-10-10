//! [`UreqHttp`] passes the `Retry-After` header through
//! [`HttpGet::get_reply`] (ADR-045 D7). It is the only production
//! [`HttpGet`] the open-interest poller receives, so a transport that kept
//! the trait's header-blind default would silently disable `Retry-After`.
//! A plain-HTTP server on the loopback interface answers; no external
//! network.

use mie_adapter_binance::transport::{HttpGet, HttpReply, NetTimeouts, UreqHttp};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

/// Serves `responses` in order, one connection each, and returns the URL.
fn serve(responses: Vec<&'static str>) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    let url = format!(
        "http://{}/fapi/v1/openInterest",
        listener.local_addr().unwrap()
    );
    let server = thread::spawn(move || {
        for response in responses {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut request = Vec::new();
            let mut buf = [0_u8; 1024];
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = stream.read(&mut buf).expect("read request");
                assert!(n > 0, "client closed before the request ended");
                request.extend_from_slice(&buf[..n]);
            }
            stream
                .write_all(response.as_bytes())
                .expect("write response");
        }
    });
    (url, server)
}

const RATE_LIMITED: &str = "HTTP/1.1 429 Too Many Requests\r\n\
    Retry-After: 30\r\n\
    Content-Type: application/json\r\n\
    Content-Length: 2\r\n\
    Connection: close\r\n\
    \r\n\
    {}";

const OK: &str = "HTTP/1.1 200 OK\r\n\
    Content-Type: application/json\r\n\
    Content-Length: 2\r\n\
    Connection: close\r\n\
    \r\n\
    {}";

#[test]
fn ureq_http_returns_the_retry_after_header() {
    let (url, server) = serve(vec![RATE_LIMITED, OK, RATE_LIMITED]);
    let http = UreqHttp::new(NetTimeouts::default()).expect("client");
    assert_eq!(
        http.get_reply(&url).expect("429 reply"),
        HttpReply {
            status: 429,
            body: b"{}".to_vec(),
            retry_after: Some("30".to_owned()),
        }
    );
    assert_eq!(
        http.get_reply(&url).expect("200 reply"),
        HttpReply {
            status: 200,
            body: b"{}".to_vec(),
            retry_after: None,
        }
    );
    // `get` still returns the status and body alone.
    assert_eq!(http.get(&url).expect("429"), (429, b"{}".to_vec()));
    server.join().expect("server");
}
