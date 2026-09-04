//! Test-only real local HTTP/1.1 server (a **test double**, allowed in `tests/`).
//!
//! Shared by the `forward_auth` and `usage_sink` integration test crates; each crate only uses a
//! subset of the harness, so the superset surface is intentionally `dead_code`-free via allow.
#![allow(dead_code)]
//!
//! Not part of the implementation crate — it is the harness the integration tests use to make
//! `reqwest` (forward-auth / usage sink) perform real HTTP I/O against a capturable endpoint. It:
//! - records every request (method, target, headers, body);
//! - responds with a configurable status + headers, optionally after a delay;
//! - supports a "black-hole" mode (accept the connection, never respond) to exercise the client
//!   timeout → FAIL_OPEN path.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

/// One captured request.
#[derive(Clone, Debug)]
pub struct RecordedRequest {
    pub method: String,
    pub target: String,
    /// `(lowercased-name, value)` pairs, in wire order.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl RecordedRequest {
    /// First value of a header (case-insensitive), `None` if absent.
    pub fn header(&self, name: &str) -> Option<&str> {
        let n = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| k.as_str() == n)
            .map(|(_, v)| v.as_str())
    }

    /// `true` if the header is present (case-insensitive).
    pub fn has_header(&self, name: &str) -> bool {
        let n = name.to_ascii_lowercase();
        self.headers.iter().any(|(k, _)| k.as_str() == n)
    }
}

struct State {
    captured: Mutex<Vec<RecordedRequest>>,
    status: AtomicU16,
    resp_headers: Mutex<Vec<(String, String)>>,
    delay_ms: AtomicU64,
    blackhole: AtomicBool,
    shutdown: Notify,
}

/// A real local HTTP server for integration tests.
pub struct TestServer {
    addr: SocketAddr,
    state: Arc<State>,
}

impl TestServer {
    pub async fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind 127.0.0.1:0");
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(State {
            captured: Mutex::new(Vec::new()),
            status: AtomicU16::new(200),
            resp_headers: Mutex::new(Vec::new()),
            delay_ms: AtomicU64::new(0),
            blackhole: AtomicBool::new(false),
            shutdown: Notify::new(),
        });
        let accept_state = state.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = accept_state.shutdown.notified() => break,
                    res = listener.accept() => {
                        let (sock, _peer) = match res {
                            Ok(x) => x,
                            Err(_) => continue,
                        };
                        let st = accept_state.clone();
                        tokio::spawn(handle_connection(sock, st));
                    }
                }
            }
        });
        Self { addr, state }
    }

    /// `http://127.0.0.1:PORT` base URL.
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn set_status(&self, code: u16) {
        self.state.status.store(code, Ordering::SeqCst);
    }

    /// Response headers to emit (replaces the previous set).
    pub fn set_headers(&self, headers: Vec<(&'static str, &'static str)>) {
        let h: Vec<(String, String)> = headers
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        *self.state.resp_headers.lock().unwrap() = h;
    }

    pub fn set_delay_ms(&self, ms: u64) {
        self.state.delay_ms.store(ms, Ordering::SeqCst);
    }

    /// Black-hole mode: accept connections and hold them open without responding (for timeout tests).
    pub fn set_blackhole(&self, on: bool) {
        self.state.blackhole.store(on, Ordering::SeqCst);
    }

    /// Wait until at least `n` requests have been captured (bounded by a timeout to avoid hangs).
    pub async fn wait_for(&self, n: usize) -> Vec<RecordedRequest> {
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            let len = self.state.captured.lock().unwrap().len();
            if len >= n {
                return self.state.captured.lock().unwrap().clone();
            }
            if Instant::now() > deadline {
                let got = self.state.captured.lock().unwrap().clone();
                panic!("timed out waiting for {n} request(s), got {got:?}");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// The current number of captured requests.
    pub fn count(&self) -> usize {
        self.state.captured.lock().unwrap().len()
    }

    pub fn shutdown(&self) {
        self.state.shutdown.notify_waiters();
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

async fn handle_connection(mut sock: TcpStream, state: Arc<State>) {
    let mut buf: Vec<u8> = Vec::new();
    let mut tmp = [0u8; 4096];

    // Read until the header block is complete (`\r\n\r\n`).
    loop {
        if state.blackhole.load(Ordering::SeqCst) {
            // Hold open until shutdown (black-hole) so the client times out.
            state.shutdown.notified().await;
            return;
        }
        match sock.read(&mut tmp).await {
            Ok(0) => return,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(_) => return,
        }
        if let Some(e) = find_subseq(&buf, b"\r\n\r\n", 0) {
            if e + 4 <= buf.len() {
                break;
            }
        }
        if buf.len() > (1 << 20) {
            return; // header guard
        }
    }

    let header_end = find_subseq(&buf, b"\r\n\r\n", 0).unwrap() + 4;
    let head = String::from_utf8_lossy(&buf[..header_end]);
    let (method, target, headers) = parse_request_head(&head);

    // Body: read exactly Content-Length bytes (after the headers, already partly in `buf`).
    let content_length = headers
        .iter()
        .find(|(k, _)| k.as_str() == "content-length")
        .map(|(_, v)| v.parse::<usize>().unwrap_or(0))
        .unwrap_or(0);
    let mut body = buf[header_end..].to_vec();
    while body.len() < content_length {
        match sock.read(&mut tmp).await {
            Ok(0) => break,
            Ok(n) => body.extend_from_slice(&tmp[..n]),
            Err(_) => break,
        }
    }
    if body.len() > content_length {
        let mut trimmed = body;
        trimmed.truncate(content_length);
        body = trimmed;
    }

    // Record.
    state.captured.lock().unwrap().push(RecordedRequest {
        method,
        target,
        headers,
        body,
    });

    let is_blackhole = state.blackhole.load(Ordering::SeqCst);
    if is_blackhole {
        // Hold the connection open (no response) until shutdown → client timeout.
        state.shutdown.notified().await;
        return;
    }

    let delay_ms = state.delay_ms.load(Ordering::SeqCst);
    if delay_ms > 0 {
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }

    let status = state.status.load(Ordering::SeqCst);
    let resp_headers = state.resp_headers.lock().unwrap().clone();
    let mut resp = String::new();
    resp.push_str(&format!(
        "HTTP/1.1 {} {}\r\n",
        status,
        reason_phrase(status)
    ));
    for (k, v) in &resp_headers {
        resp.push_str(&format!("{}: {}\r\n", k, v));
    }
    resp.push_str("Content-Length: 0\r\n");
    resp.push_str("Connection: close\r\n");
    resp.push_str("\r\n");
    let _ = sock.write_all(resp.as_bytes()).await;
    let _ = sock.flush().await;
    let _ = sock.shutdown().await;
}

fn reason_phrase(code: u16) -> &'static str {
    match code {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        301 => "Moved Permanently",
        302 => "Found",
        304 => "Not Modified",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Unknown",
    }
}

fn parse_request_head(head: &str) -> (String, String, Vec<(String, String)>) {
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.splitn(3, ' ');
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("").to_string();
    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if let Some(idx) = line.find(':') {
            let name = line[..idx].trim().to_ascii_lowercase();
            let value = line[idx + 1..].trim().to_string();
            headers.push((name, value));
        }
    }
    (method, target, headers)
}

fn find_subseq(hay: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if needle.is_empty() {
        return (from..=hay.len()).find(|_| true);
    }
    if hay.len() < from + needle.len() {
        return None;
    }
    let last = hay.len() - needle.len();
    let mut i = from.min(last);
    while i <= last {
        if &hay[i..i + needle.len()] == needle {
            return Some(i);
        }
        i += 1;
    }
    None
}
