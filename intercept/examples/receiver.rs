//! Trivial workload, receiver half: accepts one connection and
//! reads `count` fixed-width (8-byte) messages via `read_exact`, so each
//! maps to exactly one intercepted syscall, matching `sender.rs`'s framing.

use std::io::Read as _;
use std::net::TcpListener;

fn main() {
    let addr = std::env::args()
        .nth(1)
        .expect("usage: receiver <bind-addr:port> <count>");
    let count: u32 = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);

    let listener = TcpListener::bind(&addr).expect("bind failed");
    println!("app: listening on {addr}");
    let (mut stream, _) = listener.accept().expect("accept failed");

    for i in 0..count {
        let mut buf = [0u8; 8];
        stream.read_exact(&mut buf).expect("recv failed");
        let msg = String::from_utf8_lossy(&buf).to_string();
        println!("app: recv seq={i} msg={msg}");
    }
}
