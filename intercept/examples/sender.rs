//! Trivial workload, sender half: connects and sends `count`
//! fixed-width (8-byte) messages, one per `write_all` call, so each maps to
//! exactly one intercepted syscall — no framing ambiguity for `check_order`
//! to resolve. `interval_ms` (default 50, 0 allowed) controls spacing
//! between sends — 0 removes the artificial gap between messages, stressing
//! millisecond-clock-resolution ordering instead of giving each message
//! room to land in its own tick.

use std::io::Write as _;
use std::net::TcpStream;
use std::time::Duration;

fn main() {
    let addr = std::env::args()
        .nth(1)
        .expect("usage: sender <addr:port> <count> [interval_ms]");
    let count: u32 = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let interval_ms: u64 = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);

    let mut stream = loop {
        match TcpStream::connect(&addr) {
            Ok(s) => break s,
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    };

    for i in 0..count {
        let msg = format!("msg-{i:04}");
        stream.write_all(msg.as_bytes()).expect("send failed");
        println!("app: sent seq={i} msg={msg}");
        if interval_ms > 0 {
            std::thread::sleep(Duration::from_millis(interval_ms));
        }
    }
}
