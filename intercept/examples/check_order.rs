//! Merges a sender's and receiver's independent shim logs and
//! checks whether wall-clock order agrees with the known application-level
//! order (send *i* always precedes recv *i*, since `sender.rs`/`receiver.rs`
//! exchange fixed-width messages one-to-one over a single TCP stream).
//! This is the direct test of the open question carried forward from the
//! boundary-recording work: is an NTP-synced wall clock good enough for
//! cross-node ordering?

use std::fs;

use intercept::MessageEvent;

fn read_events(path: &str) -> Vec<MessageEvent> {
    fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("failed to read {path}: {e}"))
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("bad line in {path}: {e}: {l}")))
        .collect()
}

fn main() {
    let sender_log = std::env::args()
        .nth(1)
        .expect("usage: check_order <sender-log> <receiver-log>");
    let receiver_log = std::env::args()
        .nth(2)
        .expect("usage: check_order <sender-log> <receiver-log>");

    let sends: Vec<MessageEvent> = read_events(&sender_log)
        .into_iter()
        .filter(|e| e.direction == "send")
        .collect();
    let recvs: Vec<MessageEvent> = read_events(&receiver_log)
        .into_iter()
        .filter(|e| e.direction == "recv")
        .collect();

    println!(
        "{} send event(s), {} recv event(s)",
        sends.len(),
        recvs.len()
    );
    if sends.len() != recvs.len() {
        eprintln!("send/recv count mismatch — framing assumption violated");
        std::process::exit(2);
    }

    let mut inversions = 0;
    for (i, (s, r)) in sends.iter().zip(recvs.iter()).enumerate() {
        let delta = r.timestamp_ms as i128 - s.timestamp_ms as i128;
        let ok = delta >= 0;
        if !ok {
            inversions += 1;
        }
        println!(
            "msg {i}: send={} recv={} delta={delta}ms {}",
            s.timestamp_ms,
            r.timestamp_ms,
            if ok { "OK" } else { "INVERTED" }
        );
    }

    if inversions > 0 {
        eprintln!(
            "{inversions} inversion(s) out of {} message(s)",
            sends.len()
        );
        std::process::exit(1);
    }
    println!("all {} message(s) ordered correctly", sends.len());
}
