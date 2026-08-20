//! Stress check: validates a message log written by *multiple
//! concurrent processes* (possibly on different containers) to the *same
//! file* — the exact scenario found broken pre-`flock` (concurrent
//! appenders interleaving partial writes into corrupted JSONL framing).
//! Confirms every line parses, then groups by `pid` and checks each
//! process's own event stream is non-empty and internally well-formed.

use std::collections::BTreeMap;
use std::fs;

use intercept::MessageEvent;

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: check_concurrent <log-file>");
    let raw = fs::read_to_string(&path).unwrap_or_else(|e| panic!("failed to read {path}: {e}"));
    let lines: Vec<&str> = raw.lines().filter(|l| !l.trim().is_empty()).collect();

    let mut malformed = 0;
    let mut events: Vec<MessageEvent> = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        match serde_json::from_str::<MessageEvent>(line) {
            Ok(e) => events.push(e),
            Err(err) => {
                malformed += 1;
                eprintln!("line {i}: malformed JSON ({err}): {line}");
            }
        }
    }

    println!(
        "{} line(s) total, {} malformed, {} parsed OK",
        lines.len(),
        malformed,
        events.len()
    );

    let mut by_pid: BTreeMap<u32, Vec<&MessageEvent>> = BTreeMap::new();
    for event in &events {
        by_pid.entry(event.pid).or_default().push(event);
    }
    println!(
        "{} distinct pid(s) wrote to this file concurrently",
        by_pid.len()
    );
    for (pid, evs) in &by_pid {
        let sends = evs.iter().filter(|e| e.direction == "send").count();
        let recvs = evs.iter().filter(|e| e.direction == "recv").count();
        println!(
            "  pid {pid}: {} event(s) ({sends} send, {recvs} recv)",
            evs.len()
        );
    }

    if malformed > 0 {
        eprintln!(
            "FAIL: {malformed} malformed line(s) — concurrent writers corrupted the shared file"
        );
        std::process::exit(1);
    }
    println!("all lines valid JSONL under concurrent multi-process writes");
}
