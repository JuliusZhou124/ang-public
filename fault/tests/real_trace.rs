//! Runs `seed_search`/`seed_search_stats` against a real recorded trace
//! instead of a hand-built fixture. `fixtures/real_job_1.eventlog.jsonl`
//! is the actual Boundary EventLog a 3-container Slurm cluster produced for a
//! genuine `srun --ntasks=2 --nodes=2 hostname` job spanning `node1` and
//! `node2` — not assembled by hand, no synthetic env vars. Checked in as
//! JSONL (the real recorded artifact, human-readable, small) rather than a
//! binary SQLite snapshot; ingestion into an in-memory `TraceStore` happens
//! at test time via the same `ingest_job_eventlog` path a live system uses.
//! See `fixtures/README.md` for provenance and how to reproduce it.

use std::path::Path;

use recorder::TraceStore;
use replay::JobReplayer;

fn load_real_job() -> Vec<replay::TimelineEvent> {
    let mut store = TraceStore::open(Path::new(":memory:")).unwrap();
    store
        .ingest_job_eventlog(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/real_job_1.eventlog.jsonl"
        )))
        .unwrap();
    JobReplayer::new(&store, "1").events().unwrap()
}

#[test]
fn real_trace_ingests_as_seven_correlated_events() {
    let events = load_real_job();
    assert_eq!(
        events.len(),
        7,
        "2 node_allocated + 2 task_started + 2 task_exited + 1 job_exited"
    );
}

#[test]
fn clean_real_trace_has_no_violations() {
    let events = load_real_job();
    assert_eq!(fault::invariant::check(&events), vec![]);
}

#[test]
fn seed_search_against_the_real_trace_is_deterministic() {
    let events = load_real_job();
    let a = fault::seed_search(&events, 0..100);
    let b = fault::seed_search(&events, 0..100);
    assert_eq!(a, b);
}

#[test]
fn seed_search_finds_a_violation_in_the_real_trace() {
    // With 7 events across 2 nodes, all three registered invariants
    // (node-allocated-before-task-started, task-started-before-exited,
    // job-exited-is-last) are reachable — unlike the 4-event synthetic
    // fixture used in fault/src/lib.rs's unit tests, this is genuine
    // multi-node data, not hand-assembled.
    let events = load_real_job();
    let (_, violations) = fault::seed_search(&events, 0..100)
        .expect("expected at least one failing seed in 100 tries");
    assert!(!violations.is_empty());
}

#[test]
fn seed_search_stats_against_the_real_trace_is_deterministic() {
    let events = load_real_job();
    let a = fault::seed_search_stats(&events, 0..1000);
    let b = fault::seed_search_stats(&events, 0..1000);
    assert_eq!(a, b);
}

#[test]
fn seed_search_stats_against_the_real_trace_finds_a_real_failure_rate() {
    // This is the number exhaustive mode exists to produce — computed
    // against a genuine 2-node/2-task trace, not a synthetic fixture.
    let events = load_real_job();
    let stats = fault::seed_search_stats(&events, 0..1000);
    assert_eq!(stats.total, 1000);
    assert!(
        stats.failing > 0,
        "expected at least one failing seed among 1000 against real multi-node data"
    );
    assert!(
        stats.failing < stats.total,
        "expected at least one clean seed too — not every fault should matter"
    );
    assert!(stats.first_failure.is_some());
}
