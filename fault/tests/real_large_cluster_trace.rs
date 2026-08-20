//! Same shape as `real_trace.rs`/`real_merged_trace.rs`, against a larger
//! real cluster instead of the 2-node one those reuse: 8 worker nodes
//! (`cluster/`), one `srun --ntasks=8 --nodes=8`
//! job forming four independent sender/receiver pairs, each exchanging 20
//! messages — 25 boundary rows + 164 message rows = 189 events, versus the
//! 2-node trace's 7 boundary + 11 message = 18. Exists to answer a
//! question `real_merged_trace.rs`'s small fixture couldn't: does the real
//! per-seed failure rate and per-invariant breakdown hold up, and how does
//! it shift, against a genuinely bigger real multi-node trace with all five
//! registered invariants — including the two message-aware ones —
//! actually reachable at once? See `fixtures/README.md`
//! for provenance and reproduction steps.

use std::path::Path;

use recorder::TraceStore;
use replay::JobReplayer;

fn fixture(name: &str) -> std::path::PathBuf {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures")).join(name)
}

fn load_real_large_job() -> Vec<replay::TimelineEvent> {
    let mut store = TraceStore::open(Path::new(":memory:")).unwrap();
    store
        .ingest_job_eventlog(&fixture("real_job_3.eventlog.jsonl"))
        .unwrap();
    for node in 1..=8 {
        store
            .ingest_message_eventlog(&fixture(&format!("real_job_3.node{node}.messages.jsonl")))
            .unwrap();
    }
    JobReplayer::new(&store, "1").events().unwrap()
}

#[test]
fn real_large_cluster_trace_ingests_as_189_correlated_events() {
    let events = load_real_large_job();
    // 8 node_allocated + 8 task_started + 8 task_exited + 1 job_exited
    // (25 boundary) + 4 connect + 80 send + 80 recv (164 message) = 189.
    assert_eq!(events.len(), 189);
}

#[test]
fn clean_real_large_cluster_trace_has_no_violations() {
    let events = load_real_large_job();
    assert_eq!(fault::invariant::check(&events), vec![]);
}

#[test]
fn seed_search_stats_against_the_large_trace_is_deterministic() {
    let events = load_real_large_job();
    let a = fault::seed_search_stats(&events, 0..1000);
    let b = fault::seed_search_stats(&events, 0..1000);
    assert_eq!(a, b);
}

#[test]
fn seed_search_stats_against_the_large_trace_reaches_all_five_invariants() {
    // Unlike the 2-node trace (real_trace.rs, 3 reachable invariants;
    // real_merged_trace.rs's 18-event trace has too few message
    // rows to reliably surface both message-aware invariants across only
    // 1000 seeds), 189 events and four independent send/recv pairs give
    // from_seed's single mutated index enough surface to trip every
    // registered check at least once in 1000 seeds.
    let events = load_real_large_job();
    let stats = fault::seed_search_stats(&events, 0..1000);
    assert_eq!(stats.total, 1000);
    assert!(stats.failing > 0);
    assert!(stats.failing < stats.total);
    for name in [
        "task_exited_requires_prior_task_started",
        "job_exited_is_last",
        "task_started_requires_prior_node_allocated",
        "connect_precedes_activity",
        "recv_precedes_task_exited",
    ] {
        assert!(
            stats.by_invariant.get(name).copied().unwrap_or(0) > 0,
            "expected {name} to fire at least once against the large real trace"
        );
    }
}
