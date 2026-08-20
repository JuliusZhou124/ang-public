//! `recv_precedes_task_exited`, checked against real captured data instead
//! of the hand-built fixtures it was first written against. Unlike
//! `real_trace.rs`'s `real_job_1.eventlog.jsonl` (a plain `hostname` job,
//! no messages), `real_job_2.*` is a genuine `srun --ntasks=2 --nodes=2`
//! job on a 3-container Slurm cluster whose workload itself
//! exchanges TCP messages: task 0 (`node1`) runs `msg-receiver`, task 1
//! (`node2`) runs `msg-sender`, both `LD_PRELOAD`ed with the release-built
//! shim, both correlated by the same real `SLURM_JOB_ID`. Boundary hooks
//! (`node_allocated`/`task_started`/`task_exited`/`job_exited`) fired the
//! the same way they do for a plain `hostname` job — nothing about the
//! cluster changed here, only what the workload does.
//!
//! `real_job_2.node1.messages.jsonl`/`real_job_2.node2.messages.jsonl` are
//! each node's own independently-`flock`ed log (the shim's per-process
//! capture model), checked in separately rather than hand-merged into one
//! file — that's the actual shape two real containers wrote, and
//! `ingest_message_eventlog` doesn't care about file boundaries or line
//! order since `events_for_job` sorts by `timestamp_ms` at query time.
//! See `fixtures/README.md` for reproduction steps.

use std::path::Path;

use recorder::TraceStore;
use replay::JobReplayer;

fn fixture(name: &str) -> std::path::PathBuf {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures")).join(name)
}

fn load_real_merged_job() -> Vec<replay::TimelineEvent> {
    let mut store = TraceStore::open(Path::new(":memory:")).unwrap();
    store
        .ingest_job_eventlog(&fixture("real_job_2.eventlog.jsonl"))
        .unwrap();
    store
        .ingest_message_eventlog(&fixture("real_job_2.node1.messages.jsonl"))
        .unwrap();
    store
        .ingest_message_eventlog(&fixture("real_job_2.node2.messages.jsonl"))
        .unwrap();
    JobReplayer::new(&store, "1").events().unwrap()
}

#[test]
fn real_merged_job_ingests_boundary_and_message_rows_together() {
    let events = load_real_merged_job();
    // 2 node_allocated + 2 task_started + 2 task_exited + 1 job_exited
    // (7 boundary) + 5 recv (node1) + 1 connect + 5 send (node2, 11
    // message rows) = 18.
    assert_eq!(events.len(), 18);
}

#[test]
fn clean_real_merged_trace_has_no_violations() {
    // The real workload's every recv lands before its own task's exit and
    // every send/recv follows its own connect — `recv_precedes_task_exited`
    // and `connect_precedes_activity` both pass against genuine multi-node
    // data, not just the hand-built fixtures they were introduced with.
    let events = load_real_merged_job();
    assert_eq!(fault::invariant::check(&events), vec![]);
}

#[test]
fn dropping_the_real_task_exited_row_makes_recv_precedes_task_exited_moot_but_reordering_trips_it()
{
    // Confirms recv_precedes_task_exited is genuinely order-sensitive
    // against real data, not just the synthetic fixtures in
    // fault/src/invariant.rs's own unit tests: swapping node1's
    // task_exited to before its last real recv reproduces the exact
    // violation shape on captured, not hand-assembled, rows.
    let events = load_real_merged_job();
    let task_exited_pos = events
        .iter()
        .position(|e| matches!(e, replay::TimelineEvent::Boundary(je) if matches!(je.kind, recorder::JobEventKind::TaskExited { ref node, .. } if node == "node1")))
        .expect("node1's task_exited must be present");
    let last_node1_recv_pos = events
        .iter()
        .rposition(|e| matches!(e, replay::TimelineEvent::Message(m) if m.direction == "recv" && m.node.as_deref() == Some("node1")))
        .expect("node1 must have at least one recv");
    assert!(
        task_exited_pos > last_node1_recv_pos,
        "fixture must start clean for this swap to be meaningful"
    );

    let reordered = fault::apply(
        events,
        &fault::Fault::ReorderEvents {
            i: last_node1_recv_pos,
            j: task_exited_pos,
        },
    );
    let violations = fault::invariant::check(&reordered);
    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].invariant, "recv_precedes_task_exited");
    assert!(violations[0].description.contains("node=node1"));
}
