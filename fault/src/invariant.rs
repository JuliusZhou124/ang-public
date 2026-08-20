//! Checks a Job's decoded (and possibly fault-mutated) timeline for
//! structural inconsistencies. A separate pass from replay
//! itself — `drain_timeline_events` has no notion of whether the stream it
//! prints is internally consistent, only what's present in it.
//!
//! Invariants are a registry of independent, pure functions (each
//! `&[TimelineEvent] -> Vec<Violation>`) rather than cases hand-folded into
//! one scan. With one invariant either shape would have looked the same;
//! a second invariant (`job_exited_is_last`) is the point at which it
//! stopped being obvious, so the registry shape is a deliberate choice,
//! not an accretion. `check` runs every
//! registered invariant and concatenates their violations.

use std::collections::HashMap;
use std::collections::HashSet;

use recorder::JobEventKind;
use replay::TimelineEvent;

/// One structural inconsistency found in a timeline.
#[derive(Debug, Clone, PartialEq)]
pub struct Violation {
    /// Name of the invariant that produced this violation, matching its
    /// name in `CHECKS`.
    pub invariant: &'static str,
    pub description: String,
}

type InvariantCheck = fn(&[TimelineEvent]) -> Vec<Violation>;

const CHECKS: &[InvariantCheck] = &[
    task_exited_requires_prior_task_started,
    job_exited_is_last,
    task_started_requires_prior_node_allocated,
    connect_precedes_activity,
    recv_precedes_task_exited,
];

/// Runs every registered invariant against `events` and concatenates their
/// violations. Pure and I/O-free, run over a timeline independently of
/// whether it's ever drained to a stream.
pub fn check(events: &[TimelineEvent]) -> Vec<Violation> {
    CHECKS.iter().flat_map(|check| check(events)).collect()
}

/// Every `TaskExited` must have an earlier `TaskStarted` for the
/// same `(step_id, task_id, node)`. Returns one `Violation` per
/// `TaskExited` that has none.
fn task_exited_requires_prior_task_started(events: &[TimelineEvent]) -> Vec<Violation> {
    let mut started: HashSet<(String, String, String)> = HashSet::new();
    let mut violations = Vec::new();

    for event in events {
        let TimelineEvent::Boundary(job_event) = event else {
            continue;
        };
        match &job_event.kind {
            JobEventKind::TaskStarted {
                step_id,
                task_id,
                node,
            } => {
                started.insert((step_id.clone(), task_id.clone(), node.clone()));
            }
            JobEventKind::TaskExited {
                step_id,
                task_id,
                node,
            } => {
                let key = (step_id.clone(), task_id.clone(), node.clone());
                if !started.contains(&key) {
                    violations.push(Violation {
                        invariant: "task_exited_requires_prior_task_started",
                        description: format!(
                            "task_exited step_id={step_id} task_id={task_id} node={node} with no prior task_started"
                        ),
                    });
                }
            }
            JobEventKind::NodeAllocated { .. } | JobEventKind::JobExited { .. } => {}
        }
    }

    violations
}

/// `JobExited`, if present, must be the last event in the timeline — a
/// reorder that moves it earlier now has a downstream effect it didn't
/// have when `ReorderEvents` had no invariant able to catch it.
/// Reports at most one violation: the position of the first `JobExited`
/// found, if anything follows it.
fn job_exited_is_last(events: &[TimelineEvent]) -> Vec<Violation> {
    let is_job_exited = |event: &TimelineEvent| {
        matches!(
            event,
            TimelineEvent::Boundary(job_event) if matches!(job_event.kind, JobEventKind::JobExited { .. })
        )
    };
    let Some(pos) = events.iter().position(is_job_exited) else {
        return Vec::new();
    };
    if pos + 1 == events.len() {
        return Vec::new();
    }
    vec![Violation {
        invariant: "job_exited_is_last",
        description: format!(
            "job_exited at position {pos} is not the last event ({} more event(s) follow)",
            events.len() - pos - 1
        ),
    }]
}

/// Every `TaskStarted` must have an earlier `NodeAllocated` for the same
/// `node` — same shape as `task_exited_requires_prior_task_started`
/// (sequential scan, `HashSet` built up as allocated nodes are seen), so a
/// task cannot be reported as started on a node the timeline never
/// allocated it on, or allocated only after the fact. Validated against
/// the real 2-node cluster fixture (`fault/tests/real_trace.rs`), which
/// this invariant was added specifically to give more surface to.
fn task_started_requires_prior_node_allocated(events: &[TimelineEvent]) -> Vec<Violation> {
    let mut allocated: HashSet<String> = HashSet::new();
    let mut violations = Vec::new();

    for event in events {
        let TimelineEvent::Boundary(job_event) = event else {
            continue;
        };
        match &job_event.kind {
            JobEventKind::NodeAllocated { node, .. } => {
                allocated.insert(node.clone());
            }
            JobEventKind::TaskStarted {
                step_id,
                task_id,
                node,
            } => {
                if !allocated.contains(node) {
                    violations.push(Violation {
                        invariant: "task_started_requires_prior_node_allocated",
                        description: format!(
                            "task_started step_id={step_id} task_id={task_id} node={node} with no prior node_allocated"
                        ),
                    });
                }
            }
            JobEventKind::TaskExited { .. } | JobEventKind::JobExited { .. } => {}
        }
    }

    violations
}

/// Every `send`/`recv` `Message` row must not appear earlier in
/// the timeline than its own `(pid, fd)`'s `connect` row, if one was
/// logged for that pair. Checked by array *position*, not raw
/// `timestamp_ms` comparison: `events_for_job` already returns rows in
/// timestamp order, so "activity before its own connect"
/// always shows up as a position inversion, never a same-position
/// timestamp tie a naive `<` comparison would miss. A violation here means
/// either a process outliving its own socket lifecycle or a clock/ordering
/// problem in the captured message log. Scoped to `(pid, fd)` rather than
/// `pid` alone so one process's several sockets don't cross-contaminate
/// each other's checks.
fn connect_precedes_activity(events: &[TimelineEvent]) -> Vec<Violation> {
    let mut first_connect_index: HashMap<(u32, i32), usize> = HashMap::new();
    for (i, event) in events.iter().enumerate() {
        let TimelineEvent::Message(msg) = event else {
            continue;
        };
        if msg.direction == "connect" {
            first_connect_index.entry((msg.pid, msg.fd)).or_insert(i);
        }
    }

    let mut violations = Vec::new();
    for (i, event) in events.iter().enumerate() {
        let TimelineEvent::Message(msg) = event else {
            continue;
        };
        if msg.direction != "send" && msg.direction != "recv" {
            continue;
        }
        let key = (msg.pid, msg.fd);
        if let Some(&connect_i) = first_connect_index.get(&key) {
            if i < connect_i {
                violations.push(Violation {
                    invariant: "connect_precedes_activity",
                    description: format!(
                        "pid={} fd={} {} at position {i} (timestamp_ms={}) precedes its own connect at position {connect_i}",
                        msg.pid, msg.fd, msg.direction, msg.timestamp_ms
                    ),
                });
            }
        }
    }

    violations
}

/// Every `recv` `Message` row attributed to a `(step_id, task_id, node)`
/// must not appear later in the timeline than that task's `TaskExited`
/// Boundary row, if one exists. Position-based, same rationale as
/// `connect_precedes_activity`:
/// `events_for_job` already returns timestamp-sorted rows, so "after its
/// own task exited" always shows up as a position inversion). `Message`
/// rows with no attribution (`step_id`/`task_id`/`node` all absent — a
/// shim running with no Slurm task context) are skipped, not flagged, the
/// same way `connect_precedes_activity` only checks `(pid, fd)` pairs that
/// actually logged a `connect`.
fn recv_precedes_task_exited(events: &[TimelineEvent]) -> Vec<Violation> {
    let mut task_exited_index: HashMap<(String, String, String), usize> = HashMap::new();
    for (i, event) in events.iter().enumerate() {
        let TimelineEvent::Boundary(job_event) = event else {
            continue;
        };
        if let JobEventKind::TaskExited {
            step_id,
            task_id,
            node,
        } = &job_event.kind
        {
            task_exited_index
                .entry((step_id.clone(), task_id.clone(), node.clone()))
                .or_insert(i);
        }
    }

    let mut violations = Vec::new();
    for (i, event) in events.iter().enumerate() {
        let TimelineEvent::Message(msg) = event else {
            continue;
        };
        if msg.direction != "recv" {
            continue;
        }
        let (Some(step_id), Some(task_id), Some(node)) = (&msg.step_id, &msg.task_id, &msg.node)
        else {
            continue;
        };
        let key = (step_id.clone(), task_id.clone(), node.clone());
        if let Some(&exited_i) = task_exited_index.get(&key) {
            if i > exited_i {
                violations.push(Violation {
                    invariant: "recv_precedes_task_exited",
                    description: format!(
                        "step_id={step_id} task_id={task_id} node={node} recv at position {i} (pid={}, fd={}) follows its own task_exited at position {exited_i}",
                        msg.pid, msg.fd
                    ),
                });
            }
        }
    }

    violations
}

#[cfg(test)]
mod tests {
    use super::*;
    use intercept::MessageEvent;
    use recorder::JobEvent;

    fn boundary(kind: JobEventKind) -> TimelineEvent {
        TimelineEvent::Boundary(JobEvent::new("j1", kind))
    }

    fn message(pid: u32, fd: i32, direction: &str, timestamp_ms: u128) -> TimelineEvent {
        TimelineEvent::Message(MessageEvent {
            pid,
            fd,
            direction: direction.to_string(),
            peer: None,
            bytes: 8,
            timestamp_ms,
            job_id: Some("j1".to_string()),
            step_id: None,
            task_id: None,
            node: None,
        })
    }

    fn attributed_message(
        pid: u32,
        fd: i32,
        direction: &str,
        timestamp_ms: u128,
        step_id: &str,
        task_id: &str,
        node: &str,
    ) -> TimelineEvent {
        TimelineEvent::Message(MessageEvent {
            pid,
            fd,
            direction: direction.to_string(),
            peer: None,
            bytes: 8,
            timestamp_ms,
            job_id: Some("j1".to_string()),
            step_id: Some(step_id.to_string()),
            task_id: Some(task_id.to_string()),
            node: Some(node.to_string()),
        })
    }

    #[test]
    fn clean_timeline_has_no_violations() {
        let events = vec![
            boundary(JobEventKind::NodeAllocated {
                node: "n1".into(),
                user: "u".into(),
                work_dir: "/tmp".into(),
            }),
            boundary(JobEventKind::TaskStarted {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
            boundary(JobEventKind::TaskExited {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
        ];
        assert_eq!(check(&events), vec![]);
    }

    #[test]
    fn missing_task_started_is_a_violation() {
        let events = vec![boundary(JobEventKind::TaskExited {
            step_id: "0".into(),
            task_id: "0".into(),
            node: "n1".into(),
        })];
        let violations = check(&events);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].description.contains("step_id=0"));
        assert!(violations[0].description.contains("task_id=0"));
        assert!(violations[0].description.contains("node=n1"));
    }

    #[test]
    fn only_the_broken_task_is_flagged_among_several() {
        let events = vec![
            boundary(JobEventKind::NodeAllocated {
                node: "n1".into(),
                user: "u".into(),
                work_dir: "/tmp".into(),
            }),
            boundary(JobEventKind::TaskStarted {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
            boundary(JobEventKind::TaskExited {
                step_id: "0".into(),
                task_id: "1".into(),
                node: "n2".into(),
            }),
            boundary(JobEventKind::TaskExited {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
        ];
        let violations = check(&events);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].description.contains("task_id=1"));
    }

    #[test]
    fn dropping_task_started_via_fault_makes_the_violation_appear() {
        // Ties the DropEvent fault to the invariant: a fault that removes
        // a task_started row has a visible, reported downstream effect.
        let clean = vec![
            boundary(JobEventKind::NodeAllocated {
                node: "n1".into(),
                user: "u".into(),
                work_dir: "/tmp".into(),
            }),
            boundary(JobEventKind::TaskStarted {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
            boundary(JobEventKind::TaskExited {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
        ];
        assert_eq!(check(&clean), vec![]);

        let faulted = crate::apply(clean, &crate::Fault::DropEvent { index: 1 });
        assert_eq!(check(&faulted).len(), 1);
    }

    #[test]
    fn reordering_task_started_after_task_exited_makes_the_violation_appear() {
        // Unlike DropEvent, ReorderEvents leaves both events present —
        // only their relative order changes. check() is unmodified by the
        // addition of this fault kind; this proves it was already
        // order-sensitive (a sequential scan, not a set-membership test).
        let clean = vec![
            boundary(JobEventKind::NodeAllocated {
                node: "n1".into(),
                user: "u".into(),
                work_dir: "/tmp".into(),
            }),
            boundary(JobEventKind::TaskStarted {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
            boundary(JobEventKind::TaskExited {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
        ];
        assert_eq!(check(&clean), vec![]);

        let reordered = crate::apply(clean, &crate::Fault::ReorderEvents { i: 1, j: 2 });
        assert_eq!(
            reordered.len(),
            3,
            "all events must still be present, only reordered"
        );
        let violations = check(&reordered);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].description.contains("task_id=0"));
    }

    #[test]
    fn seeded_faults_reach_both_a_violation_and_a_clean_outcome() {
        // Fault::from_seed composes with check() exactly like the
        // manually-specified faults above — no new code path in check()
        // itself, only a new way to pick a Fault to feed it.
        let clean = vec![
            boundary(JobEventKind::NodeAllocated {
                node: "n1".into(),
                user: "u".into(),
                work_dir: "/tmp".into(),
            }),
            boundary(JobEventKind::TaskStarted {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
            boundary(JobEventKind::TaskExited {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
        ];

        let mut saw_violation = false;
        let mut saw_clean = false;
        for seed in 0..50 {
            let faulted = crate::apply(clean.clone(), &crate::from_seed(seed, clean.len()));
            if check(&faulted).is_empty() {
                saw_clean = true;
            } else {
                saw_violation = true;
            }
        }
        assert!(
            saw_violation,
            "expected at least one seed to trip the invariant"
        );
        assert!(
            saw_clean,
            "expected at least one seed to leave the timeline clean"
        );
    }

    #[test]
    fn reordering_unrelated_events_produces_no_violation() {
        // Two independent, already-allocated tasks on two different nodes:
        // swapping their TaskStarted events touches none of the three
        // registered invariants, since each still has its own
        // NodeAllocated before it and its own TaskExited after it.
        let events = vec![
            boundary(JobEventKind::NodeAllocated {
                node: "n1".into(),
                user: "u".into(),
                work_dir: "/tmp".into(),
            }),
            boundary(JobEventKind::NodeAllocated {
                node: "n2".into(),
                user: "u".into(),
                work_dir: "/tmp".into(),
            }),
            boundary(JobEventKind::TaskStarted {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
            boundary(JobEventKind::TaskStarted {
                step_id: "0".into(),
                task_id: "1".into(),
                node: "n2".into(),
            }),
            boundary(JobEventKind::TaskExited {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
            boundary(JobEventKind::TaskExited {
                step_id: "0".into(),
                task_id: "1".into(),
                node: "n2".into(),
            }),
        ];
        let reordered = crate::apply(events, &crate::Fault::ReorderEvents { i: 2, j: 3 });
        assert_eq!(check(&reordered), vec![]);
    }

    #[test]
    fn clean_timeline_with_job_exited_last_has_no_violations() {
        let events = vec![
            boundary(JobEventKind::NodeAllocated {
                node: "n1".into(),
                user: "u".into(),
                work_dir: "/tmp".into(),
            }),
            boundary(JobEventKind::TaskStarted {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
            boundary(JobEventKind::TaskExited {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
            boundary(JobEventKind::JobExited {
                exit_code: 0,
                signal: 0,
                job_name: "j".into(),
                node_list: "n1".into(),
            }),
        ];
        assert_eq!(check(&events), vec![]);
    }

    #[test]
    fn reordering_job_exited_before_the_last_event_is_a_violation() {
        let clean = vec![
            boundary(JobEventKind::NodeAllocated {
                node: "n1".into(),
                user: "u".into(),
                work_dir: "/tmp".into(),
            }),
            boundary(JobEventKind::TaskStarted {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
            boundary(JobEventKind::JobExited {
                exit_code: 0,
                signal: 0,
                job_name: "j".into(),
                node_list: "n1".into(),
            }),
            boundary(JobEventKind::TaskExited {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
        ];
        // clean here means "no violation from this invariant in isolation";
        // JobExited is already not last, so this fixture starts faulted.
        let violations = check(&clean);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].description.contains("job_exited"));

        let reordered = crate::apply(clean, &crate::Fault::ReorderEvents { i: 2, j: 3 });
        assert_eq!(
            check(&reordered),
            vec![],
            "swap puts job_exited back at the end"
        );
    }

    #[test]
    fn missing_node_allocated_is_a_violation() {
        let events = vec![boundary(JobEventKind::TaskStarted {
            step_id: "0".into(),
            task_id: "0".into(),
            node: "n1".into(),
        })];
        let violations = task_started_requires_prior_node_allocated(&events);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].description.contains("node=n1"));
    }

    #[test]
    fn node_allocated_for_a_different_node_does_not_satisfy_the_invariant() {
        let events = vec![
            boundary(JobEventKind::NodeAllocated {
                node: "n2".into(),
                user: "u".into(),
                work_dir: "/tmp".into(),
            }),
            boundary(JobEventKind::TaskStarted {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
        ];
        assert_eq!(task_started_requires_prior_node_allocated(&events).len(), 1);
    }

    #[test]
    fn reordering_node_allocated_after_task_started_makes_the_violation_appear() {
        let clean = vec![
            boundary(JobEventKind::NodeAllocated {
                node: "n1".into(),
                user: "u".into(),
                work_dir: "/tmp".into(),
            }),
            boundary(JobEventKind::TaskStarted {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
        ];
        assert_eq!(check(&clean), vec![]);

        let reordered = crate::apply(clean, &crate::Fault::ReorderEvents { i: 0, j: 1 });
        let violations = check(&reordered);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].description.contains("task_started"));
    }

    #[test]
    fn timeline_with_no_job_exited_is_not_flagged() {
        let events = vec![boundary(JobEventKind::TaskStarted {
            step_id: "0".into(),
            task_id: "0".into(),
            node: "n1".into(),
        })];
        assert_eq!(job_exited_is_last(&events), vec![]);
    }

    /// Confirms rather than assumes that
    /// `job_exited_is_last` already generalizes to `Message` rows with zero
    /// code changes — a `Message` row after `JobExited` is still "something
    /// following the last event," regardless of its kind.
    #[test]
    fn message_after_job_exited_is_already_caught_by_job_exited_is_last() {
        let events = vec![
            boundary(JobEventKind::NodeAllocated {
                node: "n1".into(),
                user: "u".into(),
                work_dir: "/tmp".into(),
            }),
            boundary(JobEventKind::JobExited {
                exit_code: 0,
                signal: 0,
                job_name: "j".into(),
                node_list: "n1".into(),
            }),
            message(1, 3, "send", 500),
        ];
        let violations = job_exited_is_last(&events);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].description.contains("job_exited"));
    }

    #[test]
    fn clean_message_sequence_has_no_connect_violation() {
        let events = vec![
            message(1, 3, "connect", 100),
            message(1, 3, "send", 150),
            message(2, 4, "recv", 150),
        ];
        assert_eq!(connect_precedes_activity(&events), vec![]);
    }

    #[test]
    fn send_before_its_own_connect_is_a_violation() {
        let events = vec![message(1, 3, "send", 50), message(1, 3, "connect", 100)];
        let violations = connect_precedes_activity(&events);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].description.contains("pid=1"));
        assert!(violations[0].description.contains("fd=3"));
    }

    #[test]
    fn unrelated_fd_on_the_same_pid_is_not_cross_contaminated() {
        // Two sockets in one process: fd 3's connect must not satisfy fd
        // 4's activity, and vice versa.
        let events = vec![
            message(1, 3, "connect", 100),
            message(1, 4, "send", 50), // fd 4 has no connect logged at all
        ];
        assert_eq!(connect_precedes_activity(&events), vec![]);

        let events = vec![
            message(1, 3, "connect", 200),
            message(1, 4, "connect", 50),
            message(1, 4, "send", 60),
        ];
        assert_eq!(connect_precedes_activity(&events), vec![]);
    }

    #[test]
    fn recv_before_task_exited_has_no_violation() {
        let events = vec![
            boundary(JobEventKind::NodeAllocated {
                node: "n1".into(),
                user: "u".into(),
                work_dir: "/tmp".into(),
            }),
            boundary(JobEventKind::TaskStarted {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
            attributed_message(1, 3, "recv", 130, "0", "0", "n1"),
            boundary(JobEventKind::TaskExited {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
        ];
        assert_eq!(recv_precedes_task_exited(&events), vec![]);
    }

    #[test]
    fn recv_after_its_own_task_exited_is_a_violation() {
        let events = vec![
            boundary(JobEventKind::NodeAllocated {
                node: "n1".into(),
                user: "u".into(),
                work_dir: "/tmp".into(),
            }),
            boundary(JobEventKind::TaskStarted {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
            boundary(JobEventKind::TaskExited {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
            attributed_message(1, 3, "recv", 200, "0", "0", "n1"),
        ];
        let violations = recv_precedes_task_exited(&events);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].description.contains("step_id=0"));
        assert!(violations[0].description.contains("task_id=0"));
        assert!(violations[0].description.contains("node=n1"));
    }

    #[test]
    fn unattributed_recv_is_skipped_not_flagged() {
        // A Message row with no step_id/task_id/node (e.g. captured by an
        // older shim, or a shim run outside a Slurm task)
        // must not be flagged just because *some* task_exited exists.
        let events = vec![
            boundary(JobEventKind::TaskExited {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
            message(1, 3, "recv", 200),
        ];
        assert_eq!(recv_precedes_task_exited(&events), vec![]);
    }

    #[test]
    fn recv_after_an_unrelated_tasks_exit_is_not_cross_contaminated() {
        // task_id=1's recv arrives after task_id=0's exit but before its
        // own — must not be flagged by task_id=0's unrelated TaskExited.
        let events = vec![
            boundary(JobEventKind::TaskExited {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
            attributed_message(2, 5, "recv", 200, "0", "1", "n2"),
            boundary(JobEventKind::TaskExited {
                step_id: "0".into(),
                task_id: "1".into(),
                node: "n2".into(),
            }),
        ];
        assert_eq!(recv_precedes_task_exited(&events), vec![]);
    }

    #[test]
    fn message_rows_do_not_trip_boundary_invariants() {
        // A merged timeline with Message rows interleaved among clean
        // Boundary rows produces no false positives from the three
        // Boundary-only invariants.
        let events = vec![
            boundary(JobEventKind::NodeAllocated {
                node: "n1".into(),
                user: "u".into(),
                work_dir: "/tmp".into(),
            }),
            boundary(JobEventKind::TaskStarted {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
            message(1, 3, "connect", 120),
            message(1, 3, "send", 130),
            boundary(JobEventKind::TaskExited {
                step_id: "0".into(),
                task_id: "0".into(),
                node: "n1".into(),
            }),
            boundary(JobEventKind::JobExited {
                exit_code: 0,
                signal: 0,
                job_name: "j".into(),
                node_list: "n1".into(),
            }),
        ];
        assert_eq!(check(&events), vec![]);
    }
}
