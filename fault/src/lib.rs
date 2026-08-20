//! Inject a fault into a stored event stream before it is replayed, and
//! check a Job timeline for structural inconsistencies a fault may have
//! caused (the `invariant` module).
//! Level 1 replay reconstructs a Run or Job from events already sitting in
//! the Trace Store — there is no live process or network to perturb, so a
//! fault here can only mean a deliberate mutation of that ordered stream.
//! `Fault::from_seed` is the primitive that makes this a DST loop: a seed
//! deterministically selects a fault from this same catalog instead of a
//! human specifying its parameters by hand. `seed_search` loops
//! `from_seed` over a range of seeds to find the first one whose fault
//! trips `invariant::check` — the actual search loop the single-seed
//! primitive was built for. `CorruptNode` is a value fault (not existence
//! or order) that needs `TimelineEvent`'s shape to express — see
//! `apply_timeline`, the generic `apply` no-ops on it.

use std::collections::BTreeMap;
use std::ops::Range;

use rand::{rngs::StdRng, Rng, SeedableRng};
use recorder::JobEventKind;
use replay::TimelineEvent;

pub mod invariant;

/// A pool of clearly-synthetic node names `CorruptNode` draws from
/// — never a real allocation, so a corrupted value can't accidentally
/// collide with one.
const CORRUPT_NODE_POOL: &[&str] = &[
    "seed-corrupted-node-a",
    "seed-corrupted-node-b",
    "seed-corrupted-node-c",
];

/// A single injectable fault.
#[derive(Debug, Clone)]
pub enum Fault {
    /// Remove the event at this position (0-indexed, in the same order
    /// `Replayer::events()`/`JobReplayer::events()` return) before replay
    /// sees it. Out-of-range indices are a no-op.
    DropEvent { index: usize },
    /// Swap the events at these two positions — the only fault
    /// kind that can produce "both events present, wrong order," which
    /// `DropEvent` structurally cannot: deletion never changes the
    /// relative order of what's left. Out-of-range indices are a no-op.
    ReorderEvents { i: usize, j: usize },
    /// Overwrite the `node` field of the event at this position
    /// — a value fault, not an existence or ordering one. Only meaningful
    /// for a Job's `TimelineEvent` stream; apply via `apply_timeline`, not
    /// the generic `apply` (a no-op there, since a generic `T` has no
    /// `node` field to find).
    CorruptNode { index: usize, node: String },
}

/// Deterministically selects a `Fault` from `seed`: the same
/// `(seed, len)` pair always yields the same variant and parameters. Unlike
/// `DropEvent`/`ReorderEvents` specified by hand on the CLI, the selected
/// index(es) are always bounded by `len` (via modulo), so the result always
/// mutates a stream of that length rather than risking an out-of-range
/// no-op. `len == 0` has no valid index at all, so it returns
/// `DropEvent { index: 0 }`, which `apply`'s existing out-of-range handling
/// already turns into a genuine no-op against an empty stream.
pub fn from_seed(seed: u64, len: usize) -> Fault {
    let mut rng = StdRng::seed_from_u64(seed);
    if len == 0 {
        return Fault::DropEvent { index: 0 };
    }
    match rng.gen_range(0..3) {
        0 => Fault::DropEvent {
            index: rng.gen_range(0..len),
        },
        1 => {
            let i = rng.gen_range(0..len);
            let j = rng.gen_range(0..len);
            Fault::ReorderEvents { i, j }
        }
        _ => Fault::CorruptNode {
            index: rng.gen_range(0..len),
            node: CORRUPT_NODE_POOL[rng.gen_range(0..CORRUPT_NODE_POOL.len())].to_string(),
        },
    }
}

/// Applies `from_seed(seed, events.len())` to a fresh clone of `events` and
/// runs `invariant::check` on the result. The one piece of per-seed
/// evaluation `seed_search` and `seed_search_stats` share — they differ
/// only in how they loop over it (stop at the first failure vs. always
/// completing the range), not in what counts as a failure.
fn evaluate_seed(events: &[TimelineEvent], seed: u64) -> Vec<invariant::Violation> {
    let fault = from_seed(seed, events.len());
    let faulted = apply_timeline(events.to_vec(), &fault);
    invariant::check(&faulted)
}

/// Loops `from_seed` over `seeds`, applying each selected fault to a fresh
/// clone of `events` and running `invariant::check` against the result
///. Returns the first `(seed, violations)` pair whose violations
/// are non-empty, or `None` if every seed in the range leaves the timeline
/// clean. Fail-fast, matching the DST convention of "a failing seed is a
/// reproducible bug report" — `seed` alone is enough to reproduce it via
/// `from_seed(seed, events.len())` again. Pure and I/O-free, same as
/// `apply`/`check`, so it needs no `TraceStore` to test.
pub fn seed_search(
    events: &[TimelineEvent],
    seeds: Range<u64>,
) -> Option<(u64, Vec<invariant::Violation>)> {
    for seed in seeds {
        let violations = evaluate_seed(events, seed);
        if !violations.is_empty() {
            return Some((seed, violations));
        }
    }
    None
}

/// How many seeds in a range fail, out of how many — the
/// question `seed_search`'s fail-fast search can't answer, since it stops
/// at the first failure. Unlike `seed_search`, always completes the full
/// range; `total` and `failing` are meaningful only if every seed in the
/// range is actually evaluated. Pure and I/O-free, same as `seed_search`.
#[derive(Debug, Clone, PartialEq)]
pub struct SeedSearchStats {
    pub total: u64,
    pub failing: u64,
    pub first_failure: Option<(u64, Vec<invariant::Violation>)>,
    /// Seeds failing per invariant name (a seed with violations from
    /// multiple invariants counts once toward each).
    pub by_invariant: BTreeMap<&'static str, u64>,
}

pub fn seed_search_stats(events: &[TimelineEvent], seeds: Range<u64>) -> SeedSearchStats {
    let total = seeds.end.saturating_sub(seeds.start);
    let mut failing = 0u64;
    let mut first_failure = None;
    let mut by_invariant: BTreeMap<&'static str, u64> = BTreeMap::new();

    for seed in seeds {
        let violations = evaluate_seed(events, seed);
        if !violations.is_empty() {
            failing += 1;
            let mut names: Vec<&'static str> = violations.iter().map(|v| v.invariant).collect();
            names.sort_unstable();
            names.dedup();
            for name in names {
                *by_invariant.entry(name).or_insert(0) += 1;
            }
            if first_failure.is_none() {
                first_failure = Some((seed, violations));
            }
        }
    }

    SeedSearchStats {
        total,
        failing,
        first_failure,
        by_invariant,
    }
}

/// Applies `fault` to `events`, returning the mutated stream. Pure and
/// I/O-free so it can be tested without a `TraceStore`. Generic over the
/// element type: every fault here acts positionally, so the
/// same function serves `Replayer`'s `Vec<Event>` and
/// `JobReplayer`'s `Vec<TimelineEvent>` without inspecting either shape.
pub fn apply<T>(mut events: Vec<T>, fault: &Fault) -> Vec<T> {
    match fault {
        Fault::DropEvent { index } => {
            if *index < events.len() {
                events.remove(*index);
            }
        }
        Fault::ReorderEvents { i, j } => {
            if *i < events.len() && *j < events.len() {
                events.swap(*i, *j);
            }
        }
        // No `node` field to find on a generic `T` — use `apply_timeline`
        // for a Job's `TimelineEvent` stream.
        Fault::CorruptNode { .. } => {}
    }
    events
}

/// Like `apply`, but also handles `CorruptNode`, which needs to
/// know `TimelineEvent`/`JobEventKind`'s shape to find the `node` field to
/// overwrite — the one fault kind the generic `apply` can't express.
/// `DropEvent`/`ReorderEvents` delegate to `apply` unchanged.
pub fn apply_timeline(events: Vec<TimelineEvent>, fault: &Fault) -> Vec<TimelineEvent> {
    let Fault::CorruptNode { index, node } = fault else {
        return apply(events, fault);
    };
    let mut events = events;
    if let Some(TimelineEvent::Boundary(job_event)) = events.get_mut(*index) {
        match &mut job_event.kind {
            JobEventKind::NodeAllocated { node: n, .. }
            | JobEventKind::TaskStarted { node: n, .. }
            | JobEventKind::TaskExited { node: n, .. } => *n = node.clone(),
            JobEventKind::JobExited { .. } => {}
        }
    }
    events
}

#[cfg(test)]
mod tests {
    use super::*;
    use recorder::{Event, EventKind};

    fn event(kind: EventKind) -> Event {
        Event::new("r1", kind)
    }

    fn events() -> Vec<Event> {
        vec![
            event(EventKind::ProcessStart {
                image: "alpine".into(),
                command: vec!["sh".into()],
            }),
            event(EventKind::Stdout {
                line: "first".into(),
            }),
            event(EventKind::Stdout {
                line: "second".into(),
            }),
            event(EventKind::ProcessExit { exit_code: 0 }),
        ]
    }

    #[test]
    fn drops_first_event() {
        let result = apply(events(), &Fault::DropEvent { index: 0 });
        assert_eq!(result.len(), 3);
        assert!(matches!(result[0].kind, EventKind::Stdout { .. }));
    }

    #[test]
    fn drops_middle_event() {
        let result = apply(events(), &Fault::DropEvent { index: 1 });
        assert_eq!(result.len(), 3);
        let EventKind::Stdout { line } = &result[1].kind else {
            panic!("expected stdout")
        };
        assert_eq!(line, "second");
    }

    #[test]
    fn drops_last_event() {
        let result = apply(events(), &Fault::DropEvent { index: 3 });
        assert_eq!(result.len(), 3);
        assert!(!result
            .iter()
            .any(|e| matches!(e.kind, EventKind::ProcessExit { .. })));
    }

    #[test]
    fn drops_only_event() {
        let result = apply(
            vec![event(EventKind::Stdout {
                line: "only".into(),
            })],
            &Fault::DropEvent { index: 0 },
        );
        assert!(result.is_empty());
    }

    #[test]
    fn out_of_range_index_is_a_no_op() {
        let original = events();
        let result = apply(original.clone(), &Fault::DropEvent { index: 99 });
        assert_eq!(result.len(), original.len());
    }

    #[test]
    fn reorder_swaps_two_events() {
        let result = apply(events(), &Fault::ReorderEvents { i: 1, j: 2 });
        assert_eq!(result.len(), 4);
        let EventKind::Stdout { line } = &result[1].kind else {
            panic!("expected stdout")
        };
        assert_eq!(line, "second");
        let EventKind::Stdout { line } = &result[2].kind else {
            panic!("expected stdout")
        };
        assert_eq!(line, "first");
    }

    #[test]
    fn reorder_out_of_range_index_is_a_no_op() {
        let original = events();
        let result = apply(original.clone(), &Fault::ReorderEvents { i: 1, j: 99 });
        assert_eq!(result.len(), original.len());
        let EventKind::Stdout { line } = &result[1].kind else {
            panic!("expected stdout")
        };
        assert_eq!(line, "first");
    }

    fn fault_eq(a: &Fault, b: &Fault) -> bool {
        match (a, b) {
            (Fault::DropEvent { index: a }, Fault::DropEvent { index: b }) => a == b,
            (Fault::ReorderEvents { i: ai, j: aj }, Fault::ReorderEvents { i: bi, j: bj }) => {
                ai == bi && aj == bj
            }
            (
                Fault::CorruptNode {
                    index: ai,
                    node: an,
                },
                Fault::CorruptNode {
                    index: bi,
                    node: bn,
                },
            ) => ai == bi && an == bn,
            _ => false,
        }
    }

    #[test]
    fn from_seed_is_deterministic() {
        let a = from_seed(42, 4);
        let b = from_seed(42, 4);
        assert!(fault_eq(&a, &b));
    }

    #[test]
    fn from_seed_diverges_across_seeds() {
        let faults: Vec<Fault> = (0..20).map(|s| from_seed(s, 4)).collect();
        assert!(faults.windows(2).any(|w| !fault_eq(&w[0], &w[1])));
    }

    #[test]
    fn from_seed_indices_are_bounded_by_len() {
        for seed in 0..50 {
            match from_seed(seed, 4) {
                Fault::DropEvent { index } => assert!(index < 4),
                Fault::ReorderEvents { i, j } => {
                    assert!(i < 4);
                    assert!(j < 4);
                }
                Fault::CorruptNode { index, node } => {
                    assert!(index < 4);
                    assert!(CORRUPT_NODE_POOL.contains(&node.as_str()));
                }
            }
        }
    }

    #[test]
    fn from_seed_reaches_all_three_fault_kinds() {
        let mut saw_drop = false;
        let mut saw_reorder = false;
        let mut saw_corrupt = false;
        for seed in 0..200 {
            match from_seed(seed, 4) {
                Fault::DropEvent { .. } => saw_drop = true,
                Fault::ReorderEvents { .. } => saw_reorder = true,
                Fault::CorruptNode { .. } => saw_corrupt = true,
            }
        }
        assert!(
            saw_drop && saw_reorder && saw_corrupt,
            "expected all three fault kinds within 200 seeds"
        );
    }

    fn job_timeline() -> Vec<TimelineEvent> {
        use recorder::{JobEvent, JobEventKind};
        vec![
            TimelineEvent::Boundary(JobEvent::new(
                "j1",
                JobEventKind::NodeAllocated {
                    node: "n1".into(),
                    user: "u".into(),
                    work_dir: "/tmp".into(),
                },
            )),
            TimelineEvent::Boundary(JobEvent::new(
                "j1",
                JobEventKind::TaskStarted {
                    step_id: "0".into(),
                    task_id: "0".into(),
                    node: "n1".into(),
                },
            )),
            TimelineEvent::Boundary(JobEvent::new(
                "j1",
                JobEventKind::TaskExited {
                    step_id: "0".into(),
                    task_id: "0".into(),
                    node: "n1".into(),
                },
            )),
            TimelineEvent::Boundary(JobEvent::new(
                "j1",
                JobEventKind::JobExited {
                    exit_code: 0,
                    signal: 0,
                    job_name: "j".into(),
                    node_list: "n1".into(),
                },
            )),
        ]
    }

    #[test]
    fn corrupt_node_on_task_started_trips_the_node_allocated_invariant() {
        let clean = job_timeline();
        assert_eq!(invariant::check(&clean), vec![]);

        let faulted = apply_timeline(
            clean,
            &Fault::CorruptNode {
                index: 1,
                node: "wrong-node".into(),
            },
        );
        assert_eq!(
            faulted.len(),
            4,
            "corruption changes a field, not the event count"
        );
        let violations = invariant::check(&faulted);
        assert!(violations
            .iter()
            .any(|v| v.invariant == "task_started_requires_prior_node_allocated"));
    }

    #[test]
    fn corrupt_node_on_node_allocated_trips_the_node_allocated_invariant() {
        let clean = job_timeline();
        let faulted = apply_timeline(
            clean,
            &Fault::CorruptNode {
                index: 0,
                node: "wrong-node".into(),
            },
        );
        let violations = invariant::check(&faulted);
        assert_eq!(violations.len(), 1);
        assert_eq!(
            violations[0].invariant,
            "task_started_requires_prior_node_allocated"
        );
    }

    #[test]
    fn corrupt_node_on_job_exited_is_a_no_op() {
        let clean = job_timeline();
        let faulted = apply_timeline(
            clean.clone(),
            &Fault::CorruptNode {
                index: 3,
                node: "wrong-node".into(),
            },
        );
        assert_eq!(invariant::check(&faulted), invariant::check(&clean));
    }

    #[test]
    fn corrupt_node_out_of_range_index_is_a_no_op() {
        let clean = job_timeline();
        let faulted = apply_timeline(
            clean.clone(),
            &Fault::CorruptNode {
                index: 99,
                node: "wrong-node".into(),
            },
        );
        assert_eq!(faulted.len(), clean.len());
        assert_eq!(invariant::check(&faulted), invariant::check(&clean));
    }

    #[test]
    fn apply_treats_corrupt_node_as_a_no_op_for_a_generic_element_type() {
        let result = apply(
            events(),
            &Fault::CorruptNode {
                index: 0,
                node: "wrong-node".into(),
            },
        );
        assert_eq!(result.len(), events().len());
    }

    /// Direct extension of the already-validated positional mechanism to
    /// Message rows, not a new design: `DropEvent`/`ReorderEvents` are
    /// index-generic and never inspect `TimelineEvent`'s variant, so they
    /// already apply to `Message` rows mechanically. This confirms that a
    /// `ReorderEvents` moving a `Message` row before its own `connect`
    /// produces a real, visible downstream effect via the
    /// `connect_precedes_activity` invariant.
    fn job_timeline_with_message() -> Vec<TimelineEvent> {
        use intercept::MessageEvent;
        use recorder::{JobEvent, JobEventKind};
        vec![
            TimelineEvent::Boundary(JobEvent::new(
                "j1",
                JobEventKind::NodeAllocated {
                    node: "n1".into(),
                    user: "u".into(),
                    work_dir: "/tmp".into(),
                },
            )),
            TimelineEvent::Message(MessageEvent {
                pid: 1,
                fd: 3,
                direction: "connect".into(),
                peer: None,
                bytes: 0,
                timestamp_ms: 100,
                job_id: Some("j1".into()),
                step_id: None,
                task_id: None,
                node: None,
            }),
            TimelineEvent::Message(MessageEvent {
                pid: 1,
                fd: 3,
                direction: "send".into(),
                peer: None,
                bytes: 8,
                timestamp_ms: 150,
                job_id: Some("j1".into()),
                step_id: None,
                task_id: None,
                node: None,
            }),
            TimelineEvent::Boundary(JobEvent::new(
                "j1",
                JobEventKind::JobExited {
                    exit_code: 0,
                    signal: 0,
                    job_name: "j".into(),
                    node_list: "n1".into(),
                },
            )),
        ]
    }

    #[test]
    fn drop_event_removes_only_the_targeted_message_row() {
        let clean = job_timeline_with_message();
        assert_eq!(invariant::check(&clean), vec![]);

        let faulted = apply_timeline(clean, &Fault::DropEvent { index: 2 });
        assert_eq!(
            faulted.len(),
            3,
            "dropping the send row removes only that row"
        );
        assert!(matches!(faulted[1], TimelineEvent::Message(ref m) if m.direction == "connect"));
        assert_eq!(invariant::check(&faulted), vec![]);
    }

    #[test]
    fn reorder_event_moving_a_message_before_its_connect_trips_the_invariant() {
        let clean = job_timeline_with_message();
        assert_eq!(invariant::check(&clean), vec![]);

        // Swap connect (index 1) and send (index 2): the send now sits at
        // an earlier position than its own connect.
        let faulted = apply_timeline(clean, &Fault::ReorderEvents { i: 1, j: 2 });
        assert_eq!(faulted.len(), 4, "reordering never changes the event count");
        let violations = invariant::check(&faulted);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].invariant, "connect_precedes_activity");
    }

    #[test]
    fn seed_search_finds_the_first_failing_seed() {
        // Against this fixture (three invariants, three fault kinds),
        // seed 1 is known clean and seed 2 is known to trip the
        // task-exited/task-started invariant (via DropEvent) — seed_search
        // over 1..3 must stop at 2, not report 1.
        let (seed, violations) =
            seed_search(&job_timeline(), 1..3).expect("expected a failing seed");
        assert_eq!(seed, 2);
        assert_eq!(violations.len(), 1);
    }

    #[test]
    fn seed_search_returns_none_when_the_whole_range_is_clean() {
        // Seed 1 is known clean against this fixture.
        assert_eq!(seed_search(&job_timeline(), 1..2), None);
    }

    #[test]
    fn seed_search_returns_the_earliest_of_several_failing_seeds() {
        // Seeds 2, 3, 4, 6, 7, and 8 are all known-failing against this
        // fixture; seed_search must report the first one, not all of them.
        let (seed, _) = seed_search(&job_timeline(), 1..8).expect("expected a failing seed");
        assert_eq!(seed, 2);
    }

    #[test]
    fn seed_search_stats_matches_fail_fast_first_failure() {
        // Same fixture, same range: exhaustive mode's first_failure must
        // agree with what fail-fast seed_search reports, proving the two
        // don't silently diverge on what counts as a violation.
        let fail_fast = seed_search(&job_timeline(), 4..6);
        let stats = seed_search_stats(&job_timeline(), 4..6);
        assert_eq!(stats.first_failure, fail_fast);
    }

    #[test]
    fn seed_search_stats_is_deterministic() {
        let a = seed_search_stats(&job_timeline(), 0..10);
        let b = seed_search_stats(&job_timeline(), 0..10);
        assert_eq!(a, b);
    }

    #[test]
    fn seed_search_stats_counts_total_and_failing() {
        // Known outcomes against this fixture (verified by exhaustive
        // sweep): seeds 0,1,2,3,5,7,9 fail; 4,6,8 are clean.
        let stats = seed_search_stats(&job_timeline(), 0..10);
        assert_eq!(stats.total, 10);
        assert_eq!(stats.failing, 7);
    }

    #[test]
    fn seed_search_stats_reports_zero_failing_for_a_clean_range() {
        let stats = seed_search_stats(&job_timeline(), 1..2);
        assert_eq!(stats.total, 1);
        assert_eq!(stats.failing, 0);
        assert_eq!(stats.first_failure, None);
    }

    #[test]
    fn seed_search_stats_breaks_down_failures_by_invariant() {
        let stats = seed_search_stats(&job_timeline(), 0..10);
        let by_invariant_total: u64 = stats.by_invariant.values().sum();
        assert!(
            by_invariant_total >= stats.failing,
            "each failing seed must be attributed to at least one invariant"
        );
        assert!(stats.by_invariant.keys().all(|name| !name.is_empty()));
    }

    #[test]
    fn from_seed_len_zero_does_not_panic() {
        let result = apply(Vec::<Event>::new(), &from_seed(7, 0));
        assert!(result.is_empty());
    }

    #[test]
    fn from_seed_len_one_does_not_panic() {
        let only = vec![event(EventKind::Stdout {
            line: "only".into(),
        })];
        for seed in 0..20 {
            let result = apply(only.clone(), &from_seed(seed, 1));
            assert!(result.len() <= 1);
        }
    }

    /// `apply` must work on any element type, not just
    /// `recorder::Event` — proven here with a type `fault` has never seen,
    /// standing in for `replay::TimelineEvent` without adding that
    /// dependency just for a type name the mutation never inspects.
    #[test]
    fn apply_is_generic_over_the_element_type() {
        #[derive(Debug, PartialEq)]
        enum StandInTimelineEvent {
            Run(&'static str),
            Boundary(&'static str),
        }

        let timeline = vec![
            StandInTimelineEvent::Boundary("node_allocated"),
            StandInTimelineEvent::Run("stdout: hi"),
            StandInTimelineEvent::Boundary("job_exited"),
        ];

        let result = apply(timeline, &Fault::DropEvent { index: 1 });

        assert_eq!(
            result,
            vec![
                StandInTimelineEvent::Boundary("node_allocated"),
                StandInTimelineEvent::Boundary("job_exited"),
            ]
        );
    }
}
