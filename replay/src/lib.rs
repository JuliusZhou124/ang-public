//! Replay a Run or a Job's merged timeline from the
//! Trace Store.
//!
//! Reconstructs observable output — stdout, stderr, exit code — from
//! stored events, with no container re-execution.

use intercept::MessageEvent;
use recorder::{Event, EventKind, JobEvent, JobEventKind, TraceStore};

/// Reads a Run's ordered `Event` stream back out of a `TraceStore`.
pub struct Replayer<'a> {
    store: &'a TraceStore,
    run_id: String,
}

impl<'a> Replayer<'a> {
    pub fn new(store: &'a TraceStore, run_id: impl Into<String>) -> Self {
        Self {
            store,
            run_id: run_id.into(),
        }
    }

    /// The Run's events, decoded from the Trace Store's stored payloads, in
    /// the order `events_for_run` returns them (timestamp order).
    pub fn events(&self) -> anyhow::Result<Vec<Event>> {
        self.store
            .events_for_run(&self.run_id)?
            .iter()
            .map(|stored| Ok(serde_json::from_str(&stored.payload)?))
            .collect()
    }

    /// Drains the Run's events, writing `Stdout`/`Stderr` lines to `out`/
    /// `err` in recorded order. Returns the recorded exit code, or `None`
    /// if the Run's EventLog has no `ProcessExit` event.
    pub fn replay(
        &self,
        out: &mut dyn std::io::Write,
        err: &mut dyn std::io::Write,
    ) -> anyhow::Result<Option<i32>> {
        drain_events(self.events()?, out, err)
    }
}

/// Drains an already-decoded `Event` stream, writing `Stdout`/`Stderr` lines
/// to `out`/`err` in the stream's order. Returns the last `ProcessExit`
/// code seen, or `None` if the stream has no `ProcessExit` event — the same
/// rule `Replayer::replay` uses, exposed so a caller can replay a stream
/// that's been mutated (e.g. by `fault::apply`) rather than read
/// straight from the Trace Store.
pub fn drain_events(
    events: Vec<Event>,
    out: &mut dyn std::io::Write,
    err: &mut dyn std::io::Write,
) -> anyhow::Result<Option<i32>> {
    let mut exit_code = None;
    for event in events {
        match event.kind {
            EventKind::Stdout { line } => writeln!(out, "{line}")?,
            EventKind::Stderr { line } => writeln!(err, "{line}")?,
            EventKind::ProcessExit { exit_code: code } => exit_code = Some(code),
            EventKind::ProcessStart { .. } => {}
        }
    }
    Ok(exit_code)
}

/// One row of a Job's merged timeline (`events_for_job`): a Run event, a
/// Job Boundary event, or a captured cross-node `MessageEvent`.
/// Discriminated by `kind` first — a `message_`-prefixed kind
/// decodes as `Message`, since `MessageEvent` has no `"kind"`-tagged enum
/// shape for `serde_json` to try; anything else falls back to the original
/// `run_id` nullability split (`Some` means a Run row, `None` a Boundary
/// row).
#[derive(Debug, Clone)]
pub enum TimelineEvent {
    Run(Event),
    Boundary(JobEvent),
    Message(MessageEvent),
}

/// Reads a Job's merged Boundary+Run timeline back out of a `TraceStore`.
pub struct JobReplayer<'a> {
    store: &'a TraceStore,
    job_id: String,
}

impl<'a> JobReplayer<'a> {
    pub fn new(store: &'a TraceStore, job_id: impl Into<String>) -> Self {
        Self {
            store,
            job_id: job_id.into(),
        }
    }

    /// The Job's merged timeline, decoded from the Trace Store's stored
    /// payloads, in the order `events_for_job` returns them.
    pub fn events(&self) -> anyhow::Result<Vec<TimelineEvent>> {
        self.store
            .events_for_job(&self.job_id)?
            .iter()
            .map(|stored| {
                if stored.kind.starts_with("message_") {
                    Ok(TimelineEvent::Message(serde_json::from_str(
                        &stored.payload,
                    )?))
                } else if stored.run_id.is_some() {
                    Ok(TimelineEvent::Run(serde_json::from_str(&stored.payload)?))
                } else {
                    Ok(TimelineEvent::Boundary(serde_json::from_str(
                        &stored.payload,
                    )?))
                }
            })
            .collect()
    }

    /// Drains the Job's merged timeline. Run events replay exactly as
    /// `Replayer` does (`Stdout`/`Stderr` lines to their streams). Boundary
    /// events have no stdout/stderr shape, so they replay as a one-line
    /// `[job]` status annotation on `err` — visible alongside Run output
    /// without being mistaken for it. Returns the Job's own exit code from
    /// its `JobExited` event, not any single Run's `ProcessExit` — a Job
    /// can have multiple Runs, so only the Job-level event is authoritative
    /// for how the Job as a whole ended.
    pub fn replay(
        &self,
        out: &mut dyn std::io::Write,
        err: &mut dyn std::io::Write,
    ) -> anyhow::Result<Option<i32>> {
        drain_timeline_events(self.events()?, out, err)
    }
}

/// Drains an already-decoded Job timeline, writing `Stdout`/`Stderr` lines
/// to `out`/`err` and Boundary events as `[job] ...` annotations to `err`,
/// in the stream's order. Returns the `JobExited` exit code, or `None` if
/// the stream has no `JobExited` event — the same rule
/// `JobReplayer::replay` uses, exposed so a caller can replay a timeline
/// that's been mutated (e.g. by `fault::apply`) rather than read
/// straight from the Trace Store.
pub fn drain_timeline_events(
    events: Vec<TimelineEvent>,
    out: &mut dyn std::io::Write,
    err: &mut dyn std::io::Write,
) -> anyhow::Result<Option<i32>> {
    let mut exit_code = None;
    for event in events {
        match event {
            TimelineEvent::Run(event) => match event.kind {
                EventKind::Stdout { line } => writeln!(out, "{line}")?,
                EventKind::Stderr { line } => writeln!(err, "{line}")?,
                EventKind::ProcessExit { .. } | EventKind::ProcessStart { .. } => {}
            },
            TimelineEvent::Boundary(job_event) => match &job_event.kind {
                JobEventKind::NodeAllocated {
                    node,
                    user,
                    work_dir,
                } => writeln!(
                    err,
                    "[job] node_allocated node={node} user={user} work_dir={work_dir}"
                )?,
                JobEventKind::TaskStarted {
                    step_id,
                    task_id,
                    node,
                } => writeln!(
                    err,
                    "[job] task_started step_id={step_id} task_id={task_id} node={node}"
                )?,
                JobEventKind::TaskExited {
                    step_id,
                    task_id,
                    node,
                } => writeln!(
                    err,
                    "[job] task_exited step_id={step_id} task_id={task_id} node={node}"
                )?,
                JobEventKind::JobExited {
                    exit_code: code,
                    signal,
                    job_name,
                    node_list,
                } => {
                    writeln!(
                        err,
                        "[job] job_exited exit_code={code} signal={signal} job_name={job_name} node_list={node_list}"
                    )?;
                    exit_code = Some(*code);
                }
            },
            TimelineEvent::Message(msg) => {
                let peer = msg.peer.as_deref().unwrap_or("?");
                writeln!(
                    err,
                    "[msg] {} peer={peer} bytes={}",
                    msg.direction, msg.bytes
                )?;
            }
        }
    }
    Ok(exit_code)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn write_fixture(name: &str, lines: &[&str]) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "ang-replay-test-{}-{}-{name}.jsonl",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut file = std::fs::File::create(&path).unwrap();
        use std::io::Write as _;
        for line in lines {
            writeln!(file, "{line}").unwrap();
        }
        path
    }

    #[test]
    fn replays_stdout_stderr_and_exit_code_in_recorded_order() {
        let path = write_fixture(
            "run",
            &[
                r#"{"run_id":"r1","timestamp_ms":100,"kind":"process_start","image":"alpine","command":["sh"]}"#,
                r#"{"run_id":"r1","timestamp_ms":200,"kind":"stdout","line":"out-line"}"#,
                r#"{"run_id":"r1","timestamp_ms":300,"kind":"stderr","line":"err-line"}"#,
                r#"{"run_id":"r1","timestamp_ms":400,"kind":"process_exit","exit_code":7}"#,
            ],
        );
        let mut store = TraceStore::open(Path::new(":memory:")).unwrap();
        store.ingest_run_eventlog(&path).unwrap();

        let replayer = Replayer::new(&store, "r1");
        let mut out = Vec::new();
        let mut err = Vec::new();
        let exit_code = replayer.replay(&mut out, &mut err).unwrap();

        assert_eq!(String::from_utf8(out).unwrap(), "out-line\n");
        assert_eq!(String::from_utf8(err).unwrap(), "err-line\n");
        assert_eq!(exit_code, Some(7));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn replay_returns_none_when_no_process_exit_recorded() {
        let path = write_fixture(
            "no-exit",
            &[r#"{"run_id":"r2","timestamp_ms":100,"kind":"stdout","line":"hi"}"#],
        );
        let mut store = TraceStore::open(Path::new(":memory:")).unwrap();
        store.ingest_run_eventlog(&path).unwrap();

        let replayer = Replayer::new(&store, "r2");
        let mut out = Vec::new();
        let mut err = Vec::new();
        let exit_code = replayer.replay(&mut out, &mut err).unwrap();

        assert_eq!(exit_code, None);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn unknown_run_id_replays_as_empty_with_no_exit_code() {
        let store = TraceStore::open(Path::new(":memory:")).unwrap();
        let replayer = Replayer::new(&store, "does-not-exist");
        let mut out = Vec::new();
        let mut err = Vec::new();
        let exit_code = replayer.replay(&mut out, &mut err).unwrap();

        assert!(out.is_empty());
        assert!(err.is_empty());
        assert_eq!(exit_code, None);
    }

    #[test]
    fn job_replay_merges_boundary_and_run_events_in_recorded_order() {
        // The scenario events_for_job exists for: a Job's
        // Boundary EventLog plus one Run launched under it.
        let job_path = write_fixture(
            "job",
            &[
                r#"{"job_id":"30","timestamp_ms":100,"kind":"node_allocated","node":"n1","user":"u","work_dir":"/tmp"}"#,
                r#"{"job_id":"30","timestamp_ms":150,"kind":"task_started","step_id":"0","task_id":"0","node":"n1"}"#,
                r#"{"job_id":"30","timestamp_ms":350,"kind":"task_exited","step_id":"0","task_id":"0","node":"n1"}"#,
                r#"{"job_id":"30","timestamp_ms":400,"kind":"job_exited","exit_code":0,"signal":0,"job_name":"j","node_list":"n1"}"#,
            ],
        );
        let run_path = write_fixture(
            "job-run",
            &[
                r#"{"run_id":"r30","job_id":"30","timestamp_ms":200,"kind":"process_start","image":"alpine","command":["echo","hi"]}"#,
                r#"{"run_id":"r30","job_id":"30","timestamp_ms":250,"kind":"stdout","line":"hi"}"#,
                r#"{"run_id":"r30","job_id":"30","timestamp_ms":300,"kind":"process_exit","exit_code":0}"#,
            ],
        );
        let mut store = TraceStore::open(Path::new(":memory:")).unwrap();
        store.ingest_job_eventlog(&job_path).unwrap();
        store.ingest_run_eventlog(&run_path).unwrap();

        let replayer = JobReplayer::new(&store, "30");
        let mut out = Vec::new();
        let mut err = Vec::new();
        let exit_code = replayer.replay(&mut out, &mut err).unwrap();

        assert_eq!(String::from_utf8(out).unwrap(), "hi\n");
        let err = String::from_utf8(err).unwrap();
        let err_lines: Vec<&str> = err.lines().collect();
        assert_eq!(
            err_lines,
            vec![
                "[job] node_allocated node=n1 user=u work_dir=/tmp",
                "[job] task_started step_id=0 task_id=0 node=n1",
                "[job] task_exited step_id=0 task_id=0 node=n1",
                "[job] job_exited exit_code=0 signal=0 job_name=j node_list=n1",
            ]
        );
        assert_eq!(exit_code, Some(0));

        std::fs::remove_file(&job_path).ok();
        std::fs::remove_file(&run_path).ok();
    }

    /// A captured message log merges into the same Job timeline
    /// as Boundary/Run rows, correctly discriminated from a `JobEvent` row
    /// despite sharing `run_id` NULL/`job_id` set with Boundary rows — the
    /// exact ambiguity the `message_` kind-prefix check exists to resolve.
    #[test]
    fn job_replay_merges_message_events_alongside_boundary_and_run_events() {
        let job_path = write_fixture(
            "msg-job",
            &[
                r#"{"job_id":"60","timestamp_ms":100,"kind":"node_allocated","node":"n1","user":"u","work_dir":"/tmp"}"#,
                r#"{"job_id":"60","timestamp_ms":400,"kind":"job_exited","exit_code":0,"signal":0,"job_name":"j","node_list":"n1"}"#,
            ],
        );
        let message_path = write_fixture(
            "msg-log",
            &[
                r#"{"pid":1,"fd":3,"direction":"send","peer":"10.0.0.2:9000","bytes":8,"timestamp_ms":200,"job_id":"60"}"#,
                r#"{"pid":2,"fd":4,"direction":"recv","peer":"10.0.0.1:54321","bytes":8,"timestamp_ms":250,"job_id":"60"}"#,
            ],
        );
        let mut store = TraceStore::open(Path::new(":memory:")).unwrap();
        store.ingest_job_eventlog(&job_path).unwrap();
        store.ingest_message_eventlog(&message_path).unwrap();

        let replayer = JobReplayer::new(&store, "60");
        let mut out = Vec::new();
        let mut err = Vec::new();
        let exit_code = replayer.replay(&mut out, &mut err).unwrap();

        let err = String::from_utf8(err).unwrap();
        let err_lines: Vec<&str> = err.lines().collect();
        assert_eq!(
            err_lines,
            vec![
                "[job] node_allocated node=n1 user=u work_dir=/tmp",
                "[msg] send peer=10.0.0.2:9000 bytes=8",
                "[msg] recv peer=10.0.0.1:54321 bytes=8",
                "[job] job_exited exit_code=0 signal=0 job_name=j node_list=n1",
            ]
        );
        assert_eq!(exit_code, Some(0));

        std::fs::remove_file(&job_path).ok();
        std::fs::remove_file(&message_path).ok();
    }

    /// `fault::apply` genericized to work on `Vec<TimelineEvent>` the
    /// same way it already worked on `Vec<Event>`. Dropping
    /// a Boundary row removes only its annotation and leaves the exit code
    /// intact; dropping a Run row removes only that output line; dropping
    /// `JobExited` is the only drop that flips the reported outcome to
    /// absent — the distinction that made genericizing worth doing.
    #[test]
    fn job_level_fault_drops_only_the_targeted_row() {
        let job_path = write_fixture(
            "fault-job",
            &[
                r#"{"job_id":"40","timestamp_ms":100,"kind":"node_allocated","node":"n1","user":"u","work_dir":"/tmp"}"#,
                r#"{"job_id":"40","timestamp_ms":150,"kind":"task_started","step_id":"0","task_id":"0","node":"n1"}"#,
                r#"{"job_id":"40","timestamp_ms":350,"kind":"task_exited","step_id":"0","task_id":"0","node":"n1"}"#,
                r#"{"job_id":"40","timestamp_ms":400,"kind":"job_exited","exit_code":0,"signal":0,"job_name":"j","node_list":"n1"}"#,
            ],
        );
        let run_path = write_fixture(
            "fault-job-run",
            &[r#"{"run_id":"r40","job_id":"40","timestamp_ms":250,"kind":"stdout","line":"hi"}"#],
        );
        let mut store = TraceStore::open(Path::new(":memory:")).unwrap();
        store.ingest_job_eventlog(&job_path).unwrap();
        store.ingest_run_eventlog(&run_path).unwrap();
        let replayer = JobReplayer::new(&store, "40");

        // Index 2 is the Run's `Stdout` row (timestamp 250 sorts between
        // task_started@150 and task_exited@350) — dropping it removes the
        // output line but leaves the exit code intact.
        let events = fault::apply(
            replayer.events().unwrap(),
            &fault::Fault::DropEvent { index: 2 },
        );
        let mut out = Vec::new();
        let mut err = Vec::new();
        let exit_code = drain_timeline_events(events, &mut out, &mut err).unwrap();
        assert!(
            out.is_empty(),
            "dropped Run row should remove the only stdout line"
        );
        assert_eq!(exit_code, Some(0));

        // Index 3 is the Boundary `task_exited` row — dropping it removes
        // only that annotation, leaving output and exit code intact.
        let events = fault::apply(
            replayer.events().unwrap(),
            &fault::Fault::DropEvent { index: 3 },
        );
        let mut out = Vec::new();
        let mut err = Vec::new();
        let exit_code = drain_timeline_events(events, &mut out, &mut err).unwrap();
        assert_eq!(String::from_utf8(out).unwrap(), "hi\n");
        assert!(!String::from_utf8(err).unwrap().contains("task_exited"));
        assert_eq!(exit_code, Some(0));

        // Index 4 is `job_exited` — the only drop that flips the reported
        // outcome to absent.
        let events = fault::apply(
            replayer.events().unwrap(),
            &fault::Fault::DropEvent { index: 4 },
        );
        let mut out = Vec::new();
        let mut err = Vec::new();
        let exit_code = drain_timeline_events(events, &mut out, &mut err).unwrap();
        assert_eq!(String::from_utf8(out).unwrap(), "hi\n");
        assert_eq!(exit_code, None);

        std::fs::remove_file(&job_path).ok();
        std::fs::remove_file(&run_path).ok();
    }

    #[test]
    fn job_replay_returns_none_when_no_job_exited_recorded() {
        let job_path = write_fixture(
            "no-exit-job",
            &[
                r#"{"job_id":"31","timestamp_ms":100,"kind":"node_allocated","node":"n1","user":"u","work_dir":"/tmp"}"#,
            ],
        );
        let mut store = TraceStore::open(Path::new(":memory:")).unwrap();
        store.ingest_job_eventlog(&job_path).unwrap();

        let replayer = JobReplayer::new(&store, "31");
        let mut out = Vec::new();
        let mut err = Vec::new();
        let exit_code = replayer.replay(&mut out, &mut err).unwrap();

        assert_eq!(exit_code, None);

        std::fs::remove_file(&job_path).ok();
    }
}
