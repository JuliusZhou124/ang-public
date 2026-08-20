//! EventLog model for a Run and Boundary EventLog model for a
//! Slurm Job.
//!
//! Both are append-only, ordered records of observable events, correlated
//! by RunID and JobID respectively.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

pub mod trace_store;
pub use trace_store::{StoredEvent, TraceStore};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventKind {
    ProcessStart { image: String, command: Vec<String> },
    Stdout { line: String },
    Stderr { line: String },
    ProcessExit { exit_code: i32 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub run_id: String,
    /// The Slurm Job this Run executed under, if any — read from
    /// `SLURM_JOB_ID` per event, the same env var the Prolog/Epilog hooks
    /// read for `JobEvent`. `None` outside Slurm, preserving standalone
    /// (non-Slurm) behavior.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub job_id: Option<String>,
    pub timestamp_ms: u128,
    #[serde(flatten)]
    pub kind: EventKind,
}

impl Event {
    pub fn new(run_id: &str, kind: EventKind) -> Self {
        Self {
            run_id: run_id.to_string(),
            job_id: std::env::var("SLURM_JOB_ID").ok(),
            timestamp_ms: now_ms(),
            kind,
        }
    }
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis()
}

fn open_append(path: &Path) -> anyhow::Result<File> {
    // A Job's Boundary EventLog is written by multiple hook identities
    // across a job's lifecycle (root for Prolog/EpilogSlurmctld, the job's
    // own user for TaskProlog/TaskEpilog). Whoever creates the file first
    // must leave it writable by the others, and chmod-after-open doesn't
    // work here since a non-owner can't chmod an existing file (EPERM).
    // Zeroing umask for the creating open() guarantees 0666 regardless of
    // creation order.
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let old_umask = unsafe { libc::umask(0) };
        let result = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o666)
            .open(path);
        unsafe { libc::umask(old_umask) };
        Ok(result?)
    }
    #[cfg(not(unix))]
    Ok(OpenOptions::new().create(true).append(true).open(path)?)
}

fn append_line<T: Serialize>(file: &mut File, value: &T) -> anyhow::Result<()> {
    let line = serde_json::to_string(value)?;
    // A Job's Boundary EventLog can be appended to by multiple hook
    // processes at once, potentially on different nodes sharing one
    // filesystem. Without serializing the write+flush, two
    // concurrent appenders can interleave partial writes and corrupt the
    // JSONL framing (`}{` with no newline between them) even though each
    // process's own `writeln!` call looks atomic in isolation. An exclusive
    // `flock` around the write closes that window — POSIX guarantees it's
    // released automatically when the fd closes, so no unlock bookkeeping
    // is needed even on an early return via `?`.
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        let fd = file.as_raw_fd();
        let lock_result = unsafe { libc::flock(fd, libc::LOCK_EX) };
        anyhow::ensure!(
            lock_result == 0,
            "flock(LOCK_EX) failed: {}",
            std::io::Error::last_os_error()
        );
        let write_result = (|| -> anyhow::Result<()> {
            writeln!(file, "{line}")?;
            file.flush()?;
            Ok(())
        })();
        unsafe { libc::flock(fd, libc::LOCK_UN) };
        write_result
    }
    #[cfg(not(unix))]
    {
        writeln!(file, "{line}")?;
        file.flush()?;
        Ok(())
    }
}

/// Append-only JSONL writer for a Run's EventLog.
pub struct EventLog {
    run_id: String,
    file: File,
}

impl EventLog {
    pub fn create(run_id: &str, path: &Path) -> anyhow::Result<Self> {
        Ok(Self {
            run_id: run_id.to_string(),
            file: open_append(path)?,
        })
    }

    pub fn record(&mut self, kind: EventKind) -> anyhow::Result<()> {
        append_line(&mut self.file, &Event::new(&self.run_id, kind))
    }
}

/// A Job's Boundary EventLog: the events that don't belong
/// to any single Run — job submitted/node allocated, task started/exited.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum JobEventKind {
    NodeAllocated {
        node: String,
        user: String,
        work_dir: String,
    },
    TaskStarted {
        step_id: String,
        task_id: String,
        node: String,
    },
    TaskExited {
        step_id: String,
        task_id: String,
        node: String,
    },
    JobExited {
        exit_code: i32,
        signal: i32,
        job_name: String,
        node_list: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobEvent {
    pub job_id: String,
    pub timestamp_ms: u128,
    #[serde(flatten)]
    pub kind: JobEventKind,
}

impl JobEvent {
    pub fn new(job_id: &str, kind: JobEventKind) -> Self {
        Self {
            job_id: job_id.to_string(),
            timestamp_ms: now_ms(),
            kind,
        }
    }
}

/// Append-only JSONL writer for a Job's Boundary EventLog.
pub struct JobEventLog {
    job_id: String,
    file: File,
}

impl JobEventLog {
    pub fn create(job_id: &str, path: &Path) -> anyhow::Result<Self> {
        Ok(Self {
            job_id: job_id.to_string(),
            file: open_append(path)?,
        })
    }

    pub fn record(&mut self, kind: JobEventKind) -> anyhow::Result<()> {
        append_line(&mut self.file, &JobEvent::new(&self.job_id, kind))
    }
}
