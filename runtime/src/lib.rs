//! Container-based Controlled Execution Context.
//!
//! Launches a program inside a container via the `docker` CLI and records
//! process start/exit and stdout/stderr into the Run's EventLog.

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;

use recorder::{EventKind, EventLog};

pub struct RunSpec {
    pub image: String,
    pub command: Vec<String>,
}

/// Runs `spec` inside a container, recording every observable event into
/// `eventlog`. Returns the container process's exit code.
pub fn run(spec: &RunSpec, eventlog: &mut EventLog) -> anyhow::Result<i32> {
    eventlog.record(EventKind::ProcessStart {
        image: spec.image.clone(),
        command: spec.command.clone(),
    })?;

    let mut cmd = Command::new("docker");
    cmd.arg("run")
        .arg("--rm")
        .arg(&spec.image)
        .args(&spec.command);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn()?;
    let stdout = child.stdout.take().expect("stdout was piped");
    let stderr = child.stderr.take().expect("stderr was piped");

    // stdout/stderr readers run concurrently but funnel through one channel
    // so the EventLog (a single append-only writer) sees one event at a time.
    let (tx, rx) = mpsc::channel::<EventKind>();

    let tx_out = tx.clone();
    let out_handle = thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            let _ = tx_out.send(EventKind::Stdout { line });
        }
    });

    let tx_err = tx.clone();
    let err_handle = thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            let _ = tx_err.send(EventKind::Stderr { line });
        }
    });

    drop(tx);
    for kind in rx {
        eventlog.record(kind)?;
    }

    out_handle.join().expect("stdout reader thread panicked");
    err_handle.join().expect("stderr reader thread panicked");

    let status = child.wait()?;
    let exit_code = status.code().unwrap_or(-1);
    eventlog.record(EventKind::ProcessExit { exit_code })?;

    Ok(exit_code)
}
