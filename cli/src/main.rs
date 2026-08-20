//! `ang`: a container Run + EventLog, a Slurm Job's Boundary EventLog, the
//! SQLite Trace Store, Run/Job replay, and the seeded fuzz loop.

mod fuzz;
mod job_event;
mod replay;
mod trace_store;

use std::io::Write;
use std::path::PathBuf;

use recorder::EventLog;
use runtime::RunSpec;

fn usage(out: &mut dyn Write) -> std::io::Result<()> {
    writeln!(out, "usage: ang <image> [command args...]")?;
    writeln!(
        out,
        "       ang job-event <node-allocated|task-started|task-exited|job-exited>"
    )?;
    writeln!(
        out,
        "       ang trace-store <ingest-run|ingest-job|ingest-message|query> ..."
    )?;
    writeln!(out, "       ang replay --run-id <id> --db <path>")?;
    writeln!(out, "       ang replay --job-id <id> --db <path>")?;
    writeln!(
        out,
        "       ang fuzz --job-id <id> --db <path> --seeds <start> <end> [--exhaustive]"
    )
}

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    // A bare `ang`, or a leading flag, is a usage request — not an image name.
    let first = match args.next() {
        Some(arg) if !arg.starts_with('-') => arg,
        other => {
            let help = matches!(other.as_deref(), Some("-h") | Some("--help"));
            let mut stdout = std::io::stdout();
            let mut stderr = std::io::stderr();
            let out: &mut dyn Write = if help { &mut stdout } else { &mut stderr };
            usage(out)?;
            std::process::exit(if help { 0 } else { 2 });
        }
    };

    if first == "job-event" {
        let kind = args.next().unwrap_or_else(|| {
            eprintln!("usage: ang job-event <node-allocated|task-started|task-exited|job-exited>");
            std::process::exit(2);
        });
        return job_event::run(&kind);
    }

    if first == "trace-store" {
        return trace_store::run(args);
    }

    if first == "replay" {
        return replay::run(args);
    }

    if first == "fuzz" {
        return fuzz::run(args);
    }

    run_container(first, args.collect())
}

fn run_container(image: String, command: Vec<String>) -> anyhow::Result<()> {
    let run_id = uuid::Uuid::new_v4().to_string();
    let log_path = PathBuf::from(format!("{run_id}.eventlog.jsonl"));
    let mut eventlog = EventLog::create(&run_id, &log_path)?;

    let spec = RunSpec { image, command };
    let exit_code = runtime::run(&spec, &mut eventlog)?;

    println!("run_id: {run_id}");
    println!("eventlog: {}", log_path.display());
    println!("exit_code: {exit_code}");

    std::process::exit(exit_code);
}
