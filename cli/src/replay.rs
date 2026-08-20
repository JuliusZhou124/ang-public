//! `ang replay`: reconstruct a Run's stdout/stderr/exit code from the Trace
//! Store with no container re-execution, or replay a Job's merged
//! Boundary+Run timeline. `--drop-event <n>` injects a fault into the event
//! stream before replay drains it; `--reorder-events <i> <j>` is a second
//! fault kind that swaps two events instead of removing one; `--seed <n>`
//! deterministically selects a fault from the same catalog instead of a
//! human specifying its kind and parameters by hand. A Job timeline
//! (post-fault, if any) is checked for structural violations before being
//! drained, reported to stderr as `[invariant] ...`.

use std::path::PathBuf;

use fault::Fault;
use recorder::TraceStore;
use replay::{JobReplayer, Replayer};

fn usage() -> ! {
    eprintln!(
        "usage: ang replay --run-id <id> --db <path> [--drop-event <n> | --reorder-events <i> <j> | --corrupt-node <n> <node> | --seed <n>]"
    );
    eprintln!(
        "       ang replay --job-id <id> --db <path> [--drop-event <n> | --reorder-events <i> <j> | --corrupt-node <n> <node> | --seed <n>]"
    );
    std::process::exit(2);
}

/// A fault as specified on the CLI, before it's resolved against a
/// timeline's length (`--seed` needs `events.len()` to bound its selected
/// index(es), which isn't known until the timeline is loaded).
enum FaultSpec {
    Explicit(Fault),
    Seeded(u64),
}

impl FaultSpec {
    fn resolve(&self, len: usize) -> Fault {
        match self {
            FaultSpec::Explicit(f) => f.clone(),
            FaultSpec::Seeded(seed) => fault::from_seed(*seed, len),
        }
    }
}

pub fn run(mut args: impl Iterator<Item = String>) -> anyhow::Result<()> {
    let mut run_id = None;
    let mut job_id = None;
    let mut db = None;
    let mut fault: Option<FaultSpec> = None;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--run-id" => run_id = Some(args.next().unwrap_or_else(|| usage())),
            "--job-id" => job_id = Some(args.next().unwrap_or_else(|| usage())),
            "--db" => db = Some(args.next().unwrap_or_else(|| usage())),
            "--drop-event" => {
                if fault.is_some() {
                    usage();
                }
                let n = args.next().unwrap_or_else(|| usage());
                fault = Some(FaultSpec::Explicit(Fault::DropEvent {
                    index: n.parse().unwrap_or_else(|_| usage()),
                }));
            }
            "--reorder-events" => {
                if fault.is_some() {
                    usage();
                }
                let i = args.next().unwrap_or_else(|| usage());
                let j = args.next().unwrap_or_else(|| usage());
                fault = Some(FaultSpec::Explicit(Fault::ReorderEvents {
                    i: i.parse().unwrap_or_else(|_| usage()),
                    j: j.parse().unwrap_or_else(|_| usage()),
                }));
            }
            "--corrupt-node" => {
                if fault.is_some() {
                    usage();
                }
                let n = args.next().unwrap_or_else(|| usage());
                let node = args.next().unwrap_or_else(|| usage());
                fault = Some(FaultSpec::Explicit(Fault::CorruptNode {
                    index: n.parse().unwrap_or_else(|_| usage()),
                    node,
                }));
            }
            "--seed" => {
                if fault.is_some() {
                    usage();
                }
                let n = args.next().unwrap_or_else(|| usage());
                fault = Some(FaultSpec::Seeded(n.parse().unwrap_or_else(|_| usage())));
            }
            _ => usage(),
        }
    }
    let db: PathBuf = db.unwrap_or_else(|| usage()).into();
    let store = TraceStore::open(&db)?;

    let exit_code = match (run_id, job_id) {
        (Some(run_id), None) => {
            let replayer = Replayer::new(&store, run_id.clone());
            let exit_code = match &fault {
                Some(spec) => {
                    let events = replayer.events()?;
                    let f = spec.resolve(events.len());
                    let events = fault::apply(events, &f);
                    replay::drain_events(events, &mut std::io::stdout(), &mut std::io::stderr())?
                }
                None => replayer.replay(&mut std::io::stdout(), &mut std::io::stderr())?,
            };
            exit_code
                .ok_or_else(|| anyhow::anyhow!("no process_exit event found for run_id '{run_id}'"))
        }
        (None, Some(job_id)) => {
            let replayer = JobReplayer::new(&store, job_id.clone());
            let mut events = replayer.events()?;
            if let Some(spec) = &fault {
                let f = spec.resolve(events.len());
                events = fault::apply_timeline(events, &f);
            }
            for violation in fault::invariant::check(&events) {
                eprintln!("[invariant] {}", violation.description);
            }
            let exit_code = replay::drain_timeline_events(
                events,
                &mut std::io::stdout(),
                &mut std::io::stderr(),
            )?;
            exit_code
                .ok_or_else(|| anyhow::anyhow!("no job_exited event found for job_id '{job_id}'"))
        }
        _ => usage(),
    };

    match exit_code {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    }
}
