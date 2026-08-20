//! `ang fuzz`: loop `Fault::from_seed` over a range of seeds against a
//! Job's timeline, running the same unmodified `invariant::check` on each,
//! and report the first seed that produces a violation. A failing seed is
//! immediately reproducible by hand via
//! `ang replay --job-id <id> --db <path> --seed <that seed>`.
//! `--exhaustive` runs the whole range instead of stopping at the first
//! failure, reporting how many seeds failed out of how many.

use std::path::PathBuf;

use recorder::TraceStore;
use replay::JobReplayer;

fn usage() -> ! {
    eprintln!("usage: ang fuzz --job-id <id> --db <path> --seeds <start> <end> [--exhaustive]");
    std::process::exit(2);
}

fn print_violations(violations: &[fault::invariant::Violation]) {
    for violation in violations {
        println!("[invariant] {}", violation.description);
    }
}

pub fn run(mut args: impl Iterator<Item = String>) -> anyhow::Result<()> {
    let mut job_id = None;
    let mut db = None;
    let mut seeds = None;
    let mut exhaustive = false;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--job-id" => job_id = Some(args.next().unwrap_or_else(|| usage())),
            "--db" => db = Some(args.next().unwrap_or_else(|| usage())),
            "--seeds" => {
                let start = args.next().unwrap_or_else(|| usage());
                let end = args.next().unwrap_or_else(|| usage());
                seeds = Some((
                    start.parse::<u64>().unwrap_or_else(|_| usage()),
                    end.parse::<u64>().unwrap_or_else(|_| usage()),
                ));
            }
            "--exhaustive" => exhaustive = true,
            _ => usage(),
        }
    }
    let job_id = job_id.unwrap_or_else(|| usage());
    let db: PathBuf = db.unwrap_or_else(|| usage()).into();
    let (start, end) = seeds.unwrap_or_else(|| usage());

    let store = TraceStore::open(&db)?;
    let replayer = JobReplayer::new(&store, job_id.clone());
    let events = replayer.events()?;

    if exhaustive {
        let stats = fault::seed_search_stats(&events, start..end);
        let rate = if stats.total == 0 {
            0.0
        } else {
            100.0 * stats.failing as f64 / stats.total as f64
        };
        println!("total: {}", stats.total);
        println!("failing: {} ({rate:.2}%)", stats.failing);
        for (name, count) in &stats.by_invariant {
            println!("  {name}: {count}");
        }
        match stats.first_failure {
            Some((seed, violations)) => {
                println!("first failure: seed {seed}");
                print_violations(&violations);
                std::process::exit(2);
            }
            None => return Ok(()),
        }
    }

    match fault::seed_search(&events, start..end) {
        Some((seed, violations)) => {
            println!("seed {seed} fails:");
            print_violations(&violations);
            std::process::exit(2);
        }
        None => {
            println!("no violations in {} seed(s)", end.saturating_sub(start));
        }
    }

    Ok(())
}
