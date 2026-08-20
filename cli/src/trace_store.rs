//! `ang trace-store`: ingest EventLogs/Boundary EventLogs (JSONL)
//! into a queryable SQLite Trace Store, and query them back.

use std::path::{Path, PathBuf};

use recorder::TraceStore;

fn usage() -> ! {
    eprintln!("usage: ang trace-store ingest-run --db <db> <eventlog.jsonl>...");
    eprintln!("       ang trace-store ingest-job --db <db> <eventlog.jsonl>...");
    eprintln!("       ang trace-store ingest-message --db <db> <message.jsonl>...");
    eprintln!("       ang trace-store query --db <db> <--run-id ID|--job-id ID|--kind KIND>");
    std::process::exit(2);
}

fn take_db_flag(args: &mut impl Iterator<Item = String>) -> anyhow::Result<PathBuf> {
    let flag = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("expected --db <path>"))?;
    anyhow::ensure!(flag == "--db", "expected --db <path>, got '{flag}'");
    let path = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("--db requires a path"))?;
    Ok(PathBuf::from(path))
}

pub fn run(mut args: impl Iterator<Item = String>) -> anyhow::Result<()> {
    let sub = args.next().unwrap_or_else(|| usage());
    match sub.as_str() {
        "ingest-run" => ingest(args, TraceStore::ingest_run_eventlog),
        "ingest-job" => ingest(args, TraceStore::ingest_job_eventlog),
        "ingest-message" => ingest(args, TraceStore::ingest_message_eventlog),
        "query" => query(args),
        _ => usage(),
    }
}

fn ingest(
    mut args: impl Iterator<Item = String>,
    ingest_fn: fn(&mut TraceStore, &Path) -> anyhow::Result<usize>,
) -> anyhow::Result<()> {
    let db = take_db_flag(&mut args)?;
    let mut store = TraceStore::open(&db)?;
    let mut total = 0usize;
    for path in args {
        let n = ingest_fn(&mut store, Path::new(&path))?;
        println!("{path}: {n} event(s)");
        total += n;
    }
    println!("total: {total} event(s) ingested into {}", db.display());
    Ok(())
}

fn query(mut args: impl Iterator<Item = String>) -> anyhow::Result<()> {
    let db = take_db_flag(&mut args)?;
    let store = TraceStore::open(&db)?;

    let flag = args.next().unwrap_or_else(|| usage());
    let value = args.next().unwrap_or_else(|| {
        eprintln!("{flag} requires a value");
        std::process::exit(2);
    });

    let events = match flag.as_str() {
        "--run-id" => store.events_for_run(&value)?,
        "--job-id" => store.events_for_job(&value)?,
        "--kind" => store.events_by_kind(&value)?,
        _ => usage(),
    };

    for event in &events {
        println!("{} {}", event.timestamp_ms, event.payload);
    }
    println!("{} event(s)", events.len());
    Ok(())
}
