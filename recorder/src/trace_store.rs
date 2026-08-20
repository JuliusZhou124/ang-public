//! SQLite-backed Trace Store.
//!
//! Ingests the JSONL EventLogs and Boundary EventLogs
//! that `EventLog`/`JobEventLog` already produce, and makes them queryable.
//! Ingestion is batch, after the fact — the capture path itself is
//! unchanged by this module.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use intercept::MessageEvent;
use rusqlite::{params, Connection, Row};

use crate::{Event, JobEvent};

/// One row of the Trace Store: a Job Boundary event has `job_id` set and
/// `run_id` unset; a Run event has `run_id` set and `job_id`
/// also set if the Run executed under a Slurm Job — that shared `job_id` is
/// what lets `events_for_job` return a merged Job+Run timeline.
#[derive(Debug, Clone)]
pub struct StoredEvent {
    pub run_id: Option<String>,
    pub job_id: Option<String>,
    pub timestamp_ms: i64,
    pub kind: String,
    pub payload: String,
}

fn row_to_stored(row: &Row) -> rusqlite::Result<StoredEvent> {
    Ok(StoredEvent {
        run_id: row.get(0)?,
        job_id: row.get(1)?,
        timestamp_ms: row.get(2)?,
        kind: row.get(3)?,
        payload: row.get(4)?,
    })
}

fn kind_tag(line: &str) -> anyhow::Result<String> {
    let value: serde_json::Value = serde_json::from_str(line)?;
    Ok(value
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string())
}

pub struct TraceStore {
    conn: Connection,
}

impl TraceStore {
    /// Opens (or creates) a Trace Store at `path`.
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                job_id TEXT,
                timestamp_ms INTEGER NOT NULL,
                kind TEXT NOT NULL,
                -- The raw JSONL line, which already serializes run_id,
                -- job_id, timestamp_ms, and kind — so it alone is a
                -- sufficient dedup key. (A UNIQUE(run_id, job_id, ...) over
                -- the split-out columns wouldn't work: SQLite treats every
                -- NULL as distinct from every other NULL, so it would never
                -- dedupe Job rows, whose run_id is always NULL, or non-Slurm
                -- Run rows, whose job_id is always NULL.)
                payload TEXT NOT NULL UNIQUE
            );
            CREATE INDEX IF NOT EXISTS idx_events_run_id ON events(run_id);
            CREATE INDEX IF NOT EXISTS idx_events_job_id ON events(job_id);
            CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp_ms);
            CREATE INDEX IF NOT EXISTS idx_events_kind ON events(kind);",
        )?;
        Ok(Self { conn })
    }

    /// Ingests a Run's EventLog: one JSONL `Event` per line, `run_id` set.
    pub fn ingest_run_eventlog(&mut self, path: &Path) -> anyhow::Result<usize> {
        let file = File::open(path)?;
        let tx = self.conn.transaction()?;
        let mut count = 0;
        for line in BufReader::new(file).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let event: Event = serde_json::from_str(&line)?;
            let kind = kind_tag(&line)?;
            count += tx.execute(
                "INSERT OR IGNORE INTO events (run_id, job_id, timestamp_ms, kind, payload)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    event.run_id,
                    event.job_id,
                    event.timestamp_ms as i64,
                    kind,
                    line
                ],
            )?;
        }
        tx.commit()?;
        Ok(count)
    }

    /// Ingests a Job's Boundary EventLog: one JSONL `JobEvent` per line,
    /// `job_id` set.
    pub fn ingest_job_eventlog(&mut self, path: &Path) -> anyhow::Result<usize> {
        let file = File::open(path)?;
        let tx = self.conn.transaction()?;
        let mut count = 0;
        for line in BufReader::new(file).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let event: JobEvent = serde_json::from_str(&line)?;
            let kind = kind_tag(&line)?;
            count += tx.execute(
                "INSERT OR IGNORE INTO events (run_id, job_id, timestamp_ms, kind, payload)
                 VALUES (NULL, ?1, ?2, ?3, ?4)",
                params![event.job_id, event.timestamp_ms as i64, kind, line],
            )?;
        }
        tx.commit()?;
        Ok(count)
    }

    /// Ingests a `MessageEvent` log written by the `intercept` shim: one
    /// JSONL `MessageEvent` per line, `run_id` NULL, `job_id` from the
    /// event's own `SLURM_JOB_ID` read (`None` if not captured under Slurm).
    /// `kind` is derived from `direction` (`message_send`/`message_recv`/
    /// `message_connect`) rather than scraped via `kind_tag` — `MessageEvent`
    /// has no `"kind"` JSON field, unlike `Event`/`JobEvent`.
    pub fn ingest_message_eventlog(&mut self, path: &Path) -> anyhow::Result<usize> {
        let file = File::open(path)?;
        let tx = self.conn.transaction()?;
        let mut count = 0;
        for line in BufReader::new(file).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let event: MessageEvent = serde_json::from_str(&line)?;
            let kind = format!("message_{}", event.direction);
            count += tx.execute(
                "INSERT OR IGNORE INTO events (run_id, job_id, timestamp_ms, kind, payload)
                 VALUES (NULL, ?1, ?2, ?3, ?4)",
                params![event.job_id, event.timestamp_ms as i64, kind, line],
            )?;
        }
        tx.commit()?;
        Ok(count)
    }

    pub fn events_for_run(&self, run_id: &str) -> anyhow::Result<Vec<StoredEvent>> {
        let mut stmt = self.conn.prepare(
            "SELECT run_id, job_id, timestamp_ms, kind, payload FROM events
             WHERE run_id = ?1 ORDER BY timestamp_ms",
        )?;
        let rows = stmt.query_map(params![run_id], row_to_stored)?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    pub fn events_for_job(&self, job_id: &str) -> anyhow::Result<Vec<StoredEvent>> {
        let mut stmt = self.conn.prepare(
            "SELECT run_id, job_id, timestamp_ms, kind, payload FROM events
             WHERE job_id = ?1 ORDER BY timestamp_ms",
        )?;
        let rows = stmt.query_map(params![job_id], row_to_stored)?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    pub fn events_by_kind(&self, kind: &str) -> anyhow::Result<Vec<StoredEvent>> {
        let mut stmt = self.conn.prepare(
            "SELECT run_id, job_id, timestamp_ms, kind, payload FROM events
             WHERE kind = ?1 ORDER BY timestamp_ms",
        )?;
        let rows = stmt.query_map(params![kind], row_to_stored)?;
        Ok(rows.collect::<Result<_, _>>()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_fixture(name: &str, lines: &[&str]) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "ang-trace-store-test-{}-{}-{name}.jsonl",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut file = File::create(&path).unwrap();
        for line in lines {
            writeln!(file, "{line}").unwrap();
        }
        path
    }

    #[test]
    fn ingests_and_queries_run_eventlog_by_run_id() {
        let path = write_fixture(
            "run",
            &[
                r#"{"run_id":"r1","timestamp_ms":100,"kind":"process_start","image":"alpine","command":["echo","hi"]}"#,
                r#"{"run_id":"r1","timestamp_ms":200,"kind":"stdout","line":"hi"}"#,
                r#"{"run_id":"r1","timestamp_ms":300,"kind":"process_exit","exit_code":0}"#,
            ],
        );
        let mut store = TraceStore::open(Path::new(":memory:")).unwrap();
        let n = store.ingest_run_eventlog(&path).unwrap();
        assert_eq!(n, 3);

        let events = store.events_for_run("r1").unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].kind, "process_start");
        assert_eq!(events[1].kind, "stdout");
        assert_eq!(events[2].kind, "process_exit");
        assert!(events.iter().all(|e| e.job_id.is_none()));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn ingests_and_queries_job_eventlog_by_job_id_and_kind() {
        let path = write_fixture(
            "job",
            &[
                r#"{"job_id":"10","timestamp_ms":100,"kind":"node_allocated","node":"n1","user":"u","work_dir":"/tmp"}"#,
                r#"{"job_id":"10","timestamp_ms":200,"kind":"task_exited","step_id":"0","task_id":"0","node":"n1"}"#,
                r#"{"job_id":"10","timestamp_ms":300,"kind":"job_exited","exit_code":0,"signal":0,"job_name":"j","node_list":"n1"}"#,
            ],
        );
        let mut store = TraceStore::open(Path::new(":memory:")).unwrap();
        let n = store.ingest_job_eventlog(&path).unwrap();
        assert_eq!(n, 3);

        let events = store.events_for_job("10").unwrap();
        assert_eq!(events.len(), 3);
        assert!(events.iter().all(|e| e.run_id.is_none()));

        let exits = store.events_by_kind("task_exited").unwrap();
        assert_eq!(exits.len(), 1);
        assert_eq!(exits[0].job_id.as_deref(), Some("10"));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn events_are_ordered_by_timestamp_not_insertion_order() {
        let path = write_fixture(
            "order",
            &[
                r#"{"run_id":"r2","timestamp_ms":300,"kind":"process_exit","exit_code":0}"#,
                r#"{"run_id":"r2","timestamp_ms":100,"kind":"process_start","image":"alpine","command":[]}"#,
            ],
        );
        let mut store = TraceStore::open(Path::new(":memory:")).unwrap();
        store.ingest_run_eventlog(&path).unwrap();

        let events = store.events_for_run("r2").unwrap();
        assert_eq!(events[0].timestamp_ms, 100);
        assert_eq!(events[1].timestamp_ms, 300);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn events_for_job_returns_merged_job_and_run_timeline() {
        // A Run launched under Slurm Job 20 stamps job_id on every Event
        //; ingestion must carry that through so events_for_job
        // returns both the Boundary events and this Run's events, merged
        // and ordered by timestamp.
        let job_path = write_fixture(
            "join-job",
            &[
                r#"{"job_id":"20","timestamp_ms":100,"kind":"node_allocated","node":"n1","user":"u","work_dir":"/tmp"}"#,
                r#"{"job_id":"20","timestamp_ms":400,"kind":"job_exited","exit_code":0,"signal":0,"job_name":"j","node_list":"n1"}"#,
            ],
        );
        let run_path = write_fixture(
            "join-run",
            &[
                r#"{"run_id":"r20","job_id":"20","timestamp_ms":200,"kind":"process_start","image":"alpine","command":["echo","hi"]}"#,
                r#"{"run_id":"r20","job_id":"20","timestamp_ms":300,"kind":"process_exit","exit_code":0}"#,
            ],
        );
        let mut store = TraceStore::open(Path::new(":memory:")).unwrap();
        store.ingest_job_eventlog(&job_path).unwrap();
        store.ingest_run_eventlog(&run_path).unwrap();

        let timeline = store.events_for_job("20").unwrap();
        let kinds: Vec<&str> = timeline.iter().map(|e| e.kind.as_str()).collect();
        assert_eq!(
            kinds,
            vec![
                "node_allocated",
                "process_start",
                "process_exit",
                "job_exited"
            ]
        );
        assert_eq!(timeline[1].run_id.as_deref(), Some("r20"));
        assert_eq!(timeline[1].job_id.as_deref(), Some("20"));

        std::fs::remove_file(&job_path).ok();
        std::fs::remove_file(&run_path).ok();
    }

    /// Re-ingesting the same non-Slurm Run EventLog file (job_id NULL on
    /// every row) used
    /// to duplicate every row. `payload UNIQUE` now dedupes it, and the
    /// second ingest's returned count reflects that nothing new landed.
    #[test]
    fn reingesting_same_run_eventlog_is_idempotent() {
        let path = write_fixture(
            "dup-run",
            &[r#"{"run_id":"r3","timestamp_ms":100,"kind":"process_exit","exit_code":0}"#],
        );
        let mut store = TraceStore::open(Path::new(":memory:")).unwrap();
        assert_eq!(store.ingest_run_eventlog(&path).unwrap(), 1);
        assert_eq!(store.ingest_run_eventlog(&path).unwrap(), 0);

        let events = store.events_for_run("r3").unwrap();
        assert_eq!(events.len(), 1);

        std::fs::remove_file(&path).ok();
    }

    /// A captured `MessageEvent` log ingests into the same
    /// `events` table as Job/Run rows, correlated by the same `job_id` a
    /// Boundary EventLog uses, and queryable by its derived `message_*`
    /// kind — no new table, no new query method needed beyond ingestion.
    #[test]
    fn ingests_and_queries_message_eventlog_by_job_id_and_kind() {
        let path = write_fixture(
            "message",
            &[
                r#"{"pid":1,"fd":3,"direction":"connect","peer":"10.0.0.2:9000","bytes":0,"timestamp_ms":100,"job_id":"50"}"#,
                r#"{"pid":1,"fd":3,"direction":"send","peer":"10.0.0.2:9000","bytes":8,"timestamp_ms":150,"job_id":"50"}"#,
                r#"{"pid":2,"fd":4,"direction":"recv","peer":"10.0.0.1:54321","bytes":8,"timestamp_ms":150,"job_id":"50"}"#,
            ],
        );
        let mut store = TraceStore::open(Path::new(":memory:")).unwrap();
        let n = store.ingest_message_eventlog(&path).unwrap();
        assert_eq!(n, 3);

        let events = store.events_for_job("50").unwrap();
        assert_eq!(events.len(), 3);
        assert!(events
            .iter()
            .all(|e| e.run_id.is_none() && e.job_id.as_deref() == Some("50")));

        let sends = store.events_by_kind("message_send").unwrap();
        assert_eq!(sends.len(), 1);
        let recvs = store.events_by_kind("message_recv").unwrap();
        assert_eq!(recvs.len(), 1);
        let connects = store.events_by_kind("message_connect").unwrap();
        assert_eq!(connects.len(), 1);

        std::fs::remove_file(&path).ok();
    }

    /// Same fix, but for Job Boundary rows, where run_id (not job_id) is
    /// always NULL — the case that would defeat a naive
    /// `UNIQUE(run_id, job_id, ...)` over the split-out columns instead of
    /// over `payload` directly (see the comment on the `payload` column).
    #[test]
    fn reingesting_same_job_eventlog_is_idempotent() {
        let path = write_fixture(
            "dup-job",
            &[
                r#"{"job_id":"40","timestamp_ms":100,"kind":"node_allocated","node":"n1","user":"u","work_dir":"/tmp"}"#,
            ],
        );
        let mut store = TraceStore::open(Path::new(":memory:")).unwrap();
        assert_eq!(store.ingest_job_eventlog(&path).unwrap(), 1);
        assert_eq!(store.ingest_job_eventlog(&path).unwrap(), 0);

        let events = store.events_for_job("40").unwrap();
        assert_eq!(events.len(), 1);

        std::fs::remove_file(&path).ok();
    }
}
