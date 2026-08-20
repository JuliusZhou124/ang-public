//! Called from Slurm Prolog/Epilog/TaskProlog/TaskEpilog/EpilogSlurmctld
//! hooks to record one Boundary EventLog event.
//! No CLI args beyond the event kind — everything else comes from the
//! `SLURM_*` environment variables Slurm sets for each hook.

use std::env;
use std::path::PathBuf;

use recorder::{JobEventKind, JobEventLog};

fn boundary_dir() -> PathBuf {
    env::var("ANG_BOUNDARY_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/var/log/ang/boundary"))
}

fn env_var(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| "unknown".to_string())
}

pub fn run(kind: &str) -> anyhow::Result<()> {
    let job_id = env::var("SLURM_JOB_ID")
        .map_err(|_| anyhow::anyhow!("SLURM_JOB_ID not set; must be run from a Slurm hook"))?;

    let event_kind = match kind {
        "node-allocated" => JobEventKind::NodeAllocated {
            node: env_var("SLURMD_NODENAME"),
            user: env_var("SLURM_JOB_USER"),
            work_dir: env_var("SLURM_JOB_WORK_DIR"),
        },
        "task-started" => JobEventKind::TaskStarted {
            step_id: env_var("SLURM_STEP_ID"),
            task_id: env_var("SLURM_PROCID"),
            node: env_var("SLURMD_NODENAME"),
        },
        "task-exited" => JobEventKind::TaskExited {
            step_id: env_var("SLURM_STEP_ID"),
            task_id: env_var("SLURM_PROCID"),
            node: env_var("SLURMD_NODENAME"),
        },
        "job-exited" => {
            let (exit_code, signal) = parse_exit_code2(&env_var("SLURM_JOB_EXIT_CODE2"));
            JobEventKind::JobExited {
                exit_code,
                signal,
                job_name: env_var("SLURM_JOB_NAME"),
                node_list: env_var("SLURM_JOB_NODELIST"),
            }
        }
        other => anyhow::bail!("unknown job-event kind: {other}"),
    };

    let dir = boundary_dir();
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{job_id}.eventlog.jsonl"));
    let mut log = JobEventLog::create(&job_id, &path)?;
    log.record(event_kind)?;
    Ok(())
}

/// Parses Slurm's `SLURM_JOB_EXIT_CODE2` format, "exit_code:signal".
fn parse_exit_code2(raw: &str) -> (i32, i32) {
    let mut parts = raw.splitn(2, ':');
    let exit_code = parts.next().and_then(|s| s.parse().ok()).unwrap_or(-1);
    let signal = parts.next().and_then(|s| s.parse().ok()).unwrap_or(-1);
    (exit_code, signal)
}
