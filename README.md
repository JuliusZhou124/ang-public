# ANG

Deterministic simulation testing for Slurm jobs.

ANG records what a distributed HPC job did — across nodes, down to individual
socket messages — stores it as a single causally-ordered timeline, then mutates
that timeline under a seeded fault and checks whether the job's causal
invariants still hold. A `u64` seed is a complete, reproducible description of
one fault, so any failure found by the fuzz loop is reproducible by replaying
that seed.

Recording works on a stock cluster: no patched Slurm, no recompiled workload.

- **Level 1 (boundary):** Slurm `Prolog`/`TaskProlog`/`TaskEpilog`/`EpilogSlurmctld`
  hooks record node allocations, task starts and exits, and job exit.
- **Level 2 (message):** an `LD_PRELOAD` shim interposes `connect`/`send`/`recv`/
  `write`/`read` on socket fds inside unmodified task binaries, capturing
  cross-node message order.

Both write JSONL, ingest into one SQLite trace store, merge into one timeline,
and are fuzzed by the same seeded fault engine.

## Requirements

A Rust toolchain. Docker is needed only to capture new traces from a live
cluster; the checked-in traces replay and fuzz without it.

## Quick start

Reproduces the project's headline result from a real 8-node cluster trace
checked in under `fault/tests/fixtures/`:

```bash
cargo test --workspace          # 82 tests

F=fault/tests/fixtures
cargo run -p cli -- trace-store ingest-job     --db /tmp/trace.db $F/real_job_3.eventlog.jsonl
cargo run -p cli -- trace-store ingest-message --db /tmp/trace.db $F/real_job_3.node*.messages.jsonl
cargo run -p cli -- fuzz --job-id 1 --db /tmp/trace.db --seeds 0 1000 --exhaustive
```

```
total: 1000
failing: 147 (14.70%)
  connect_precedes_activity: 28
  job_exited_is_last: 8
  recv_precedes_task_exited: 27
  task_exited_requires_prior_task_started: 44
  task_started_requires_prior_node_allocated: 72
first failure: seed 7
```

CI asserts this exact output on every push. To inspect the clean timeline, or
the specific corruption a failing seed produced:

```bash
cargo run -p cli -- replay --job-id 1 --db /tmp/trace.db
cargo run -p cli -- replay --job-id 1 --db /tmp/trace.db --seed 7
```

## CLI

```
ang <image> [command args...]                 # run a workload in a container, recording a Run EventLog
ang job-event <kind>                          # record one boundary event; called from a Slurm hook
ang trace-store ingest-run     --db <db> <eventlog.jsonl>...
ang trace-store ingest-job     --db <db> <eventlog.jsonl>...
ang trace-store ingest-message --db <db> <message.jsonl>...
ang trace-store query --db <db> <--run-id ID | --job-id ID | --kind KIND>
ang replay --run-id <id> --db <db> [fault]    # replay one node's Run
ang replay --job-id <id> --db <db> [fault]    # replay a Job's merged cross-node timeline
ang fuzz --job-id <id> --db <db> --seeds <start> <end> [--exhaustive]
```

`job-event` kinds: `node-allocated`, `task-started`, `task-exited`, `job-exited`.

Faults accepted by `replay`, either specified directly or derived from a seed:

```
--drop-event <n>              # remove the event at position n
--reorder-events <i> <j>      # swap two events
--corrupt-node <n> <node>     # overwrite an event's node field
--seed <n>                    # derive one of the above deterministically from n
```

`replay` reports invariant violations on stderr as `[invariant] ...`, then
replays the timeline and exits with the replayed job's own exit code. `fuzz`
stops at the first failing seed unless `--exhaustive`, which runs the whole
range and reports a failure rate with a per-invariant breakdown; it exits 2 if
any seed failed.

Environment: `ANG_BOUNDARY_DIR` (default `/var/log/ang/boundary`) is where hooks
write a job's boundary log; `ANG_MESSAGE_LOG` is the path the shim appends to.
Both are appended under an exclusive `flock`, so concurrent writers on separate
nodes sharing one volume do not corrupt the JSONL framing.

## Recording from a live cluster

`cluster/` is a working 8-node Slurm cluster (`slurm-wlm` 21.08.5, Docker
Compose, no `--privileged`) with the hooks and shim already wired in. Point
`slurm.conf` at the four one-line wrappers in `cluster/ang-hooks/` to enable
Level 1 on your own cluster:

```
Prolog=/etc/slurm/ang-hooks/prolog.sh
TaskProlog=/etc/slurm/ang-hooks/task-prolog.sh
TaskEpilog=/etc/slurm/ang-hooks/task-epilog.sh
EpilogSlurmctld=/etc/slurm/ang-hooks/epilog-slurmctld.sh
```

Level 2 needs no cluster configuration — export the shim into the job:

```bash
srun --export=ALL,LD_PRELOAD=/usr/local/lib/libintercept.so ...
```

`fault/tests/fixtures/README.md` has full capture and reproduction commands for
each checked-in trace.

## Layout

| Crate | Role |
|---|---|
| [`fault`](fault/src) | Seeded fault selection, timeline mutation, the invariant registry, `seed_search` |
| [`recorder`](recorder/src) | EventLog (JSONL, `flock`-serialised) and the SQLite trace store |
| [`intercept`](intercept/src) | The `LD_PRELOAD` shim |
| [`cli`](cli/src) | `ang` |
| [`replay`](replay/src) | Decodes stored rows into a discriminated, ordered timeline |
| [`runtime`](runtime/src) | Container Run execution |

`unsafe` appears only where libc requires it: the shim, and four calls in
`recorder` (`umask`, `flock`).

## Invariants

Pure functions over `&[TimelineEvent]`, registered in a `const CHECKS` table in
[`fault/src/invariant.rs`](fault/src/invariant.rs); adding one is a one-line
change.

| Invariant | Rule |
|---|---|
| `task_exited_requires_prior_task_started` | a task can't exit before it started |
| `task_started_requires_prior_node_allocated` | a task can't start on an unallocated node |
| `job_exited_is_last` | nothing follows job exit |
| `connect_precedes_activity` | a `send`/`recv` can't precede its own `(pid, fd)`'s `connect` |
| `recv_precedes_task_exited` | a task can't receive a message after it has exited |

## Status

Validated on real multi-node Slurm clusters: cross-node Job/Step/Task
correlation through hooks; `LD_PRELOAD` capture surviving `srun` task launch to
separate nodes; merging independent per-node logs into one inversion-free
timeline (0 inversions across 2,000+ tight-loop messages, 10 trials); concurrent
multi-container writes to shared storage (1005/1005 lines intact); and the full
ingest → replay → seeded-fault → invariant-check loop end-to-end through the CLI.

Current limits:

- **No re-execution.** Faults mutate a recorded trace; the job is never re-run
  under them. Faults answer "would this invariant survive this corruption?",
  not "what would the system have done next?"
- **Wall-clock ordering.** Cross-node ordering uses NTP-synced millisecond wall
  clocks, measured on containers that share a host clock source. No evidence
  from physically separate machines.
- **One atomic fault per seed.** No fault sequences, and no shrinking (not
  well-posed under this model — every fault is already minimal, and a seed's
  magnitude is unrelated to fault complexity).
- **No same-node IPC.** Shared-memory MPI traffic is uncaptured.
- **Boundary recording says nothing about computation.** Level 1 can show a
  job's structure is causally consistent, not that it computed the right answer.

The 14.70% figure measures this fault model against this trace: of 1000 seeded
single-event corruptions of a real 8-node job's timeline, 147 produced a
detectable causal violation. It is not a claim about Slurm's reliability.

## Next steps

1. **Logical clocks.** Attach Lamport timestamps in the shim so cross-node
   ordering is correct by construction rather than by measurement — the one open
   risk that can't otherwise be closed without physically separate hardware.
2. **Compound fault sequences.** Derive a *sequence* of faults from a seed
   instead of one, which both reaches conjunction bugs and makes shrinking
   well-posed. Changes the meaning of every previously recorded failure rate.
3. **Re-execution.** Replay a job under a fault rather than mutating its
   recorded trace — the largest gap between ANG and a general DST system.
4. **Same-node IPC capture.** Extend interposition past sockets to shared
   memory, where much real HPC traffic moves.
5. **Single-writer collector.** Replace `flock`-on-shared-storage with a local
   collector daemon, making capture portable to backends whose POSIX advisory
   lock behaviour isn't verified (e.g. NFS).
6. **Branch executions / execution explorer.** Not started.

## License

[MIT](LICENSE).
