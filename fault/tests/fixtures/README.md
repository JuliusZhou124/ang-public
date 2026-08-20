# Fixtures

Every trace in this directory is a real recorded artifact, captured off a real
containerised Slurm cluster — not hand-assembled, and not produced by faking
`SLURM_*` environment variables. They are checked in as JSONL (the exact bytes
the hooks and the shim wrote) rather than as a derived SQLite snapshot, so the
tests ingest them at run time through the same `ingest_job_eventlog` /
`ingest_message_eventlog` paths a live system uses.

The cluster harness that produced `real_job_3` is checked in at
[`cluster/`](../../../cluster) — 8 worker nodes running stock `slurm-wlm`
21.08.5, no `--privileged`, with the ANG boundary hooks and the `LD_PRELOAD`
shim wired in. `real_job_1` and `real_job_2` come from a 2-worker variant of
that same harness: identical `Dockerfile`, hooks and `entrypoint.sh`, with a
`slurm.conf` listing 2 `NodeName` entries instead of 8. To reproduce them,
copy `cluster/` and cut the node list down to `node1`/`node2`.

## `real_job_1.eventlog.jsonl`

The Boundary EventLog a 2-worker cluster produced for a genuine cross-node
job:

```
cargo build --release -p cli           # from the repo root first
cd <2-worker cluster>
docker compose up -d --build
docker compose exec slurmctld srun --ntasks=2 --nodes=2 hostname
docker compose cp slurmctld:/var/log/ang/boundary/1.eventlog.jsonl - > real_job_1.eventlog.jsonl
docker compose down -v
```

This is what the real hooks (`ang job-event ...`, four one-line wrappers under
`cluster/ang-hooks/`) actually wrote when `node_allocated` / `task_started` /
`task_exited` / `job_exited` fired as genuinely separate OS processes on
genuinely separate containers, correlated by JobID/StepID/TaskID/node through
a shared Docker volume.

7 events: 2× `node_allocated` (one per node), 2× `task_started` / `task_exited`
(one per task, one per node), 1× `job_exited`.

## `real_job_2.eventlog.jsonl`, `real_job_2.node1.messages.jsonl`, `real_job_2.node2.messages.jsonl`

Same cluster shape as `real_job_1`, but the workload itself exchanges messages
instead of just running `hostname` — so a real captured message log can be
correlated with a real Boundary EventLog in one Trace Store, which is what
`recv_precedes_task_exited` needs in order to be tested against anything but a
hand-built fixture.

```
cargo build --release -p cli -p intercept --lib --examples   # from the repo root first
cd <2-worker cluster>
docker compose up -d --build
docker compose exec slurmctld srun --ntasks=2 --nodes=2 \
  --export=ALL,LD_PRELOAD=/usr/local/lib/libintercept.so \
  bash -c 'export ANG_MESSAGE_LOG=/var/log/ang/messages/${SLURMD_NODENAME}.jsonl; \
           if [ "$SLURM_PROCID" = "0" ]; then msg-receiver 0.0.0.0:9000 5; \
           else sleep 1; msg-sender node1:9000 5 50; fi'
docker compose cp slurmctld:/var/log/ang/boundary/1.eventlog.jsonl - > real_job_2.eventlog.jsonl
docker compose cp node1:/var/log/ang/messages/node1.jsonl - > real_job_2.node1.messages.jsonl
docker compose cp node1:/var/log/ang/messages/node2.jsonl - > real_job_2.node2.messages.jsonl
docker compose down -v
```

Task 0 (`node1`) runs `msg-receiver`, task 1 (`node2`) runs `msg-sender`; both
are the same real Slurm job (`SLURM_JOB_ID=1`), so the boundary and message
logs correlate through the job ID exactly as a live system's captures would.
The two message logs are checked in separately, one per node, rather than
hand-merged — that's the real shape two independently-`flock`ed containers
wrote, and `ingest_message_eventlog` doesn't care about file boundaries or line
order since `events_for_job` sorts by `timestamp_ms` at query time.
`fault/tests/real_merged_trace.rs` ingests all three.

18 events: 7 boundary rows + 11 message rows.

## `real_job_3.eventlog.jsonl`, `real_job_3.node{1..8}.messages.jsonl`

The headline trace, from the 8-node [`cluster/`](../../../cluster). `real_job_2`'s
18 events were too few for all five registered invariants to be reachable at
once; this one answers what the per-seed failure rate and per-invariant
breakdown look like against a trace big enough that they are.

```
cargo build --release -p cli -p intercept --lib --examples   # from the repo root first
cd cluster
docker compose up -d --build
docker compose exec slurmctld srun --ntasks=8 --nodes=8 \
  --nodelist=node1,node2,node3,node4,node5,node6,node7,node8 \
  --export=ALL,LD_PRELOAD=/usr/local/lib/libintercept.so \
  bash -c 'export ANG_MESSAGE_LOG=/var/log/ang/messages/${SLURMD_NODENAME}.jsonl; \
           if [ $((SLURM_PROCID % 2)) -eq 0 ]; then msg-receiver 0.0.0.0:9000 20; \
           else sleep 1; msg-sender node${SLURM_PROCID}:9000 20 20; fi'
docker compose cp slurmctld:/var/log/ang/boundary/1.eventlog.jsonl - > real_job_3.eventlog.jsonl
for i in 1 2 3 4 5 6 7 8; do
  docker compose cp node1:/var/log/ang/messages/node${i}.jsonl - > real_job_3.node${i}.messages.jsonl
done
docker compose down -v
```

Even task IDs (0, 2, 4, 6) run `msg-receiver`; the following odd task ID sends
to it — four independent pairs, 20 messages each. With `--nodelist` fixing node
assignment order, task *N*'s node is always `node(N+1)`, so a sender at task *N*
(odd) always targets `node${SLURM_PROCID}` — its own procid number names its
pair's receiver's node, and no coordination beyond that is needed.

189 events: 25 boundary rows + 164 message rows. This is the trace behind the
`failing: 147 (14.70%)` figure in the top-level README, reproduced in CI on
every push. `fault/tests/real_large_cluster_trace.rs` also ingests it directly
via `TraceStore`/`seed_search_stats` for fast, no-CLI-process coverage.
