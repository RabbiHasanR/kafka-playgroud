# 002 — Consumer Groups, Rebalancing, and Partition Assignment: Tasks

Implements [requirements.md](requirements.md) per [design.md](design.md).

Each task cites the requirement IDs it satisfies and, where relevant, the design decision
it follows.

**Order matters here more than it did in 001.** T1–T4 are the protocol switch, and every
experiment from T13 on is run under a configuration that switch selects. Building the
runtime before the config builder means writing the consume loop twice.

## Configuration and the protocol switch

- [x] **T1** — Extend `Settings` in `config.py` with `consumer_instance_id` (defaulting to
  the hostname), `consumer_group_protocol` (defaulting to `classic`),
  `consumer_assignment_strategy`, `consumer_remote_assignor`,
  `consumer_session_timeout_ms`, `consumer_max_poll_interval_ms`, `handler_delay_seconds`
  (defaulting to no delay), and `consumer_instance_id_static` — the last treating an empty
  string as unset, because Compose interpolation yields `""` rather than removing the
  variable. Every default must leave 001's behaviour unchanged.
  — *R2.7, R2.17, R2.18, R2.23, R2.24, R2.28, R2.34* — D3, D4, D9, D10
- [x] **T2** — Implement `_build_consumer_config()` in `runtime.py`, branching on the
  protocol: `classic` sends `partition.assignment.strategy` and, when set,
  `session.timeout.ms`; `consumer` sends `group.remote.assignor` and **never** sends
  `session.timeout.ms` or `heartbeat.interval.ms`, which librdkafka rejects as broker-side.
  Both send `group.instance.id` when static membership is on and
  `max.poll.interval.ms` when set. — *R2.19, R2.20, R2.30* — D4, D10
- [x] **T3** — Validate the protocol/setting combination before constructing the
  `Consumer`, and fail with an error naming both the offending setting and the selected
  protocol. This must at minimum catch `CONSUMER_REMOTE_ASSIGNOR` under `classic` and
  `CONSUMER_ASSIGNMENT_STRATEGY` under `consumer` — librdkafka rejects only the second,
  and silently ignores the first, which would make an experiment report on an assignor
  that was never in effect. Exit non-zero without joining the group.
  — *R2.21* — D4
- [x] **T4** — Log the instance identity, the protocol, and the assignor actually in force
  in the startup banner, so a log excerpt pasted into a result carries its own
  configuration. — *R2.22* — D3, D4

## Runtime — membership and state

- [ ] **T5** — Add `on_assign`, `on_revoke`, and `on_lost` to `subscribe()`. Each logs its
  partition list with a stable marker, with revocation and loss distinguishable from
  assignment and from each other. **None of them calls `assign()`, `unassign()`,
  `incremental_assign()`, or `incremental_unassign()`** — the client applies the correct
  default for whichever protocol and assignor is in force, which is what keeps one callback
  body valid across all four combinations and what makes each partition resume from the
  group's last committed offset. — *R2.9, R2.10, R2.16* — D5
- [ ] **T6** — Re-key the fold store from `dict[str, OrderFold]` to
  `dict[int, dict[str, OrderFold]]`, partition first. Drop exactly the revoked or lost
  partitions' folds in the callbacks and retain the rest, and let a partition arriving
  without a fold produce the ordinary sequence-gap violations 001 already detects, rather
  than suppressing them. — *R2.14, R2.15* — D6, D7
- [ ] **T7** — Add the instance identity to every consumed-record log line, alongside the
  service name and record coordinates 001's logging requirement already puts there, so
  three interleaved streams from one group can be separated by filtering alone.
  — *R2.8* — D3
- [ ] **T8** — Apply the handler delay per event, after the handler returns and before the
  commit, so that a delayed handler holds up the poll loop exactly as a slow real handler
  would. — *R2.23* — D9
- [ ] **T9** — Guard the offset commit: catch the failure raised when the member no longer
  owns the partition, log it at `WARNING` with a stable marker **distinct from** 001's
  `VIOLATION` marker, and continue polling so the member rejoins rather than exiting.
  — *R2.26, R2.27* — D8

## Wiring

- [ ] **T10** — Replace `notification-consumer` in `docker-compose.yml` with
  `notification-consumer-1`, `-2`, and `-3` generated from one YAML anchor, sharing a group
  id and each carrying its own `CONSUMER_INSTANCE_ID`. Put `-2` and `-3` behind a
  `scale-out` profile so a plain `up` starts the group with one member and the others join
  while the logs are being watched, and wire the static identity as
  `${STATIC_MEMBERSHIP:+notification-N}` so one variable toggles it for all three. Change
  no broker configuration and no inventory or analytics service.
  — *R2.1, R2.28, R2.36* — D2, D10
- [ ] **T11** — Write `scripts/place_orders.sh`: place N orders against a running order
  service, printing the `order_id`, partition, and offset of each, with an optional flag to
  advance every order it created through `PACKED → SHIPPED → DELIVERED`. Extract the two
  response fields with `sed` rather than depending on `jq`. It must carry no rate control,
  no concurrency, and no throughput reporting. — *R2.31, R2.32, R2.33* — D11
- [ ] **T12** — Confirm the scaled-out group runs from the host against `localhost:9092`
  as well as inside the compose network against `kafka:19092`, changing only environment
  variables — three shells with three `CONSUMER_INSTANCE_ID` values and one shared
  `CONSUMER_GROUP_ID`. — *R2.35*

## Verification experiments

Each is run and observed, not merely coded. Tick only after actually running it, and
record what was seen in Results.

- [ ] **T13** — **Scale-out divides.** Place 20 orders with one notification instance, then
  start `-2` and `-3` and place 20 more. Confirm the three members hold disjoint partition
  sets, that every event was handled by exactly one of them, and that
  `kafka-consumer-groups.sh --describe --group notification-service --members --verbose`
  reports the same split the logs do. — *R2.2, R2.3, R2.11, R2.12*
- [ ] **T14** — **Fan-out survives scale-out.** Throughout T13, confirm inventory and
  analytics each still received every event, with lag returning to 0 — the 001 shape and
  the 002 shape running on one topic at once. — *R2.6*
- [ ] **T15** — **The partition count is the ceiling.** Start a fourth notification
  instance and confirm `--members --verbose` shows it in the group holding zero partitions,
  consuming nothing, without error. — *R2.4*
- [ ] **T16** — **Ordering survives scale-out.** With three members running, advance one
  order through all four events and confirm every one was handled by the same instance, in
  sequence order, because the key pinned it to one partition. — *R2.5*
- [ ] **T17** — **A rebalance loses the fold.** Advance several orders part-way, kill one
  instance, and confirm its partitions move to the survivors and that the receiving
  instance reports `SEQUENCE_GAP` for the in-flight orders on the partition it inherited —
  the same amnesia as 001's T35, now caused by routine scaling rather than a crash, while
  the offsets themselves resume correctly from the group's last commit.
  — *R2.13, R2.15, R2.16*
- [ ] **T18** — **Eager versus cooperative.** Run the same join-and-leave under
  `CONSUMER_ASSIGNMENT_STRATEGY=range` and under `cooperative-sticky`, and compare how many
  partitions were revoked from members that did not need to give anything up.
  — *R2.19*
- [ ] **T19** — **Classic versus KIP-848.** Run the same join-and-leave under
  `CONSUMER_GROUP_PROTOCOL=consumer` with `CONSUMER_REMOTE_ASSIGNOR=uniform`, and compare
  the rebalance log shape against T18's. Confirm from the startup banner which protocol and
  assignor were in force, and confirm that a consumer started with no protocol setting still
  joins as `classic`. — *R2.17, R2.18, R2.20, R2.22*
- [ ] **T20** — **Eviction of a live consumer.** Lower the poll interval (and, under
  `classic`, the session timeout with it — the client enforces
  `max.poll.interval.ms >= session.timeout.ms`), set `HANDLER_DELAY_SECONDS` past it on one
  instance only, and confirm that instance is removed from the group and its partitions
  reassigned **while its process is still running**, that its next commit fails with the
  distinct marker, and that it then rejoins rather than exiting. — *R2.25, R2.26, R2.27*
- [ ] **T21** — **Static membership.** `docker restart notification-consumer-2` without a
  static identity and count the rebalances the other two members log; repeat with
  `STATIC_MEMBERSHIP` set and confirm the member returns to the same partitions with no
  redistribution. — *R2.29, R2.30*

## Documentation

- [ ] **T22** — Write `docs/consumer-groups.md`: scale-out against 001's fan-out, what
  triggers a rebalance, how the two protocols differ in who computes the assignment, a
  runnable walkthrough of growing and shrinking the group, and a closing section naming
  which observed behaviours are accepted limitations and which spec closes each.
  — *R2.37, R2.39*
- [ ] **T23** — Amend §10 of `docs/concurrency-and-confluent-kafka.md`. Its bolded claim
  *"The broker does not compute the assignment — a client does"* is false under KIP-848,
  and its JoinGroup/SyncGroup table is classic-only. Scope both to the classic protocol and
  add the KIP-848 path beside them rather than deleting what is there. — *R2.38*
- [ ] **T24** — Add a `README.md` section for this feature: how to run the scaled-out
  group, how to grow it with the `scale-out` profile, the environment surface, and a link
  to the new document. Update the two existing forward references to 002 — in `README.md`
  and in the `docker-compose.yml` header comment — to describe what was built.
  — *R2.37*

## Results

To be recorded after the experiments are run, in the same shape as
[001's results table](../001-prepaid-order-service/tasks.md). Each row names what was
actually observed — partition assignments, member ids, rebalance counts, and the markers
that appeared — not what was expected.

| Task | Observed |
|---|---|
| T13 | — |
| T14 | — |
| T15 | — |
| T16 | — |
| T17 | — |
| T18 | — |
| T19 | — |
| T20 | — |
| T21 | — |

## Notes

**Why the config builder comes first.** T1–T4 are not preparatory scaffolding; they are the
switch every later experiment is run through. T18, T19, T20, and T21 each differ from the
others only in environment variables, so if the switch is wrong, four experiments produce
confident readings of a configuration that was never in effect. T3 exists specifically
because librdkafka will not catch that for us in the direction that matters.

**T6 is the task that makes T17 honest.** Keeping 001's flat fold dict would leave two
choices at a rebalance — drop everything, or keep everything — and both would fake the
result. Dropping everything makes cooperative assignment look identical to eager;
keeping everything hides the amnesia behind state that happens to still be correct. Only
partition-keyed folds let a revoked partition lose its state while the retained ones keep
theirs, which is what T17 and T18 are each measuring.

**T20 is 002's `force` flag.** 001 needed `force: true` because a service that guards its
own transitions never emits an illegal one to detect. 002 needs the handler delay for the
same structural reason: a member being thrown out of its group while alive and healthy
cannot be produced by `docker stop`, which kills the process, and cannot be produced by an
honest workload whose handlers only write a log line. Without the lever, R2.25 is
unreachable.

**Nothing here fixes the amnesia T17 exposes.** That is 003 for durable state and 007 for
state that migrates with the partition, per D7 and X3. A local fix inside this feature would
delete the evidence that motivates both.
