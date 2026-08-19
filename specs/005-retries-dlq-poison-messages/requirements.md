# 005 — Retries, Dead-Letter Topic, and Poison Messages

**Status:** draft — awaiting approval
**Depends on:** [004-replication-acks-failover](../004-replication-acks-failover/requirements.md)

## Overview

The consume loop has no failure path. `runtime.py` says so in the type it declares —
*"Raising is not part of the handler contract at this spec."* — and the one failure it does
handle shows what the absence costs: a message that cannot be decoded is logged and **its
offset is committed anyway**, because the alternative is stalling the partition forever. That
is silent data loss, and it was the right call only because there was nowhere else to put the
message.

This feature builds the somewhere else, and in doing so separates three things the current
code cannot tell apart.

**A transient failure and a poison message are opposites.** A dropped database connection, a
downstream `503`, a lock timeout — retrying works. Malformed JSON, a schema violation, an
`order_id` that will never exist — retrying produces the identical exception every time. The
distinction is not cosmetic: retrying a poison message burns the whole retry budget on a
message that was doomed at attempt one, and never retrying a transient one throws away work
that would have succeeded 200ms later.

**In Kafka, giving up in place is not an option.** A partition is read in order, so a consumer
that keeps retrying offset 847 never commits past 847, and every message behind it on that
partition waits. One bad message poisons the whole partition — worse than in a queue, where a
bad message blocks only itself. This is why the message has to *move*, not wait.

**Retrying elsewhere buys throughput with ordering.** The message is published to a retry topic
and the source offset is committed immediately, so the main partition never stalls. The price
is that a committed offset stops meaning "processed", and a later event for the same order can
be folded before an earlier one that is still in the retry lane. That trade is the feature's
central lesson and is meant to be visible, not hidden.

**004 left a debt here and this feature pays it.** `min.insync.replicas` is unset, so `acks=all`
is satisfied by an in-sync set that has shrunk to one. 004 D8 deferred it explicitly, on the
grounds that honouring a refusal means building the retry path a `NOT_ENOUGH_REPLICAS` answer
needs. That path is this feature, so the producer's half is in scope here.

## Out of scope

Each is a later feature or deliberately deferred; none may be built here.

- **Tiered delay topics.** One retry topic carrying per-message delays can invert — a message
  waiting 120s stalls messages behind it that are ready in 30s. The production fix is one topic
  per delay value. It is deliberately left open so the stall is observable; the documentation
  names it as the fix.
- **Automatic DLQ draining.** The dead-letter topic is terminal by discipline. A process that
  drains it on a loop turns it into an unbounded retry topic and destroys the one property it
  is built for.
- **Alerting on dead-letter depth.** Named in the documentation as the missing operational half.
- **Unclean leader election** and a committed-data-loss demonstration — still open from 004.
- **Exactly-once and transactional retry** (008), where the retry publication and the source
  offset commit become one operation instead of two.
- Log compaction and tombstones (006); local state stores and changelog topics (007)
- Any change to the event contract, to the fold's shape, or to 002's protocol, assignor and
  membership levers
- Authentication, TLS, ACLs — the environment stays PLAINTEXT per 000

## User stories

**US-1** — As a developer, I want a handler to be allowed to fail, so that the difference
between "this broke" and "this can never work" is something the system acts on rather than
something only I know.

**US-2** — As a developer, I want a failing message to leave the partition immediately, so that
one bad order cannot hold up every order behind it.

**US-3** — As a developer, I want a bounded number of attempts with a growing delay, so that a
downstream outage is survived rather than either ignored or retried forever.

**US-4** — As a developer, I want a message that has run out of attempts to land somewhere I can
read, with enough context to tell which message it was and what went wrong, so that giving up is
recoverable rather than silent.

**US-5** — As a developer, I want to replay the dead-letter topic by hand after fixing the cause,
so that the failure is reversible and I can watch the idempotency 003 built absorb the duplicate.

**US-6** — As a developer, I want to make handlers fail on demand, so that every path above is
something I trigger rather than something I wait for.

**US-7** — As a developer, I want `acks=all` to mean more than one replica, and to see what the
producer does when the cluster cannot satisfy it, so that 004's open gap is closed rather than
carried forward.

**US-8** — As a developer, I want a document covering this feature end to end, so that I can
re-read later which failure paths exist, what non-blocking retry cost, and which gaps are still
open.

## Acceptance criteria

### Classifying a failure

- **R5.1** — THE SYSTEM SHALL classify every message-processing failure as either **retryable**
  or **non-retryable** before deciding where the message goes, and SHALL record which
  classification was applied.
- **R5.2** — IF a message cannot be decoded as UTF-8 JSON, or does not satisfy the event schema,
  THEN THE SYSTEM SHALL classify the failure as non-retryable and SHALL make no processing
  attempt beyond the first.
- **R5.3** — IF a handler raises an exception that is not declared as either kind THEN THE
  SYSTEM SHALL classify the failure as retryable.
- **R5.4** — THE SYSTEM SHALL permit a service handler to raise, and SHALL treat a handler that
  returns normally as having succeeded.

### The retry path

- **R5.5** — WHEN a handler fails retryably and the message has attempts remaining THE SYSTEM
  SHALL publish it to the retry topic carrying the originating service, the attempt number, and
  the earliest time the next attempt may run.
- **R5.6** — WHEN a message is published to the retry topic THE SYSTEM SHALL commit the source
  offset only after the publication has been acknowledged by the broker, and SHALL then commit
  it without waiting for the retry to be attempted.
- **R5.7** — WHILE a message is awaiting a retry THE SYSTEM SHALL continue consuming and
  committing subsequent messages from the partition it came from.
- **R5.8** — WHILE the message at the head of a retry partition is not yet due THE SYSTEM SHALL
  pause that partition and continue polling, so that waiting cannot exceed
  `max.poll.interval.ms` and evict the member.
- **R5.9** — WHEN the retry worker reads a message THE SYSTEM SHALL select the handler of the
  service named in that message's header, and SHALL fold the result into that service's
  consumer-group state rather than the worker's own.
- **R5.10** — THE SYSTEM SHALL read the maximum attempt count and the per-attempt backoff from
  the environment, defaulting to **3 attempts** and to backoffs of **30s** and **120s** for
  attempts 2 and 3 respectively.
- **R5.11** — IF a handler fails THEN THE SYSTEM SHALL leave that order's folded state
  unchanged, so that the fold reflects only messages the service actually processed.

### The dead-letter topic

- **R5.12** — WHEN a failure is non-retryable, or a retryable failure has exhausted its
  attempts, THE SYSTEM SHALL publish the message to a single dead-letter topic shared by all
  three services.
- **R5.13** — WHEN a message is published to the dead-letter topic THE SYSTEM SHALL carry, as
  headers, the consumer group and service that gave up, the original topic, partition, offset
  and timestamp, the number of attempts made, the exception class and message, and the time of
  failure.
- **R5.14** — WHEN a message has been published to the dead-letter topic and acknowledged THE
  SYSTEM SHALL commit the offset it was read from, so the partition it came from advances.
- **R5.15** — THE SYSTEM SHALL NOT consume the dead-letter topic as part of normal operation.
- **R5.16** — THE SYSTEM SHALL log each of scheduling a retry, exhausting the attempts,
  detecting a non-retryable failure, and publishing to the dead-letter topic under its own
  stable, greppable marker at WARNING or above.

### Replaying

- **R5.17** — THE SYSTEM SHALL provide a manually invoked tool that reads the dead-letter topic
  and republishes messages to the topic they originated from, preserving the message key, and
  SHALL report what it would do without publishing anything unless republication is requested
  explicitly.
- **R5.18** — THE SYSTEM SHALL allow that tool to be restricted to the messages one named
  service gave up on.
- **R5.25** — WHERE a dead letter's recorded error class is non-retryable THE SYSTEM SHALL
  exclude it from republication unless inclusion is requested explicitly, and SHALL report it
  as excluded rather than omitting it from the listing.

### Making it fail

- **R5.19** — THE SYSTEM SHALL read from the environment a lever that makes handlers fail for
  named orders, selecting between a failure that succeeds after a configurable number of
  attempts and one that never succeeds, and defaulting to not failing at all.

### The producer's half

- **R5.20** — THE SYSTEM SHALL set `min.insync.replicas` on the lifecycle topic from the
  environment, defaulting to **2**, so that `acks=all` cannot be satisfied by a single replica.
- **R5.21** — THE SYSTEM SHALL read the producer's retry count, retry backoff and message
  timeout from the environment, and SHALL log the values in effect in the startup banner
  alongside the `acks` value R4.8 already puts there.
- **R5.22** — IF the cluster cannot satisfy the topic's `min.insync.replicas` THEN THE SYSTEM
  SHALL retry the write for the configured message timeout and, if it still cannot be
  satisfied, SHALL fail the request naming the broker error and the partition it applied to.

### Configuration and documentation

- **R5.23** — THE SYSTEM SHALL read every setting this feature introduces from environment
  variables, and SHALL leave every default such that a producer or consumer started with none of
  them behaves as 004 recorded, except that a failing handler now retries instead of being
  impossible.
- **R5.24** — THE SYSTEM SHALL provide a document covering the retryable/non-retryable
  distinction, what non-blocking retry costs in ordering, the header contract of the retry and
  dead-letter topics, a runnable walkthrough of a transient failure recovering and a poison
  message reaching the dead-letter topic, and the manual replay path; and SHALL state in it that
  tiered delay topics, dead-letter depth alerting, and unclean leader election remain open,
  naming where each is closed. The known-gaps tables in `README.md` and `docs/replication.md`
  that name 005 SHALL be updated to match.

## Notes

**Why the retry topic is shared and the retry worker is separate.** A shared retry topic would
be wrong if the three service consumers subscribed to it — a message only inventory failed on
would be redelivered to notification and analytics, which already succeeded on it. They do not
subscribe. One worker in its own consumer group reads the retry topic and dispatches by header
(R5.9), which is why one topic suffices. The worker is a separate process because waiting is the
one thing the main consumers must never do, and a separate process means its waiting cannot
reach them.

**Why R5.11 produces new violations rather than suppressing them.** Leaving the fold unchanged
on failure means the *next* event for that order, arriving on the main topic while the failed one
is still in the retry lane, reports a genuine `SEQUENCE_GAP` under 001's R1.38. That is correct:
the service really has not processed the earlier event. Advancing the fold anyway would record
work that never happened and would make the eventual retry look like a duplicate. The warning is
the visible price of R5.7, and the documentation R5.24 requires must say so.

**Why an unclassified exception is retryable (R5.3).** The two mistakes are not symmetric. A
handler bug misfiled as retryable costs three attempts and then reaches the dead-letter topic
anyway. A genuine outage misfiled as poison is discarded on the first attempt. The cheaper
mistake is chosen deliberately.

**Why nothing drains the dead-letter topic (R5.15).** A process that replays it automatically
is an unbounded retry topic wearing a different name: the message that could not be processed
comes back, fails, and returns. The value of the topic is that it is terminal and someone has to
look. R5.17's tool defaults to reporting rather than publishing for the same reason.

**What replay demonstrates.** Republishing to `order-lifecycle` (R5.17) delivers the message to
all three consumer groups, not only the one that failed. The two that already succeeded absorb
it through 003's sequence guard and log `DUPLICATE_ABSORBED`. That is the at-least-once cost 003
recorded, arriving from a new direction, and 008 is where it goes to zero.

**Criteria count.** 24, against the roughly 12–15 the size budget recorded as
[X11](../../DECISIONS.md) sets. The overrun is the producer's half — R5.20 through R5.22 close
004's deferred `min.insync.replicas` gap — sitting on top of a consumer-side feature that is
already two mechanisms and a replay tool. `design.md` carries the sentence X11 requires.
