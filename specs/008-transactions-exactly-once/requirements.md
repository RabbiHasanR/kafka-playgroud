# 008 — Transactions and Exactly-Once Semantics

**Status:** draft — awaiting approval
**Depends on:** [007-local-state-stores-changelog](../007-local-state-stores-changelog/requirements.md)

## Overview

This rung has been booked since 003. [X4](../../DECISIONS.md) chose Postgres knowing it was the
wrong long-term answer, precisely so the **dual-write problem** would be impossible to miss: the
offset went to Kafka, the fold went somewhere else, and no single operation covered both. 007
collected half the bill — the fold moved to a compacted changelog topic, so both writes became
Kafka operations. It deliberately left the other half open. This is that half.

**Two different problems are open, and they need two different mechanisms.** Conflating them is
the most common way exactly-once is misunderstood, so this feature builds them as two things.

**The producer can duplicate and, worse, reorder.** All three producers in this repository set
`acks=all` and a retry count, and none of them enable idempotence. A retry after a lost
acknowledgement writes the record twice. A retry while later batches are already in flight
writes the records *out of order*. Duplication this project already absorbs — the sequence guard
of R3.11 exists for it. Reordering it does not absorb, and reordering is the worse failure here
because the entire domain is an ordered lifecycle: a `SHIPPED` that overtakes a `PACKED` makes
`is_legal_transition` report a violation that never occurred and makes the sequence guard report
a gap that never occurred. On the changelog it is worse still — an older fold overwriting a
newer one under the same key is preserved by compaction and faithfully rebuilt by `restore()`.
Silent, durable, and invisible to every marker this repository logs. An idempotent producer
costs one configuration line and removes both.

**The read-process-write cycle is not atomic.** A consumer writes the fold to the changelog and
then commits the offset. Two operations, a window between them, and 003's `STATE_CRASH_AFTER`
lever parked in that window on purpose. `handled_count` outruns `last_sequence` as a result —
the residue [X4](../../DECISIONS.md) predicted and 007 restated. A transaction covering the
produce *and* the offset submission closes it, and it can only be written now because 007 made
both sides Kafka operations. A database never could have offered this, which is the whole point
of the detour.

**A transaction is bounded by Kafka, and the boundary is the lesson.** What becomes exactly-once
is the *committed output*, not the *execution*. A redelivery after an abort runs the handler
again; the notification service prints its customer message again. What the transaction
guarantees is that nothing that ran twice was ever committed twice. This feature is required to
say so in its documentation rather than let the phrase "exactly-once" carry a promise Kafka does
not make.

**The local store sits outside the transaction, and that has consequences.** RocksDB is a disk
write, not a Kafka operation, so no transaction can roll it back. An aborted transaction
therefore leaves the local store holding folds that were never committed, and the only correct
repair is to discard those partitions and rebuild them from the changelog's committed records.
That in turn makes 007's checkpoint mode unsafe under this guarantee, and makes `read_committed`
mandatory on the restore reader. This is the part of exactly-once that configuration alone does
not buy, and it is why Kafka Streams wipes state stores on unclean shutdown.

**Everything here is a toggle, defaulting off.** The at-least-once path stays exactly as 007
left it, for the same reason `MemoryStateStore` was kept unchanged at 003: the before is the
control for the after, and a lesson with no control is an assertion.

## Out of scope

Each is a later feature or a gap this rung deliberately leaves open; none may be built here.

- **Making the retry worker's republish transactional.** It consumes the retry topic, produces
  back to the source topic and commits — the same shape the service consumers are getting fixed.
  It is left at-least-once on purpose: it is a second transactional producer with its own
  identity lifecycle and its own pause-and-seek logic, and it would add surface without adding
  a mechanism this feature has not already shown. Named as an open gap, not solved.
- **Closing the `POST /orders` boundary.** A client whose request times out after the broker
  accepted the write retries and produces a genuinely new event with a new `event_id`. No
  producer setting and no transaction can see that. The fixes are a client-supplied idempotency
  key or a transactional outbox, and this rung claims neither — `README.md` already says no spec
  does. The documentation explains why the boundary is outside every guarantee bought here.
- **Deduplicating by `event_id`.** 003 chose the sequence guard instead and placed dedup out of
  scope, pointing it here. It stays unclaimed: the transaction removes the duplicate *effect*
  that dedup existed to prevent, so a dedup table would now be a second mechanism for a problem
  already closed.
- **Making external side effects exactly-once.** The notification service's message is a log
  line standing in for an email. Nothing in Kafka can un-send it. The residue is documented,
  not removed.
- **Adopting Kafka Streams or any framework that would supply this.** The mechanism is the
  lesson ([X5](../../DECISIONS.md)); a framework is 009's business.
- **Broker configuration changes.** The transaction state log's replication factor and minimum
  in-sync set were pinned by R0.17 and by 004; this feature inherits them and alters neither.
- Any change to the event contract, to topic cleanup policies, to 002's protocol and membership
  levers, or to 005's retry and dead-letter routing beyond re-routing which producer publishes.
- Authentication, TLS, ACLs — the environment stays PLAINTEXT per 000.

## User stories

**US-1** — As a developer, I want every producer to be idempotent, so that a retry cannot
duplicate a record and — the part I actually care about — cannot reorder one.

**US-2** — As a developer, I want the fold write and the offset commit to be one transaction, so
that the dual-write window 003 opened on purpose is finally closed by the mechanism the whole
ladder was built to reach.

**US-3** — As a developer, I want a failed message's move to the retry or dead-letter topic to be
in the same transaction as its offset, so that "committed" stops meaning "no longer ours" and
goes back to meaning "safely elsewhere".

**US-4** — As a developer, I want to choose how many messages one transaction covers, so that the
throughput cost of exactly-once is a number I measured rather than a warning I read.

**US-5** — As a developer, I want to read the changelog with `read_uncommitted` and *see* the
records an aborted transaction left behind, so that I understand a transaction as a marker over
records that were really written, not as records that were never written.

**US-6** — As a developer, I want the local store rebuilt when a transaction aborts, so that I
meet the one part of exactly-once that configuration does not buy and understand why Streams
wipes state on unclean shutdown.

**US-7** — As a developer, I want a document covering what this guarantee does and does not
cover, so that I can say precisely what survives — and what still runs twice — instead of
repeating the phrase "exactly-once".

## Acceptance criteria

### The idempotent producer

- **R8.1** — THE SYSTEM SHALL enable idempotent production on every producer it creates —
  lifecycle and snapshot, retry and dead-letter, and changelog — such that the broker
  deduplicates a retried produce and rejects an out-of-order sequence, and SHALL do so without
  weakening the acknowledgement settings R4.7 established.
- **R8.2** — WHERE idempotence is disabled by configuration THE SYSTEM SHALL start with the
  producer settings unchanged from 007, so that the duplicate and reordering behaviour of
  001–007 remains reachable as a control.

### The transactional producer

- **R8.3** — WHERE the exactly-once guarantee is selected THE SYSTEM SHALL create exactly one
  transactional producer per consumer instance, carrying a transactional identity that is stable
  across restarts and derived from the consumer group and the instance id, and SHALL initialise
  transactions before consuming any message.
- **R8.4** — THE SYSTEM SHALL publish both the changelog record and the retry or dead-letter
  record through that single producer, so that one transaction can cover writes that are
  presently made by two independently constructed producers.
- **R8.5** — IF a producer is fenced — because another instance took its transactional identity
  — THEN THE SYSTEM SHALL log the fencing under a stable, greppable marker naming the identity
  and SHALL exit rather than retry, so that a duplicated instance id fails loudly instead of
  looping.

### The atomic read-process-write cycle

- **R8.6** — WHILE the exactly-once guarantee is in effect THE SYSTEM SHALL enclose the changelog
  write and the consumed offsets in one transaction, submitting those offsets through the
  transactional API together with the consumer's group metadata rather than committing them
  against the consumer directly.
- **R8.7** — WHILE the exactly-once guarantee is in effect THE SYSTEM SHALL enclose a failed
  message's retry or dead-letter write and its consumed offset in one transaction, such that an
  offset cannot advance past a message that was not published.
- **R8.8** — THE SYSTEM SHALL commit the open transaction when either a configured number of
  messages or a configured elapsed time is reached, whichever comes first, and SHALL treat a
  configured count of one as meaning one transaction per message.
- **R8.9** — WHEN partitions are revoked or lost THE SYSTEM SHALL abort any open transaction
  before releasing them, so that no offset is submitted for a partition the member no longer
  owns.

### Isolation and the state store

- **R8.10** — THE SYSTEM SHALL read with `read_committed` isolation on every consumer it creates
  — the three services, the retry worker, and the changelog restore reader — and SHALL allow
  `read_uncommitted` to be selected so that the records left by an aborted transaction are
  observable.
- **R8.11** — WHEN a transaction is aborted THE SYSTEM SHALL discard the local store of every
  partition that transaction wrote to and rebuild it from the changelog before consuming
  further, because a local store write is outside the transaction and cannot be rolled back.
- **R8.12** — IF the exactly-once guarantee is selected together with checkpoint-based rebuild
  THEN THE SYSTEM SHALL refuse to start and SHALL name both settings in the error, because a
  checkpoint asserts the store matches the changelog and an aborted batch makes that false.

### Levers, configuration and documentation

- **R8.13** — THE SYSTEM SHALL provide a crash point inside an open transaction, and SHALL leave
  the write-order and crash-point levers of R3.16 and R3.17 working unchanged on the
  at-least-once path.
- **R8.14** — THE SYSTEM SHALL read every setting this feature introduces from environment
  variables with no credential among them, SHALL default the exactly-once guarantee to **off**,
  and SHALL log the guarantee in effect, the transactional identity, the isolation level and the
  commit interval at startup on one stable, greppable line.
- **R8.15** — THE SYSTEM SHALL provide a document covering why reordering is the worse half of
  what idempotence prevents, what one transaction does and does not cover, why the local store
  must be rebuilt after an abort, what `read_committed` costs in latency and in offset
  continuity, and what remains at-least-once afterwards. The known-gaps rows in `README.md` that
  name 008 SHALL be updated to match, **including the correction that this rung drives
  `handled_count` to `last_sequence` for the durable fold but does not stop a handler from
  running twice.**

## Notes

**Why idempotence is a separate criterion from transactions rather than folded into them.**
Enabling transactions forces idempotence on, so R8.1 could have been left implicit. It is stated
separately because the two mechanisms protect different things and are reached for at different
times: idempotence is free, applies to the HTTP producer that will never be transactional, and
should be on in every service in this repository regardless of what the consumers do. Folding it
into the transaction criteria would teach that you only get it when you pay for the other thing.

**Why R8.4 exists as a criterion at all.** It reads like an implementation note, and it is the
single largest change in the feature. Every write inside one transaction must come from one
producer instance — but `LocalStateStore` and `FailureRouter` each construct their own today.
The criterion states the constraint rather than the refactor, so a later feature cannot
reintroduce a private producer inside a component that participates in transactions.

**Why the transactional identity is stable rather than random.** A random identity per process
start makes fencing useless: the restarted instance cannot fence the zombie it replaced, because
the zombie holds a different identity. Deriving it from the group and the instance id promotes
`CONSUMER_INSTANCE_ID` from a log field into a correctness-critical setting, and R8.5 is what
makes a duplicated one fail visibly instead of two members fencing each other in a loop.

**Why R8.11 is the interesting requirement.** R8.6 through R8.9 are the transaction API used as
documented. R8.11 is the part configuration does not buy: the guarantee covers Kafka operations,
and the local store is not one. Discarding and rebuilding is expensive and is the honest cost of
holding state outside the transaction — the same trade Kafka Streams makes, for the same reason.

**Why the offsets go through the producer instead of the consumer.** Committing against the
consumer would put the offset outside the transaction, which is the exact defect being fixed.
Submitting offsets with the consumer's group metadata is also what lets one producer per
instance be safe under rebalancing, rather than needing one transactional identity per partition.

**What is deliberately still open afterwards.** The retry worker's republish, the HTTP request
boundary, and every external side effect. The first is a decision recorded above; the second and
third are outside what Kafka can offer at all. R8.15 requires the document to name all three
rather than let the feature's title imply otherwise.

**Criteria count.** 15, at the top of the roughly 12–15 that [X11](../../DECISIONS.md) sets — the
same count 007 carried, and for a comparable reason: two mechanisms plus a store-recovery rule
that neither of them implies.
