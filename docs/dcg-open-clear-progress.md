# Unrelated progress while a SQL clear is open

Status: native capture and local component validation, 2026-11-07. The user
rejected a connector-wide metadata pause. The ordinary candidate must buffer the
uncommitted transaction while delivering other committed transactions, including
cycle-zero rows. This is a transaction boundary, not a table-only filter: every
captured operation in the open transaction remains withheld until its outcome.

Transactional streaming remains disabled. No production source task or Kafka sink
was run for this experiment. F/CR PR #114 is unchanged. The previous whole-cut
candidate is still conservative and still fails its table observations during an
open clear; this work does not bypass that guard or declare it ready to ship.

## Native experiment

The owned edgeqa IBM i 7.5 library `DCOC1107` contained A and B with one nonnullable
integer primary key. The controller retained the journal's existing fixed fields,
enabled thread recording before transactions, and used an owned receiver without
RMVINTENT. The fixture-only baseline was sequence 29. Baseline establishment here
is not a production snapshot or transaction-census protocol.

The controller opened a clear/reload transaction on B, inserted independently into
A, retrieved C/D/F/R records while the clear was still open, and only then committed
or rolled back B. Six parameterized metadata queries ran on separate bounded
connections during the open transaction. All queries returned the expected single
row before the clear and after its terminal outcome.

| Query during either open clear | Observed result |
| --- | --- |
| SYSTABLES exact mapping | One row in about 0.5 seconds; no physical member/format proof |
| SYSFILES exact format | Read timeout after about 15.4 seconds |
| IFS_OBJECT_STATISTICS exact file and member paths | SQL0443, path not found, ERRNO 3029 |
| SYSMEMBERSTAT and SYSPARTITIONSTAT | No rows after about 11 seconds; some calls returned SQL0462 warnings |

A five-second statement query timeout did not bound these calls to five seconds.
The receive timeout and diagnostic transport settings were local to the probe;
production transport settings were not changed. Missing current observations are
not proof of removal, and SYSTABLES alone cannot replace physical identity checks.
Other native member APIs have not been exhausted by this experiment.

Raw journal reads succeeded during both open transactions:

| Case | Open cut before terminal | Later terminal |
| --- | --- | --- |
| TRUNCATE B, reload ID 100, commit | D/CG subtype 8 at 38; B insert at 40; independent A ID 8 at 42 | C/CM 43, C/EC 44 |
| Optimized DELETE B, reload ID 200, rollback | D/CG subtype 3 at 53; B insert at 55; independent A ID 9 at 56 | Compensation 58, C/RB 59, lifecycle tail through 64 |

The metadata gap does not prevent the native reader from seeing independent
committed changes. It blocks the candidate's requirement to recollect every table's
current identity/layout before accepting a cut.

## Local replay and historical-binding experiment

`As400OpenClearProgressTest` uses the actual native records through the strict row
decoder, normalizer, ordinary journal lifecycle, transaction buffer, replay
coordinator and real Debezium dispatcher. No sequence, thread ID or commit-cycle
substitutions are used. Producer header fields and header JIDs are anonymized;
payloads remain captured bytes. Repeated observations can have different retrieval
headers/padding; the fixture keeps the first actual encoding of each sequence and
checks that its fixed source fields agree with later observations.

The controlled fixture supplies the table bindings. B's binding was observed before
the clear; A's was observed afterward with only controller-owned A inserts in the
interval. The integer codec comes from the owned DDL. This is not proof that a
production worker can safely discover or recover historical bindings.

- Through the first open cut, output contains only A ID 8. F remains at transaction
  start 31 (inclusive), W advances to 42, and volatile R reaches 42.
- A fresh instance reconstructed from serialized F/W emits B truncate then B ID
  100 after commit, without repeating completed A ID 8. W becomes 43.
- Through the second open cut, A ID 9 is delivered while B remains buffered.
  F is 46, W is 56. A fresh instance through rollback emits no B data; W becomes 59
  while R can reach the lifecycle tail at 64.
- Restart at every native entry preserves the same final output. Interrupted B
  batch delivery retains F=31/previous-W=42, replays the whole incomplete batch,
  and does not repeat the independently completed A row. This is component
  at-least-once behavior, not a new Kafka exactly-once claim.
- Missing bindings and mismatched physical-file or member JIDs reject before
  publication and leave W unchanged. Existing unsupported-transition, savepoint,
  nullable/defaulted/minimized-row and strict-profile tests stay intact.

An additional test writes the bindings and column schema with Debezium's existing
`JsonTableChangeSerializer`, using standard table attributes for file/member JIDs,
native format and the fixture's explicit codec identifier. A replacement reads the
serialized history and checkpoint from separate files, reconstructs the bindings,
and recovers across the still-open clear without current table metadata queries.

This proves **format feasibility only**. It does not exercise `SchemaHistory`
storage, Kafka history durability, historical epoch selection, a general native
codec serializer, or crash-safe initialization. Prefer these existing Debezium
facilities over a new external binding registry, subject to those remaining tests.

## Required runtime change and acceptance

The next implementation is a separate ordinary-profile binding-history adapter:

Update: the [binding-history adapter and local Kafka validation](dcg-binding-history-validation.md)
now implement persistence/recovery and a separate reader path. The original native
experiment below remains the evidence for source visibility; live task integration
and consistent initialization are still required.

1. At explicit initialization, establish and durably store the physical file/member
   association, single-member scope, native format, full effective codecs, column/key
   mapping and supported administration restrictions at a qualified boundary.
   Persist that history before a dependent F/W checkpoint can be accepted.
2. On resume, load the exact binding epoch required by F/W and fingerprint it with
   the effective capture policy. Reject absent, conflicting, incomplete or future
   history; do not rediscover current metadata as a substitute for lost history.
3. During replay/streaming, use those retained bindings and existing native
   identity/lifecycle/schema checks. Keep current metadata queries out of the
   open-clear publication path. Continue validating journal/receiver identity and
   retained history; never skip an unverified entry or move F past an open cycle.
4. Validate a fresh replacement while B remains open, continuing A output before
   B's terminal record, through the actual source task and Kafka. Cover both commit
   and rollback, failed output, receiver rollover, missing history, changed schema,
   changed JIDs, and stop/reassignment. Retain the existing stricter tests.

Do not implement this as a catch-and-ignore around missing metadata or as a timeout
that fails the whole task. Unknown identity/schema/history still fails closed.
Normal capacity/backpressure limits remain: an unbounded transaction can exhaust a
bounded buffer, and its pinned F increases retention/restart work. No zero-lag or
unlimited-resource guarantee is implied by unrelated-transaction progress.

## Evidence and cleanup

Local evidence directory:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-operator-retention/20261107/`.
`live-03/raw-visibility-01/capture.json` and `.bin` files retain metadata responses,
actual thread IDs and raw cuts; the directory also contains the exact helpers,
classpath/class hashes, recording settings and fixture provenance. Credentials
remained in memory and Java stdin. `pause-integration` contains the local checks.

Independent final queries confirmed A=[1,2,3,8,9], B=[100], no file/journal locks,
and no pending receiver transactions. Owned inventory was checked before cleanup.
The controller dropped only A/B and `DCOC1107` using native RESTRICT checks. Only
the exact CPA7025 inquiries from that owned drop job were answered. Independent
schema, library and pending-inquiry absence checks passed in `live-03/cleanup.json`.

References: [SYSFILES](https://www.ibm.com/docs/en/i/7.5.0?topic=services-sysfiles-view),
[SYSMEMBERSTAT](https://www.ibm.com/docs/en/i/7.4.0?topic=services-sysmemberstat-view),
[SYSPARTITIONSTAT](https://www.ibm.com/docs/en/i/7.5.0?topic=views-syspartitionstat).
The serializer experiment uses the installed matching Debezium snapshot source
`5eea03dd2a5dd25ad7e8db16cc8c59109dac2da8`.

Maven verify, formatter/import validation, assembly packaging and diff whitespace
checks passed. The verify run executed 2,158 tests across 47 classes, including
43 new open-clear cases, with no failures, errors or skips. Test aggregation uses
only classes in this run's log, excluding stale Surefire reports. Production source
hashes are unchanged from the start of this continuation; the protected F/CR and
original feature worktree hashes and HEADs also match. Existing Kafka tests were
preserved but their external broker experiments were not rerun for these tests.

Advisor inspected the actual files and saved native evidence and found no
actionable defect in the component tests. File access succeeded. The review
explicitly leaves runtime progress and durable binding integration unimplemented
and unvalidated; it is not a production readiness approval. The single explicit
review is saved in `pause-integration/advisor-review.json`.

Update: [native acquisition and controlled initialization](dcg-native-initialization.md)
now acquire the bindings from IBM i, verify cross-user empty journal context under
a held initialization fence, prepare the snapshot before B, and persist/read back
history before the initial checkpoint. The isolated native qualification does not
remove the production Session/Connect handoff gate or enable transactional streaming.
