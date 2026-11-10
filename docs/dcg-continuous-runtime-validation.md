# Continuous D/CG runtime validation

This local work on `dbz-3025-dcg-followup` adds a continuous runtime adapter selected
only by a test connector. Production transactional D/CG streaming remains disabled.
F/CR PR #114 and its worktree are unchanged. Nothing is pushed or deployed.

## Runtime and recovery contract

`As400ContinuousRuntime` keeps one replay coordinator, normalizer, lifecycle
validator and transaction buffer alive across incremental polls. A poll begins
with the exact durable F/W returned by the previous acknowledged poll. A fresh
instance requires versioned offsets and recovered immutable binding history;
missing offsets, mixed scan/checkpoint fields or a different fingerprint reject
before source reading. The test connector separately rejects missing offsets for
an existing output/history generation.

F is the retained replay floor, W is the completed-output boundary, and R is the
volatile scan position. Restart rebuilds from F through W and the available suffix.
R is never persisted as a substitute for either recovery position. An idle poll
keeps the buffer, scans no previously processed entry, and publishes no new marker.
An unresolved clear retains its own transaction and history while unrelated
completed rows continue through the real Debezium dispatcher.

`As400OperatorCutReader.readAfter` revalidates the native journal and receiver
identities, reciprocal receiver linkage, recording profile, retained F/W anchors
and R on each interval. It reads only the unscanned suffix after R. R comparison
normalizes only the checked optional receiver-information wrapper: IBM's
[QjoRetrieveJournalEntries documentation](https://www.ibm.com/docs/ssw_ibm_i_74/apis/QJORJRNE.htm)
explains that this block appears on the first entry in a response or when the
receiver changes. A retained native pair demonstrates the same C/EC entry returned
as 256 versus 288 bytes. The comparison verifies the receiver name/library and exact `*SYSBAS` / ASP 1
identity, removes the supported 32-byte block, adjusts displacements, and compares
every remaining content byte. It also admits only zero fill outside the documented null-map and
entry-specific-data lengths, up to the next 16-byte boundary. Another retained
native pair shows the same R/PT entry as 292 versus 304 bytes with identical
content. Unexpected extra sections, nonzero fill and malformed lengths reject.
Changed source metadata, flags, null maps, payloads, overlapping sections,
wrong receivers and unsupported layouts still reject. This is not permission to
ignore a changed journal entry. The same
`As400JournalOrder` can extend its verified tail: overlapping identities and first
sequences must match, sealed ranges cannot change, and a new receiver must follow
the old range without a sequence reset. Earlier positions needed by live state
remain valid. No filename sorting or inferred gap continuity is used.

An attachment change during an unpublished read discards that interval and permits
at most two complete retries. Other identity, history, format, metadata, lifecycle,
row or publication failures stop the instance. Bounds are not eviction permission:
too much retained history, pending state or receiver history fails closed.

Every staged source record retains F/W recovery semantics. The test task places an
interval barrier after its output, waits for the qualified Connect commit callback,
and independently reads back the same source offset before polling again. Producer
acknowledgement alone is insufficient. Acknowledgement tracking retires only a
complete durable prefix, so continued polling does not accumulate a lifetime list
of old callbacks. The existing stricter acknowledgement tests and harness stay in
place.

## Native schemas and lifecycle

`As400NativeTableSchemas` builds Debezium key, value and envelope schemas from native
acquired definitions recovered from binding history. Snapshot and streaming use
the same full column model. The continuous connector does not use the old
integer-only output fixture. Supported effective codecs include bounded integer,
finite floating point, exact-scale decimal, supported CCSID character, binary and
nullable values. A complete nonnullable native primary key is required. Values
are owned before conversion; missing columns, wrong types, null required values,
wrong decimal scale or binary size and unsupported codecs reject rather than
coercing or dropping fields. Fixed text is not trimmed or sanitized. This profile
explicitly requires precise
decimals and byte values, and rejects custom converters, key overrides and column
transformations instead of silently ignoring those settings.

The test entry point reuses native initialization, qualified-session checks,
history-before-offset ordering and the existing Connect harness. The fresh fixture
controller owns all writers, DDL, maintenance and retention for its journal. Its
native locks and token do not implement a general production writer fence.
Operator-managed retention keeps every required receiver until final verification.

Terminal stop closes admission, cancels the instance's RPC reader, stops callback
admission and interrupts the source thread. JDBC handles in the continuous test
source also reject new work after terminal stop. Already admitted operations must
drain, and a stop request is reported separately from completed drain. No hard
native shutdown deadline is claimed.

## Reproduction and evidence

Run `mise exec java@temurin-21 -- tools/dcg-continuous-probe NEW_EVIDENCE_DIRECTORY DCNCdddd`
with a working legacy-mfa session and the aesql Python environment. The runner
requires a local Unix-socket Docker daemon and a fresh absent library on edgeqa.
Credentials enter worker/controller JVMs through stdin and usql through a
memory-backed configuration. They are not stored in Connect configuration or the
evidence logs.

The bounded profile uses one task, one partition per table topic, the previously
validated local Kafka timeout profile, explicit initialization and retained schema
history. The source loop has a 20-minute/1,000-poll test bound, a 16-receiver
history bound and the existing finite replay/buffer limits. This is not a load or
indefinite-retention qualification. Raw pages, incremental intervals, native
metadata, binding history, Kafka output/offsets, callbacks and runtime hashes are
retained under the run directory. Failure diagnostics consume Kafka independently.

Cleanup aborts only owned transaction controllers, removes owned local containers
and networks, verifies fixture object ownership, no pending transactions and no
file/journal locks, and drops the owned schema with RESTRICT. Any receiver inquiry
must match the exact owned receiver and the originating drop job before reply.
Independent queries verify library/schema/inquiry absence. Failure is reported
instead of assuming cleanup.

## Diagnostic attempts

`live-01` rejected a fixture SQL seed that used character hex literals for binary
columns. The runner now uses explicit binary literals as documented in the
[IBM i SQL reference](https://www.ibm.com/docs/en/ssw_ibm_i_74/pdf/rbafzpdf.pdf).
`live-02` and the page-instrumented `live-03` retained the scan-anchor rejection
until the optional receiver wrapper was established from native bytes and the API
contract. The pair is a regression fixture at
`src/test/resources/native-transactions/incremental-anchor.json` in the connector
module. These failures are not claimed as successful continuous validation.
`live-04` established the zero-alignment-fill difference and retained its separate
regression fixture (`incremental-anchor-padding.json`).
`live-05` delivered the exact mixed-column update, then failed a harness assertion
that expected a nonexistent public `source.table` field. The assertion now checks
the connector's existing receiver/library/sequence fields; table identity remains
in the topic, key and validated native binding. No public source envelope fields
were changed. The legacy envelope still omits table/schema fields and retains its
existing database-name placeholder; this experiment makes no new claim for them.
All five diagnostic libraries (`DCNC7101`–`DCNC7105`) and local resources were
removed and independently verified absent.

## Completed bounded validation (2026-11-07)

The final `live-07` run, including the Advisor correction below, passed all 47 assertions against fresh edgeqa library
`DCNC7107` and local Kafka 4.3.1. It used autonomous polling, with no controller
`.read` requests and no custom owner or receipt gates in the publication path.

- Snapshot and streaming preserved acquired mixed columns, native keys, exact
  decimal scale, fixed/variable CCSID text, binary values, large signed integers,
  finite floating point, nullable values and exact update before/after images.
- While B's clear/reload remained open, A inserts and M's mixed-column update
  reached Kafka. B's truncate and reload stayed hidden until commit. Both subtype
  8 TRUNCATE and subtype 3 optimized DELETE passed commit and rollback cases.
- A receiver rotation retained F at `QSQJRN0002:65` while W advanced to
  `QSQJRN0003:82`. A fresh task rebuilt the open transaction across that boundary,
  delivered a new unrelated row, and did not repeat completed rows or snapshot.
- At the committed clear, durable F=W was `QSQJRN0003:84` while scan R was
  `QSQJRN0003:85`; the saved evidence distinguishes scan progress from completed
  output. The replacement published the truncate before its reload rows.
- A held old truncate producer send was fenced by Kafka after reassignment.
  Independent consumers found exactly unchanged output and offsets when it was
  released. A legitimate subsequent task generation resumed normally.
- Deleting the test connector's local Kafka source offset while keeping its
  history/output caused startup rejection with zero output and unchanged history.
  Terminal source/JDBC drain was observed separately from the stop request.
- Each task generation processed each incremental entry once. Restart legitimately
  reread retained history; this assertion does not forbid replay after restart.

The final fixture's schema, library and receiver inquiries were independently
verified absent. All seven attempts, including the diagnostic failures and the successful
pre-review `live-06` run, passed both native and local cleanup. No shared auction library was used.

The full Maven verification passed 2,385 tests across 59 distinct classes, with no
failures, errors or skips. Assembly, formatter/import validation and whitespace
checks also passed. Counts are taken from the current verification log, excluding
duplicate module executions and stale report files. The earlier stricter Kafka
experiments were preserved; their complete live matrices were not rerun here.

Evidence root on this workstation:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-operator-retention/20261107/continuous-runtime/`.
The final `live-07/report.json` links native captures, raw incremental pages,
metadata, output/offset readbacks, fault records and cleanup. `verify-06-advisor-fix.log`,
`package-03-advisor-fix.log` and `tests-summary.json` record local verification. `audit.json`
checks staged source/production bytecode hashes, test-entry exclusion from the
packaged plugin, protected worktree contents and all cleanup reports. `turn.patch`
includes the changes from this task, including untracked files, without conflating
them with the branch's preexisting implementation.

## Independent review and correction

Advisor inspected the actual files and retained evidence. It found that receiver
wrapper normalization checked name/library but discarded the ASP device and
number without validating them. A changed ASP number could therefore compare as
the same anchor. The fix requires the exact padded `*SYSBAS` device and ASP number
1 before removing the wrapper, matching the restricted native profile.

Fifteen new negative cases reproduced the defect before the fix, covering every
device-name/padding byte and unsupported ASP numbers. They now reject in either
comparison direction, including two equally invalid wrappers. The native optional
wrapper and alignment-padding positive cases still pass. The complete suite then
passed 2,385 tests, and assembly/formatter/import checks passed. The review was
called once; Advisor did not rerun tests, and there is no second independent
review of the correction. The corrected artifact then repeated all 47 native/Kafka
checks in `live-07`, with cleanup verified. `advisor-review.json`, `advisor-asp-red.log`,
`verify-06-advisor-fix.log` and `package-03-advisor-fix.log` retain that sequence.

## Remaining production gates

Production task selection and transactional guards are unchanged. This bounded
experiment cannot qualify a general writer/maintenance fence, arbitrary schemas
or DDL, native release/PTF portability, other Connect modes, multiple tasks or
partitions, broker-loss durability, independent restore detection, automatic
retention release, sustained-load behavior or downstream truncate handling.
Per-poll Kafka transactions are not a claim that an entire native transaction is
atomically visible to a sink. These support and integration gates remain separate
from continuous local-source recovery.
