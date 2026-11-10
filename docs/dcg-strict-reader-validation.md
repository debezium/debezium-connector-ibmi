# Native interruption with the strict row decoder: passed

The corrected `CompleteRowEntryDecoder` passed the bounded edgeqa interruption
experiment on 2026-11-07, in the test publication path. All 32 existing assertions
passed. The test produced `c:1`, `t`, `c:2`, `c:3` in order and five committed
checkpoints including the baseline. A separate Kafka CLI consumer using
`read_committed` matched every event and checkpoint.

The harness now publishes the values returned by the strict decoder. It has no
legacy decoder instance or fallback. A strict decode exception is recorded and
propagated before the row/checkpoint publication call. The new native rows all
produced the expected IDs through strict decoding. Their empty null maps occurred
at displacements 268 and 236, demonstrating that the decoder follows the header
rather than assuming a fixed map location.

## Native behavior observed

The real old COMMAND job was held, and its reader was observed blocked inside
`ServiceProgramCall.run()`. The replacement captured an insert, F/CR clear and
reload while the old reader stayed held. Kafka rejected the old diagnostic
producer after replacement initialization. Calling the real terminal `stop()`
then cancelled and drained the old reader without output or scan advancement.
The replacement retained its original native job/connection and captured another
insert after that stop. Its committed checkpoint did not regress.

The observed reader stop/drain took 232 ms. A fresh native job observation still
reported `*ACTIVE` immediately afterward; another observation within the bounded
wait reported `*OUTQ`. These are separate observations, not a guaranteed native
shutdown deadline. The stopped reader rejected reconnect without starting a new
connection. Both reader threads and transports were cleaned up; independent job
checks found the old job ended and the replacement job absent. No emergency
release was needed in this run.

## Fixture, broker and cleanup

The controller verified AEQA identity and absence of both the SQL schema and
native library before creating a new `DCTASKQA.NATREAD`. Its only column was
`INTEGER NOT NULL PRIMARY KEY`, and it had one member. Existing auctions and
DTEST were untouched. The local Kafka broker used a unique container/network and
a loopback listener. Credentials were passed in memory/stdin, and captured console
output was redacted.

Before cleanup, the library inventory and creation identities still matched the
objects created for this run, no table locks remained, and source rows were 2
and 3. After dropping the owned table with `RESTRICT`, its journal reported zero
journaled files, members and commitment definitions. The library cleanup's
unsaved-receiver inquiry was matched to the exact owned receiver and the actual
cleanup job, then answered only for message key `0002CFC0`. The DROP client
completed successfully. Independent queries confirmed that the schema, native
library and pending fixture inquiries were absent. The owned broker and network
were removed and their absence verified.

## Evidence and scope

Evidence directory:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-native-stop/strict-20261107/`.

- `probe/native-report.json`: all 32 assertions and separate native lifecycle times.
- `probe/row-*.json`: native row bytes and the strict values actually published.
- `independent-events.jsonl`, `independent-offsets.jsonl`: separate broker reads.
- `jobs-after-probe.json`, `source-cleanup.json`, `broker-cleanup.json`: completed
  resource cleanup, backed by the saved inventories, message and absence checks.
- `runtime-sha256.json`, `validation.json`, `change.patch`: actual runtime/source
  hashes, retained assertions and unchanged production/protected worktrees.
- `verify.log`, `package.log`, `advisor-review.json`: unit/build/package checks and
  the independent review result.

All 847 unit tests and assembly/formatter/import checks passed. Advisor inspected
actual files plus the native, independent broker, runtime-hash and cleanup
evidence and passed with no actionable findings. It did not rerun tests or live
operations. The packaged decoder matches the class used in the native run;
the test probe remains excluded from the production plugin.

This remains a standalone diagnostic publication experiment using ordinary inserts
and F/CR outside commitment control. It exercises the corrected strict row decoder
and real RPC cancellation/replacement, but not the production D/CG source adapter,
Kafka Connect reassignment, transactional clear commit/rollback, or a sink.
Diagnostic checkpoint identities are fixture tokens and F equals W for these
operations. The separate existing Connect/recovery tests and assertions are
unchanged; they are not reclassified as new live transactional evidence.

Production code and transactional streaming guards were unchanged in this turn.
The corrected decoder remains covered by 847 passing unit tests. Transactional
D/CG streaming is disabled, F/CR PR #114 is unchanged, and nothing was pushed.
The [earlier native run](dcg-native-reader-stop-validation.md) and
[local empty-map correction](dcg-null-indicator-validation.md) retain their own
historical evidence.
