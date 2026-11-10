# Fail-closed legacy row processing

This change closes the row-error gap found by the
[local online-boundary validation](dcg-online-boundary-local-validation.md).
It changes one production file, `As400StreamingChangeEventSource.java`.
It does not enable migration, add a barrier to normal startup, change F/CR
handling, or repair the separate autocommit transaction-metadata limitation.

## Behavior and compatibility

The legacy RPC loop invokes the record consumer before advancing its journal
position. Previously, the consumer caught and logged dispatcher/decoder errors,
even when Debezium's dispatcher had thrown because processing mode was `fail`.
It also ignored the dispatch result. Returning normally allowed the RPC loop to
move past the failed row.

Selected row processing now follows the existing
`event.processing.failure.handling.mode` setting:

| Setting or condition | Result |
| --- | --- |
| `fail` (default): row decode/processing exception | Raise a row-processing failure with the original cause. The streaming loop reports it to the error handler and returns before RPC advancement. |
| `fail`: rejected row dispatch (`false`) | Stop through the same error-handler path. |
| Explicit `warn` | Continue past the failed row, with warning logging for decoder failures; the dispatcher retains its own warning behavior. |
| Explicit `skip` | Continue past the failed row, with debug logging for decoder failures; the dispatcher retains its own skip behavior. |
| Table exclusion, configured skipped operations, unchanged-update suppression | Continue normally. These intentional omissions are not processing failures. |
| RPC `IOException`, non-row `IOException`, or `SQLNonTransientConnectionException` | Preserve the existing reconnect/retry path before position advancement. |
| Row-decoder `IOException` | In `fail`, report to the error handler before advancement, just like other decoding failures. In explicit `warn`/`skip`, preserve the legacy local retry classification rather than silently skipping an error that previously retried. |
| Non-row controls/schema messages | Preserve their existing handling. |
| Truncate failures | Preserve the existing strict truncate behavior, including under `warn`/`skip`. |

The standard error handler retains the original cause and its existing retry
classification. Reporting an I/O cause can therefore request a task restart; the
failed row is not acknowledged or skipped. No custom retry policy is introduced.

The row cases include before-images, after-images, both insert journal types,
deletes and rollback deletes. Conversion handling remains separately controlled
by `event.converting.failure.handling.mode`; this patch does not override its
explicit lossy modes. A successful dispatch adds no extra decoding, schema lookup
or native round trip. The configuration check for a rejected dispatch runs only
when the dispatcher returns `false`.

The test-only publication barrier now uses the existing strict dispatch-policy
validator. It rejects non-`fail` processing/conversion settings, skipped operations,
unchanged-message suppression and invalid heartbeat settings before looking up
source offsets or starting the fixture. Explicit `warn`/`skip` remain supported
for ordinary legacy CDC but cannot certify a complete-output migration barrier.

A failed key change can already have placed its delete subrecord in the local
queue. This patch does not retract queued records or add atomic transactions to
ordinary legacy CDC. The local Kafka experiment separately verifies that the
candidate connector-defined transaction boundary keeps that prefix uncommitted
when the task fails before its barrier. That guarantee requires the qualified
Connect EOS profile and read-committed consumers; it is not a new guarantee for
all legacy deployments.

## Regression coverage

`As400LegacyRowFailureTest` exercises the actual legacy consumer, dispatcher and
RPC advancement loop, with controlled journal/JDBC inputs and no source connection.
Its 31 cases verify:

- Decoder errors (including malformed-data `IOException`), checked non-connection errors, actual dispatcher conversion
  errors, rejected dispatches and the original null transaction-context error
  reach the streaming error handler with their cause intact.
- A dispatcher failure after the first key-change subrecord leaves the journal
  position before the failing entry. No subsequent entry or position lookup occurs.
- Explicit `warn`/`skip`, intentional filtering/skips, successful row metadata and
  lagging offset behavior, and the existing transport-exception path remain intact.
- The test-only barrier rejects lossy processing/conversion settings before any
  offset lookup.

`tools/dcg-legacy-boundary-probe` preserves its previous ambiguity, crash and
reassignment scenarios and adds two failure schedules. One changes the encoded row payload length at entry 42 and lets the real strict
decoder reject the malformed bytes with `IOException`. The other holds the dispatcher after the first key-change
subrecord, independently observes that subrecord in Kafka with
`read_uncommitted`, and then releases a dispatch failure. Both require the error handler to fail the task or restart it from the original
durable offset, scan R still at 40, the original durable offset still at 39, no committed
data/control output, and no record/barrier acknowledgement. This checks the real
broker boundary rather than treating queue insertion or a callback as durability.

The publication barrier remains test-only. The native inventory, consistent
nonempty transaction bootstrap, receiver pagination and combined native handoff
gates described in the earlier report remain outstanding. This patch does not
turn synthetic fixture coordinates or acknowledgements into native proof.

## Verification evidence

Evidence is retained under
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-legacy-fail-closed-20261108/`:
`targeted.log`, `verify.log`, `package-validation.log`, `tests.json`, the `kafka/`
record/offset captures and logs, the exact turn patch, and preservation/verification
manifests. Run the ordinary Maven unit suite and
`mise exec java@temurin-21 -- tools/dcg-legacy-boundary-probe NEW_EVIDENCE_DIRECTORY`
to reproduce the local tests. No IBM i credentials or connection are used.

The initial full Kafka matrix passed 47 checks, including all previous restart
scenarios and both new processing-failure schedules. That run used an injected
`IllegalArgumentException` for the malformed-row schedule. A final focused rerun
uses the stricter real malformed-payload `IOException` case after refining the
production handler's I/O classification. The initial patch/plugin and the final
patch/plugin are retained separately; the initial full matrix is not mislabeled
as a run of the later build. Set `DCG_LEGACY_FAILURES_ONLY=1` when running the same
wrapper to select only the two failure schedules.

A first focused attempt exposed an overly restrictive harness expectation: a real
malformed-data `IOException` can cause Debezium to restart the task rather than
leave it permanently FAILED. That attempt is retained under
`kafka-final-failures/`. The final harness surfaces the queue's actual exception
instead of replacing it with a generic fixture exception, and verifies either
terminal failure or a fresh task loaded from the unchanged durable offset. All
no-output/no-barrier/position assertions remain required. It records the actual
error outcome separately for review.

## Final result

All 2,426 unit tests passed, including the 31 targeted row-failure/compatibility
cases. The final focused Kafka run (`kafka-final/`) passed all 10 assertions;
its decoded error is the real strict decoder's `IOException`, and its recorded
outcome confirms automatic restart from the unchanged durable offset. The partial
dispatch case fails without acknowledging any prefix or barrier. Both passing
runs and the intervening interrupted harness attempt verified owned-resource
cleanup. Maven verify/assembly and formatter/import validation passed.

The final staged test plugin matches the final compiled Java classes. The F/CR
method is byte-identical, and the protected F/CR, original truncate and D/CG
worktrees retain their files, HEADs and staged entries. No migration admission,
push or live IBM i connection/mutation occurred.
