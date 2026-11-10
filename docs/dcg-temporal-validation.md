# Native temporal columns and portable validation

This revision adds temporal codecs to the configured D/CG path. It does not change
the default `dcg.mode=disabled`, the legacy transactional-clear guard, or F/CR PR
#114. Tests use newly created isolated IBM i libraries and local Kafka only.

## Representation and admission

IBM i distinguishes physical storage from the returned record buffer. Captured
DATE is 4 storage bytes but 10 ISO EBCDIC characters in the journal; TIME is 3
storage bytes but 8 characters. The catalog names timestamp `TIMESTMP`; its
storage is `7 + floor((precision + 1) / 2)`, while the buffer is 19 characters
at precision 0 and `20 + precision` otherwise. Native precision belongs in the
schema's scale, not its buffer-length field.

Acquisition checks both lengths, field positions, precision, format/separators
and CCSID before binding. Explicit ISO DATE/TIME and TIMESTAMP(0–9) with CCSID
37/500/1140 are admitted. The raw format is `YYYY-MM-DD`, `HH.MM.SS`, or
`YYYY-MM-DD-HH.MM.SS[.fraction]`. Snapshots explicitly project temporal values
with `CHAR(column, ISO)` for DATE/TIME and `CHAR(column)` for TIMESTAMP and use
the same parser. No timezone-sensitive JDBC date object enters normalization.
Bindings persist the effective codec parameters in ordinary schema history.

Values are immutable LocalDate, Duration or LocalDateTime, then converted through
standard Debezium logical types: Date, MicroTime, Timestamp (precision 0–3),
MicroTimestamp (4–6) or NanoTimestamp (7–9). Null indicators remain authoritative;
invalid/defaulted/minimized fields cannot become null or fabricated defaults.
TIME 24:00:00 remains 86,400,000,000 microseconds. Timestamp hour 24 is rejected
until separately qualified; it is not normalized into another day.

DATE and TIMESTAMP(0–6) support years 0001–9999. TIMESTAMP(7–9) has a narrower
epoch-nanosecond range. The current shared converter rejects the partial lower
boundary day even when the final signed-long value would fit; the admitted range
starts at 1677-09-22T00:00 and ends at 2262-04-11T23:47:16.854775807, further
restricted by declared precision. Prevalidation runs outside nullable-conversion
fallback so this limitation stops capture rather than replacing a value with
null. Precision 10–12, unsupported layouts and out-of-range values fail closed.

## Evidence and reproducibility

The portable runner takes an owner-only `IBMI_TEST_CONFIG` JSON file with explicit
host, user, password, expected server and isolated-write permission. No private
AWS/aesql package is imported. See [contributor testing](dcg-contributor-testing.md).

Raw captures came from fresh libraries DCNC9382–DCNC9385. The first attempt hit
its query timeout; the second correctly rejected pointer-backed DDL history.
Subsequent bounded captures began after the known table creation. Every capture
attempt verified cleanup. TIMESTAMP(12) was captured separately as unsupported
precision evidence; the admitted-schema fixture contains only precision 0/3/6/9.

Offline tests replay actual bytes through strict decoding, owned normalization,
binding-history round trips and shared output schemas under UTC, America/Chicago
and Asia/Tokyo. Negative cases cover mismatched or absent metadata, nullability,
invalid dates/times, fractional precision loss and signed epoch overflow. The
existing defaulted/minimized/pointer and identity guards remain unchanged.

The native temporal runner reuses the packaged connector harness and adds temporal
snapshot/streaming equality, clear/reload commit and rollback, unrelated progress,
restart with an open temporal clear, durable F/W, receiver rotation, raw journal
audit, missing-offset rejection and exact fixture cleanup. It uses the distribution
plugin; test transaction controllers remain outside the worker plugin path.

Both temporal runs passed all 47 assertions. The first used DCNC9386; the final
rebuilt plugin used DCNC9387 and includes the nullable-converter boundary check.
All 139 production Java files and the distribution archive hash match the final
run's staged evidence. An independent raw audit confirms two actual subtype-8
clears on M in each run, with nonzero commit cycles and native thread IDs.
Subtype-3 commit/rollback checks remain in the reused integer-table cases; this
does not claim a separate temporal optimized-DELETE capture.

Local `verify -DskipITs` passed 2,312 tests; the optional historical safety profile
passed 2,707 tests in total, including those default tests. Assembly packaging,
formatter/import validation and the four offline native-runner safety tests passed.
The new temporal coverage adds 30 tests. No broad Kafka fault-matrix rerun is
claimed; the packaged native experiment exercises actual output, offsets,
reassignment and restart with local Kafka.

All seven owned libraries (DCNC9381–DCNC9387), their receivers and attributable
inquiries were removed with absence checks. Both temporal runs also verified
local Docker cleanup. The temporary credential file was removed after cleanup;
the retained text evidence passed a password scan. No production access, push,
deployment or F/CR change occurred.

An alternating local JVM comparison used the unchanged real-task assertions,
40 measured samples per side after warmup, with both native experiments already
cleaned up. Median 5,000-row interrupted-reload/restart time increased from
362.8 ms to 390.7 ms (+7.7%); median 30,000-row snapshot time decreased from
1,712.6 ms to 1,609.6 ms (-6.0%). The earlier artifact's class files match the
pre-temporal portable-smoke plugin. These results retain the observed restart
cost; they do not establish performance parity or production throughput. The
fixture benchmarks exclude native/broker latency, and the unchanged integer
scenarios do not measure temporal-heavy data throughput. Temporal codecs are
cached and add no native query per record.

Local reports, raw responses, source/plugin hashes and verification logs are under
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-temporal-validation-20261108/`.
Report status and source hashes define the scope of each run; a running or failed
experiment is not qualification. This does not qualify every IBM i release/PTF,
CCSID, temporal primary-key layout, external converter or production deployment.

The physical catalog definitions follow IBM's
[SYSCOLUMNS2 documentation](https://www.ibm.com/docs/en/i/7.6.0?topic=views-syscolumns2).
Captured buffers establish the specific journal representation; documentation
alone is not used as native relationship or recovery proof.
