# Native online census experiment

The user authorized a fresh isolated edgeqa fixture for the
[online census/log validation](dcg-online-boundary-validation.md). That permission
continues to apply to this experiment. It does not authorize production-cluster
access, existing auction-table mutations, publishing changes, or enabling migration.

## Current result: native collection and offline reconciliation passed, still unqualified

On 2026-11-09, read-only ODBC and JT400/JDBC preflights succeeded against edgeqa.
The fresh isolated `DCNC0913` workload completed all 13 schedules. Its 728 retained
journal entries passed the strict native decoder, normalization adapter and
transaction buffer locally. The independent SQL ledger matched 556 operations
and 419 completed batches. Every bracket/census audit was `capture-consistent`;
unrelated autocommit writes continued in every observation window (24–27 each).

The concurrent schedule completed 64 cycles. Eight complete commit/rollback
calls fell within the 255.793 ms definition-query call interval. This proves
client-call overlap only; it does not establish server enumeration order or a
complete concurrent inventory contract. Census sizes ranged from 171 to 176
visible definitions. Receiver rotation, clear commit/rollback, savepoints and
optimized DELETE were included. This was one bounded repetition, not a sustained
load or performance qualification.

Fresh-instance recovery and interrupted-delivery replay passed 41 sampled local
cases: 12 restart points, 12 false enqueue results, 12 enqueue exceptions, and
five missing-offset/history/identity rejection cases. These exercised the real
recovery coordinator, dispatcher and acquired schemas with modeled acknowledgements
and fixture startup identities. They did not use Kafka or a durable offset store.
The replay retained distinct scan R, replay floor F and completed output W.

The original live run report remains `failed`: collection and automatic cleanup
succeeded, but the first offline comparison incorrectly required identical native
transport bytes across reads. After the test-only comparison correction described
below, a separate offline audit of copies of the same captures reports
`captured-unqualified`. Original reports, raw bytes and raw SHA-256 values were not
rewritten. Every result retains `startup_proof=false`.

Both fresh fixtures used during this retry were removed and their schema, library
and owned inquiry absence verified: `DCNC0912` after a safely blocked first cleanup,
and `DCNC0913` by automatic cleanup. No existing auction fixture was used. Production
connector code, migration admission and F/CR are unchanged; nothing was pushed.

## Earlier attempt: blocked before fixture creation

Two ODBC preflights and an independent JT400/JDBC preflight failed during sign-on.
The bounded experiment was then invoked twice for fresh library `DCNC0911`; both
JDBC preflights failed at the same point. AWS legacy-MFA identity and credential
retrieval succeeded. ODBC reported communication error `08S01` / `CWBCO1047`;
JT400 reported `ConnectionDroppedException` while reading the port-mapper reply
during sign-on. Maintenance is a possible explanation, not a verified diagnosis.

No schema creation or workload SQL ran. The experiment report records an empty
attempt list and `fixture_cleanup.status=not-created`. There was no native fixture
to remove; this is not a claim that cleanup of a created fixture was live-tested.
The temporary owner-only credential configuration was removed.

No transaction-visibility, terminal-coverage, receiver-pagination or throughput
conclusion can be drawn from this attempt. There are no new native row captures
to replay through the strict decoder/normalizer/buffer. Production code,
transactional/migration admission, and the F/CR branch remain unchanged.

## Prepared test-only harness

`tools/dcg-online-census-probe.py` accepts an explicit `IBMI_TEST_CONFIG`, a new
evidence directory, a fresh `DCNCdddd` library and 1–10 repetitions. It reuses the
existing native fixture manager's absence checks, expected-server check, retained
receivers, fixed-field recording configuration, object ownership checks, lock
checks and verified cleanup. JDBC connectivity and cross-user census authority
are checked before fixture creation.

The controller uses separate fixture sessions for a silent open transaction,
changing transactions and unrelated autocommit writes. Its operation ledger records
the actual server jobs and operation intervals. A job-local query-option override
requests optimized DELETE; it does not modify the system QAQQINI configuration.
Prepared schedules cover completion between definition enumeration and resource
lookup, definition reuse, new definitions, late involvement of another table,
savepoints/zero pending changes, SQL clear commit/rollback, optimized DELETE and
receiver rotation. The local continuation below adds a bounded writer and timing
gate for cycle completion during the enumeration call itself. The current retry
above exercised it against IBM i. Concurrent creation/removal of commitment definitions and a
deliberately held in-progress rollback still need additional qualification;
neither the deterministic between-query hooks nor client-call timing establish
those stronger cases.

The census reads all visible commitment definitions, with no captured-table or
positive-pending-count filter, and looks up each definition's resource for the
fixture journal. Both authority checks, each query interval, native lock-space
and cycle IDs, states, resource results and errors are retained. Warnings,
timeouts, duplicate definitions, response overflow, unsupported distributed/XA
scope and incomplete observations reject the attempt. Zero pending changes or
100% rollback progress never act as transaction completion.

These fields follow IBM's descriptions of
[DB_TRANSACTION_INFO](https://www.ibm.com/docs/en/i/7.5.0?topic=services-db-transaction-info-view)
and [DB_TRANSACTION_JOURNAL_INFO](https://www.ibm.com/docs/en/i/7.5.0?topic=services-db-transaction-journal-info-table-function).
Those descriptions do not establish the required concurrent-enumeration guarantee.
The earlier blocked preflight could not recheck field availability; the current
retry successfully queried those fields.

`As400OnlineCensusCapture` is a read-only test entry point. It reuses native binding
acquisition, journal evidence collection and the real bounded `RetrieveJournal`
API. It stores exact readable anchors, owned native entry bytes, SHA-256 values,
actual header job/thread/JID fields, request bounds, native continuations and
receiver-profile observations. It uses no synthetic thread IDs. The stored bytes
are the existing reader's owned entry copies, with the next-entry link zeroed;
they are not the entire original service-program response buffer. The page report
retains entry counts, returned byte counts and continuation values separately.

The candidate oracle is a second capture across the controlled fixture
interval. Each earlier bracket must match the same entry positions and documented content
sections in that capture. Original raw checksums are verified independently; see
the transport-comparison correction below.
The audit then compares its crossing cycles with the union of census cycles and
bracket cycles. Missing starts or missing cycles reject. Two matching captures
cannot establish that neither omitted the same work. The subsequent
[offline replay and writer-ledger check](dcg-census-offline-ledger.md) now runs the
real lifecycle validator, strict decoder, normalizer and buffers before comparing
the controlled workload's row IDs/jobs, outcomes and committed changes with the
SQL ledger. Matching observations report `capture-consistent`; successful
collection still reports `captured-unqualified`, with the separate
`writer_ledger_replay` result. The earlier blocked attempts predate this addition.
A zero process exit denotes bounded evidence collection, controlled-workload
reconciliation and cleanup only. Every report keeps `startup_proof=false`.

## Bounds and remaining gates

The workload has a ten-minute deadline. Each census and raw capture has a
60-second budget; SQL queries have a five-second timeout. Census results are
limited to 256 definitions and one matching journal resource per definition.
Raw captures are limited to 16 receivers, 512 pages and 20,000 entries. The
native response buffer is 4 KiB for bracket reads and 1 MiB for the final capture;
buffer exhaustion supplies the native continuation. The remaining 20,000-entry
budget is a response rejection limit, not an entry-count pagination request.
No `NBRENT` criterion is sent. A record too large for the selected buffer rejects
the capture rather than being skipped.
The independent writer is bounded to 2,000 inserts. Cleanup has the existing fixture
manager's separate bounded drain/lock/inquiry checks and can reject rather than
delete when ownership or drain is uncertain.

This helper does not enable an online bootstrap or acquire an arbitrary legacy
checkpoint's complete transaction context. It does not execute a Kafka barrier,
the filtered legacy RPC/dispatcher pagination experiment, or a native-to-Kafka
handoff. After a successful capture, the retained bytes and native acquired
schemas must pass the new offline strict replay and writer-ledger check. The current native captures and offline fresh-instance/interrupted-output checks
pass, but they have not been composed with the existing Kafka publication-barrier
proof. Concurrent inventory completeness, retained historical bindings and the
combined migration handoff remain admission gates.
Finite successful races would remain qualification evidence, not a new platform
API guarantee. The existing missing-history, identity and savepoint checks remain
mandatory.

## Reproduction and evidence

Use the public fixture configuration described in `docs/dcg-contributor-testing.md`
and compile tests before running the Python entry point under JDK 21. The Python
environment needs `pyodbc` and the IBM i Access ODBC driver. Generate the test
classpath at `debezium-connector-ibmi/target/connect-probe-classpath.txt` with the
existing Maven dependency-classpath goal. No AWS integration is part of the
repository harness; the local private host adapter only supplies the explicit
temporary test configuration.

The evidence directory is
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-online-census-native-20261109/`.
It contains the sign-on failures, `live-01/report.json`, `live-02/report.json`, credential cleanup result,
local verification logs, exact patch, source-preservation checks and Advisor result.
Local unit cases in `tools/test_dcg_online_census_probe.py` test missing persistent
cycles, disappearing cycles, reused definitions, incomplete history, delayed or
filtered bracket entries, changed bytes, denied authority, incomplete queries and
preflight failure before fixture creation. They do not substitute for the blocked
native experiment.

Local verification passed: Maven `verify` (2,464 tests after the review correction), plugin
assembly, formatter/import validation, 13 new Python controller checks, and the
four existing fixture-manager checks. The main connector jar excludes the new
test entry point. Older Surefire XML files remain in the checkout; the test count
comes from this run's Maven log rather than summing stale reports.

## Independent review and correction

Advisor inspected the actual files and found that the original helper used limits
of seven and 500 entries as if they controlled native pagination. The raw reader
uses that argument only to reject oversized responses, so valid larger responses
would have failed. The helper now controls the native response buffer separately
from its total entry budget. Seven focused cases passed through the actual raw
reader with controlled JT400 responses: eight-entry and 501-entry responses,
native continuation with a numeric gap, entry-budget rejection and invalid buffer
sizes. These synthetic responses are regression coverage, not new native captures.
The sign-on failures happened before that code ran; at that stage the corrected pagination path
had not been exercised on edgeqa; the current retry exercises it. Advisor was called once; the correction was
locally reverified afterward, without a second independent verdict.

## Local continuation: concurrent definition-query coverage

The `enumeration-concurrent` schedule adds one dedicated transactional fixture
session per repetition. It writes unique IDs in M, alternates commit and rollback,
and completes at most 64 cycles. The silent transaction and unrelated autocommit
writer remain active. No new tables, source settings or public connector options
are introduced. This reuses a commitment definition while its cycles change; it
does not force definition creation or destruction during enumeration.

The controller opens the dedicated session and waits for readiness before the
census. Only after the census authority check succeeds does it release the writer,
immediately before starting the definition query. The gate is outside the recorded
query interval, so a delay in the gate cannot manufacture successful overlap.
The writer has a separate connection; the observer never shares that connection.

`audit_enumeration_overlap` requires exactly one complete definition query and a
unique transactional writer job. Every writer INSERT must have its successful
commit/rollback in order. Missing, failed, reordered or malformed timing evidence
rejects. At least one entire successful terminal call must fall within the
definition-query call's monotonic interval. The report references its exact writer
ledger entries and also records whether the entire insert/outcome pair fits.
Partial overlap, an unrelated insert or a completion after enumeration does not
count. Attempts whose timing misses the race are retained as `not-observed`.
The complete run requires at least one `client-overlap-observed` attempt and no
rejected overlap evidence, as well as all existing inventory, writer-ledger,
strict-replay, recovery and unrelated-progress checks. A matching journal alone
cannot substitute for reaching this schedule.

This is **client-call timing evidence**. Network transfer, driver work and result
materialization can occur inside those intervals. Even a completely contained
terminal call does not establish that the server enumerated a particular row
before or after that transaction's outcome. It cannot prove a linearizable or
complete transaction inventory. The existing unrelated-progress counter spans
the broader observation/capture window; it is not used as this narrower timing
proof. Every report continues to keep `startup_proof=false`.

Readiness and admission waits are bounded to 20 seconds. The ten-minute workload
deadline is checked before opening the dedicated session and before each new
cycle. A stop request finishes an already admitted insert/outcome pair, then
starts no new cycle. The controller signals stop and joins the worker before
capturing the upper anchor. Writer errors and census observations are retained
even if the schedule fails. Cleanup also joins this worker; if it remains active,
cleanup reports blocked and neither closes shared connections nor deletes the
fixture. This distinguishes a stop request from completed drain.

The local tests use controlled SQL responses and real Python threads without
importing an ODBC driver or opening a source connection. They cover accepted
timing, missed/partial overlap, invalid or failed evidence, denied authority,
sign-on failure, a writer error, the 64-cycle bound, deadline
rejection, stop during an INSERT and cleanup with an undrained worker. They do not
replace a native run. The evidence directory is
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-census-concurrency-20261109/`.

Local verification passed: 34 controller tests (11 added here), four existing
fixture-manager tests, Python syntax and whitespace checks. Advisor inspected
the actual files, exact turn patch and verification evidence and passed without
an actionable finding. File hashes confirm that production and Java test sources,
migration guards, repository HEAD/index and all protected worktrees stayed
unchanged. No IBM i connection, Kafka run, push or publication occurred. The
earlier Java/Kafka evidence remains separate; it was not rerun for this Python-only
harness extension. Native behavior was still unvalidated at that stage; the current retry above
supersedes that limitation only for the recorded scenarios.


## Retry corrections and evidence

The first connected retry used fresh `DCNC0912`. The 128-definition bound rejected
six censuses, and a read-only count found 169 visible definitions. The bounded
limit is now 256; overflows, incomplete resource lookups, authority loss and the
60-second deadline still reject. This is a test-fixture capacity adjustment, not
a connector-wide promise about arbitrarily large transaction inventories.

That run then failed at a savepoint rollback. The cause was the Python driver's
[cursor context manager](https://github.com/mkleehammer/pyodbc/wiki/Cursor#context-manager):
a successful `with connection.cursor()` block commits when autocommit is disabled.
The harness now uses `contextlib.closing` for cursor lifetime, leaving transaction
completion exclusively to the explicit ledger commit/rollback operations. Driver
pooling is disabled before the first connection so fixture session close releases
its resources. Regression tests model the driver's implicit commit behavior and
verify explicit completion, error/warning cleanup and the bounded census limit.

The first cleanup correctly rejected active owned writer locks. After those
sessions exited, a resumed cleanup also rejected the JSON-string versus native
`datetime` representation of inventory timestamps. Exact normalized inventory
comparison showed no additions, removals or changes. Cleanup was retried with
native typed inventory values only after that equality check, retaining the
existing lock, transaction, ownership and inquiry checks. `cleanup-retry-02`
records verified removal. No unrelated session was terminated or lock guard waived.

The second run, `DCNC0913`, completed the native workload and cleanup. The replay
comparison then revealed documented transport differences in repeated reads:
a 292-byte entry had 12 trailing alignment bytes in the full capture, and
interior entries inherited receiver information from an earlier entry rather
than repeating that section. IBM documents the header displacements, variable
sections and receiver inheritance in
[Retrieve Journal Entries, RJNE0200](https://www.ibm.com/docs/ssw_ibm_i_74/apis/QJORJRNE.htm).

`As400JournalCaptureContent` is test-only. It first validates bounded, nonoverlapping
RJNE0200 sections and the expected receiver/sequence. It hashes all fixed source
bytes after the six displacement fields, the entire null-map and entry-specific
sections, and the resolved receiver coordinate. It permits only bounded zero
alignment gaps and a complete zero tail to the next 16-byte boundary (or no tail),
with the original raw hash verified separately. The supported comparison profile
is the captured SYSBAS, inline, VARLEN format. XA/LUW, pointer-backed data,
nonzero unexplained bytes, malformed lengths, overlaps and unsupported receiver
context reject. It does not synthesize thread IDs, relax file/member identity,
ignore source payload changes or promote receiver names to incarnation proof.

Two redacted native frame pairs reproduce padding and receiver inheritance in
`native-transactions/census-comparison.json`. Nineteen Java cases cover those
pairs, changed source fields/payload and malformed/unsupported layouts. The
Python audit separately rejects changed content, receiver coordinates and mixed
comparison representations. The raw source evidence stays outside the repository;
redactions apply only to the checked-in test fixture copy.

Retry evidence is retained under:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-census-native-retry-20261109/`.
`live-01` and `live-02` preserve the original failures. `cleanup-retry-02` and
`live-02/report.json` record verified removal. `replay-02-sections/provenance.json`,
`replay.json` and `offline-audit.json` record the separate local replay and ledger,
inventory and recovery audits of the retained `live-02` captures. No additional
source connection was needed for that replay.

The next unresolved gate is the complete online migration handoff: combine a
qualified inventory/history boundary with the already-tested legacy publication
barrier and Kafka recovery. This run supports the controlled workload's inventory
and journal agreement, not a guarantee that every possible concurrent definition
creation, rollback-in-progress or delayed terminal becomes visible in time.
Migration remains disabled until those requirements can be established or the
supported migration contract is narrowed explicitly.

Local verification for this retry passed: 2,510 Java tests via Maven `verify`
with assembly and formatter/import validation, 38 Python controller tests and
four fixture-manager tests. The packaged connector jar excludes the new test
helpers. `preservation.json` records unchanged production source, HEAD/index and
protected worktree hashes; `turn.patch` contains only this retry's seven files.

Advisor was called once after these checks and inspected the actual repository
files, exact turn patch and retained evidence. It passed with no actionable issue.
The review explicitly retained the native completeness and combined Kafka handoff
limitations. Its result is saved in `advisor-result.json` in the retry evidence.
