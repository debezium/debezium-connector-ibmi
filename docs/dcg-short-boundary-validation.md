# Isolated native short-boundary validation

Status: test-only, `startup_proof=false`, `maintenance_visibility_proven=false`.
The minimal allocate/marker/release program runs on edgeqa. The later continuation
below joins its captured observations to the local handoff and standard Kafka
binding-history tests. The normal connector does not admit this boundary.
Automatic migration remains disabled. This work changes Python test tools, Java
test sources, a captured test fixture and documentation only. Production source,
the F/CR worktree and existing recovery guards remain unchanged.

## Reproduction and evidence

Use the existing [contributor fixture configuration](dcg-contributor-testing.md)
with explicit isolated-write permission, an expected server identity, the IBM i
ODBC driver and pyodbc. Compile the test runtime with JDK 21 and Maven verify.
Run `tools/dcg-short-boundary-probe.py NEW_DIRECTORY DCNCdddd` using that Python
environment, `IBMI_TEST_CONFIG` and JDK 21 on PATH. The directory and library must
be fresh. The optional final argument `contention` runs two separate maintenance
scheduling cases instead of the ordinary matrix. There is no connection or
credential access on module import. Do not reuse the libraries below.

Evidence root for the 2026-11-09 runs:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-short-boundary-native-20261109/`.
Each run preserves its Python source, immutable Java runtime manifest, actor
ledger, raw metadata queries/results, generated statement and hash, marker bytes,
job/thread identity, timing, lock observations and cleanup reports. Failed runs
are retained. `live-06` is the completed ordinary matrix; `live-07` is the
contention experiment. All seven owned libraries DCNC0951 through DCNC0957 were
removed, with schema/library/inquiry absence verified and no connection-cleanup
errors. No production cluster or application data was accessed.

## What was exercised

`As400OnlineFenceEvidence.validateOffLock` reuses the native scope, member,
file/member JID, format, schema, journal and receiver identity validators. Its
new profile explicitly identifies observations made outside allocations. The
original locked profile cannot be relabeled as off-lock proof, or vice versa.
Neither profile is promoted to startup proof. Native discovery found five files,
including EXTRA with actual member DATA, without using fixture table definitions
as the allocation list.

The runner deposits A, collects metadata outside the locks, submits the generated
server-owned pulse, verifies return and independent lock absence, recollects
metadata, then deposits and forces Z. The pulse contains no inventory, schema,
identity, receiver, journal-read, storage-force or host rendezvous operation.
Result insertion happens after release. A timeout means drain is unknown: no
result read, session close or fixture deletion is permitted while its registered
worker remains active. A command error is not converted to a successful boundary
because a marker happens to exist.

| Case | Observed result |
| --- | --- |
| Three ordinary five-file pulses, live-06 | Holds 10.803, 10.125 and 9.414 ms; calls 1.262, 1.110 and 1.155 s; independent locks absent after return |
| Injected error immediately after N | Server handler released allocations; caller received failure; no success result row; observed N did not authorize a boundary |
| Existing open update | Acquisition refused; no N and no partial holder allocation; A/Z only; call 2.117 s |
| Create then drop a new file after release | Before/after metadata matched, but the journal retained D/CT, D/JF, F/JM, F/MC, D/DT, F/MD and other transitions; candidate rejected |
| New file after release | Scope comparison and journal transitions both invalidated the candidate |
| ALTER scheduled during a deliberately extended hold, live-07 | Holder returned and locks were absent; maintenance completed after release; measured hold 1.039 s including the explicit 1 s test delay |
| ENDJRNPF scheduled during the same test variant | Holder returned and locks were absent; maintenance completed after release; hold 1.042 s including the explicit delay; owned journaling restored before cleanup |

The contention variant injects DLYJOB(1) solely to allow an observer to see the
held allocation before starting maintenance. It is not the generated minimal
program and is never a candidate or healthy latency measurement. Its ledger
retains the exact modified SQL and labels the delay. The actor's native timestamp
before the command falls inside the held interval, and its completion timestamp
is after release. These establish the observed schedule; they do not identify
the internal lock dependency or prove every command arrived before release.
Neither case reproduced the earlier 35.910-second metadata/journaling overlap.
That old failing reproduction remains unchanged. These small samples establish
neither a hard native timeout nor a percentile/large-journal performance guarantee.

## Marker and audit fidelity

The native U/DN marker fields are compared against the actual dedicated holder
job/thread, receiver, sequence, empty object identity, zero commit cycle and exact
CCSID 37 bytes. A/N/Z must share one unchanged receiver. Missing, duplicate,
foreign, out-of-order, truncated or mismatched entries reject. Receiver rotation
rejection is locally tested; live rotation during this new pulse was not tested.

The test-only `audit` mode of `As400OnlineCensusCapture` uses the real incremental
RPC reader and its continuation, bounds, pointer, cancellation and drain checks.
A reflective **test-source-only** builder replacement omits journal-code selection
key 7. The documented native default is all codes; enumerating the existing Java
enum would omit native codes such as Y. Tests inspect the actual ServiceProgramCall
parameters and require no count, code, type, file or object selection in audit
mode. The ordinary reader keeps its existing criteria. This reflection is a lab
seam, not a proposed connector API or production configuration.

The captured raw format has a five-byte zoned-decimal length and eleven reserved
bytes before user data, followed by up to fifteen zero alignment bytes. The narrow
marker decoder checks that prefix, actual entry bounds, flags, payload, padding
and owned-file hash. It accepts the observed positive F-zone digits only. Source
bytes are retained in `native-transactions/short-boundary-markers.json`; no thread
or object IDs were synthesized. The prefix follows IBM's documented RJNE0200
[entry-specific data layout][retrieve].

The five non-maintenance cases in live-06 matched SQL observations to markers
captured through the real RPC reader. Both maintenance-race cases retained the
existing **pointer-backed data rejection**. Their bounded all-code SQL audit is
useful negative evidence, never replacement strict replay. Even the successful
raw checks establish marker fidelity, not a semantic audit of every possible
native record or complete maintenance visibility.

SQL diagnostic queries use exact explicit receiver endpoints, parameterized
values and bounded results. [DISPLAY_JOURNAL][display] starts inclusively, so the query
excludes only the already-existing lower anchor with `SEQUENCE_NUMBER > ?`.
Actual padded receiver/job names, null object fields and fixed BLOB padding are
preserved in the fixture and validated. The final diagnostic additionally rejects
foreign receivers, coordinates outside the interval, duplicate/unordered entries,
truncated or nonzero-padded bytes and excessive payloads. All seven retained
live-06 audits were revalidated after that tightening; results were unchanged.

Early attempts exposed diagnostic defects rather than being discarded:
live-01 found padded/null native fields; live-02 found the inclusive previous-Z
anchor; live-05 captured the raw length prefix and alignment. Their regressions,
failed reports and successful cleanup remain available. live-03 and live-04
passed earlier, smaller matrices. No failed attempt authorized startup.

## Checks and remaining gates

Maven `verify -DskipITs` passed: 200 journal tests and 2,502 connector tests,
including the existing stricter recovery suites. All 124 Python tests passed.
Java changes add six off-lock profile checks and two actual-parameter selection
cases. Python coverage includes captured native bytes, marker/interval rejection,
equal-end-state maintenance invalidation, unknown drain, error release, existing
holder locks and explicit contention instrumentation. The final byte/coordinate
tightening was also replayed against the retained native SQL observations. Maven
verification is not a rerun of the earlier Docker/Kafka fault matrix; no new Kafka
delivery or unrelated-table CDC claim is made by this marker-only experiment.

This advances SB1–SB6 and part of SB7 in the [design checklist][design], with the
following limits. Cross-library attachment, operations already underway before A
and completing after Z, live rotation, lost-response/network-black-hole and stalled
native-service cases are not qualified by these runs. The open transactional update
case is not new proof about every unfinished autocommit operation. General SB8–SB11
remain open. Existing tests and the previous autocommit characterization remain.

At the end of the marker-only slice, the unresolved platform premise was
**complete and timely maintenance-change visibility**. The continuation below
narrows the membership consequence but does not claim universal schema-change
coverage. Locks on discovered members cannot prevent a new file joining the
journal. Matching metadata cannot detect change-and-revert by itself. Observed
journal changes let this experiment reject, but a clean finite test does not prove
that every operation must leave its trace before affected data can be accepted.
The linked IBM APIs do not establish that general ordering contract. Pointer-backed
maintenance captures also remain unsupported by the strict profile.

Do not enable migration by extending the timing budget, treating SQL as raw replay,
setting F=N, accepting orphan markers or weakening history/identity checks. General native admission still requires a documented visibility contract or a
supported exclusion/detection mechanism for the accepted maintenance scope. The
continuation below tests a prospective F/N alternative without changing the
normal connector guards or promoting candidate evidence.

[design]: dcg-short-boundary-design.md
[retrieve]: https://www.ibm.com/docs/ssw_ibm_i_74/apis/QJORJRNE.htm
[display]: https://www.ibm.com/docs/en/i/7.4.0?topic=services-display-journal-table-function


## Continued work: late membership and prospective schema history

Evidence for this continuation is retained at
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-boundary-adversarial-20261109/`.
The two new native runners use the existing explicit disposable configuration,
server checks, native metadata collection, immutable runtime staging, strict RPC
capture and owned-fixture cleanup. They create no Kafka resources themselves.

### New file after Z

`live-02` (DCNC0959) creates and journals LATE after a successful closing marker.
The complete all-code trace remains rejected for pointer-backed maintenance.
Separate, explicitly incomplete row projections test the later identity guard;
they are never substituted for that complete trace. `As400BoundaryTailReplay`
feeds those bytes into the real normalizer, replay coordinator and task dispatcher.

The autocommit case publishes only the preceding known A11 row, then rejects
LATE99 at sequence 57 without changing F/W=56. The transactional case buffers
A13, then rejects LATE100 at sequence 62 without output or changing F/W=58.
The failed coordinator refuses further input. JSON-round-tripped offsets loaded
by fresh coordinator instances encounter the same unknown binding, emit nothing
new and retain the saved checkpoint. This means a late unknown identity need not
be physically locked forever for this final guard to reject it. It does not prove
schema safety for a changed known object, catalog ABA, or an omitted maintenance
record. The earlier complete-reader failure remains mandatory evidence.

`live-01` (DCNC0958) is the first capture without a separately retained baseline
row. It is supplementary only. The initial offline replay used the wrong test
partition (`test` versus the existing dispatcher's `offline`) and failed before
qualifying the known prefix. The final replay uses matching actual task and
coordinator partitions; the failure log is retained.

### Wait for old history to drain while legacy CDC continues

`As400ProspectiveHandoff` is a test-only conditional state model. It takes a
schema origin N, complete retained C/J controls and prospective C/D/F/R records.
Only C controls before N enter the control state machine; today's schema never
decodes pre-N row bytes. Original starts, producer identity, supported terminals,
savepoint resolution and the overlap of independently captured C projections are
checked. Pending prior cycles produce a typed temporary refusal only after the
prospective prefix has passed strict normalization. Lifecycle overlap, corrupt
rows, missing history and rows after a prior cycle's terminal remain errors.

After prior cycles complete, the existing handoff logic still checks selected
already-published prefixes and derives W from actual completed work. The result
must satisfy both F >= N and F > the final prior-cycle terminal. The second check
matters: a newer transaction can retain a replay floor overlapping old history,
even though the older transaction has now completed. Such a cut rejects; the
reader must never silently filter old cycles out of its durable replay. The local
model covers this case and preserves the ordinary version-2 F/W representation.
No new history or ownership format is introduced.

The first native attempt, `live-03` (DCNC0960), rolled back an insert to a savepoint
but left its transaction open. IBM i retained conflicting locks; acquisition
failed and no N was deposited. The runner expected success and rejected the
missing marker. This is retained negative evidence, not a successful boundary.

`live-04` (DCNC0961) explicitly checks that busy refusal and absence of partial
allocations. A subsequent unrelated A9 insert completes while the old transaction
is still open, and its real raw record is captured. This is source progress, not
by itself a Kafka CDC measurement. After the writer commits, a new pulse succeeds.
Its native N is 59. The measured healthy lock hold was 6.732 ms; the whole
call took 0.992 s. The rejected busy call took 1.912 s. These observations are
not a hard duration guarantee or a workload percentile. The controlled legacy workload then completes A10, M21 and A12;
the candidate cut is the actual A12 record at 66. Older control history is captured
separately. No pre-N row image is supplied to the prospective replay adapter.

After that cut, a B truncate/reload remains open while A14 completes. The capture
has D/CG at 75, B101 at 77, A14 at 78 and the real commit at 79. Offline replay
emits A14 first, followed by keyless `op=t`, then B101. The existing recovery matrix
checks 23 restart, interrupted enqueue, missing-offset/anchor and changed-identity
cases. This native profile requires no old cycles still open at N; the local
conditional old-cycle model is broader and is not native admission. The exact
legacy cut is still a modeled acknowledgement in this offline result.

All four owned libraries DCNC0958–DCNC0961, their receivers and associated inquiries
were verified absent, with no connection-cleanup errors. The temporary credential
file was removed. No production source, ordinary transactional admission, public
configuration, F/CR worktree or published branch changed.

### Reproduction and qualification limits

After compiling the test classes with JDK 21, run the appropriate native runner
with the existing fixture configuration and fresh directory/library:

- `tools/dcg-boundary-tail-probe.py NEW_DIRECTORY DCNCdddd`
- `tools/dcg-prospective-probe.py NEW_DIRECTORY DCNCdddd`

The offline main classes `As400BoundaryTailReplay` and `As400ProspectiveReplay`
accept a cleaned capture directory and a new bundle filename. They never connect
to IBM i. Committed source fixtures `late-membership.json` and
`prospective-native.json` contain original native bytes and observed identities;
they do not replace raw captures with synthetic job/thread IDs. Modeled mutations
and origin assumptions in unit tests are explicitly separate from those captures.

IBM's [journal management overview][journal-order] describes recording changes in
the receiver before applying them to the object's main-storage image. That is a
useful basis for ordered replay; it is not a blanket certificate that every schema
or membership change has been classified by this implementation. Unknown-member
rejection closes the tested late-file failure mode, not that entire platform
question. The API's [code/type inventory][entry-types] must still be mapped to the
supported maintenance scope and strict full-audit reader. Normal startup must also
join origin qualification, retained control coverage, durable binding history and
the actual legacy acknowledgement. These experiments do not skip those gates.

[journal-order]: https://www.ibm.com/docs/en/i/7.5.0?topic=concepts-how-journal-management-works
[entry-types]: https://www.ibm.com/docs/en/i/7.5.0?topic=information-all-journal-entries-by-code-type

### Real Kafka acknowledgement of the prospective fixture

`kafka-prospective-01` passes all 34 assertions in the original five-scenario
local Connect matrix: death before the barrier, lost barrier callback, lost
migration callback, interrupted truncate/reload and reassignment with the old
worker returning. Only an ordinary legacy offset at 61 is seeded. The real
legacy RPC loop and dispatcher publish M21 and A12, then Connect commits the
exact barrier at 66. The task publishes ordinary F/W=66 through Connect before
the buffered suffix. Each case ends with exactly five committed data records,
one truncate, no snapshot and final F=W=79. The source A14 insert appears before
the clear, despite its later journal sequence, because the clear waits for its
actual commit. Missing offsets fail without output or a replacement checkpoint.

The returning source producer reports `InvalidProducerEpochException` and
`ProducerFencedException`; independent saved broker offsets and committed output
remain unchanged after it returns. No custom owner or receipt authority is in the
candidate artifact. Cleanup removed the owned local Docker resources. This run
uses the captured native source bytes, native-acquired schemas and observed
identities; JT400 responses are controlled locally. It is not a simultaneous
live IBM i/Kafka handoff or a claim of native startup qualification.

The existing `dcg-online-handoff-probe.py` accepts this exact prospective bundle
as its source argument. The dedicated profile pins this ledger's N=59/L=66;
it is deliberately not a general native exporter. It reloads and validates the
prospective adapter in every actual task. The new single-receiver/closed-history
assertions are separate from the original defaults, which still require multiple
receivers and distinct R/F/W. No original fault assertion is removed.

The original native capture's `migrationcallback,reassign` cases are rerun at
cut 162 in `kafka-original-regression`, retaining their separate positions,
receiver rotation, missing-offset and fencing assertions. The complete original
five-case evidence remains in the earlier deferred-handoff report.

### Verification of this continuation

Maven verify passed all 2,721 tests (200 parser and 2,521 connector). This adds 19
regressions covering the prospective model, native replay and late-member failure
boundary. All 124 Python tests passed. Assembly packaging and formatter/import
validation passed. The report excludes stale XML files from older runs. Staged
Kafka artifacts were compared with the final compiled classes and saved hashes;
338 local classes matched in each artifact. Worktree comparison confirms no
production-source change in this continuation and unchanged protected worktrees,
branch heads and staged entries. All changes remain local and uncommitted.

The explicit Advisor review inspected the actual repository files and retained
native/Kafka evidence and passed. It confirmed the checkpoint safeguards,
test-only scope and stated limitations; no file-access limitation was reported.
The original two-case Kafka regression passed 16 assertions and cleanup; its
returning producer also reported `InvalidProducerEpochException`. This does not
promote the native boundary to automatic-startup proof.

## Joined boundary evidence, active writers and schema replacement

Evidence for this continuation is retained under
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-schema-boundary-20261109/`.
Automatic migration remains disabled. These changes are test sources, fixtures,
local experiment runners and documentation; production sources and F/CR are
unchanged.

### Actual ALTER and reversal

`dcg-boundary-tail-probe.py NEW_DIRECTORY DCNCdddd schema` performs an owned
INTEGER -> REAL -> INTEGER change, inserts exactly representable values and
captures both the complete native journal and explicitly incomplete row-only
projections. The complete capture is the replay authority for this negative test.
It is never replaced with the SQL DISPLAY_JOURNAL diagnostic.

The first three runs (DCNC0962–DCNC0964) were rejected and cleaned. The initial
interpretation as a statement timeout was incorrect: retained job messages identify
SQL0952 reason 10, CPD32CE cancellation and CPD32CC's potential-data-loss inquiry.
The successful DCNC0965 run uses IBM's job-local
[OVERRIDE_QAQQINI / SUPPRESS_INQUIRY_MESSAGES option][ddl-inquiry] in its dedicated
DDL session. No system reply list or global message description was changed.

The native fixture proves two distinct identity facts:

- Both file/member **journal IDs** remain the same, and reverting the type restores
  the original four-byte format definition. The REAL row at sequence 61 contains
  101.0; using the old integer decoder would read 1120534528.
- The native file/member **object identities** change during replacement. Thus the
  complete off-lock identity comparison also rejects these endpoints; this capture
  is not evidence that the complete native metadata validator misses the change.

`As400SchemaBoundaryTest` checks the codec distinction and replays the complete
capture through the actual lifecycle/recovery coordinator. F/CH subtype 0 at 52
is rejected before output; F/W stays at the controlled completed baseline 48.
A fresh instance restored from JSON offsets rejects again with no publication or
checkpoint change. D/CG subtype 0 and the journaling companions remain in the raw
fixture; they are never relabeled as a truncate.

### Joined native boundary observations

`As400ShortBoundaryEvidence` now checks the collected scope, native metadata on
both sides, exact minimal program, successful/drained holder, native timing,
A/N/Z raw payloads and producers, receiver identities and complete audit interval.
It reconstructs the program to compare with the actual saved SQL; it does not trust
a program hash as proof of its contents. Native metadata query observations go
through the existing validators. Missing evidence, changed scope/schema, unknown
journal effects, foreign markers, gaps, pointer/minimized data or ambiguous
release reject before the prospective handoff adapter runs.

This first assembler requires one complete audit page and consecutive returned
sequences. Those are conservative admission bounds, not claims that all valid
IBM i journals always have consecutive sequences. Larger, rotated or incomplete
intervals are refused; no audit record is dropped to make them fit. Known ordinary
row/control work is checked and admitted, while complete transaction lifetime
reconstruction remains the existing prospective adapter's responsibility. The
assembler never interprets N as completed output or an empty-transaction census.

The new `prospective-native-boundary-v1` profile requires these observations and
rejects attempts to remove them or downgrade to the original fixture-only profile.
The original `prospective-native-fixture-v1` proof and stricter matrices remain
separate and unchanged. Neither profile enables normal startup.

### Writers continuing during metadata acquisition

`dcg-prospective-probe.py NEW_DIRECTORY DCNCdddd active` adds one bounded ordinary
writer during preparation and post-lock collection. DCNC0966 completed 32 inserts
without writer errors. The complete A..Z audit contains 13 ordinary row entries
before N and 11 after N. Actual A/N/Z are 58/72/84; the acknowledged-cut candidate
is 98, and later clear/reload recovery completes through 111. The successful
allocation held for 0.007957 seconds; the complete native call took 1.088504321
seconds. These are measured observations, not a hard latency guarantee.

The joined native capture passed the existing 23-case offline recovery matrix,
including interrupted delivery and fresh-instance restart. No synthetic thread,
journal, receiver or row identities are substituted. The native run performed no
Kafka publication; its subsequent local replay remains explicitly identified.
All five disposable libraries in this continuation were independently verified
absent, with no lingering fixture inquiries, and the temporary credential file
was removed.

### Actual local Kafka handoff

The DCNC0961 joined boundary bundle passed all five existing local Connect fault
scenarios and 34 assertions (`kafka-boundary/`): failure before barrier,
lost barrier callback, lost migration callback, interrupted truncate delivery and
replacement with the old worker returning. The same exact native source prefix
and truncate/reload suffix were observed, without snapshot records. Actual broker
fencing and missing-offset rejection remain asserted. Local Kafka cleanup passed.

The test-only integration uses the existing `As400BindingHistory` and
`As400KafkaBindingHistory` to persist pending bindings **before** enqueueing the
legacy barrier. This closes the crash gap between barrier acknowledgement and
history persistence. It records the acquired schema epoch at N, recovers it through
a fresh history backend, and preserves the independently computed F/W. Only actual
legacy-barrier acknowledgement permits publishing that F/W offset.
The ordinary history fingerprint replaces the fixture-byte fingerprint for
buffered recovery; source captures still supply the controlled reader transport.
An explicit test-only flag preserves the existing fixture-only experiments.
Pending history can be recovered independently of source metadata while a valid
legacy offset exists; it is returned as candidate evidence, not accepted W. Missing
offsets remain rejected. A durable barrier whose binding history has disappeared
cannot recreate those bindings from the fixture. History-before-barrier crash,
barrier/history loss and missing-history cases extend the existing broker matrix.
The seven scenarios in `kafka-pending-history/` passed all 51 assertions and
cleanup. `kafka-history/` retains an earlier failed run: the new test adapter
omitted its output-policy version and rejected admission before publication.
The corrected run and the subsequent pending-before-barrier matrix are separate
artifacts; the failed evidence has not been removed. A final focused run after
capture-filter fingerprint tightening passed 21 assertions for history-before-barrier
crash, barrier/history loss and reassignment in `kafka-final-history/`; cleanup
also passed. All 347 staged local classes match the final compiled artifact.
The earlier seven-case artifact predates only that fingerprint-filter tightening.
This is not a continuously connected native reader experiment.

The durable fingerprint includes all four ordinary schema/table include/exclude
filters and the explicitly supported output behavior. Rotating converter
credentials does not change it. Local regression coverage rejects changed
filters/output, missing history, and attempts to use pending history without an
existing processed legacy offset. Pending history does not authorize replay or
synthesize an accepted checkpoint.

### Normal-startup integration boundary

Actual `As400ConnectorTask.startRecovery` delegates history admission to
`As400RecoverySource.prepare`. That code requires an initialized binding manifest,
including an acknowledged external baseline. The short-boundary adapter deliberately
stores a **pending schema epoch without that initialization receipt**. The existing
`As400RecoveryTaskTest.historyFailureReachesActualTaskPollWithZeroOutput` covers
this rejection for uninitialized history. The new handoff has therefore not been
routed into normal recovery, and the fixture coordinator must not be described as
normal startup.

The smallest normal-startup change must model a distinct acknowledged legacy
handoff, not manufacture a snapshot receipt or remove the initialization check.
The pending schema record must survive before the publication barrier; a separate
activation must bind the native origin, durable legacy barrier, schema fingerprint
and independently reconstructed F/W. Restart may activate only from the actual
Connect offset and matching retained history. Crashes before either acknowledgement,
missing/deleted offsets or history, a foreign barrier and changed capture selection
must fail closed. This is a production admission change outside the test-only
implementation here. The legacy and initialized-snapshot paths must retain their
existing acceptance rules.

A concrete next patch would touch these existing components:

- `As400BindingHistory`: represent the legacy-handoff origin separately from
  `Initialization`; preserve compatibility with existing initialized manifests.
  Pending bindings alone remain insufficient for normal recovery.
- `As400RecoverySource.prepare`: accept only an independently validated activated
  handoff or the existing acknowledged initial baseline. Never substitute N or
  a candidate checkpoint for the durable legacy barrier.
- `As400ConnectorTask` and its test admission: hand the activated source to the
  existing normal coordinator, dispatcher, acknowledgements and stop/drain
  lifecycle. Keep public automatic migration disabled until the combined native
  and Kafka admission tests pass.
- The existing handoff/recovery harness: cover history-before-barrier crash,
  barrier-before-activation crash, activation-before-F/W crash, lost callbacks,
  interrupted clear output and replacement using actual normal-task recovery.
  Reject foreign/missing offsets or manifests without publication, checkpoint
  advancement or an implicit snapshot. Retain the original stricter matrix.

This cannot be accomplished solely by changing the test coordinator: the normal
source intentionally refuses uninitialized history. Removing that refusal or
calling a handoff a completed snapshot would bypass the requirement being tested.
No production-source edit implementing this next patch is included here.

The native observations qualify only the bounded fixed-schema profile exercised
here. They do not establish universal maintenance visibility, cross-library
membership exclusion, a hard lock-duration bound or recovery after journal history
is lost. Single-page/single-receiver bounds, operator-managed retention, unsupported
maintenance rejection and all identity checks remain. Simultaneous live-source /
Kafka migration and normal-task activation are still unvalidated; automatic
migration remains disabled.

### Verification and cleanup of the joined continuation

Maven verification passed all 2,740 Java tests; the final filter-fingerprint change
also passed its targeted history regression. All 124 Python tests passed. Assembly
packaging and formatter/import validation passed. Native fixture cleanup verified
DCNC0962 through DCNC0966 absent, including outstanding inquiries, and removed the
temporary credential file. The final runner now explicitly removes its job-local
QAQQINI override before closing the dedicated DDL connection; that extra explicit
restore statement was added after the successful native capture and has not been
rerun against IBM i. The captured job-local override ended with its dedicated
connection. No global message or reply-list settings were changed.

[ddl-inquiry]: https://ibm-power-systems.ideas.ibm.com/ideas/IBMI-I-3376

The explicit Advisor review inspected the actual repository files and retained
evidence and passed, with no access limitations. It confirmed the normal-startup
guards remain intact and the final focused Kafka run passed 21 checks and cleanup.
