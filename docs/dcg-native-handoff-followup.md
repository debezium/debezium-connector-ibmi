# Online handoff: native evidence follow-up

This continues [the native evidence assessment](dcg-native-handoff-evidence.md).
Automatic migration remains disabled. Native observations, controlled workload
results and general platform guarantees are separate claims.

## Additional IBM documentation

IBM's [open commitment definition troubleshooting article][open] explicitly
recommends `DB_TRANSACTION_INFO` joined to `DB_TRANSACTION_JOURNAL_INFO` for
finding all open commits for a journal. It also describes finding unmatched
transaction starts in retained journal entries. This is a stronger documented
basis for the chosen interfaces than the column descriptions alone.

The article's shorter SQL filters `LOCAL_CHANGES_PENDING='YES'`. We do not copy
that optimization: the existing zero-pending/savepoint cases remain in scope,
and our collector preserves definitions and journal cycles even when their
pending-change count is zero. The article does not specify an atomic snapshot
across definition enumeration and later resource queries, or explicitly resolve
persistent-row omission and resource-reuse races. The existing before/after
journal bracket and original-start checks remain necessary; this additional
source does not promote the test's visibility assumption to startup proof.

IBM also documents that [CPF7024 reason 2][deletion] prevents deleting a receiver
needed by an unfinished commitment-control transaction. The [same troubleshooting
article][open] describes the related sequence-reset rejection. These restrictions
are useful evidence for investigating a retained-history alternative. They do
not establish that an arbitrary oldest *available* receiver is a complete origin
for a lagging legacy publication position: transactions might have finished and
their receivers been deleted before discovery, while the old connector is still
behind. Restore, damage, earlier schema changes and unsupported lifecycle events
must also be excluded or detected. No receiver deletion or sequence-reset probe
is added to the migration path.

The [commit cycle identifier][cycle] gives the original C/SC sequence for a
transaction. That makes targeted back-reading possible once an event reveals a
cycle. It does not discover a still-silent cycle before handoff. Back-reading only
when that cycle later commits can discover an already-published dirty prefix too
late: the new task could already have published its migration checkpoint. We
therefore retain the current pre-publication rejection instead of adding a lazy
backfill path that bypasses complete startup context.

The [receiver directory][receivers] supplies a more precise candidate for a full
history origin: for a nonempty local receiver, a null previous-receiver field is
documented as meaning that no receiver was attached before it. This is stronger
than choosing the oldest receiver still present. A restricted origin route would
need that original receiver, the complete linked chain through the chosen cut,
native identities, complete raw reads, all required recording fields and schema
history from that origin. Every existing restore/maintenance and dirty-prefix
restriction still applies. This remains a candidate validation route, not an
implemented admission rule. The current fixture starts its supported capture
after configuring thread recording, so it does not qualify this full-origin
alternative merely by retaining its earlier receiver.

## Bounded native validation

Evidence directory:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-online-origin-followup-20261109/`.

After the user renewed the AWS session, the existing native census controller was
started with a fresh `DCNC0914` library. It uses the existing fixture ownership,
time/entry bounds and cleanup checks. The native identity/provenance extension
is the version verified in the preceding assessment; the running Java capture
classes are not modified during collection.

The run covers the existing thirteen schedules, including silent/open work,
commit, rollback, reuse, a new definition, savepoint/zero-pending states, concurrent
enumeration, transactional clear and receiver rotation. An independent writer
continues inserting into table A. Its successful writes are distinct from proof
of Kafka delivery; the subsequent local handoff experiment supplies the latter.
Every source capture remains explicitly unqualified for automatic migration.
The fixture contains four single-column INTEGER primary-key tables in the
supported local, non-XA journal profile. Schema creation, subsequent DDL absence
and retention are controlled by the fixture. This is not new coverage for
arbitrary column types or an existing installation's unknown schema history.

`live-01/report.json` finished as `captured-unqualified`: all thirteen schedules
passed their controlled-history checks. The final interval contains 1,006 raw
journal entries. The independent SQL ledger matched 834 operations and 697
completed replay batches. Table A made 660 unrelated inserts, with at least 38
successful inserts in each observation interval. The four captured D/CG records
include subtype 8 and subtype 3; savepoint/full-rollback handling leaves only the
committed clears eligible for output.

All 42 anchor/interval captures contain before/after native identity evidence.
The binding capture contains its native acquisition coordinates and exact schema
bytes/hash. The final interval crosses the configured receivers 2 and 3; native
observations retain the unchanged journal identity and both distinct receiver
identities. Capture reports retain `startup_proof=false`, and strict replay does
not convert these observations into a general visibility or schema-history proof.

Fixture cleanup passed: transactions and connections were drained, owned objects
were checked, and independent queries verified that both schema and library
`DCNC0914` were absent. Only exact fixture receiver inquiries from the owned drop
job were answered; no pending inquiries remained. The temporary credential file
was also removed. The earlier expired-session attempt made no source connection;
this successful retry is recorded separately rather than overwriting that result.

## Local Connect replay of this capture

The handoff uses the original bytes and native observations from `live-01`, with
cut R=104 at the next C/SC after the first changing transaction rolled back. The
silent U transaction remains open; the next transaction has no selected row
prefix yet. W=103 is a real completed-output boundary, not the lagging legacy
offset. `cut-selection.json` records the source entries used to choose this cut.
The exporter predicts 53 legacy records and 704 total records per scenario.

The first local run (`kafka-01`) passed its three startup/callback crash cases,
including missing-offset rejection, but failed the partial-batch injection
assertion. It is retained as failed evidence, with successful Docker cleanup.
The independently fetched broker records showed that the clear and following row
were both uncommitted; committed data and F/W had not advanced past them.

The fault controller had counted one marker per replay batch. Native lifecycle
validation released the unrelated rows at 804 and 807 together; the actual
coordinator drained both before publishing one checkpoint marker. The estimated
poll count was consequently one too high. This was an injection error, not
evidence that a partially committed clear is acceptable.

The test-only task now holds subsequent polls after the actual first truncate is
polled, with a fixed one-record batch size. The controller independently waits
for that truncate to reach the broker before injecting the fault. The exporter
requires following selected output in the same clear batch and records the
exact truncate sequence. The original uncommitted/committed-output, offset and
fencing assertions remain; the sequence check is additional. Returning old
workers explicitly release the hold so the broker still has to fence actual
publication attempts. No normal-task or production coordinator code changes.

An offline audit of the fresh native bundle passed eight checks: conditional
candidate preparation with F=46/W=103/R=104, dirty selected-prefix rejection,
missing original start, changed/missing native identity, partial native evidence,
late schema acquisition and attempted startup-proof promotion. The audit uses
the actual loader/preparation code and writes no Kafka records or offsets. Its
temporary driver and results are retained in thread evidence, not added as
another connector implementation.

The corrected five-scenario run, `kafka-02`, passed all 34 assertions. Each scenario
produced exactly 704 committed records including two truncates and no snapshot
records; final F=W=1063. Missing offsets were rejected without new output or a
replacement checkpoint. Returning old-worker publication left committed output
and F/W unchanged; the independent worker-log audit found the actual source
producer's `InvalidProducerEpochException`. Docker cleanup passed.

`broker-audit.json` independently checks all 3,515 non-tombstone source timestamps,
receiver coordinates, final native identity offsets and expected records. For
each committed clear, it also verifies that unrelated table-A records whose
source sequences lie between that clear and its terminal appear before the clear
in Kafka output. Thus unrelated publication continues while clear work is buffered.
This is replay of the native capture through real local Connect; it is not a
single simultaneously connected IBM i-to-Kafka deployment test.

`fault-correction-audit.json` confirms that the failed and corrected runs used
identical source bundles and expected legacy/buffered records. The change is the
fault trigger. `artifact-audit.json` verifies all 319 staged local class files
against the final build and checks harness hashes. The earlier native audit
recorded unchanged capture classes during collection and initial staging; the
later test-only poll-gate change did not modify the native collector classes.

Final Maven verify/assembly, formatter/import validation and all 2,545 Java tests
passed. All 54 Python harness tests passed, including a regression rejecting a
first truncate with no following selected output for the intended fault. The
preservation manifest records unchanged production sources and protected F/CR,
original truncate and earlier D/CG worktrees. Nothing was committed or pushed.

Advisor inspected the actual files and retained evidence and passed the stated
conditional result. It reported that direct access to the IBM receiver reference
returned HTTP 403. The indexed primary reference and accessible IBM support
articles are identified above; general migration is still unproved.

## Journal-wide membership counter experiment

Follow-up evidence is retained in
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-closed-boundary-20261109/`.
This experiment adds no normal-task admission, source configuration or publication.

IBM's [JOURNAL_INFO reference][journal-info] defines
`JOURNALED_COMMITMENT_DEFINITIONS` as the total currently implicitly journaled
commitment definitions. The [underlying API][journal-api] exposes this scalar in
the base format and warns that totals can change during retrieval. It is not a
count of unfinished transactions, and separate reads are not an atomic census.
The [commitment-control sequence][sequence] documents C/BC and C/EC for journal
association, including a journal that is not the definition's default journal.

The test-only Python census harness now has a bounded, parameterized scalar
collector and an additional membership audit. The audit requires equal before
and after counts matching the number of distinct observed journal resources,
including resources with null cycle identifiers or zero pending changes. It
rejects malformed/missing counts, wrong journals/profiles, incomplete or unordered
observations, duplicate identities, and C/BC, C/EC or C/BA within the raw bracket.
Balanced departure/arrival must reject even when both counts are equal. Existing
census/history audits, native identity checks and strict replay remain separate
requirements. A passing result is deliberately named
`membership-consistent-unqualified`; both proof flags remain false. The existing
census entry point and its timing/safety assertions are unchanged.

The rationale is a candidate set-completeness argument: if every associated
definition contributes to the scalar, membership is stable across enumeration,
and every distinct returned resource belongs to that set, matching cardinalities
exclude a silently omitted member. These premises require a documented relation
between the scalar, resource enumeration and lifecycle journal entries. Merely
matching two counts, or observing a zero count once, does not establish them.

`live-01` hit SQL0666 before recording a usable sample with the existing five-second
ODBC limit; its `DCNC0915` fixture was cleaned up. `live-02`, using a fresh
`DCNC0916` fixture and a bounded 30-second diagnostic query timeout, collected
thirteen scalar/receiver observations and 83 raw journal entries. The strictly
decoded capture replayed successfully and the unrelated writer inserted 53 rows.
The count detected an open transaction without a default journal and remained
one across savepoint rollback with zero pending changes. It also remained one
after commit while the session was open. Receiver pending flags distinguished
these states and followed an open transaction across receiver rotation; neither
the flags nor this controlled history are promoted to a general closed-origin
certificate. Cleanup passed.

`live-03` (`DCNC0917`) brackets six schedules with counts, census reads and raw
native identity captures. The census retains its five-second query limit; only
the separate scalar connection uses the diagnostic 30-second limit. Four
schedules were consistent: no target resources, silent open work, zero-pending
savepoint work and a reused definition with a new transaction. Balanced definition
replacement correctly rejected despite equal counts. The closed-session case
also rejected: both counts were one but the census had no journal resource.
This discrepancy remains evidence to explain, not a value to subtract or ignore.

The original `live-03/report.json` remains failed: the temporary controller used
`rollback-savepoint` for an operation whose exact SQL was `ROLLBACK TO SAVEPOINT P`,
while the existing ledger auditor expects `rollback-to-savepoint`. Its sample
directory names also differed from the existing replay comparison glob. The
offline `MembershipCaptureAudit.java` uses the existing raw loader, content
normalizer and native identity validator to check each original bracket directly.
`audit-membership.py` records the single label translation and original report
hash without rewriting source evidence. This separate audit matched 317 writer
operations and 308 completed batches, checked all six history brackets, and
preserved both membership rejections. Fixture cleanup passed.

`live-04` (`DCNC0918`) then exercised seven clear-only schedules, without a row
insert to establish the transaction first. Both native TRUNCATE (D/CG subtype 8)
and optimized DELETE (subtype 3) exposed positive commit-cycle identifiers with
zero pending changes. Commit/rollback during enumeration exposed the original
terminal in the raw bracket. A clear-only savepoint rollback retained its cycle
with zero pending changes. All seven membership observations were consistent;
this is why filtering the inventory on pending changes would be unsafe. A fresh
counter connection also returned one after the writer disconnected, while the
census had no resource, so the earlier count discrepancy was not confined to the
long-lived observer connection.

The original whole-stream replay **failed** at sequence 413. Its savepoint undo
started with F/EJ, whereas the lifecycle classifier required F/MD. This revealed
a previously uncovered native variant. The small production change permits F/EJ
only at an expected F/MD step inside a matched clear savepoint undo window. It
still requires F/EJ's internal flags, the same native producer, exact paired
file/member identities, the full D/EF–D/JF–F/JM restoration and replacement C/SB
scope. An isolated F/EJ remains rejected; incomplete or mismatched transitions
cannot advance completed output. No additional native calls or buffers are added.
IBM's [journal entry reference][entry-types] distinguishes these two record types;
the exact alternative sequence is supported by the retained native capture, not
claimed as an exhaustive list of IBM i lifecycle implementations.

`replay-fixed-04` contains byte-identical evidence copies with only comparison
directory names adapted to the existing replay entry point. The original failed
capture/report is unchanged. The corrected build replayed all 461 captured
entries, and the independent ledger audit matched 416 operations and 408 batches.
All seven raw history brackets matched, and 41 sampled restart/failure checks
passed. These checks model acknowledgements locally; they are not another Kafka
broker run. The offline ledger audit records one additional projection: a new
read-only observer reused a drained writer's job number, which the older ledger
auditor rejects as a duplicate session. Only that observer registration is omitted;
the audit verifies that it has no subsequent writer operation. No journal record,
writer operation or source offset is rewritten. Cleanup passed.

`live-05` is the fresh confirmation in `DCNC0919`: for each clear subtype, establish
a savepoint, clear B, roll back to the savepoint, insert a new B row and **commit**.
Independent source SELECTs verified that the original rows survived and each new
row was added. Strict replay of the full 45-entry concurrent capture emitted only
the two later B inserts, with no truncate from either rolled-back clear. The writer
ledger matched all 15 operations and seven completed batches; all 39 local recovery
checks passed. The unrelated A writer made five inserts. Cleanup passed.

The checked-in regression fixture retains the original bytes, thread identifiers,
positions and paired native identities of the two confirmed producer transactions.
Its scope explicitly identifies the producer projection; the full concurrent
captures remain in evidence. Existing dispatcher/RPC and fresh-instance tests now
exercise these cases, including every boundary in each 19-entry transaction.
Targeted corruptions cover a missing undo start/companion, flags, object identity,
producer, recording profile and replacement scope. The original sequence-413
failure is retained separately, including a failing pre-fix unit-test log.

Final local verification passed 2,600 Java tests and 64 Python tests, Maven verify,
assembly packaging, formatter/import validation and `git diff --check`. The
packaged lifecycle class matches the final build. Preservation evidence identifies
only this lifecycle implementation as changed production code in this follow-up;
normal migration admission, F/CR and the protected worktrees remain unchanged.
All five fresh libraries (`DCNC0915` through `DCNC0919`) have verified cleanup.
No commit, push or publication was performed.

Advisor inspected the actual files and retained evidence and passed the scoped
lifecycle fix. It retained the membership-completeness limitation and explicitly
noted that Kafka was not rerun in this follow-up.

The additional [commit][commit] and [rollback][rollback] documentation orders the
terminal journal write before the definition is marked ended. That provides a
documented basis for the terminal ordering used in the local, unfiltered journal
profile. The experiment still does not assert that every combination of SQL
resource observation, counter observation and native journal retrieval satisfies
the proposed complete-window contract. In particular, the closed-session count
disagreement must be understood before treating this cross-check as admission.

## Remaining guarantee

The native identity/provenance integration and conditional captured-history
handoff now have fresh native and broker evidence. Automatic migration remains
disabled. The general concurrent-discovery contract and historical binding
coverage for arbitrary existing legacy deployments are still not established by
this controlled fixture. The additional IBM sources provide a documented basis
for the interfaces and a narrower full-origin alternative; neither is silently
treated as an unconditional migration certificate. No new writer pause, public
configuration, ownership service or retention-release mechanism is introduced.

[open]: https://www.ibm.com/support/pages/msgcpf7024-rc2msgcpf7018-rc2-determining-open-commit-definition
[deletion]: https://www.ibm.com/support/pages/journaling-problem-determination-cpf7024
[cycle]: https://www.ibm.com/docs/en/i/7.5.0?topic=objects-commit-cycle-identifier
[receivers]: https://www.ibm.com/docs/en/i/7.5.0?topic=services-journal-receiver-info-view
[journal-info]: https://www.ibm.com/docs/ssw_ibm_i_76/rzajq/rzajqviewjournalinfo.htm
[journal-api]: https://www.ibm.com/docs/en/i/7.4.0?topic=ssw_ibm_i_74%2Fapis%2FQJORJRNI.html
[sequence]: https://www.ibm.com/docs/en/i/7.6.0?topic=objects-sequence-journal-entries-under-commitment-control
[commit]: https://www.ibm.com/docs/en/i/7.5.0?topic=work-commit-operation
[rollback]: https://www.ibm.com/docs/en/i/7.6.0?topic=work-rollback-operation
[entry-types]: https://www.ibm.com/docs/en/i/7.5.0?topic=information-all-journal-entries-by-code-type
