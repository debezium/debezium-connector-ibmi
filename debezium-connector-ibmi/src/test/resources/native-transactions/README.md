# Native D/CG transaction characterization

Historical `docs/dcg-*.md` references below identify reports in the
[development archive](https://github.com/gardnmi/debezium-connector-ibmi/tree/fc4ffaa2aa7bf8acc5d9630d493450391814f8c0/docs).
Current behavior and test commands are in the [contributor guide](../../../../../docs/dcg-contributor-testing.md).

Captured on isolated edgeqa IBM i 7.5 on 2026-11-07; database group SF99950
level 11 was installed. These are bounded native observations, not startup proof
or approval to enable transactional streaming.

`dcg.json` contains seven scenarios with actual subtype 8 TRUNCATE and subtype 3
optimized DELETE, including commit, full rollback, repeated rollback to a SQL
savepoint with an intervening nested savepoint, and interleaved transactions on
separate tables. `initial.json` preserves eight earlier controls: same-table
uncommitted writes blocked TRUNCATE and caused DELETE to use individual deletes;
TRUNCATE IMMEDIATE produced cycle-zero F/CR and survived rollback.

The known layout is one `INTEGER NOT NULL PRIMARY KEY` column. Tables began with
IDs 1, 2, 3. The operation scripts, independent source checks, complete unmodified
native response buffers, and SQL journal metadata remain in the local evidence
directory documented in `docs/dcg-native-transaction-validation.md`.

All 287 entries returned in the scenario intervals are included, including
object/journaling transitions. The local component test deliberately selects all
C controls plus R/D-CG input; it does **not** authorize dropping the other entries
from a future production stream. The complete offline replay also attempts all
scoped records and rejects them before output. The independent SQL inventory
additionally records internal F/DE entries omitted by the native retrieval profile.

Sanitization changes fixed-header job, user, program, system and address fields.
Job numbers and nonempty JIDs are mapped consistently to synthetic values:
different jobs remain different, and file and member JIDs remain distinct.
Record numbers, thread IDs, journal/system sequences, timestamps, commit cycles,
flags, object types, object/member names, entry-specific payloads, savepoint
targets, null-map offsets/counts and row values are retained. Entry boundaries
are made standalone by setting the next-entry displacement to zero. Unused buffer
tail on a final entry is removed after retaining every declared payload, null map
and receiver field. Local `fixture-provenance.json` records source page, offset,
length and derived hash for each entry. These transformations do not make the
fixtures native identity evidence.

The original run exposed rows identifying `*QDDS` and D/CG identifying `*FILE`,
with distinct JIDs. The paired-identity follow-up records the corresponding saved
D/CT and F/JM observations in `bindings.json`, using the same sanitization mapping.
These are controlled fixture observations only, not native startup/history proof.

Original rows still fail the existing missing-thread guard (their thread IDs are
zero). Tests preserve that rejection and swapped-JID rejection. Separately named
component tests clone row headers and supply synthetic thread ID 1 to exercise
paired-ID normalization, owned values and commit/rollback ordering; the original
JSON captures remain untouched. Unresolved savepoints and complete streams with
unvalidated journaling transitions still reject. See
`docs/dcg-file-member-identity.md` for the evidence contract and limits.

The subsequent offline thread audit confirmed that bytes 40–47 are already zero
in every original scenario entry; sanitization and decoding do not lose them.
Regressions preserve those values at nonzero buffer offsets and keep the
normalizer stopped even if later terminal controls are supplied. The per-receiver
thread recording requirement and missing evidence for a fresh positive capture
are documented in `docs/dcg-native-thread-identity.md`.

`thread-enabled.json` contains a later, separately owned fixture with eight
scenarios and 180 native entries. The receiver was configured to retain its
existing job/user/program fields and add thread recording before the tables and
transactions were created. All 33 R records have actual nonzero thread IDs; those
bytes are unchanged by sanitization. `thread-enabled-bindings.json` uses the same
candidate D/CT/F/JM association approach, with an independent JID mapping for this
fixture. It is not protected startup/history proof.

`As400NativeThreadReplayTest` exercises commit, full rollback, interleaving and an
actual UB/UP pair without synthetic thread substitution. It keeps unresolved
savepoints and cycle-zero F/CH fatal. The original zero-thread files and tests
remain negative coverage. Source settings, raw replay, cleanup and limitations
are recorded in `docs/dcg-native-thread-validation.md`.

`As400NativeLifecycleRecoveryTest` reuses the same bytes for fresh-instance F/W
recovery rejection, post-C/RB tails, unflagged F/MD observations and a comparison
of both explicit component-only savepoint policies. Identical final changes under
the two policies do not select a production disposition. No fixture, production
guard or default policy is changed; see `docs/dcg-native-lifecycle-validation.md`.

`statistics-request.json` is a separate 2026-11-08 capture from isolated library
DCNC7707. It preserves the unmodified owned RJNE0200 bytes of F/CB subtype C at
sequence 183, including actual thread ID, member JID, payload and null-map prefix.
The ordinary-reader regression accepts it without output or durable F/W movement;
the strict profile continues to reject it. Its fixed test binding supplies only
the observed member identity needed for this decoder test. The unused file JID,
physical decoder and starting checkpoint are synthetic, not startup or association
proof. The complete packaged native run independently validates acquired bindings,
recovery and later output; see `docs/dcg-rrn-validation.md`.

`lifecycle-discrimination.json` adds 61 native entries from a fresh isolated
fixture: two committed clears, a deliberate end/write/start journaling gap,
repeated SQL savepoint rollback, a duplicate-key INSERT with a surviving prefix,
and an explicit atomic compound failure. `lifecycle-discrimination-bindings.json`
contains candidate paired identities from the separately captured creation
history. The real gap contains no journal row for 999 although the source contains
it. Actual thread bytes and payloads remain unchanged. Raw captures, independent
source checks and verified cleanup are described in
`docs/dcg-native-lifecycle-discrimination.md`.

`As400IncrementalLifecycleTest` sends these complete captures through the shipped
incremental lifecycle validator, replay coordinator and dispatcher. It checks
commit/rollback, every-entry restart, missing/mismatched companions, member
restoration and references to consumed savepoints. The earlier two-pass hypothesis
checker was removed; these regressions now exercise the actual implementation.
Default unresolved-policy rejection remains in `As400SourceNormalizerTest` and
`As400NativeThreadReplayTest`.

`temporal-rows.json` contains eight actual R/PT, R/UB, R/UP and R/DL images
captured on 2026-11-08 in fresh isolated library DCNC9385. The schema has integer
ID, nullable DATE, TIME and TIMESTAMP(0/3/6/9). It covers null indicators, leap
dates, pre-epoch fractions, year 0001/9999 and TIME 24:00:00. Native row payload,
null-map fields, timestamps, thread IDs, record numbers and sequence/cycle bytes
are retained. Header names/addresses and JIDs are mapped to generic LIB/M and
distinct file/member identities; these are not startup proof. The included extreme
TIMESTAMP(9) rows intentionally fail the epoch-nanos range check.

`temporal-columns.json` preserves the captured type/length/buffer/CCSID/precision
and date/time format attributes. Table/system names, ordinals and simple-table
flags are fixture definitions for this known CREATE TABLE, not independently
collected identity evidence. The packaged native run independently acquires all
catalog fields. Source capture hashes and sanitization details are retained in
the local report described in `docs/dcg-temporal-validation.md`.

`automatic-fence-native-scope.json` preserves the successful current-metadata
candidate from isolated DCNC0946 on 2026-11-09. Its journal inventory comes from
WRKJRNA's native outfile and independent JOURNAL_INFO counts; exact member queries
find EXTRA/DATA in addition to A/B/M/U. These are unmodified metadata responses,
not application data or certified historical bindings. The native identity values
belong to that removed disposable fixture. The test requires candidate-only status
and reuses the original native binding validators. See
`docs/dcg-lock-scope-validation.md` for reproduction, negative coverage and limits.


`late-membership.json` is the native DCNC0959 negative fixture. Its row-only
projections deliberately omit earlier maintenance so the last unknown-binding
check can be exercised independently. The complete all-code capture is separately
rejected for pointer-backed maintenance; the fixture never authorizes that omission
in a connector. `prospective-native.json` is the DCNC0961 prospective-schema
fixture: native origin 59, actual completed row cut 66, then clear/reload with an
unrelated row before commit. It includes older C/J history and observed journal,
receiver, file/member and schema evidence. Both remain `startup_proof=false`.
See [the validation report](https://github.com/gardnmi/debezium-connector-ibmi/blob/fc4ffaa2aa7bf8acc5d9630d493450391814f8c0/docs/dcg-short-boundary-validation.md#continued-work-late-membership-and-prospective-schema-history)
for capture, cleanup, failed attempts and qualification limits.

`equal-width-alter.json` preserves native DCNC0965 INTEGER -> REAL -> INTEGER
metadata and the complete all-code journal capture. File/member journal IDs remain
constant; native object identities change, and the format definition returns to
its original value. The actual F/CH subtype 0 rejects before the REAL row is
misdecoded or F/W advances. Cleanup was verified.

`prospective-boundary.json` is the separate DCNC0966 active-writer bundle. It
contains the actual requested scope and SQL program, drained holder evidence,
before/after metadata, full raw A/N/Z audit, older controls and prospective rows.
Rows occurred both before and after N while the metadata collector ran. It remains
`startup_proof=false`; original fixtures are not overwritten or promoted. See
[the joined-boundary report](https://github.com/gardnmi/debezium-connector-ibmi/blob/fc4ffaa2aa7bf8acc5d9630d493450391814f8c0/docs/dcg-short-boundary-validation.md#joined-boundary-evidence-active-writers-and-schema-replacement)
for limits, rejected attempts and native/local testing distinctions.

`pending-receiver-boundary.json` preserves the actual pending-receiver samples,
complete native directory/identity observations and raw all-code audit bytes from
DCNC0976 on 2026-11-09. The busy attempt discovers receiver floor 4; the later
candidate discovers floor 46, including the original cycle referenced by a
terminal after the pending flag cleared. Neither floor was supplied from the
fixture's known transaction start. Bytes and original SHA-256 values are retained;
these are disposable fixture records and remain candidate evidence, not startup
proof. Regression mutations exercise observations outside the audit interval,
receiver replacement, incomplete pages, missing records and unknown pending flags.
Fixture cleanup was verified. See `docs/dcg-native-startup-collection.md` for the
exact live-versus-local verification boundary.
