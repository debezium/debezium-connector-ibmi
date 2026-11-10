# Local draft: IBM i D/CG journal lifecycle questions

**Unpublished technical question packet.** No support case or PR comment has been
sent. This concerns transactional D/CG support only; F/CR PR #114 is separate.

**Later evidence:** the [discrimination experiment](dcg-native-lifecycle-discrimination.md)
answers enough of these questions to implement conservative target consumption and
test a bounded lifecycle candidate. The questions below remain useful for broader
source guarantees; they are no longer treated as prerequisites for that work.

## Context and reproducible evidence

On an isolated IBM i 7.5 fixture, SQL TRUNCATE without IMMEDIATE produces D/CG
subtype 8; optimized bulk DELETE produces subtype 3. The receiver retains job,
user, program and thread fields. The tables have one non-null integer primary
key, one member and both before/after images. Native captures, independently
checked final rows and completed cleanup are preserved locally.

The [assessment](dcg-native-lifecycle-validation.md) gives exact sequences and
reference links. Sanitized fixtures are in
`debezium-connector-ibmi/src/test/resources/native-transactions/thread-enabled.json`
and `thread-enabled-bindings.json`. They retain actual thread IDs, cycles,
sequences, payloads and flags; private header labels and JIDs are mapped. Raw
pages remain local and should be reviewed for disclosure before sharing.

## Questions for broader source guarantees

1. **Internal journal transitions versus actual loss of coverage.** Clears show
   F/CH followed by cycle-zero F/EJ, D/EF, D/JF, F/JM and then transactional D/CG.
   The four end/start entries have the ignore-on-APY/RMV bit set. What supported
   fields and ordering rules establish that these are completely represented by
   D/CG and do not invalidate file/member identity, row layout, RRN pairing or
   journal-history continuity? Is that bit sufficient for these specific types,
   or is another relationship required? How must interrupted/failed operations,
   concurrent jobs and receiver rollover be handled? We are not proposing a
   blanket ignore-bit filter for row/control entries.
2. **Lifecycle tails after rollback.** Native C/RB can precede the cycle-zero
   EJ/EF/JF/JM tail. Savepoint rollback can be followed by F/MD with no ignore bit
   and no commit cycle, plus journal-start records. What identifies and completes
   these tails, and distinguishes F/MD cleanup from an actual RMVM/DLTF? Can a CDC
   reader restart immediately after C/RB, or must it retain earlier context or
   defer the completion boundary until a documented later marker? Does C/SU have
   the same limitation?
3. **Journal savepoint target lifetime.** C/SQ and C/SU name the C/SB sequence.
   Does SU release that target and its descendants, or retain the target? Is the
   answer different for SQL SAVEPOINT and internal nested cycles? Repeated SQL
   rollback to the same name produces new SB sequences in our captures. Both
   retention choices currently reproduce the correct final rows, so those rows
   are not a sufficient oracle. C/SB data is documented as internal; please
   identify a supported rule/API rather than an interpretation of its `40`/`80`
   bytes. The catalog's SU text appears to repeat SQ's description.

Please identify applicable release/PTF restrictions and a supported public
reference or API contract. IBM SQL Replication's documented full-refresh handling
of F/EJ/F/MD is relevant context, but we are asking whether a transaction-aware
CDC implementation can safely continue incrementally under a narrower contract.

## Remaining validation, with or without clarification

- Encode validated type-specific rules or an explicitly conservative policy that
  rejects unvalidated continuations; preserve all other unsupported,
  minimized, pointer, identity and schema checks. Require complete evidence before
  publishing any affected batch.
- Use a fresh isolated fixture to compare genuine ENDJRNPF/STRJRNPF, member
  replacement and failed clear paths with the supported internal sequences. These
  must not masquerade as a validated internal operation.
- Exercise the clarified savepoint distinction, repeated rollback/release, nested
  internal failures and interleaved producers using actual native identifiers.
- Restart at every precursor/tail position, including immediately after C/RB,
  across receiver rollover and interrupted delivery. Verify R/F/W separately,
  whole-batch replay, commit ordering, no output for rolled-back changes and no
  advancement on ambiguous input.
- Complete the remaining startup/history/baseline and Kafka integration gates
  before enabling transactional streaming. Clarification alone is not approval
  to bypass those gates.
