# Empty null-indicator maps in strict D/CG row decoding

The strict decoder now accepts a bounded, zero-length `*VARLEN` null-indicator
map only when every column in the pinned physical layout is non-nullable. The
four-byte length word must fit inside the current entry and must not overlap
the entry-specific prefix or row. This is the same non-nullability requirement
already applied when the null-indicator displacement is zero.

## Documented semantics and observed representation

IBM documents that null indicators are present for a file with nullable fields
or for minimized record images. In the API's `*VARLEN` representation, a binary
four-byte returned length precedes the indicator bytes. A nonzero displacement
locates that section; it does not imply a nonempty indicator array. For populated
maps, the indicators distinguish real non-null values, nulls and uncollected
fields supplied with default data.

References:

- [DSPJRN NULLINDLEN](https://www.ibm.com/docs/en/i/7.4.0?topic=ssw_ibm_i_74%2Fcl%2Fdspjrn.html): conditions under which record indicators are present.
- [QjoRetrieveJournalEntries API](https://www.ibm.com/docs/ssw_ibm_i_74/apis/QJORJRNE.htm): variable-length section layout, returned length and indicator meanings.
- [Minimized entry-specific data](https://www.ibm.com/docs/en/i/7.5.0?topic=wjei-considerations-entries-which-contain-minimized-entry-specific-data): minimization can use indicators even for a file without nullable fields.

The concrete empty-section representation was observed on IBM i 7.5 in the
previous isolated native test: three R/PT records for a single required INTEGER
had null-map displacement 236, returned count 0, payload displacement 240 and
four bytes of integer data. This is captured evidence, not an assumption that
all missing maps imply non-null values. Context7 had no matching API entry;
the references above were checked directly in IBM's indexed documentation.

## Correction and regression coverage

`CompleteRowEntryDecoder` continues to reject unsupported/minimized flags and
pointer-backed/incomplete records before decoding their fields. It validates
map displacement, the length word, span and overlap before accepting an empty
map. Both absent and empty maps reject a layout containing any nullable column.
Every nonempty map still requires exactly one indicator per column; defaulted
(`F9`), invalid, null-for-required and mismatched maps remain errors.

The captured records are retained as three sanitized hex resources under
`journal-parsing/src/test/resources/complete-rows/`, with provenance in their
README. Six regression cases decode the actual captured structure through the
real header decoder and strict row decoder, at offsets zero and 19. All six
failed with the original rejection before the correction. Additional cases
exercise mixed nullable layouts, multiple required columns, exact-end and
truncated length words, overlap, invalid displacements, unsigned length overflow,
defaulted fields, mismatched maps and unsupported/minimized flags. Existing
nullable, codec and field-boundary tests are preserved.

All 847 unit tests passed, including 55 decoder cases (29 new cases). The three
original, untrimmed captures also decoded correctly in a separate offline replay.
Assembly and formatter/import validation passed, and the packaged decoder class
matches the tested compiled class. Advisor inspected actual files and the saved
evidence and passed with no actionable findings; it did not rerun tests or live
operations.

Local evidence is saved under
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-null-map/`:

- `fixture-provenance.json`: captured/fixture hashes and unchanged row regions.
- `regression-before.log`: six captured-row failures on the original decoder.
- `regression-after.log`: all 55 focused decoder cases passed.
- `verify.log`, `package.log`: full unit/build and assembly/format/import checks.
- `raw-replay.json`: offline replay of the original, untrimmed, unsanitized captures.
- `validation.json`, `change.patch`, `advisor-review.json`: final changes, preserved
  streaming guards/protected worktrees and review result.

## Limits

This closes the specific empty-map rejection using local replay. No new IBM i
connections or mutations were needed, and the previous native run's evidence
remains unchanged: that run used the legacy decoder for lifecycle publication
and recorded the strict rejection separately. A subsequent
[native interruption run](dcg-strict-reader-validation.md) passed with the
corrected strict decoder in the test publication path and no legacy fallback.
That separate report records its scope and cleanup evidence.

Transactional D/CG streaming remains disabled. Native transactional rollback,
recovery/startup and integration gates remain separate. F/CR PR #114 is unchanged;
no commits or pushes were made.
