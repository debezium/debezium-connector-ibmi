# D/CG follow-up validation

This follow-up tracks debezium/dbz#3027 and depends on the F/CR-only change for
debezium/dbz#3025.
It preserves the prototype decoder and dispatch logic, but does not support
commitment-controlled clears. It is not ready to claim support for the live
D/CG operations observed below.

## Live observations

Tested with a disposable DTEST.TESTT table and 1,200 synthetic rows per case on
IBM i 7.5, Db2 PTF group SF99950 level 11. Connections used either JDBC
TRANSACTION_NONE or TRANSACTION_READ_COMMITTED with explicit commit/rollback.

| Operation | Journal entry | Commit cycle | Rows after |
| --- | --- | --- | --- |
| none-delete | F/CR | zero | 0 |
| none-truncate | F/CR | zero | 0 |
| commit-delete | D/CG subtype 3 | nonzero | 0 |
| rollback-delete | D/CG subtype 3 | nonzero | 1200 |
| commit-truncate | D/CG subtype 8 | nonzero | 0 |
| rollback-truncate | D/CG subtype 8 | nonzero | 1200 |

No case produced individual R/DL delete entries for the clear itself. The
rollback cases restored all 1,200 rows. Therefore a D/CG subtype alone is not
sufficient to emit an irreversible table-wide event.

The decoder read all four D/CG entries through the real QjoRetrieveJournalEntries
RPC path: correct file/library, blank member, and subtype 3 or 8. The 16-byte API
prefix declared 4,500 bytes of entry-specific data. Sanitized fixtures preserve
the fields used by the decoder and the observed lengths; see
`journal-parsing/src/test/resources/dcg/README.md`.

## Remaining work

Before supporting these transactional operations, define and implement buffering
through commit, discard on rollback, ordering with row changes, and durable
restart/replay behavior. Validate both committed and rolled-back clear/reload
sequences and restart at every relevant transaction boundary. Keep the current
nonzero-commit-cycle rejection until those guarantees are established.

The observations do not prove that D/CG can never occur with commit cycle zero
on another release or SQL path. That currently permitted path has synthetic unit
coverage but no successful live event-emission test. The RPC decoder checks are
not an end-to-end connector or sink test.

Reference: [IBM: SQL fast delete](https://www.ibm.com/support/pages/system/files/inline-files/Get%20Up%20to%20Speed%20on%20SQL.pdf).
