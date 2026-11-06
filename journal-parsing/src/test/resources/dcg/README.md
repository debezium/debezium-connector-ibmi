# D/CG decoder fixtures

Derived from QjoRetrieveJournalEntries responses for a disposable DTEST.TESTT
fixture on IBM i 7.5, Db2 PTF SF99950 level 11. The tests used 1,200 synthetic rows.

- dcg-3.hex: optimized DELETE under commitment control.
- dcg-8.hex: SQL TRUNCATE without IMMEDIATE under commitment control.

The live decoder correctly read both operations in committed and rolled-back
transactions. Each response had a 16-byte API prefix declaring 4,500 bytes of
entry-specific data, an entry-specific-data offset of 272, and a blank member.
The single-entry response's end offset was 131008 (including unused buffer space).

These fixtures retain the actual length bytes, file/library/member fields, and
subtype. Reserved bytes and unparsed payload metadata are zeroed to avoid retaining
system-specific identifiers. They contain no row data, credentials, or job headers.

All observed D/CG clears had nonzero commit cycles. Rollback restored the 1,200
rows without row-level delete events. These samples validate decoding; they do
not establish safe emission of truncate events before transaction completion.
