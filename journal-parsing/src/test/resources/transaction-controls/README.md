# Synthetic transaction-control fixtures

These are hand-constructed RJNE0200 entries using the documented IBM field
offsets, encoded in EBCDIC CCSID 37. They are **not captured IBM i responses**.
They independently pin the wire layout rather than serializing the production
decoder's AS400Structure in tests. No credentials or source records are present.

| Fixture | Type | Indicator at 216 | Nested level at 232 | Payload |
| --- | --- | --- | --- | --- |
| commit.hex | C/CM | 0 | 0 | None |
| rollback.hex | C/RB | 2 | 0 | None |
| savepoint-release.hex | C/SQ | blank | 2 | Target 9223372036854775825 |

Sequence is an unsigned 8-byte integer at 24; commit cycle is 10 at 64.
The savepoint entry sequence is 18446744073709551614, greater than its target.
The separate bit-flags byte at 218 is 0x80, deliberately distinct from JOFLAG.
The fixed header is 236 bytes. SQ has entry-specific-data displacement 240,
a 16-byte prefix declaring 20 payload bytes, and a 20-character decimal target.
There is no next entry, receiver-information displacement, XA identifier, or LUW.

Tests mutate these fixtures to cover other outcomes, unsupported controls,
savepoint rollback, unsigned values, concatenated entries, and malformed bounds.
Release-specific live control fixtures and savepoint lifecycle validation remain
required before transactional clears can be enabled. Earlier live probes recorded
the control types but did not retain their payloads or fixed headers.

References:
- [RJNE0200 and entry-specific-data prefix](https://www.ibm.com/docs/ssw_ibm_i_74/apis/QJORJRNE.htm)
- [CM/RB outcomes and SQ/SU target layout](https://www.ibm.com/docs/en/ssw_ibm_i_74/rzaki/rzakivarlength.htm)
- [Indicator flag / JOFLAG terminology](https://www.ibm.com/docs/en/i/7.4.0?topic=information-layouts-fixed-length-portion-journal-entries)
