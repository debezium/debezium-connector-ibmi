# Required INTEGER rows with an empty variable-length null map

These three hexadecimal RJNE0200 fixtures derive from the owned IBM i test
`DCTASKQA.NATREAD` capture on 2026-11-07 (IBM i 7.5). The source table had one
`INTEGER NOT NULL PRIMARY KEY` column; operations used commitment level NONE.
They represent R/PT entries at sequences 16, 18 and 19 with ID values 1, 2 and 3.

Each fixture retains the first 260 captured bytes: the 236-byte header, a
four-byte zero null-indicator count, the 16-byte entry-specific prefix and the
four-byte integer. The capture's final-entry displacement was zero; unused
receiver-buffer capacity after the inline row is omitted. The header decoder
therefore derives the boundary from the fixture's length. The bytes at offsets
236–259 are unchanged from the native capture, including the map length, zoned
payload length, reserved prefix and integer value.

For redistributable test data, the following header bytes are sanitized:

- 82–97: remote address; filled with EBCDIC blanks.
- 101–156: job/user/number and program/library metadata; EBCDIC blanks.
- 187–196: effective user profile; EBCDIC blanks.
- 197–206: opaque object journal identifier; binary zeros.
- 208–215: system name; EBCDIC blanks.

Structural offsets, sequence numbers, record numbers, flags and row bytes are
retained. These are decoder fixtures, not production identity/startup evidence.
The test passes each through the real `EntryHeaderDecoder` and strict row
decoder, including at a nonzero enclosing-buffer offset.

Original captured-byte SHA-256 values (before trimming/sanitization):

- ID 1: `2a6bff97abf82654def5c834e979c53de66f5df525e55a1b875790513d62aa74`
- ID 2: `7846dbb80ba8e3067e1f0e91ca7bb81daa104c7a283c41e4653772c692c38cff`
- ID 3: `81bf8b4687f0ef55abdd0afb380bd974f622752a52decc3a7ac982c542f52887`

The native capture and local validation are documented in
[validation scope](../../../../../docs/dcg-validation.md).
