# Synthetic startup evidence fixtures

`sql-observations.json` uses the documented column names and result shapes of
IBM i SQL services. It is hand-authored; it contains no captured auction data or
production metadata. `ZOLD` precedes `ANEW` by explicit links, and the sequence
resets from 92 to 1. Neither names nor timestamps are incarnation identities.

The JDBC fixture harness exercises the real collector, bound query parameters,
metadata/history decoding, limits and resource cleanup. It does not execute Db2
SQL, prove IBM i service availability/authority, or establish startup protection.
Even the complete-looking fixture must be rejected by the startup validator.
See [API evidence assessment](../../../../../docs/dcg-evidence-apis.md).

`native-identities.json` contains hand-authored projected rows for exact journal,
receiver, physical file and member paths. It records the collector's bounded
`PATH_NAME`/original `PATH_LENGTH` projection and `HAS_SYMBOLIC_LINK` expression,
plus the documented native identity columns. Values above 2^53, a zero generation
and the SQL BIGINT maximum exercise exact numeric ownership and comparison. They
are not real IBM i identities and say nothing about native ID lifetime.

`As400JdbcNativeIdentityEvidenceTest` drives the actual prepared query and decoder
through mocked JDBC, including a two-row cap that exposes a duplicate sentinel.
It mutates each tuple component independently between observations and simulates
reuse of a borrowed raw buffer. Mismatch or failed re-observation yields no read
result. This does not execute SQL, prove the service returns the starting *FILE
directory itself, or supply an actual RPC source binding or protection session.
See the [native identity validation status](../../../../../docs/dcg-validation.md#native-object-identity-candidate-collection).
