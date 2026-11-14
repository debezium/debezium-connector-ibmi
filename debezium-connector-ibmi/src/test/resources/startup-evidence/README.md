# Synthetic startup evidence fixtures

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
See the [validation scope](../../../../../docs/dcg-validation.md).
