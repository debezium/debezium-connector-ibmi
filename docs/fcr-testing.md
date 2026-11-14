# Testing F/CR clear capture

Run the unit tests and build the plugin with JDK 21:

```sh
./mvnw clean install -Passembly -DskipITs \
  -Dformat.formatter.goal=validate -Dformat.imports.goal=check
```

The main branch uses Debezium snapshot dependencies; build the matching core
modules as described in `.github/workflows/cross-maven.yml` first if those
artifacts are unavailable. `-DskipITs` keeps this build independent of a live IBM i.

The live `As400ConnectorIT` class uses a disposable IBM i library named `DTEST`
by default and table `TESTT`. Set `-Dtest.database.schema=YOURTEST` to use a separate
library for concurrent testing; replace every `DTEST` reference in the fixture
setup and cleanup below with that name. The name must contain only uppercase
letters, digits, and underscores, begin with a letter, and be at most ten characters.
The test deletes rows during setup and clears and reloads the table.
Use a dedicated test environment and an account allowed to create this fixture,
read its journal, and execute `CLRPFM`. Do not point it at an application library.

Using an IBM i SQL client with commitment control disabled (`*NONE`), execute
`CREATE SCHEMA DTEST` first. **Stop if it fails or DTEST already exists**; do not
reuse or drop someone else's library. After successful creation, create the table:

```sql
CREATE SCHEMA DTEST;
CREATE TABLE DTEST.TESTT (
    ID INTEGER NOT NULL PRIMARY KEY,
    NAME VARCHAR(40) CCSID 37 NOT NULL
);
```

SQL schema creation supplies the fixture's `QSQJRN` journal and receiver. Verify
that `DTEST.TESTT` is journaled to `DTEST.QSQJRN` and has one member named `TESTT`.
Keep the fixture on a disposable system where creating and clearing it is safe.

Create a private Java properties file outside the checkout with the connection
settings below; restrict access to that file and do not commit it:

```properties
database.hostname=<test-host>
database.user=<test-user>
database.password=<test-password>
```

After the build above, run only this integration-test class from the repository root:

```sh
./mvnw -pl debezium-connector-ibmi \
  -Dit.test=As400ConnectorIT \
  -Dfailsafe.systemPropertiesFile=/absolute/path/to/private-test.properties \
  -DenablePropertiesElement=false \
  failsafe:integration-test failsafe:verify
```

The properties flag prevents the test report from listing JVM system properties;
review logs before sharing them because connection information can still appear.
The tests run the connector in a local embedded Debezium engine and consume records
in memory. They cover snapshot/insert streaming and clear/reload using both
`CLRPFM` and SQL `TRUNCATE TABLE ... IMMEDIATE`, with commitment control disabled,
under `initial` and `no_data` snapshot modes. Both modes load table schemas;
`no_data` skips snapshot rows so the clear is the first captured data event. Use a separate
`-Ddbz.test.data.dir=/absolute/path/to/test-offsets` for concurrent runs.
Each clear case asserts a keyless truncate followed by an insert. The SQL case
requires a system where the operation produces `F/CR`; it does not establish
support for `D/CG`. Inspect the fixture journal to record the entry type observed.
These tests do not exercise Kafka, transforms, or a downstream sink.

After the connector has stopped, remove only the fixture created for this run.
Verify the library still contains only its test table and generated catalog,
journal, and receiver objects, and that its journal has no other users. With
commitment control disabled, drop `DTEST.TESTT RESTRICT`, then `DTEST RESTRICT`:

```sql
DROP TABLE DTEST.TESTT RESTRICT;
DROP SCHEMA DTEST RESTRICT;
```

If the server asks whether to delete the fixture's unsaved journal receiver
(`CPA7025`), confirm only the receiver belonging to this disposable fixture.
The SQL client can appear to hang while this inquiry is pending. Verify that both
the SQL schema and the IBM i library are gone, and remove the private credentials
file. Do not use `CASCADE` to bypass unexpected dependencies.

These F/CR integration tests do not qualify transactional D/CG or optimized
DELETE. Their separate native, restart and runtime evidence is recorded in the
[transactional validation report](dcg-validation.md). Downstream handling
still depends on the selected transform and consumer.

