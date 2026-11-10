# Public online configuration validation — 2026-11-10

## Current result

The normal connector now has a default-off `truncate.transactional.enabled`
setting and uses the existing native handoff/recovery adapters. The configuration
contract is in [online configuration](dcg-online-configuration.md). This does not
change the separate F/CR PR or claim unrestricted source/runtime support.

Local verification passes: **2,985 Java tests**, **143 Python tests**, Maven
`verify -DskipITs`, assembly packaging with formatter/import validation, and
`git diff --check`. The assembled connector's 212 class files and 86 source files
match the checked workspace outputs. Prior deterministic handoff, fencing,
recovery and stricter history tests remain present.

**The ordinary-plugin native run passed all 17 assertions** in fresh isolated
Edgeqa library **DCNC1009**, using local Kafka Connect **4.3.1**. This closes the
pending native gate for the documented public opt-in profile, including the
empty-origin handoff fix. No further connector-code change was needed after the
credential refresh. Native and local cleanup passed.

Advisor previously inspected the actual repository files and retained evidence
and passed the targeted public admission/idle-origin review. That review did not
claim a new audit of every historical feature or substitute for this native run.
After native-07, Advisor also passed the targeted actual-file and retained-evidence
review of this qualification and its limitations. The independent review did not
rerun live tests or perform an exhaustive audit.

## What changed and why

- Public admission validates the existing runtime/retention contract before
  native initialization. Missing or partial offsets, conflicting initialization
  settings, WARN processing and missing Kafka transaction context reject.
- The session shares the actual task configuration and service registry. An
  isolated run caught a second configuration instance missing the converter
  registry; the normal-task regression now checks object identity.
- An idle legacy dispatcher can stop before the accepted native U/DN marker.
  The connector now observes the exact object-free marker and excludes all
  intervening CDC before offering its ordinary Kafka barrier. It never uses
  receiver-tail metadata as completed output.
- When the prospective interval is empty, complete retained C/J history must
  prove no transaction is open. Only the exact persisted origin can initialize
  exclusive F=W. Recovery rechecks its retained marker and leaves ordinary
  CDC baselines and every other F/W anchor strict. The marker is not table output.
- The bounded reader reserves space for an origin check while returning later
  CDC records. R remains separate from acknowledged F/W.
- Once the handoff barrier or versioned F/W is durable, disabling the option
  cannot reinterpret it as a legacy offset.

## Retained ordinary-plugin attempts

Evidence root on the development host:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-public-online-20261110/`.
Source-connected attempts retain their source archives and independent broker
observations; the credential failure did not create a fixture or worker.

| Attempt / fixture | Result and correction | Cleanup |
| --- | --- | --- |
| native-01 / DCNC1004 | Test plugin directory did not group dependencies; corrected the harness layout. | Native and local cleanup passed. |
| native-02 / DCNC1005 | Existing legacy RPC selection rejected regex input; use unchanged concrete table names across transition. | Native and local cleanup passed. |
| native-03 / DCNC1006 | Ordinary snapshot/streaming passed; migration exposed missing converter registry. Independent Kafka offset remained 62. | Native and local cleanup passed. |
| native-04 / DCNC1007 | Busy-boundary legacy progress passed; idle legacy offset 80 could not reach origin 84. | Native and local cleanup passed. |
| native-05 / DCNC1008 | Exact marker bridge reached durable legacy 84; empty prospective interval rejected before HANDOFF/MIGRATION. Independent raw journal retained U/DN 84; broker control records contained LEGACY only. | Native and local cleanup passed. |
| native-06 / no fixture created | Expired AWS credential session prevented credential retrieval, before any IBM i connection or local fixture creation. | No resources created. |
| native-07 / DCNC1009 | Ordinary packaged connector passed all 17 assertions, including idle handoff, receiver rotation/open clear, task replacement, commit/rollback, both native clear subtypes and rejected unsafe startup. | Native and local cleanup passed; credentials removed. |

Native-05's independent audit ends at U/DN 85. Offset 84 was an observed marker,
not that tail. Its last accepted offset remained legacy; no versioned F/W or
truncate publication was accepted by the failed attempt. Its inherited report
text incorrectly called initialization seeded; the retained configs, actual
snapshot/stream records and `controller_seeded_offset=false` establish that the
ordinary connector produced it. The harness metadata is corrected for reruns.

## Successful run evidence

`native-07/connect/report.json` records the 17 assertions and exact distribution
and dependency hashes. Its `plugin/ibmi/` contains the ordinary assembly; test
providers and custom owner/receipt gates are absent from the worker plugin.
`provenance.json` matches the tested distribution to the previously verified
workspace class/source outputs. The connector JAR SHA-256 is
`4ff73c3e091af406ae5832e3cf3393012915e379f55603968815afa6f7420470`.

The independent `read_committed` records under `native-07/connect/control/`
show the actual legacy offset 62, later legacy progress to 80 and marker 84,
the acknowledged HANDOFF at 84, then exclusive F=W=84. Enabling capture adds no
snapshot records or duplicate legacy rows. The measured wait from release of
the deliberately held busy transaction to completed handoff was **126.883 s**.
That includes asynchronous metadata acquisition, auditing and Kafka
acknowledgement; it is **not** a write-lock duration or an activation-time SLA.

With B's truncate still open, A's new row reached completed output **W=99** in
`QSQJRN0003`, while replay floor **F=87** remained in `QSQJRN0002`. Killing the
owned worker and starting its replacement preserved the immutable binding
history and rebuilt the open transaction. Rollback produced no B clear/reload.
The next committed B transaction emitted `t,c` in that order. M's optimized
DELETE rollback likewise emitted nothing; its commit emitted `t,c`. The mixed
update preserved keys, precise decimal and binary values, non-ASCII text, nulls,
and integers beyond JavaScript's exact-number range. Final durable F=W reached
**149**, exclusive, in `QSQJRN0003`.

`native-07/connect/transactions-raw.json` independently identifies actual D/CG
subtypes: sequence **94**, cycle **87**, subtype **8** (rollback); **114**, cycle
**107**, subtype **8** (commit); **125**, cycle **118**, subtype **3** (rollback);
**143**, cycle **136**, subtype **3** (commit). The successful run's raw SQL audit
covers these later transactions, not the initial marker. The marker is validated
by the native reader in the exact tested connector; native-05 separately retains
an independent raw audit of its own corresponding idle marker case.

The normal stop completed native drain. A new connector with missing offsets but
existing history failed for the expected reason, with unchanged data/history.
Disabling the flag against durable F/W also failed without data/history changes.
Independent native queries verified the expected final A/B/M/U rows. Cleanup
verified schema, library and inquiries absent, removed all three owned receivers,
removed labeled local resources, and deleted the temporary credential file.

## Reproducing the run and limits

Run `tools/dcg-public-online-probe.py NEW_DIRECTORY DCNCdddd` using the portable,
owner-only `IBMI_TEST_CONFIG` described in [contributor testing](dcg-contributor-testing.md).
It stages the actual assembly under one worker plugin directory. Test helpers
remain on independent audit processes' classpaths. The experiment obtains the
legacy offset from an ordinary snapshot and streaming row; it does not seed one.

The successful run covers the public setting transition, no second snapshot,
busy-boundary progress, open clear plus receiver rotation, fresh-task recovery,
rollback suppression, committed truncate before reload, actual optimized DELETE
subtype 3, mixed-column values, missing-offset rejection, unsafe-downgrade
rejection and completed drain. Raw native records, read-committed broker records,
exact artifact hashes and fixture cleanup accompany the result.

Earlier successful test-provider crash runs remain useful evidence for the same
underlying adapters and complement this ordinary-plugin gate. See
[task-owned boundary validation](dcg-task-owned-boundary-validation.md). There is
no claim of dynamic schema evolution, automatic retention release, exactly-once
source delivery, universal pause duration, or validation of every IBM i/Kafka
configuration. No changes have been pushed or deployed.
