# Selected first-release D/CG recovery contract

The implementation has since reached the configured normal-task path. See the
[current source release checklist](dcg-source-release-checklist.md) for completed
work and remaining validation. The dated decision below records the selected
operator-retention contract; its earlier implementation gaps are historical.

Status: operator-managed retention selected by the user on 2026-11-07.
Implementation and validation are authorized; transactional streaming remains gated.
This does not declare D/CG feature complete or accepted upstream.

The buffer, normalization, F/W recovery and retained reader handoff exist. Native
captures now also cover abandoned optimized clears and two-table rollback tails;
see [the new validation report](dcg-multiple-clear-validation.md). The remaining
source adapter cannot honestly implement the current protected `Inspection`
contract using read-only catalog queries alone. That contract requires protection
against deletion and maintenance throughout the recovery interval. The proposed
ordinary CDC contract instead relies on operator obligations and strict detection
of unavailable history. They are different guarantees, not interchangeable API
implementations.

## Selected first release: ordinary CDC recovery

Proceed with the candidate in [the runtime profile revision](dcg-runtime-profile-revision.md),
conditional on the following explicit scope:

- Keep transaction buffering, savepoint/full rollback, serial commit output,
  immutable F/previous-W, whole-batch replay, bounded memory and fail-on-error.
- Other committed transactions must continue while a captured SQL clear remains
  uncommitted. The user rejected a connector-wide metadata pause after its
  potentially unbounded duration was clarified. A timeout that fails the whole
  task does not meet this requirement. Preserve commit ordering and F/W; retain
  fail-closed behavior for actual identity, schema or retained-history faults.
- Use standard Kafka Connect offsets and its tested task-generation fencing path.
  Qualify a supported worker/broker configuration before enabling it; the existing
  Kafka 4.3.1 experiments are evidence, not a production support declaration.
- Require operators to retain journal receivers reaching the oldest durable F,
  including downtime and open transactions. Validate identity-qualified complete
  history before dependent publication. Missing or ambiguous history stops capture;
  it never selects a newer suffix, silently resets offsets or releases retention.
- Use fixed single-member files and qualified row layouts. Unsupported schema,
  journaling, source restore/clone or journal reset requires controlled recovery.
  Validate the trusted administration restrictions; equal catalog reads do not
  prove absence of intervening maintenance.
- First initialization requires a consistent source/output baseline and complete
  starting transaction state. An explicit controlled initialization procedure must
  be separate from ordinary task startup. Normal resume always rejects missing or
  incompatible F/W; offset loss must never trigger an automatic snapshot/fresh start.
  A supported offset-seeding procedure is a candidate to investigate, not an
  implemented or qualified workflow.
- Require a truncate-aware sink, one partition per table and the tested consumer
  visibility/order rules. Make the existing at-least-once/partial-visibility claim.
- Provide no automatic receiver-deletion permission, maintenance authorization or
  independent detection that source and offsets were restored to an old backup.

Accepting this scope authorizes implementation and validation of a separate
candidate production path. It does not waive missing technical evidence, prove
operator restrictions, remove existing strict tests or enable streaming immediately.
The strict protected path stays intact. No permissive adapter may return fabricated
proof to its existing `Inspection` methods.

## Alternative: mandatory enforced history protection

Retain protection against receiver deletion and coordinated maintenance as a
mandatory prerequisite for every D/CG installation. Before its production adapter
can be completed, select an operator-owned enforcement mechanism and durable
protection/enrollment store. The current proposal describes a deletion exit,
authority boundaries and a registry, but none is selected or installed. Kafka
producer fencing cannot enforce IBM i receiver retention.

Installing or changing system-wide deletion exits/authorities on shared edgeqa
is outside authorization to mutate our isolated fixture library. That requires
operator involvement and a reviewed, concrete deployment plan. Do not install one
or choose a new production service merely to make a startup test pass.

## Selected gate mapping

| Obligations | Proposed disposition | Still required before enablement |
| --- | --- | --- |
| Transaction/output and F/W correctness; C1/C3/H9 | Preserve | Remaining native/runtime delivery cases and full source-to-sink recovery |
| Publication ownership; C2/C4/C9, publication portions of P5/P6 | Prefer standard Connect fencing; retain stricter path | Supported configuration validation and actual source task integration |
| Native identity/history; C5/C6, N/H requirements | Preserve correctness; explicitly bound administrative assumptions | Rollover, missing/deleted receiver and partial-read tests; durable historical bindings; maintained inspection-to-reader continuity |
| Initial baseline/resume; C7/C8/B | Preserve | Complete cycle census/baseline handoff and crash-safe explicit first initialization; missing-offset regression |
| Automatic pin release and maintenance permission; stronger P obligations | Outside ordinary CDC path, retained for optional strict path | No code may grant those permissions from candidate offsets or callbacks |
| Independent old-backup/restore detection; stronger N/P/B obligations | Outside ordinary CDC support contract | Observable replacement still rejects; unsupported restore requires explicit recovery |
| C10/C12 | Remain open | Native source-to-sink convergence and review of exact tested artifacts/support scope |

A user scope decision is not upstream maintainer acceptance or evidence that the
source operator already enforces these rules. Those claims must stay separate.
