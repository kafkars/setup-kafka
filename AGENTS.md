# Repository contract

This repository owns generic Apache Kafka environment lifecycle. It does not own
client qualification verdicts, scenario semantics, or production deployment.

## Before coding

- Identify whether a change belongs to selection, lifecycle, readiness,
  evidence, controls, or the GitHub adapter.
- Preserve exact versions, bounded execution, explicit ownership, and
  independently observable readiness.
- Keep Testlab-specific qualification behavior in `kafkars/testlab`.

## Non-negotiable rules

- Kafka versions and container images are exact and digest-pinned.
- Inputs never accept arbitrary shell, Compose files, image names, or broker
  configuration.
- Readiness means external client connectivity plus exact leader/full-ISR topic
  state and an idempotent production proof; a listening port is insufficient.
- One root Action owns startup, evidence, and unconditional post-job cleanup.
- Generated secrets remain private, masked, mode 0600, and absent from evidence.
- Every subprocess has a bounded capture and an absolute deadline.
- Root, `control`, and `snapshot` Actions use the same-commit state contract.

## Source shape

- Shared behavior belongs in `lib/`; Action entrypoints remain thin.
- Tests live in `test/`, never beside implementation.
- JavaScript source files remain at or below 300 lines.
- Runtime and development npm dependencies remain empty unless a concrete
  capability justifies the supply-chain cost.
- Run `./scripts/check` before handing off a change.
