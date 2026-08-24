import { actionInputs } from '../lib/inputs.ts'
import { publicOutputs } from '../lib/outputs.ts'
import { createEnvironment, startEnvironment } from '../lib/lifecycle.ts'
import {
  annotation,
  appendSummary,
  endGroup,
  mask,
  saveState,
  setOutput,
  startGroup,
} from '../lib/github.ts'
import { asSetupKafkaError } from '../lib/errors.ts'

try {
  const inputs = actionInputs()
  const { state, reservations } = await createEnvironment(inputs)
  for (const secret of state.secrets) mask(secret)
  saveState('SETUP_KAFKA_STATE', state.stateFile)
  for (const [name, value] of Object.entries(publicOutputs(state))) {
    setOutput(name, value)
  }
  startGroup(`Start Apache Kafka ${state.version}`)
  try {
    await startEnvironment(state, reservations)
  } finally {
    endGroup()
  }
  appendSummary(`
## setup-kafka

| Field | Value |
| --- | --- |
| Kafka | \`${state.version}\` |
| Image | \`${state.image}\` |
| Topology | \`${state.topology}\` |
| Security | \`${state.security.name}\` |
| Bootstrap servers | \`${state.bootstrapServers}\` |
| Compose project | \`${state.project}\` |
| Evidence | \`${state.evidenceDirectory}\` |

The environment passed broker API, external security, leader/full-ISR, and idempotent production readiness.
`)
} catch (error) {
  const normalized = asSetupKafkaError(error, 'action_failed')
  annotation('error', `${normalized.code}: ${normalized.message}`)
  process.exitCode = 1
}
