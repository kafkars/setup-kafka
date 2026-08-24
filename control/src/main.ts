import { controlEnvironment } from '../../lib/control.ts'
import { asSetupKafkaError, SetupKafkaError } from '../../lib/errors.ts'
import { annotation, getInput, mask, setOutput } from '../../lib/github.ts'
import { readState } from '../../lib/state.ts'

try {
  const stateFile = getInput('state-file').trim()
  const operation = getInput('operation').trim()
  const broker = getInput('broker').trim()
  const timeoutText = getInput('timeout-seconds').trim() || '60'
  if (!stateFile) throw new SetupKafkaError('input_missing', 'state-file is required')
  if (!/^\d+$/.test(timeoutText)) throw new SetupKafkaError('input_invalid', 'timeout-seconds must be an integer')
  const state = readState(stateFile)
  for (const secret of state.secrets) mask(secret)
  const result = await controlEnvironment(state, operation, broker, Number(timeoutText) * 1_000)
  setOutput('operation', result.operation)
  setOutput('brokers', result.brokers.join(','))
} catch (error) {
  const normalized = asSetupKafkaError(error, 'control_action_failed')
  annotation('error', `${normalized.code}: ${normalized.message}`, 'setup-kafka control')
  process.exitCode = 1
}
