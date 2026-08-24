import { readState } from '../lib/state.ts'
import { finishEnvironment } from '../lib/lifecycle.mjs'
import { annotation, appendSummary, getState, mask } from '../lib/github.ts'
import { asSetupKafkaError } from '../lib/errors.ts'

const stateFile = getState('SETUP_KAFKA_STATE')
if (stateFile) {
  try {
    const state = readState(stateFile)
    for (const secret of state.secrets) mask(secret)
    const result = await finishEnvironment(state)
    for (const issue of result.issues) {
      annotation('warning', issue, 'setup-kafka diagnostics')
    }
    appendSummary(`
### setup-kafka cleanup

- Result: \`${result.phase}\`
- Evidence archive: \`${result.archive}\`
- Archive digest: \`sha256:${result.archiveDigest}\`
`)
  } catch (error) {
    const normalized = asSetupKafkaError(error, 'post_failed')
    annotation('error', `${normalized.code}: ${normalized.message}`, 'setup-kafka cleanup')
    process.exitCode = 1
  }
}
