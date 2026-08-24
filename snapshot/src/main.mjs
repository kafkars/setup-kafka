import path from 'node:path'

import { Deadline } from '../../lib/deadline.ts'
import { finalizeEvidence } from '../../lib/evidence.ts'
import { asSetupKafkaError, SetupKafkaError } from '../../lib/errors.ts'
import { annotation, getInput, mask, setOutput } from '../../lib/github.ts'
import { captureDiagnostics } from '../../lib/diagnostics.ts'
import { readState } from '../../lib/state.ts'

try {
  const stateFile = getInput('state-file').trim()
  const name = getInput('name').trim() || 'snapshot'
  const timeoutText = getInput('timeout-seconds').trim() || '30'
  const declareText = getInput('declare-evidence').trim() || 'true'
  if (!stateFile) throw new SetupKafkaError('input_missing', 'state-file is required')
  if (!/^[a-z0-9][a-z0-9_-]{0,31}$/.test(name)) {
    throw new SetupKafkaError('input_invalid', 'name must be 1-32 lowercase letters, digits, underscores, or hyphens')
  }
  if (!/^\d+$/.test(timeoutText) || Number(timeoutText) < 5 || Number(timeoutText) > 300) {
    throw new SetupKafkaError('input_invalid', 'timeout-seconds must be between 5 and 300')
  }
  if (!['true', 'false'].includes(declareText)) {
    throw new SetupKafkaError('input_invalid', 'declare-evidence must be true or false')
  }
  const state = readState(stateFile)
  for (const secret of state.secrets) mask(secret)
  const issues = await captureDiagnostics(state, new Deadline(Number(timeoutText) * 1_000, 'evidence snapshot'))
  const archive = path.join(
    path.dirname(state.evidenceDirectory),
    `${path.basename(state.evidenceDirectory)}-${state.project}-${name}.tar`,
  )
  const result = finalizeEvidence(state, {
    phase: 'snapshot',
    diagnostic: issues.length > 0 ? issues.join('; ') : null,
    extra: { setupPhase: state.phase, snapshotName: name, diagnosticGaps: issues },
  }, {
    archive,
    declare: declareText === 'true',
  })
  for (const issue of issues) annotation('warning', issue, 'setup-kafka snapshot')
  setOutput('evidence-archive', result.archive)
  setOutput('evidence-digest', `sha256:${result.archiveDigest}`)
} catch (error) {
  const normalized = asSetupKafkaError(error, 'snapshot_action_failed')
  annotation('error', `${normalized.code}: ${normalized.message}`, 'setup-kafka snapshot')
  process.exitCode = 1
}
