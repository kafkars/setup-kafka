import { runCompose } from './compose.mjs'
import { writeSanitized } from './evidence.mjs'
import { asSetupKafkaError } from './errors.ts'
import { redactText } from './process.ts'

export async function captureDiagnostics(state, deadline) {
  const issues = []
  await capture('compose ps', state.composePsFile, ['ps', '--all', '--format', 'json'], 10_000, 2 * 1024 * 1024)
  await capture(
    'broker logs',
    state.brokerLogFile,
    ['logs', '--no-color', '--timestamps', '--tail', '2000', ...state.brokerServices],
    20_000,
    8 * 1024 * 1024,
  )
  return issues

  async function capture(label, file, args, maximumMs, captureLimitBytes) {
    if (deadline.expired()) {
      issues.push(`${label} skipped because the cleanup deadline elapsed`)
      return
    }
    try {
      const result = await runCompose(state, args, {
        timeoutMs: deadline.remainingMs(maximumMs),
        captureLimitBytes,
        operation: label.replaceAll(' ', '-'),
      })
      const content = result.stdout || result.stderr
      writeSanitized(file, content || `${label}: no output\n`, state)
      if (result.code !== 0 || result.timedOut) {
        issues.push(`${label} capture failed: ${redactText(result.stderr || result.stdout, state.secrets).slice(-1_000)}`)
      }
    } catch (error) {
      const normalized = asSetupKafkaError(error, 'diagnostic_capture_failed')
      issues.push(`${label} capture failed: ${redactText(normalized.message, state.secrets)}`)
      writeSanitized(file, `${issues.at(-1)}\n`, state)
    }
  }
}
