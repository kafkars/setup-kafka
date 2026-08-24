import assert from 'node:assert/strict'
import test from 'node:test'

import { redactText, requireSuccess, runCommand } from '../lib/process.ts'

test('command execution captures bounded output and status', async () => {
  const result = await runCommand({
    command: process.execPath,
    args: ['-e', 'process.stdout.write("ok")'],
    timeoutMs: 5_000,
  })
  assert.equal(result.code, 0)
  assert.equal(result.stdout, 'ok')
})

test('command timeout is reported', async () => {
  const result = await runCommand({
    command: process.execPath,
    args: ['-e', 'setTimeout(() => {}, 10000)'],
    timeoutMs: 20,
  })
  assert.equal(result.timedOut, true)
  assert.throws(() => requireSuccess(result, 'timed_out', 'test command'), /timed out/)
})

test('diagnostics redact every secret occurrence', () => {
  assert.equal(redactText('a-secret-a-secret', ['secret']), 'a-<redacted>-a-<redacted>')
})
