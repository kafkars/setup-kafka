import assert from 'node:assert/strict'
import test from 'node:test'

import { resolveProjectName } from '../lib/project.mjs'

test('explicit Compose project names are preserved', () => {
  assert.equal(resolveProjectName('setup-kafka-ci_1'), 'setup-kafka-ci_1')
})

test('generated Compose project names are bounded and valid', () => {
  const name = resolveProjectName('', {
    GITHUB_RUN_ID: '123',
    GITHUB_RUN_ATTEMPT: '2',
    GITHUB_JOB: 'matrix / Kafka 4.3.1',
    GITHUB_ACTION: 'setup',
  })
  assert.match(name, /^[a-z0-9][a-z0-9_-]*$/)
  assert.ok(name.length <= 63)
})

test('invalid explicit project names are rejected', () => {
  assert.throws(() => resolveProjectName('Upper Case'), /compose-project/)
  assert.throws(() => resolveProjectName(`a${'b'.repeat(63)}`), /at most 63/)
})
