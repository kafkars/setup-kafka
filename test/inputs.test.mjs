import assert from 'node:assert/strict'
import path from 'node:path'
import test from 'node:test'

import { normalizeInputs, requireLinuxRunner } from '../lib/inputs.mjs'

test('inputs normalize paths, booleans, topology, and timeouts', () => {
  const inputs = normalizeInputs({
    version: '4.3.1',
    topology: 'three-broker',
    security: 'tls',
    topics: '',
    startupTimeoutSeconds: '120',
    cleanupTimeoutSeconds: '30',
    evidenceDirectory: 'evidence',
    composeProject: '',
    cleanup: 'true',
    declareEvidence: 'false',
    workspace: '/tmp/work',
    runnerTemp: '/tmp/run',
  })
  assert.equal(inputs.clusterSize, 3)
  assert.equal(inputs.startupTimeoutMs, 120_000)
  assert.equal(inputs.cleanup, true)
  assert.equal(inputs.declareEvidence, false)
  assert.equal(inputs.evidenceDirectory, path.resolve('/tmp/work/evidence'))
})

test('inputs reject ambiguous values', () => {
  const base = {
    version: '4.3.1', topology: 'single-broker', security: 'plaintext', topics: '',
    startupTimeoutSeconds: '240', cleanupTimeoutSeconds: '60', evidenceDirectory: 'evidence',
    composeProject: '', cleanup: 'true', declareEvidence: 'true', workspace: '/tmp', runnerTemp: '/tmp',
  }
  assert.throws(() => normalizeInputs({ ...base, topology: 'cluster' }), /topology must be one of/)
  assert.throws(() => normalizeInputs({ ...base, cleanup: 'yes' }), /cleanup must be true or false/)
  assert.throws(() => normalizeInputs({ ...base, startupTimeoutSeconds: '0' }), /between 10 and 3600/)
})

test('GitHub runner identity controls Action platform validation', () => {
  assert.doesNotThrow(() => requireLinuxRunner({ RUNNER_OS: 'Linux' }))
  assert.throws(() => requireLinuxRunner({ RUNNER_OS: 'macOS' }), /requires a Linux runner/)
})
