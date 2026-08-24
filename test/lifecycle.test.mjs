import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import test from 'node:test'

import { controlEnvironment } from '../lib/control.mjs'
import { createEnvironment, finishEnvironment, startEnvironment } from '../lib/lifecycle.mjs'
import { normalizeInputs } from '../lib/inputs.ts'
import { fakeDocker } from './support/fake-docker.mjs'

for (const lane of [
  { topology: 'single-broker', security: 'plaintext' },
  { topology: 'three-broker', security: 'tls' },
  { topology: 'three-broker', security: 'scram-sha-512' },
]) {
  test(`lifecycle owns ${lane.topology} ${lane.security}`, async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'setup-kafka-lifecycle-'))
    const bin = path.join(root, 'bin')
    fs.mkdirSync(bin)
    const docker = path.join(bin, 'docker')
    fs.writeFileSync(docker, fakeDocker(), { mode: 0o755 })
    const previousPath = process.env.PATH
    const previousState = process.env.FAKE_DOCKER_STATE
    process.env.PATH = `${bin}${path.delimiter}${previousPath}`
    process.env.FAKE_DOCKER_STATE = path.join(root, 'fake-docker.json')
    try {
      const inputs = normalizeInputs({
        version: '4.3.1',
        topology: lane.topology,
        security: lane.security,
        topics: 'orders:2',
        startupTimeoutSeconds: '30',
        cleanupTimeoutSeconds: '30',
        evidenceDirectory: path.join(root, 'evidence'),
        composeProject: `setup-kafka-${lane.security.replaceAll('-', '')}`,
        cleanup: true,
        declareEvidence: false,
        workspace: root,
        runnerTemp: path.join(root, 'runner'),
      })
      const created = await createEnvironment(inputs)
      const secret = created.state.security.password || created.state.security.keystorePassword
      await startEnvironment(created.state, created.reservations)
      assert.equal(created.state.phase, 'ready')
      assert.ok(fs.existsSync(created.state.evidenceArchive))
      if (lane.security !== 'plaintext') {
        assert.ok(secret.length > 0)
        assert.equal(fs.readFileSync(created.state.composeConfigFile, 'utf8').includes(secret), false)
        assert.equal(fs.readFileSync(created.state.brokerLogFile, 'utf8').includes(secret), false)
      }
      if (lane.security === 'tls') {
        assert.ok(fs.existsSync(created.state.caCertificate))
      }
      if (lane.topology === 'three-broker') {
        await controlEnvironment(created.state, 'restart', 'broker-2', 30_000)
        await controlEnvironment(created.state, 'rolling-restart', '', 30_000)
      }
      const finished = await finishEnvironment(created.state)
      assert.equal(finished.phase, 'cleaned')
      assert.ok(fs.existsSync(created.state.finalEvidenceArchive))
      assert.equal(fs.existsSync(created.state.privateDirectory), false)
      const calls = JSON.parse(fs.readFileSync(process.env.FAKE_DOCKER_STATE, 'utf8')).calls
      assert.ok(calls.some((args) => args.includes('down')))
    } finally {
      process.env.PATH = previousPath
      if (previousState === undefined) delete process.env.FAKE_DOCKER_STATE
      else process.env.FAKE_DOCKER_STATE = previousState
      fs.rmSync(root, { recursive: true, force: true })
    }
  })
}
