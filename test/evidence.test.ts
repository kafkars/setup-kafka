import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import test from 'node:test'

import {
  finalizeEvidence,
  prepareEvidence,
  writeSanitized,
  type EvidenceState,
} from '../lib/evidence.ts'

const secret = 'this-must-never-enter-evidence'

test('evidence is redacted, checksummed, and archived without secrets', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'setup-kafka-evidence-'))
  const evidence = path.join(root, 'evidence')
  const state = fixture(evidence)
  prepareEvidence(state)
  writeSanitized(state.composeConfigFile, `password=${secret}\n`, state)
  const result = finalizeEvidence(state, { phase: 'ready', diagnostic: null, extra: {} })
  assert.ok(fs.existsSync(result.archive))
  assert.ok(fs.existsSync(state.checksumFile))
  for (const file of [...result.files, result.archive]) {
    assert.equal(fs.readFileSync(file).includes(Buffer.from(secret)), false, file)
  }
  fs.rmSync(root, { recursive: true, force: true })
})

function fixture(evidenceDirectory: string): EvidenceState {
  return {
    actionVersion: '0.1.0',
    createdAt: new Date(0).toISOString(),
    project: 'setup-kafka-test',
    implementation: 'apache-kafka',
    version: '4.3.1',
    image: 'apache/kafka@sha256:' + 'a'.repeat(64),
    imageDigest: 'sha256:' + 'a'.repeat(64),
    extractedFrom: { repository: 'kafkars/testlab', commit: 'b'.repeat(40) },
    topology: 'single-broker',
    clusterSize: 1,
    security: {
      name: 'sasl-plain', securityProtocol: 'SASL_PLAINTEXT', saslMechanism: 'PLAIN',
      username: 'kafkars', password: secret,
    },
    bootstrapServers: '127.0.0.1:9092',
    brokerServices: ['broker'],
    userTopics: [],
    readinessTopic: 'setup-kafka-readiness',
    startupTimeoutMs: 240_000,
    cleanupTimeoutMs: 60_000,
    clientProperties: '/private/client.properties',
    caCertificate: '',
    evidenceDirectory,
    evidenceArchive: path.join(path.dirname(evidenceDirectory), 'evidence.tar'),
    manifestFile: path.join(evidenceDirectory, 'manifest.json'),
    connectionFile: path.join(evidenceDirectory, 'connection.json'),
    statusFile: path.join(evidenceDirectory, 'status.json'),
    operationLog: path.join(evidenceDirectory, 'operations.jsonl'),
    checksumFile: path.join(evidenceDirectory, 'checksums.sha256'),
    composeConfigFile: path.join(evidenceDirectory, 'compose-config.yml'),
    secrets: [secret],
    declareEvidence: false,
  }
}
