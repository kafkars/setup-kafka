import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { spawnSync } from 'node:child_process'
import test from 'node:test'

import { repositoryRoot } from '../lib/paths.mjs'
import { fakeDocker } from './support/fake-docker.mjs'

test('root Action publishes outputs and post owns final evidence and cleanup', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'setup-kafka-action-'))
  try {
    const bin = path.join(root, 'bin')
    fs.mkdirSync(bin)
    fs.writeFileSync(path.join(bin, 'docker'), fakeDocker(), { mode: 0o755 })
    const files = {
      output: path.join(root, 'github-output'),
      state: path.join(root, 'github-state'),
      summary: path.join(root, 'github-summary'),
      artifacts: path.join(root, 'github-artifacts'),
      docker: path.join(root, 'fake-docker.json'),
    }
    for (const file of Object.values(files)) fs.writeFileSync(file, '')
    const environment = {
      ...process.env,
      PATH: `${bin}${path.delimiter}${process.env.PATH}`,
      FAKE_DOCKER_STATE: files.docker,
      GITHUB_ACTIONS: 'true',
      RUNNER_OS: 'Linux',
      GITHUB_WORKSPACE: root,
      RUNNER_TEMP: path.join(root, 'runner'),
      GITHUB_OUTPUT: files.output,
      GITHUB_STATE: files.state,
      GITHUB_STEP_SUMMARY: files.summary,
      GITHUB_ARTIFACTS: files.artifacts,
      GITHUB_ACTION_REPOSITORY: 'kafkars/setup-kafka',
      GITHUB_ACTION_REF: 'test-ref',
      GITHUB_ACTION_SHA: '0'.repeat(40),
      INPUT_VERSION: '4.3.1',
      INPUT_TOPOLOGY: 'three-broker',
      INPUT_SECURITY: 'scram-sha-512',
      INPUT_TOPICS: 'orders:2',
    }
    const main = run('src/main.mjs', environment)
    assert.equal(main.status, 0, `${main.stderr}\n${main.stdout}`)
    assert.match(main.stdout, /::add-mask::/)
    const outputs = parseCommandFile(files.output)
    const saved = parseCommandFile(files.state)
    assert.equal(outputs['security-protocol'], 'SASL_PLAINTEXT')
    assert.equal(outputs['sasl-mechanism'], 'SCRAM-SHA-512')
    assert.ok(outputs.password.length > 20)
    assert.equal(saved.SETUP_KAFKA_STATE, outputs['state-file'])
    assert.ok(fs.existsSync(outputs['evidence-archive']))
    assert.equal(fs.readFileSync(path.join(outputs['evidence-directory'], 'compose-config.yml'), 'utf8').includes(outputs.password), false)
    assert.equal(fs.readFileSync(files.summary, 'utf8').includes(outputs.password), false)

    const controlOutput = path.join(root, 'control-output')
    fs.writeFileSync(controlOutput, '')
    const control = run('control/src/main.mjs', {
      ...environment,
      GITHUB_OUTPUT: controlOutput,
      'INPUT_STATE-FILE': outputs['state-file'],
      INPUT_OPERATION: 'restart',
      INPUT_BROKER: 'broker-2',
    })
    assert.equal(control.status, 0, `${control.stderr}\n${control.stdout}`)
    assert.deepEqual(parseCommandFile(controlOutput), { operation: 'restart', brokers: 'broker-2' })

    const snapshotOutput = path.join(root, 'snapshot-output')
    fs.writeFileSync(snapshotOutput, '')
    const snapshot = run('snapshot/src/main.mjs', {
      ...environment,
      GITHUB_OUTPUT: snapshotOutput,
      'INPUT_STATE-FILE': outputs['state-file'],
      INPUT_NAME: 'adapter',
      'INPUT_DECLARE-EVIDENCE': 'false',
    })
    assert.equal(snapshot.status, 0, `${snapshot.stderr}\n${snapshot.stdout}`)
    const snapshotOutputs = parseCommandFile(snapshotOutput)
    assert.ok(fs.existsSync(snapshotOutputs['evidence-archive']))
    assert.match(snapshotOutputs['evidence-digest'], /^sha256:[0-9a-f]{64}$/)

    const post = run('src/post.mjs', {
      ...environment,
      STATE_SETUP_KAFKA_STATE: saved.SETUP_KAFKA_STATE,
    })
    assert.equal(post.status, 0, post.stderr)
    assert.ok(fs.existsSync(outputs['final-evidence-archive']))
    assert.equal(fs.existsSync(path.dirname(outputs['state-file'])), false)
    const artifacts = fs.readFileSync(files.artifacts, 'utf8').trim().split('\n')
    assert.deepEqual(artifacts, [outputs['evidence-archive'], outputs['final-evidence-archive']])
    const calls = JSON.parse(fs.readFileSync(files.docker, 'utf8')).calls
    assert.ok(calls.some((args) => args.includes('down')))
  } finally {
    fs.rmSync(root, { recursive: true, force: true })
  }
})

function run(relative, environment) {
  return spawnSync(process.execPath, [path.join(repositoryRoot, relative)], {
    cwd: repositoryRoot,
    env: environment,
    encoding: 'utf8',
    timeout: 30_000,
  })
}

function parseCommandFile(file) {
  const result = {}
  const lines = fs.readFileSync(file, 'utf8').split('\n')
  for (let index = 0; index < lines.length; index += 1) {
    const match = lines[index].match(/^([^<]+)<<(.+)$/)
    if (!match) continue
    const [, name, delimiter] = match
    const values = []
    index += 1
    while (index < lines.length && lines[index] !== delimiter) {
      values.push(lines[index])
      index += 1
    }
    result[name] = values.join('\n')
  }
  return result
}
