export function fakeDocker(): string {
  return String.raw`#!/usr/bin/env node
const fs = require('node:fs')
const path = require('node:path')
const args = process.argv.slice(2)
const stateFile = process.env.FAKE_DOCKER_STATE
let state = { topics: {}, calls: [] }
if (stateFile && fs.existsSync(stateFile)) { const saved = fs.readFileSync(stateFile, 'utf8').trim(); if (saved) state = JSON.parse(saved) }
state.calls.push(args)
function save() { if (stateFile) fs.writeFileSync(stateFile, JSON.stringify(state)) }
function value(flag) { const index = args.indexOf(flag); return index >= 0 ? args[index + 1] : '' }
function composeCommand() {
  if (args[0] !== 'compose') return ''
  let index = 1
  while (index < args.length) {
    if (args[index] === '--project-name' || args[index] === '--file') index += 2
    else return args[index]
  }
  return ''
}
if (args[0] === '--version') console.log('Docker version 27.5.1, build fake')
else if (args[0] === 'info') console.log('"27.5.1"')
else if (args[0] === 'pull') console.log('pulled ' + args[1])
else if (args[0] === 'image' && args[1] === 'inspect') console.log(JSON.stringify([{ RepoDigests: [args[2]] }]))
else if (args[0] === 'run') console.log('external broker API probe succeeded')
else if (args[0] === 'compose' && args[1] === 'version') console.log('2.33.1')
else {
  const command = composeCommand()
  if (command === 'config') {
    console.log('services:\n  broker:\n    environment:\n      password: ' + (process.env.KAFKA_PASSWORD || '') + '\n      keystore: ' + (process.env.KAFKA_KEYSTORE_PASSWORD || ''))
  } else if (command === 'cp') {
    const destination = args.at(-1)
    fs.mkdirSync(path.dirname(destination), { recursive: true })
    fs.writeFileSync(destination, '-----BEGIN CERTIFICATE-----\\nfake\\n-----END CERTIFICATE-----\\n')
  } else if (command === 'exec' && args.includes('--create')) {
    const topic = value('--topic')
    state.topics[topic] = { partitions: Number(value('--partitions')), replicationFactor: Number(value('--replication-factor')) }
    console.log('Created topic ' + topic)
  } else if (command === 'exec' && args.includes('--describe')) {
    const topic = value('--topic')
    const spec = state.topics[topic]
    if (!spec) process.exitCode = 1
    else {
      console.log('Topic: ' + topic + ' PartitionCount: ' + spec.partitions + ' ReplicationFactor: ' + spec.replicationFactor)
      const ids = Array.from({ length: spec.replicationFactor }, (_, index) => index + 1).join(',')
      for (let partition = 0; partition < spec.partitions; partition += 1) {
        console.log('Topic: ' + topic + ' Partition: ' + partition + ' Leader: 1 Replicas: ' + ids + ' Isr: ' + ids)
      }
    }
  } else if (command === 'ps') console.log(JSON.stringify([{ Service: 'broker', State: 'running' }]))
  else if (command === 'logs') console.log('broker ready password=' + (process.env.KAFKA_PASSWORD || '') + ' key=' + (process.env.KAFKA_KEYSTORE_PASSWORD || ''))
  else console.log('ok')
}
save()
`
}
