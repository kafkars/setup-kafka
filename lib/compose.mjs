import path from 'node:path'

import { composeRoot } from './paths.ts'
import { composeSecurityFile } from './security.ts'
import { runCommand } from './process.mjs'

export function composeFiles(topology, security) {
  const files = [path.join(composeRoot, topology === 'single-broker' ? 'single.yml' : 'three.yml')]
  const securityFile = composeSecurityFile(security, topology)
  if (securityFile) {
    files.push(path.join(composeRoot, securityFile))
  }
  return files
}

export function brokerServices(topology) {
  return topology === 'single-broker' ? ['broker'] : ['broker-1', 'broker-2', 'broker-3']
}

export function composeEnvironment(state, extra = {}) {
  const environment = {
    ...process.env,
    KAFKA_IMAGE: state.image,
    KAFKA_PROJECT: state.project,
    KAFKA_EXTERNAL_PROTOCOL: state.security.externalProtocol,
    ...extra,
  }
  if (state.ports.length === 1) {
    environment.KAFKA_HOST_PORT = String(state.ports[0])
  } else {
    state.ports.forEach((port, index) => {
      environment[`KAFKA_HOST_PORT_${index + 1}`] = String(port)
    })
  }
  if (state.security.sasl) {
    environment.KAFKA_USERNAME = state.security.username
    environment.KAFKA_PASSWORD = state.security.password
  }
  if (state.security.tls) {
    environment.KAFKA_KEYSTORE_PASSWORD = state.security.keystorePassword
  }
  return environment
}

export function composeArgs(state, tail) {
  const prefix = ['compose', '--project-name', state.project]
  for (const file of state.composeFiles) {
    prefix.push('--file', file)
  }
  return [...prefix, ...tail]
}

export function runCompose(state, tail, options = {}) {
  return runCommand({
    command: 'docker',
    args: composeArgs(state, tail),
    cwd: state.workspace,
    env: composeEnvironment(state, options.extraEnvironment),
    timeoutMs: options.timeoutMs,
    stdin: options.stdin,
    captureLimitBytes: options.captureLimitBytes,
    operationLog: state.operationLog,
    operation: options.operation || `compose-${tail[0]}`,
    secrets: state.secrets,
  })
}

export function firstBroker(state) {
  return state.brokerServices[0]
}

export function internalBootstrap() {
  return 'localhost:19092'
}
