import path from 'node:path'

import { composeRoot } from './paths.ts'
import { composeSecurityFile, type SecurityConfig } from './security.ts'
import { runCommand, type CommandResult } from './process.ts'
import type { TopologyName } from './inputs.ts'
import type { EnvironmentState } from './state.ts'

interface ComposeOptions {
  timeoutMs: number
  stdin?: string | Uint8Array
  captureLimitBytes?: number
  operation?: string
  extraEnvironment?: NodeJS.ProcessEnv
}

export function composeFiles(topology: TopologyName, security: SecurityConfig): string[] {
  const files = [path.join(composeRoot, topology === 'single-broker' ? 'single.yml' : 'three.yml')]
  const securityFile = composeSecurityFile(security, topology)
  if (securityFile) {
    files.push(path.join(composeRoot, securityFile))
  }
  return files
}

export function brokerServices(topology: TopologyName): string[] {
  return topology === 'single-broker' ? ['broker'] : ['broker-1', 'broker-2', 'broker-3']
}

export function composeEnvironment(
  state: EnvironmentState,
  extra: NodeJS.ProcessEnv = {},
): NodeJS.ProcessEnv {
  const environment: NodeJS.ProcessEnv = {
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

export function composeArgs(state: EnvironmentState, tail: readonly string[]): string[] {
  const prefix = ['compose', '--project-name', state.project]
  for (const file of state.composeFiles) {
    prefix.push('--file', file)
  }
  return [...prefix, ...tail]
}

export function runCompose(
  state: EnvironmentState,
  tail: readonly string[],
  options: ComposeOptions,
): Promise<CommandResult> {
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

export function firstBroker(state: EnvironmentState): string {
  return state.brokerServices[0]!
}

export function internalBootstrap(): string {
  return 'localhost:19092'
}
