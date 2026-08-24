import { writeTextAtomic } from './fs-utils.mjs'
import { runCommand, requireSuccess } from './process.mjs'
import { clientProperties } from './security.ts'

export async function preflight(state, deadline) {
  const commands = [
    ['docker', ['--version'], 'docker-version'],
    ['docker', ['compose', 'version', '--short'], 'compose-version'],
    ['docker', ['info', '--format', '{{json .ServerVersion}}'], 'docker-info'],
  ]
  const tools = {}
  for (const [command, args, key] of commands) {
    const result = await runCommand({
      command,
      args,
      cwd: state.workspace,
      env: process.env,
      timeoutMs: deadline.remainingMs(20_000),
      operationLog: state.operationLog,
      operation: key,
      secrets: state.secrets,
    })
    requireSuccess(result, 'docker_preflight_failed', key, state.secrets)
    tools[key] = result.stdout.trim()
  }
  return tools
}

export function writeClientFiles(state) {
  const hostCa = state.caCertificate || null
  const probeCa = state.security.tls ? '/setup-kafka-evidence/ca-certificate.pem' : null
  writeTextAtomic(
    state.clientProperties,
    clientProperties(state.security, state.bootstrapServers, hostCa),
    0o600,
  )
  writeTextAtomic(
    state.probeProperties,
    clientProperties(state.security, state.bootstrapServers, probeCa),
    0o600,
  )
}
