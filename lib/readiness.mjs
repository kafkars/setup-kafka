import { firstBroker, internalBootstrap, runCompose } from './compose.ts'
import { runCommand, requireSuccess, redactText } from './process.ts'
import { SetupKafkaError } from './errors.ts'

const attemptTimeoutMs = 5_000
const retryDelayMs = 250

export async function waitForBrokers(state, deadline) {
  for (const service of state.brokerServices) {
    let lastDiagnostic = 'broker API probe has not run'
    let ready = false
    while (!deadline.expired()) {
      const result = await runCompose(state, [
        'exec', '--no-TTY', service,
        '/opt/kafka/bin/kafka-broker-api-versions.sh',
        '--bootstrap-server', internalBootstrap(),
      ], {
        timeoutMs: remainingAttempt(deadline),
        operation: `broker-readiness-${service}`,
      })
      if (result.code === 0 && !result.timedOut) {
        ready = true
        break
      }
      lastDiagnostic = diagnostic(result, state.secrets)
      await sleepWithinDeadline(deadline)
    }
    if (!ready) {
      throw new SetupKafkaError('broker_readiness_failed', `${service} did not become ready: ${lastDiagnostic}`)
    }
  }
}

export async function prepareSecurity(state, deadline) {
  if (state.security.tls) {
    const result = await runCompose(state, [
      'cp', `${firstBroker(state)}:/etc/kafka/secrets/ca.pem`, state.caCertificate,
    ], {
      timeoutMs: deadline.remainingMs(attemptTimeoutMs),
      operation: 'copy-tls-ca',
    })
    requireSuccess(result, 'tls_ca_copy_failed', 'TLS CA copy', state.secrets)
  }
  if (state.security.name.startsWith('scram-')) {
    const mechanism = state.security.saslMechanism
    const command = [
      `/opt/kafka/bin/kafka-configs.sh --bootstrap-server ${internalBootstrap()}`,
      '--alter --entity-type users --entity-name "$SETUP_KAFKA_USERNAME"',
      `--add-config "${mechanism}=[iterations=8192,password=$SETUP_KAFKA_PASSWORD]"`,
    ].join(' ')
    const result = await runCompose(state, [
      'exec', '--no-TTY',
      '--env', 'SETUP_KAFKA_USERNAME',
      '--env', 'SETUP_KAFKA_PASSWORD',
      firstBroker(state), '/bin/bash', '-euc', command,
    ], {
      timeoutMs: deadline.remainingMs(attemptTimeoutMs),
      operation: 'scram-credential-setup',
      extraEnvironment: {
        SETUP_KAFKA_USERNAME: state.security.username,
        SETUP_KAFKA_PASSWORD: state.security.password,
      },
    })
    requireSuccess(result, 'scram_setup_failed', `${mechanism} credential setup`, state.secrets)
  }
}

export async function waitForExternalConnection(state, deadline) {
  let lastDiagnostic = 'external client probe has not run'
  while (!deadline.expired()) {
    const mounts = [
      '--mount', `type=bind,src=${state.privateDirectory},dst=/setup-kafka-private,readonly`,
    ]
    if (state.security.tls) {
      mounts.push('--mount', `type=bind,src=${state.evidenceDirectory},dst=/setup-kafka-evidence,readonly`)
    }
    const result = await runCommand({
      command: 'docker',
      args: [
        'run', '--rm', '--network', 'host',
        ...mounts,
        '--entrypoint', '/opt/kafka/bin/kafka-broker-api-versions.sh',
        state.image,
        '--bootstrap-server', state.bootstrapServers,
        '--command-config', '/setup-kafka-private/probe.properties',
      ],
      cwd: state.workspace,
      env: process.env,
      timeoutMs: remainingAttempt(deadline),
      operationLog: state.operationLog,
      operation: 'external-client-readiness',
      secrets: state.secrets,
    })
    if (result.code === 0 && !result.timedOut) {
      return
    }
    lastDiagnostic = diagnostic(result, state.secrets)
    await sleepWithinDeadline(deadline)
  }
  throw new SetupKafkaError('external_readiness_failed', `external Kafka client could not connect: ${lastDiagnostic}`)
}

export async function createTopics(state, deadline) {
  for (const topic of state.allTopics) {
    const result = await runCompose(state, [
      'exec', '--no-TTY', firstBroker(state),
      '/opt/kafka/bin/kafka-topics.sh',
      '--bootstrap-server', internalBootstrap(),
      '--create', '--if-not-exists',
      '--topic', topic.name,
      '--partitions', String(topic.partitions),
      '--replication-factor', String(topic.replicationFactor),
    ], {
      timeoutMs: deadline.remainingMs(attemptTimeoutMs),
      operation: `create-topic-${topic.name}`,
    })
    requireSuccess(result, 'topic_creation_failed', `topic ${topic.name} creation`, state.secrets)
  }
}

export async function waitForTopics(state, deadline) {
  let lastDiagnostic = 'topic metadata has not been described'
  while (!deadline.expired()) {
    let ready = true
    for (const topic of state.allTopics) {
      const result = await runCompose(state, [
        'exec', '--no-TTY', firstBroker(state),
        '/opt/kafka/bin/kafka-topics.sh',
        '--bootstrap-server', internalBootstrap(),
        '--describe', '--topic', topic.name,
      ], {
        timeoutMs: remainingAttempt(deadline),
        operation: `describe-topic-${topic.name}`,
      })
      if (result.code !== 0 || result.timedOut) {
        ready = false
        lastDiagnostic = `${topic.name}: ${diagnostic(result, state.secrets)}`
        break
      }
      const problem = topicReadinessProblem(result.stdout, topic)
      if (problem) {
        ready = false
        lastDiagnostic = problem
        break
      }
    }
    if (ready) {
      return
    }
    await sleepWithinDeadline(deadline)
  }
  throw new SetupKafkaError('topic_readiness_failed', `topics did not reach leader/full-ISR readiness: ${lastDiagnostic}`)
}

export async function proveIdempotentProduction(state, deadline) {
  const result = await runCompose(state, [
    'exec', '--no-TTY', firstBroker(state),
    '/opt/kafka/bin/kafka-producer-perf-test.sh',
    '--topic', state.readinessTopic,
    '--num-records', '1',
    '--record-size', '5',
    '--throughput', '-1',
    '--producer-props',
    `bootstrap.servers=${internalBootstrap()}`,
    'enable.idempotence=true',
    'acks=all',
    'max.in.flight.requests.per.connection=5',
    'retries=2147483647',
  ], {
    timeoutMs: deadline.remainingMs(30_000),
    operation: 'idempotent-production-proof',
    captureLimitBytes: 512 * 1024,
  })
  requireSuccess(result, 'production_proof_failed', 'idempotent production proof', state.secrets)
}

export function topicReadinessProblem(output, expected) {
  const partitions = new Map()
  for (const line of String(output).split(/\r?\n/)) {
    const match = line.match(/\bPartition:\s*(\d+).*?\bLeader:\s*(-?\d+).*?\bReplicas:\s*([0-9,\s]*).*?\bIsr:\s*([0-9,\s]*)/i)
    if (!match) continue
    const partition = Number(match[1])
    partitions.set(partition, {
      leader: Number(match[2]),
      replicas: ids(match[3]),
      isr: ids(match[4]),
    })
  }
  if (partitions.size !== expected.partitions) {
    return `topic ${expected.name} has ${partitions.size} of ${expected.partitions} partitions`
  }
  for (let partition = 0; partition < expected.partitions; partition += 1) {
    const metadata = partitions.get(partition)
    if (!metadata) {
      return `topic ${expected.name} partition ${partition} is missing`
    }
    if (metadata.leader < 0 || metadata.replicas.length !== expected.replicationFactor || metadata.isr.length !== expected.replicationFactor) {
      return `topic ${expected.name} partition ${partition} has leader ${metadata.leader}, ${metadata.replicas.length} replicas, and ${metadata.isr.length} in-sync replicas; expected ${expected.replicationFactor}`
    }
  }
  return null
}

function ids(value) {
  return value.split(',').map((entry) => entry.trim()).filter(Boolean)
}

function remainingAttempt(deadline) {
  try {
    return deadline.remainingMs(attemptTimeoutMs)
  } catch {
    return 1
  }
}

async function sleepWithinDeadline(deadline) {
  if (!deadline.expired()) {
    await new Promise((resolve) => setTimeout(resolve, retryDelayMs))
  }
}

function diagnostic(result, secrets) {
  const value = result.stderr.trim() || result.stdout.trim() || 'no diagnostic'
  return redactText(value.slice(-2_000), secrets)
}
