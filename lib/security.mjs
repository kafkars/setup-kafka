import crypto from 'node:crypto'

import { SetupKafkaError } from './errors.mjs'

export const securityNames = Object.freeze([
  'plaintext',
  'tls',
  'sasl-plain',
  'scram-sha-256',
  'scram-sha-512',
])

const presets = Object.freeze({
  plaintext: {
    externalProtocol: 'PLAINTEXT',
    securityProtocol: 'PLAINTEXT',
    saslMechanism: '',
    tls: false,
    sasl: false,
  },
  tls: {
    externalProtocol: 'SSL',
    securityProtocol: 'SSL',
    saslMechanism: '',
    tls: true,
    sasl: false,
  },
  'sasl-plain': {
    externalProtocol: 'SASL_PLAINTEXT',
    securityProtocol: 'SASL_PLAINTEXT',
    saslMechanism: 'PLAIN',
    tls: false,
    sasl: true,
  },
  'scram-sha-256': {
    externalProtocol: 'SASL_PLAINTEXT',
    securityProtocol: 'SASL_PLAINTEXT',
    saslMechanism: 'SCRAM-SHA-256',
    tls: false,
    sasl: true,
  },
  'scram-sha-512': {
    externalProtocol: 'SASL_PLAINTEXT',
    securityProtocol: 'SASL_PLAINTEXT',
    saslMechanism: 'SCRAM-SHA-512',
    tls: false,
    sasl: true,
  },
})

export function createSecurity(name) {
  const preset = presets[name]
  if (!preset) {
    throw new SetupKafkaError('security_unsupported', `unsupported security preset ${JSON.stringify(name)}; expected ${securityNames.join(', ')}`)
  }
  return {
    name,
    ...preset,
    username: preset.sasl ? 'kafkars' : '',
    password: preset.sasl ? randomSecret(32) : '',
    keystorePassword: preset.tls ? randomSecret(24) : '',
  }
}

export function composeSecurityFile(security, topology) {
  if (security.tls) {
    return topology === 'single-broker' ? 'tls-single.yml' : 'tls-three.yml'
  }
  if (security.sasl) {
    return topology === 'single-broker' ? 'sasl-single.yml' : 'sasl-three.yml'
  }
  return null
}

export function clientProperties(security, bootstrapServers, caPath) {
  const lines = [
    `bootstrap.servers=${bootstrapServers}`,
    `security.protocol=${security.securityProtocol}`,
  ]
  if (security.tls) {
    if (!caPath) {
      throw new SetupKafkaError('ca_path_missing', 'TLS client properties require a CA path')
    }
    lines.push(
      'ssl.truststore.type=PEM',
      `ssl.truststore.location=${caPath}`,
      'ssl.endpoint.identification.algorithm=https',
    )
  }
  if (security.sasl) {
    lines.push(
      `sasl.mechanism=${security.saslMechanism}`,
      `sasl.jaas.config=org.apache.kafka.common.security.${security.saslMechanism === 'PLAIN' ? 'plain.PlainLoginModule' : 'scram.ScramLoginModule'} required username="${security.username}" password="${security.password}";`,
    )
  }
  return `${lines.join('\n')}\n`
}

export function redactedConnection(security, bootstrapServers, caPath) {
  return {
    bootstrapServers,
    securityProtocol: security.securityProtocol,
    saslMechanism: security.saslMechanism || null,
    username: security.username || null,
    password: security.password ? '<masked-output>' : null,
    caCertificate: caPath || null,
    clientCertificate: null,
    clientKey: null,
  }
}

export function sensitiveValues(security) {
  return [security.password, security.keystorePassword].filter(Boolean)
}

function randomSecret(bytes) {
  return crypto.randomBytes(bytes).toString('base64url')
}
