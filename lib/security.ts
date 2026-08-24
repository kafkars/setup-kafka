import crypto from 'node:crypto'

import { SetupKafkaError } from './errors.ts'
import type { TopologyName } from './inputs.ts'

export const securityNames = [
  'plaintext',
  'tls',
  'sasl-plain',
  'scram-sha-256',
  'scram-sha-512',
] as const

export type SecurityName = (typeof securityNames)[number]

interface SecurityPreset {
  externalProtocol: string
  securityProtocol: string
  saslMechanism: string
  tls: boolean
  sasl: boolean
}

export interface SecurityConfig extends SecurityPreset {
  name: SecurityName
  username: string
  password: string
  keystorePassword: string
}

export interface RedactedConnection {
  bootstrapServers: string
  securityProtocol: string
  saslMechanism: string | null
  username: string | null
  password: '<masked-output>' | null
  caCertificate: string | null
  clientCertificate: null
  clientKey: null
}

const presets: Readonly<Record<SecurityName, SecurityPreset>> = Object.freeze({
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

export function createSecurity(name: string): SecurityConfig {
  if (!isSecurityName(name)) {
    throw new SetupKafkaError('security_unsupported', `unsupported security preset ${JSON.stringify(name)}; expected ${securityNames.join(', ')}`)
  }
  const preset = presets[name]
  return {
    name,
    ...preset,
    username: preset.sasl ? 'kafkars' : '',
    password: preset.sasl ? randomSecret(32) : '',
    keystorePassword: preset.tls ? randomSecret(24) : '',
  }
}

export function composeSecurityFile(
  security: SecurityConfig,
  topology: TopologyName,
): string | null {
  if (security.tls) {
    return topology === 'single-broker' ? 'tls-single.yml' : 'tls-three.yml'
  }
  if (security.sasl) {
    return topology === 'single-broker' ? 'sasl-single.yml' : 'sasl-three.yml'
  }
  return null
}

export function clientProperties(
  security: SecurityConfig,
  bootstrapServers: string,
  caPath: string | null,
): string {
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

export function redactedConnection(
  security: SecurityConfig,
  bootstrapServers: string,
  caPath: string | null,
): RedactedConnection {
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

export function sensitiveValues(security: SecurityConfig): string[] {
  return [security.password, security.keystorePassword].filter(Boolean)
}

function randomSecret(bytes: number): string {
  return crypto.randomBytes(bytes).toString('base64url')
}

function isSecurityName(name: string): name is SecurityName {
  return securityNames.includes(name as SecurityName)
}
