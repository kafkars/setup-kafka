import fs from 'node:fs'
import path from 'node:path'

import { ensureDirectory, regularFiles, relativeUnix, sha256File, writeJsonAtomic, writeTextAtomic } from './fs-utils.ts'
import { declareArtifacts } from './github.ts'
import { redactText } from './process.ts'
import { SetupKafkaError } from './errors.ts'
import type { CatalogSource } from './catalog.ts'
import type { TopologyName } from './inputs.ts'
import type { SecurityName } from './security.ts'
import type { TopicSpec } from './topics.ts'

interface EvidenceSecurity {
  name: SecurityName
  securityProtocol: string
  saslMechanism: string
  username: string
  password: string
}

export interface EvidenceState {
  actionVersion: string
  createdAt: string
  project: string
  implementation: 'apache-kafka'
  version: string
  image: string
  imageDigest: string
  extractedFrom: CatalogSource
  topology: TopologyName
  clusterSize: 1 | 3
  security: EvidenceSecurity
  bootstrapServers: string
  brokerServices: readonly string[]
  userTopics: readonly TopicSpec[]
  readinessTopic: string
  startupTimeoutMs: number
  cleanupTimeoutMs: number
  clientProperties: string
  caCertificate: string
  evidenceDirectory: string
  evidenceArchive: string
  manifestFile: string
  connectionFile: string
  statusFile: string
  operationLog: string
  checksumFile: string
  composeConfigFile: string
  secrets: readonly string[]
  declareEvidence: boolean
}

interface FinalEvidenceStatus {
  phase: string
  diagnostic: string | null
  extra: Readonly<Record<string, unknown>>
}

interface FinalEvidenceOptions {
  archive?: string
  declare?: boolean
}

export interface FinalEvidenceResult {
  files: string[]
  archive: string
  archiveDigest: string
}

export function prepareEvidence(state: EvidenceState): void {
  if (fs.existsSync(state.evidenceDirectory) && fs.readdirSync(state.evidenceDirectory).length > 0) {
    throw new SetupKafkaError('evidence_directory_not_empty', `evidence directory is not empty: ${state.evidenceDirectory}`)
  }
  ensureDirectory(state.evidenceDirectory, 0o700)
  writeTextAtomic(state.operationLog, '', 0o600)
  writeJsonAtomic(state.manifestFile, manifest(state), 0o644)
  writeJsonAtomic(state.connectionFile, connection(state), 0o644)
  updateEvidenceStatus(state, 'prepared')
}

export function writeSanitized(
  file: string,
  content: unknown,
  state: Pick<EvidenceState, 'secrets'>,
  mode = 0o644,
): void {
  const redacted = redactText(content, state.secrets)
  for (const secret of state.secrets) {
    if (secret && redacted.includes(secret)) {
      throw new SetupKafkaError('evidence_redaction_failed', `secret remained in evidence file ${file}`)
    }
  }
  writeTextAtomic(file, redacted, mode)
}

export function updateEvidenceStatus(
  state: Pick<EvidenceState, 'statusFile' | 'secrets'>,
  phase: string,
  diagnostic: string | null = null,
  extra: Readonly<Record<string, unknown>> = {},
): void {
  writeJsonAtomic(state.statusFile, {
    schemaVersion: 1,
    phase,
    diagnostic: diagnostic ? redactText(diagnostic, state.secrets) : null,
    updatedAt: new Date().toISOString(),
    ...extra,
  }, 0o644)
}

export function finalizeEvidence(
  state: EvidenceState,
  finalStatus: FinalEvidenceStatus,
  options: FinalEvidenceOptions = {},
): FinalEvidenceResult {
  updateEvidenceStatus(state, finalStatus.phase, finalStatus.diagnostic, finalStatus.extra)
  const checksumFile = state.checksumFile
  const excluded = new Set([checksumFile])
  const lines = regularFiles(state.evidenceDirectory, excluded)
    .map((file) => `${sha256File(file)}  ${relativeUnix(state.evidenceDirectory, file)}`)
  writeTextAtomic(checksumFile, `${lines.join('\n')}\n`, 0o644)
  const archive = options.archive || state.evidenceArchive
  writeTarArchive(state.evidenceDirectory, archive)
  if (options.declare ?? state.declareEvidence) {
    declareArtifacts([archive])
  }
  return {
    files: regularFiles(state.evidenceDirectory),
    archive,
    archiveDigest: sha256File(archive),
  }
}

function manifest(state: EvidenceState): Readonly<Record<string, unknown>> {
  return {
    schemaVersion: 1,
    actionVersion: state.actionVersion,
    createdAt: state.createdAt,
    project: state.project,
    implementation: state.implementation,
    version: state.version,
    image: state.image,
    imageDigest: state.imageDigest,
    extractedFrom: state.extractedFrom,
    topology: state.topology,
    clusterSize: state.clusterSize,
    security: {
      preset: state.security.name,
      securityProtocol: state.security.securityProtocol,
      saslMechanism: state.security.saslMechanism || null,
      tlsClientAuthentication: false,
    },
    bootstrapServers: state.bootstrapServers,
    brokerServices: state.brokerServices,
    topics: state.userTopics,
    readinessTopic: state.readinessTopic,
    readinessContract: {
      everyPartitionHasLeader: true,
      everyPartitionHasFullIsr: true,
      idempotentProductionProof: true,
      externalSecurityProbe: true,
    },
    timeouts: {
      startupMs: state.startupTimeoutMs,
      cleanupMs: state.cleanupTimeoutMs,
    },
    source: {
      repository: process.env.GITHUB_ACTION_REPOSITORY || 'local',
      ref: process.env.GITHUB_ACTION_REF || null,
      sha: process.env.GITHUB_ACTION_SHA || null,
    },
  }
}

function connection(state: EvidenceState): Readonly<Record<string, unknown>> {
  return {
    schemaVersion: 1,
    bootstrapServers: state.bootstrapServers,
    securityProtocol: state.security.securityProtocol,
    saslMechanism: state.security.saslMechanism || null,
    username: state.security.username || null,
    password: state.security.password ? '<masked action output>' : null,
    caCertificate: state.caCertificate || null,
    clientCertificate: null,
    clientKey: null,
    clientProperties: state.clientProperties,
  }
}

function writeTarArchive(directory: string, archive: string): void {
  ensureDirectory(path.dirname(archive), 0o700)
  const descriptor = fs.openSync(archive, 'w', 0o644)
  try {
    const rootName = path.basename(directory)
    for (const file of regularFiles(directory)) {
      const relative = `${rootName}/${relativeUnix(directory, file)}`
      const stat = fs.statSync(file)
      fs.writeSync(descriptor, tarHeader(relative, stat.size, stat.mode & 0o777))
      const content = fs.readFileSync(file)
      fs.writeSync(descriptor, content)
      const padding = (512 - (content.length % 512)) % 512
      if (padding > 0) fs.writeSync(descriptor, Buffer.alloc(padding))
    }
    fs.writeSync(descriptor, Buffer.alloc(1024))
  } finally {
    fs.closeSync(descriptor)
  }
}

function tarHeader(name: string, size: number, mode: number): Buffer {
  const header = Buffer.alloc(512)
  const { basename, prefix } = splitTarName(name)
  Buffer.from(basename).copy(header, 0)
  if (prefix) Buffer.from(prefix).copy(header, 345)
  octal(header, 100, 8, mode)
  octal(header, 108, 8, 0)
  octal(header, 116, 8, 0)
  octal(header, 124, 12, size)
  octal(header, 136, 12, 0)
  header.fill(0x20, 148, 156)
  header[156] = '0'.charCodeAt(0)
  Buffer.from('ustar\0').copy(header, 257)
  Buffer.from('00').copy(header, 263)
  Buffer.from('kafkars').copy(header, 265)
  Buffer.from('kafkars').copy(header, 297)
  const checksum = header.reduce((sum, value) => sum + value, 0)
  const checksumText = checksum.toString(8).padStart(6, '0')
  Buffer.from(`${checksumText}\0 `).copy(header, 148)
  return header
}

function splitTarName(name: string): { basename: string; prefix: string } {
  if (Buffer.byteLength(name) <= 100) {
    return { basename: name, prefix: '' }
  }
  const separators = [...name.matchAll(/\//g)].flatMap((match) =>
    match.index === undefined ? [] : [match.index]
  )
  for (const separator of separators.reverse()) {
    const prefix = name.slice(0, separator)
    const basename = name.slice(separator + 1)
    if (Buffer.byteLength(prefix) <= 155 && Buffer.byteLength(basename) <= 100) {
      return { basename, prefix }
    }
  }
  throw new SetupKafkaError('evidence_archive_path_too_long', `tar path cannot be represented in ustar: ${name}`)
}

function octal(buffer: Buffer, offset: number, length: number, value: number): void {
  const text = value.toString(8).padStart(length - 1, '0') + '\0'
  Buffer.from(text).copy(buffer, offset)
}
