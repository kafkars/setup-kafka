import fs from 'node:fs'
import path from 'node:path'

import { ensureDirectory, readJson, writeJsonAtomic } from './fs-utils.ts'
import { SetupKafkaError, invariant } from './errors.ts'
import type { CatalogSource } from './catalog.ts'
import type { TopologyName } from './inputs.ts'
import type { SecurityConfig } from './security.ts'
import type { TopicSpec } from './topics.ts'

export const stateSchemaVersion = 1

export interface StateLocation {
  privateDirectory: string
  stateFile: string
  clientProperties: string
  probeProperties: string
}

export interface EnvironmentState {
  schemaVersion: 1
  actionVersion: string
  createdAt: string
  updatedAt: string
  phase: string
  diagnostic: string | null
  stateFile: string
  privateDirectory: string
  clientProperties: string
  probeProperties: string
  workspace: string
  evidenceDirectory: string
  evidenceArchive: string
  finalEvidenceArchive: string
  manifestFile: string
  connectionFile: string
  statusFile: string
  operationLog: string
  checksumFile: string
  composeConfigFile: string
  imageInspectFile: string
  toolsFile: string
  composePsFile: string
  brokerLogFile: string
  caCertificate: string
  clientCertificate: string
  clientKey: string
  project: string
  implementation: 'apache-kafka'
  version: string
  image: string
  imageDigest: string
  extractedFrom: CatalogSource
  topology: TopologyName
  clusterSize: 1 | 3
  security: SecurityConfig
  secrets: string[]
  composeFiles: string[]
  brokerServices: string[]
  ports: number[]
  bootstrapServers: string
  userTopics: TopicSpec[]
  allTopics: TopicSpec[]
  readinessTopic: string
  startupTimeoutMs: number
  cleanupTimeoutMs: number
  cleanup: boolean
  declareEvidence: boolean
  upAttempted: boolean
}

export function stateLocation(runnerTemp: string, project: string): StateLocation {
  const directory = path.join(runnerTemp, 'setup-kafka', project)
  return {
    privateDirectory: directory,
    stateFile: path.join(directory, 'state.json'),
    clientProperties: path.join(directory, 'client.properties'),
    probeProperties: path.join(directory, 'probe.properties'),
  }
}

export function writeState(state: EnvironmentState): void {
  validateState(state)
  ensureDirectory(path.dirname(state.stateFile), 0o700)
  writeJsonAtomic(state.stateFile, state, 0o600)
}

export function readState(file: string): EnvironmentState {
  const state = readJson(file)
  validateState(state)
  return state
}

export function removePrivateState(state: EnvironmentState): void {
  if (state.privateDirectory && fs.existsSync(state.privateDirectory)) {
    fs.rmSync(state.privateDirectory, { recursive: true, force: true })
  }
}

export function updatePhase(
  state: EnvironmentState,
  phase: string,
  diagnostic: string | null = null,
): void {
  state.phase = phase
  state.updatedAt = new Date().toISOString()
  state.diagnostic = diagnostic
  writeState(state)
}

function validateState(state: unknown): asserts state is EnvironmentState {
  invariant(isRecord(state), 'state_invalid', 'state must be an object')
  invariant(state.schemaVersion === stateSchemaVersion, 'state_schema_invalid', 'state schemaVersion is unsupported')
  const requiredStrings = ['stateFile', 'privateDirectory', 'project', 'image', 'evidenceDirectory'] as const
  for (const key of requiredStrings) {
    invariant(typeof state[key] === 'string' && state[key].length > 0, 'state_invalid', `state ${key} is missing`)
  }
  invariant(Array.isArray(state.composeFiles) && state.composeFiles.length >= 1, 'state_invalid', 'state composeFiles are missing')
  invariant(Array.isArray(state.brokerServices) && state.brokerServices.length >= 1, 'state_invalid', 'state brokerServices are missing')
  invariant(Array.isArray(state.ports) && state.ports.length === state.brokerServices.length, 'state_invalid', 'state ports do not match broker services')
  invariant(typeof state.stateFile === 'string', 'state_invalid', 'state stateFile is missing')
  invariant(typeof state.evidenceDirectory === 'string', 'state_invalid', 'state evidenceDirectory is missing')
  if (!path.isAbsolute(state.stateFile) || !path.isAbsolute(state.evidenceDirectory)) {
    throw new SetupKafkaError('state_invalid', 'state paths must be absolute')
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
