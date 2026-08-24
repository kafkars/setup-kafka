import os from 'node:os'
import path from 'node:path'

import { getInput } from './github.mjs'
import { SetupKafkaError } from './errors.mjs'
import { securityNames } from './security.mjs'

export const topologyNames = Object.freeze(['single-broker', 'three-broker'])

export function actionInputs(environment = process.env) {
  requireLinuxRunner(environment)
  const workspace = environment.GITHUB_WORKSPACE || process.cwd()
  const runnerTemp = environment.RUNNER_TEMP || os.tmpdir()
  return normalizeInputs({
    version: required(getInput('version', environment), 'version'),
    topology: getInput('topology', environment) || 'single-broker',
    security: getInput('security', environment) || 'plaintext',
    topics: getInput('topics', environment),
    startupTimeoutSeconds: getInput('startup-timeout-seconds', environment) || '240',
    cleanupTimeoutSeconds: getInput('cleanup-timeout-seconds', environment) || '60',
    evidenceDirectory: getInput('evidence-directory', environment) || 'setup-kafka-evidence',
    composeProject: getInput('compose-project', environment),
    cleanup: getInput('cleanup', environment) || 'true',
    declareEvidence: getInput('declare-evidence', environment) || 'true',
    workspace,
    runnerTemp,
  })
}

export function normalizeInputs(raw) {
  const topology = enumValue(raw.topology, topologyNames, 'topology')
  const security = enumValue(raw.security, securityNames, 'security')
  const workspace = path.resolve(raw.workspace || process.cwd())
  const runnerTemp = path.resolve(raw.runnerTemp || os.tmpdir())
  return {
    version: required(raw.version, 'version'),
    topology,
    clusterSize: topology === 'single-broker' ? 1 : 3,
    security,
    topics: String(raw.topics ?? ''),
    startupTimeoutMs: seconds(raw.startupTimeoutSeconds, 'startup-timeout-seconds', 10, 3_600),
    cleanupTimeoutMs: seconds(raw.cleanupTimeoutSeconds, 'cleanup-timeout-seconds', 10, 600),
    evidenceDirectory: resolvePath(raw.evidenceDirectory || 'setup-kafka-evidence', workspace),
    composeProject: String(raw.composeProject ?? '').trim(),
    cleanup: boolean(raw.cleanup, 'cleanup'),
    declareEvidence: boolean(raw.declareEvidence, 'declare-evidence'),
    workspace,
    runnerTemp,
  }
}

export function requireLinuxRunner(environment = process.env) {
  const runnerOs = environment.RUNNER_OS
  const linux = runnerOs ? runnerOs === 'Linux' : process.platform === 'linux'
  if (!linux) {
    throw new SetupKafkaError('runner_unsupported', 'setup-kafka requires a Linux runner with Docker Engine and Docker Compose v2')
  }
}

function required(value, name) {
  const normalized = String(value ?? '').trim()
  if (!normalized) {
    throw new SetupKafkaError('input_missing', `${name} is required`)
  }
  return normalized
}

function enumValue(value, choices, name) {
  const normalized = String(value ?? '').trim().toLowerCase()
  if (!choices.includes(normalized)) {
    throw new SetupKafkaError('input_invalid', `${name} must be one of: ${choices.join(', ')}`)
  }
  return normalized
}

function seconds(value, name, minimum, maximum) {
  const text = String(value ?? '').trim()
  if (!/^\d+$/.test(text)) {
    throw new SetupKafkaError('input_invalid', `${name} must be an integer number of seconds`)
  }
  const parsed = Number(text)
  if (!Number.isSafeInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new SetupKafkaError('input_invalid', `${name} must be between ${minimum} and ${maximum}`)
  }
  return parsed * 1_000
}

function boolean(value, name) {
  if (value === true || value === 'true') return true
  if (value === false || value === 'false') return false
  throw new SetupKafkaError('input_invalid', `${name} must be true or false`)
}

function resolvePath(value, base) {
  const text = String(value).trim()
  if (!text || /[\r\n]/.test(text)) {
    throw new SetupKafkaError('input_invalid', 'evidence-directory must be a non-empty single-line path')
  }
  return path.isAbsolute(text) ? path.normalize(text) : path.resolve(base, text)
}
