import fs from 'node:fs'
import path from 'node:path'

import { ensureDirectory, readJson, writeJsonAtomic } from './fs-utils.mjs'
import { SetupKafkaError, invariant } from './errors.ts'

export const stateSchemaVersion = 1

export function stateLocation(runnerTemp, project) {
  const directory = path.join(runnerTemp, 'setup-kafka', project)
  return {
    privateDirectory: directory,
    stateFile: path.join(directory, 'state.json'),
    clientProperties: path.join(directory, 'client.properties'),
    probeProperties: path.join(directory, 'probe.properties'),
  }
}

export function writeState(state) {
  validateState(state)
  ensureDirectory(path.dirname(state.stateFile), 0o700)
  writeJsonAtomic(state.stateFile, state, 0o600)
}

export function readState(file) {
  const state = readJson(file)
  validateState(state)
  return state
}

export function removePrivateState(state) {
  if (state.privateDirectory && fs.existsSync(state.privateDirectory)) {
    fs.rmSync(state.privateDirectory, { recursive: true, force: true })
  }
}

export function updatePhase(state, phase, diagnostic = null) {
  state.phase = phase
  state.updatedAt = new Date().toISOString()
  state.diagnostic = diagnostic
  writeState(state)
}

function validateState(state) {
  invariant(state?.schemaVersion === stateSchemaVersion, 'state_schema_invalid', 'state schemaVersion is unsupported')
  for (const key of ['stateFile', 'privateDirectory', 'project', 'image', 'evidenceDirectory']) {
    invariant(typeof state[key] === 'string' && state[key].length > 0, 'state_invalid', `state ${key} is missing`)
  }
  invariant(Array.isArray(state.composeFiles) && state.composeFiles.length >= 1, 'state_invalid', 'state composeFiles are missing')
  invariant(Array.isArray(state.brokerServices) && state.brokerServices.length >= 1, 'state_invalid', 'state brokerServices are missing')
  invariant(Array.isArray(state.ports) && state.ports.length === state.brokerServices.length, 'state_invalid', 'state ports do not match broker services')
  if (!path.isAbsolute(state.stateFile) || !path.isAbsolute(state.evidenceDirectory)) {
    throw new SetupKafkaError('state_invalid', 'state paths must be absolute')
  }
}
