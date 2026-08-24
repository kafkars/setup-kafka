import fs from 'node:fs'
import path from 'node:path'

import { resolveImage, verifyImageInspection } from './catalog.ts'
import { brokerServices, composeFiles, runCompose } from './compose.ts'
import { Deadline } from './deadline.ts'
import { prepareEvidence, finalizeEvidence, updateEvidenceStatus, writeSanitized } from './evidence.ts'
import { captureDiagnostics } from './diagnostics.ts'
import { SetupKafkaError, asSetupKafkaError } from './errors.ts'
import { ensureDirectory, writeJsonAtomic } from './fs-utils.ts'
import { normalizeInputs } from './inputs.ts'
import { PortReservations } from './ports.ts'
import { resolveProjectName } from './project.ts'
import { runCommand, requireSuccess, redactText } from './process.ts'
import { createSecurity, sensitiveValues } from './security.ts'
import { stateLocation, updatePhase, writeState, removePrivateState } from './state.ts'
import { preflight, writeClientFiles } from './setup-support.ts'
import { allTopics, parseTopics, readinessTopic } from './topics.ts'
import {
  createTopics,
  prepareSecurity,
  proveIdempotentProduction,
  waitForBrokers,
  waitForExternalConnection,
  waitForTopics,
} from './readiness.mjs'

const actionVersion = '0.1.0'

export async function createEnvironment(rawInputs) {
  const inputs = rawInputs.clusterSize ? rawInputs : normalizeInputs(rawInputs)
  const image = resolveImage(inputs.version)
  const security = createSecurity(inputs.security)
  const project = resolveProjectName(inputs.composeProject)
  const services = brokerServices(inputs.topology)
  const userTopics = parseTopics(inputs.topics, inputs.clusterSize)
  const locations = stateLocation(inputs.runnerTemp, project)
  const reservations = await PortReservations.reserve(services.length)
  try {
    const ports = [...reservations.ports]
    const bootstrapServers = reservations.endpoints().join(',')
    const evidenceArchive = path.join(
      path.dirname(inputs.evidenceDirectory),
      `${path.basename(inputs.evidenceDirectory)}-${project}-ready.tar`,
    )
    const state = {
      schemaVersion: 1,
      actionVersion,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      phase: 'prepared',
      diagnostic: null,
      stateFile: locations.stateFile,
      privateDirectory: locations.privateDirectory,
      clientProperties: locations.clientProperties,
      probeProperties: locations.probeProperties,
      workspace: inputs.workspace,
      evidenceDirectory: inputs.evidenceDirectory,
      evidenceArchive,
      finalEvidenceArchive: path.join(
        path.dirname(inputs.evidenceDirectory),
        `${path.basename(inputs.evidenceDirectory)}-${project}-final.tar`,
      ),
      manifestFile: path.join(inputs.evidenceDirectory, 'manifest.json'),
      connectionFile: path.join(inputs.evidenceDirectory, 'connection.json'),
      statusFile: path.join(inputs.evidenceDirectory, 'status.json'),
      operationLog: path.join(inputs.evidenceDirectory, 'operations.jsonl'),
      checksumFile: path.join(inputs.evidenceDirectory, 'checksums.sha256'),
      composeConfigFile: path.join(inputs.evidenceDirectory, 'compose-config.yml'),
      imageInspectFile: path.join(inputs.evidenceDirectory, 'image-inspect.json'),
      toolsFile: path.join(inputs.evidenceDirectory, 'tools.json'),
      composePsFile: path.join(inputs.evidenceDirectory, 'compose-ps.json'),
      brokerLogFile: path.join(inputs.evidenceDirectory, 'broker.log'),
      caCertificate: security.tls ? path.join(inputs.evidenceDirectory, 'ca-certificate.pem') : '',
      clientCertificate: '',
      clientKey: '',
      project,
      implementation: image.implementation,
      version: image.version,
      image: image.image,
      imageDigest: image.digest,
      extractedFrom: image.extractedFrom,
      topology: inputs.topology,
      clusterSize: inputs.clusterSize,
      security,
      secrets: sensitiveValues(security),
      composeFiles: composeFiles(inputs.topology, security),
      brokerServices: services,
      ports,
      bootstrapServers,
      userTopics,
      allTopics: allTopics(userTopics, inputs.clusterSize),
      readinessTopic,
      startupTimeoutMs: inputs.startupTimeoutMs,
      cleanupTimeoutMs: inputs.cleanupTimeoutMs,
      cleanup: inputs.cleanup,
      declareEvidence: inputs.declareEvidence,
      upAttempted: false,
    }
    ensureDirectory(state.privateDirectory, 0o700)
    prepareEvidence(state)
    writeClientFiles(state)
    writeState(state)
    return { state, reservations }
  } catch (error) {
    await reservations.release()
    if (fs.existsSync(locations.privateDirectory)) {
      fs.rmSync(locations.privateDirectory, { recursive: true, force: true })
    }
    throw error
  }
}

export async function startEnvironment(state, reservations) {
  const deadline = new Deadline(state.startupTimeoutMs, 'startup')
  try {
    updateEvidenceStatus(state, 'preflight')
    const tools = await preflight(state, deadline)
    writeJsonAtomic(state.toolsFile, tools, 0o644)

    updateEvidenceStatus(state, 'pulling-image')
    const pull = await runCommand({
      command: 'docker',
      args: ['pull', state.image],
      cwd: state.workspace,
      env: process.env,
      timeoutMs: deadline.remainingMs(),
      captureLimitBytes: 2 * 1024 * 1024,
      operationLog: state.operationLog,
      operation: 'image-pull',
      secrets: state.secrets,
    })
    requireSuccess(pull, 'image_pull_failed', `pull ${state.image}`, state.secrets)

    const inspect = await runCommand({
      command: 'docker',
      args: ['image', 'inspect', state.image],
      cwd: state.workspace,
      env: process.env,
      timeoutMs: deadline.remainingMs(30_000),
      captureLimitBytes: 4 * 1024 * 1024,
      operationLog: state.operationLog,
      operation: 'image-inspect',
      secrets: state.secrets,
    })
    requireSuccess(inspect, 'image_inspect_failed', `inspect ${state.image}`, state.secrets)
    verifyImageInspection(inspect.stdout, state.image)
    writeSanitized(state.imageInspectFile, inspect.stdout, state)

    const config = await runCompose(state, ['config'], {
      timeoutMs: deadline.remainingMs(30_000),
      captureLimitBytes: 4 * 1024 * 1024,
      operation: 'compose-config',
    })
    requireSuccess(config, 'compose_config_failed', 'Docker Compose configuration', state.secrets)
    writeSanitized(state.composeConfigFile, config.stdout, state)

    await reservations.release()
    state.upAttempted = true
    updatePhase(state, 'starting')
    updateEvidenceStatus(state, 'starting')
    const up = await runCompose(state, ['up', '--detach', '--no-build', '--pull', 'never', '--remove-orphans'], {
      timeoutMs: deadline.remainingMs(),
      captureLimitBytes: 2 * 1024 * 1024,
      operation: 'compose-up',
    })
    requireSuccess(up, 'compose_up_failed', 'Docker Compose startup', state.secrets)

    updatePhase(state, 'broker-readiness')
    updateEvidenceStatus(state, 'broker-readiness')
    await waitForBrokers(state, deadline)
    await prepareSecurity(state, deadline)
    writeClientFiles(state)
    await waitForExternalConnection(state, deadline)

    updatePhase(state, 'topic-provisioning')
    updateEvidenceStatus(state, 'topic-provisioning')
    await createTopics(state, deadline)
    await waitForTopics(state, deadline)
    await proveIdempotentProduction(state, deadline)

    updatePhase(state, 'ready')
    const readyAt = new Date().toISOString()
    updateEvidenceStatus(state, 'ready', null, { readyAt })
    const snapshotIssues = await captureDiagnostics(state, new Deadline(20_000, 'ready evidence snapshot'))
    finalizeEvidence(state, {
      phase: 'ready',
      diagnostic: snapshotIssues.length > 0 ? snapshotIssues.join('; ') : null,
      extra: { readyAt, setupPhase: 'ready', diagnosticGaps: snapshotIssues },
    }, {
      archive: state.evidenceArchive,
      declare: state.declareEvidence,
    })
    return state
  } catch (error) {
    const normalized = asSetupKafkaError(error, 'setup_failed')
    const diagnostic = redactText(normalized.message, state.secrets)
    try {
      updatePhase(state, 'failed', diagnostic)
      updateEvidenceStatus(state, 'failed', diagnostic)
      if (state.upAttempted) {
        await captureDiagnostics(state, new Deadline(15_000, 'startup diagnostics'))
      }
    } catch {
      // Preserve the original setup failure.
    }
    throw normalized
  } finally {
    await reservations.release()
  }
}

export async function finishEnvironment(state) {
  const deadline = new Deadline(state.cleanupTimeoutMs, 'cleanup')
  const issues = []
  let cleanupFailure = null
  if (state.upAttempted) {
    const diagnosticBudget = state.cleanup
      ? Math.max(1_000, Math.min(30_000, Math.floor(state.cleanupTimeoutMs / 3)))
      : state.cleanupTimeoutMs
    issues.push(...await captureDiagnostics(state, new Deadline(diagnosticBudget, 'cleanup diagnostics')))
  }
  if (state.cleanup && state.upAttempted) {
    try {
      const seconds = Math.max(1, Math.min(20, Math.floor(deadline.remainingMs() / 1_000)))
      const down = await runCompose(state, [
        'down', '--volumes', '--remove-orphans', '--timeout', String(seconds),
      ], {
        timeoutMs: deadline.remainingMs(),
        captureLimitBytes: 2 * 1024 * 1024,
        operation: 'compose-down',
      })
      requireSuccess(down, 'compose_down_failed', 'Docker Compose cleanup', state.secrets)
    } catch (error) {
      cleanupFailure = asSetupKafkaError(error, 'cleanup_failed')
      issues.push(cleanupFailure.message)
    }
  }

  const phase = cleanupFailure
    ? 'cleanup-failed'
    : state.cleanup
      ? 'cleaned'
      : 'retained'
  const diagnostic = issues.length > 0 ? issues.map((value) => redactText(value, state.secrets)).join('; ') : null
  let finalized
  try {
    finalized = finalizeEvidence(state, {
      phase,
      diagnostic,
      extra: {
        setupPhase: state.phase,
        cleanupRequested: state.cleanup,
        cleanedAt: state.cleanup && !cleanupFailure ? new Date().toISOString() : null,
        diagnosticGaps: issues.filter((issue) => !cleanupFailure || issue !== cleanupFailure.message),
      },
    }, {
      archive: state.finalEvidenceArchive,
      declare: state.declareEvidence,
    })
  } finally {
    if (state.cleanup) {
      removePrivateState(state)
    }
  }
  if (cleanupFailure) {
    throw cleanupFailure
  }
  return { phase, issues, ...finalized }
}
