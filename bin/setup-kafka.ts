#!/usr/bin/env node

import fs from 'node:fs'
import path from 'node:path'

import { supportedVersions, resolveImage } from '../lib/catalog.ts'
import { controlEnvironment, controlOperations } from '../lib/control.ts'
import { asSetupKafkaError, SetupKafkaError } from '../lib/errors.ts'
import { normalizeInputs, requireLinuxRunner, topologyNames } from '../lib/inputs.ts'
import { publicOutputs } from '../lib/outputs.ts'
import {
  createEnvironment,
  finishEnvironment,
  startEnvironment,
} from '../lib/lifecycle.ts'
import { securityNames } from '../lib/security.ts'
import { readState } from '../lib/state.ts'
import { parseTopics } from '../lib/topics.ts'
import type { Inputs } from '../lib/inputs.ts'

type Options = Record<string, string>

const [command = 'help', ...rest] = process.argv.slice(2)
try {
  if (command === 'catalog') {
    print({ versions: supportedVersions(), topologies: topologyNames, security: securityNames })
  } else if (command === 'plan') {
    const options = parseOptions(rest)
    const inputs = localInputs(options)
    const image = resolveImage(inputs.version)
    print({
      version: image.version,
      image: image.image,
      topology: inputs.topology,
      clusterSize: inputs.clusterSize,
      security: inputs.security,
      topics: parseTopics(inputs.topics, inputs.clusterSize),
    })
  } else if (command === 'up') {
    await up(parseOptions(rest))
  } else if (command === 'down') {
    await down(parseOptions(rest))
  } else if (command === 'status') {
    status(parseOptions(rest))
  } else if (command === 'control') {
    await control(parseOptions(rest))
  } else if (command === 'help' || command === '--help' || command === '-h') {
    process.stdout.write(help())
  } else {
    throw new SetupKafkaError('command_invalid', `unknown command ${command}`)
  }
} catch (error) {
  const normalized = asSetupKafkaError(error, 'cli_failed')
  process.stderr.write(`setup-kafka: ${normalized.code}: ${normalized.message}\n`)
  process.exitCode = 1
}

async function up(options: Options): Promise<void> {
  requireLinuxRunner()
  const inputs = localInputs(options)
  const created = await createEnvironment(inputs)
  try {
    await startEnvironment(created.state, created.reservations)
    print({ stateFile: created.state.stateFile, ...publicOutputs(created.state) })
  } catch (error) {
    created.state.cleanup = options['retain-on-failure'] !== 'true'
    if (created.state.cleanup) {
      await finishEnvironment(created.state).catch(() => {})
    }
    throw error
  }
}

async function down(options: Options): Promise<void> {
  const state = readState(requiredOption(options, 'state'))
  state.cleanup = true
  const result = await finishEnvironment(state)
  print(result)
}

function status(options: Options): void {
  const state = readState(requiredOption(options, 'state'))
  print({
    stateFile: state.stateFile,
    phase: state.phase,
    project: state.project,
    version: state.version,
    topology: state.topology,
    security: state.security.name,
    bootstrapServers: state.bootstrapServers,
    evidenceDirectory: state.evidenceDirectory,
  })
}

async function control(options: Options): Promise<void> {
  const state = readState(requiredOption(options, 'state'))
  const operation = requiredOption(options, 'operation')
  const timeout = parseInteger(options['timeout-seconds'] || '60', 'timeout-seconds') * 1_000
  const result = await controlEnvironment(state, operation, options.broker || '', timeout)
  print(result)
}

function localInputs(options: Options): Inputs {
  const workspace = path.resolve(options.workspace || process.cwd())
  const topicsFile = options['topics-file']
  const topics = topicsFile
    ? fs.readFileSync(path.resolve(workspace, topicsFile), 'utf8')
    : options.topics || ''
  return normalizeInputs({
    version: requiredOption(options, 'version'),
    topology: options.topology || 'single-broker',
    security: options.security || 'plaintext',
    topics,
    startupTimeoutSeconds: options['startup-timeout-seconds'] || '240',
    cleanupTimeoutSeconds: options['cleanup-timeout-seconds'] || '60',
    evidenceDirectory: options['evidence-directory'] || path.join('.setup-kafka', 'evidence'),
    composeProject: options['compose-project'] || '',
    cleanup: false,
    declareEvidence: false,
    workspace,
    runnerTemp: options['state-directory'] || path.join(workspace, '.setup-kafka', 'state'),
  })
}

function parseOptions(args: readonly string[]): Options {
  const options: Options = {}
  for (let index = 0; index < args.length; index += 1) {
    const token = args[index]!
    if (!token.startsWith('--')) {
      throw new SetupKafkaError('option_invalid', `expected an option, got ${token}`)
    }
    const [name = '', inline] = token.slice(2).split('=', 2)
    if (!name) {
      throw new SetupKafkaError('option_invalid', 'option name must not be empty')
    }
    if (inline !== undefined) {
      options[name] = inline
      continue
    }
    const next = args[index + 1]
    if (!next || next.startsWith('--')) {
      options[name] = 'true'
    } else {
      options[name] = next
      index += 1
    }
  }
  return options
}

function requiredOption(options: Options, name: string): string {
  const value = String(options[name] || '').trim()
  if (!value) throw new SetupKafkaError('option_missing', `--${name} is required`)
  return value
}

function parseInteger(value: string, name: string): number {
  if (!/^\d+$/.test(value)) throw new SetupKafkaError('option_invalid', `--${name} must be an integer`)
  return Number(value)
}

function print(value: unknown): void {
  process.stdout.write(`${JSON.stringify(value, null, 2)}\n`)
}

function help(): string {
  return `setup-kafka\n\nCommands:\n  catalog\n  plan --version VERSION [--topology NAME] [--security NAME]\n  up --version VERSION [--topology NAME] [--security NAME] [--topics-file FILE]\n  status --state FILE\n  down --state FILE\n  control --state FILE --operation ${controlOperations.join('|')} [--broker NAME]\n\nThe up command keeps the cluster running and prints its private state file.\nThe down command captures evidence and removes all owned Compose resources.\n`
}
