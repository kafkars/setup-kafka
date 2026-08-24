import crypto from 'node:crypto'

import { SetupKafkaError } from './errors.mjs'

const pattern = /^[a-z0-9][a-z0-9_-]*$/
const maximumLength = 63

export function resolveProjectName(requested, environment = process.env) {
  const explicit = String(requested ?? '').trim()
  if (explicit) {
    validate(explicit)
    return explicit
  }
  const seed = [
    environment.GITHUB_RUN_ID,
    environment.GITHUB_RUN_ATTEMPT,
    environment.GITHUB_JOB,
    environment.GITHUB_ACTION,
    process.pid,
    crypto.randomBytes(4).toString('hex'),
  ].filter(Boolean).join('-')
  const normalized = `setup-kafka-${seed}`
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/^[^a-z0-9]+/, '')
  const digest = crypto.createHash('sha256').update(seed).digest('hex').slice(0, 8)
  const prefix = normalized.slice(0, maximumLength - digest.length - 1).replace(/[-_]+$/, '')
  const result = `${prefix}-${digest}`
  validate(result)
  return result
}

function validate(value) {
  if (value.length > maximumLength || !pattern.test(value)) {
    throw new SetupKafkaError(
      'compose_project_invalid',
      `compose-project must match ${pattern} and be at most ${maximumLength} characters`,
    )
  }
}
