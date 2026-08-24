import crypto from 'node:crypto'
import fs from 'node:fs'

export type AnnotationLevel = 'error' | 'warning' | 'notice'

export function isGitHubActions(environment: NodeJS.ProcessEnv = process.env): boolean {
  return environment.GITHUB_ACTIONS === 'true'
}

export function getInput(name: string, environment: NodeJS.ProcessEnv = process.env): string {
  return environment[`INPUT_${name.replace(/ /g, '_').toUpperCase()}`] ?? ''
}

export function setOutput(
  name: string,
  value: unknown,
  environment: NodeJS.ProcessEnv = process.env,
): void {
  appendMultiline(environment.GITHUB_OUTPUT, name, value)
}

export function saveState(
  name: string,
  value: unknown,
  environment: NodeJS.ProcessEnv = process.env,
): void {
  appendMultiline(environment.GITHUB_STATE, name, value)
}

export function getState(name: string, environment: NodeJS.ProcessEnv = process.env): string {
  return environment[`STATE_${name}`] ?? ''
}

export function mask(value: string): void {
  if (value) {
    process.stdout.write(`::add-mask::${escapeCommand(value)}\n`)
  }
}

export function annotation(
  level: AnnotationLevel,
  message: string,
  title = 'setup-kafka',
): void {
  process.stdout.write(`::${level} title=${escapeProperty(title)}::${escapeCommand(message)}\n`)
}

export function startGroup(title: string): void {
  process.stdout.write(`::group::${escapeCommand(title)}\n`)
}

export function endGroup(): void {
  process.stdout.write('::endgroup::\n')
}

export function appendSummary(markdown: string, environment: NodeJS.ProcessEnv = process.env): void {
  if (environment.GITHUB_STEP_SUMMARY) {
    fs.appendFileSync(environment.GITHUB_STEP_SUMMARY, `${markdown.trimEnd()}\n`, 'utf8')
  }
}

export function declareArtifacts(
  files: readonly string[],
  environment: NodeJS.ProcessEnv = process.env,
): void {
  if (!environment.GITHUB_ARTIFACTS) {
    return
  }
  for (const file of files) {
    const stat = fs.statSync(file)
    if (stat.isFile()) {
      fs.appendFileSync(environment.GITHUB_ARTIFACTS, `${file}\n`, 'utf8')
    }
  }
}

function appendMultiline(file: string | undefined, name: string, value: unknown): void {
  if (!file) {
    return
  }
  const delimiter = `setup_kafka_${crypto.randomBytes(12).toString('hex')}`
  fs.appendFileSync(file, `${name}<<${delimiter}\n${String(value)}\n${delimiter}\n`, 'utf8')
}

function escapeCommand(value: unknown): string {
  return String(value).replaceAll('%', '%25').replaceAll('\r', '%0D').replaceAll('\n', '%0A')
}

function escapeProperty(value: unknown): string {
  return escapeCommand(value).replaceAll(':', '%3A').replaceAll(',', '%2C')
}
