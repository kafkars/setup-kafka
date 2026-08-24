import crypto from 'node:crypto'
import fs from 'node:fs'

export function isGitHubActions(environment = process.env) {
  return environment.GITHUB_ACTIONS === 'true'
}

export function getInput(name, environment = process.env) {
  return environment[`INPUT_${name.replace(/ /g, '_').toUpperCase()}`] ?? ''
}

export function setOutput(name, value, environment = process.env) {
  appendMultiline(environment.GITHUB_OUTPUT, name, value)
}

export function saveState(name, value, environment = process.env) {
  appendMultiline(environment.GITHUB_STATE, name, value)
}

export function getState(name, environment = process.env) {
  return environment[`STATE_${name}`] ?? ''
}

export function mask(value) {
  if (value) {
    process.stdout.write(`::add-mask::${escapeCommand(value)}\n`)
  }
}

export function annotation(level, message, title = 'setup-kafka') {
  process.stdout.write(`::${level} title=${escapeProperty(title)}::${escapeCommand(message)}\n`)
}

export function startGroup(title) {
  process.stdout.write(`::group::${escapeCommand(title)}\n`)
}

export function endGroup() {
  process.stdout.write('::endgroup::\n')
}

export function appendSummary(markdown, environment = process.env) {
  if (environment.GITHUB_STEP_SUMMARY) {
    fs.appendFileSync(environment.GITHUB_STEP_SUMMARY, `${markdown.trimEnd()}\n`, 'utf8')
  }
}

export function declareArtifacts(files, environment = process.env) {
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

function appendMultiline(file, name, value) {
  if (!file) {
    return
  }
  const delimiter = `setup_kafka_${crypto.randomBytes(12).toString('hex')}`
  fs.appendFileSync(file, `${name}<<${delimiter}\n${String(value)}\n${delimiter}\n`, 'utf8')
}

function escapeCommand(value) {
  return String(value).replaceAll('%', '%25').replaceAll('\r', '%0D').replaceAll('\n', '%0A')
}

function escapeProperty(value) {
  return escapeCommand(value).replaceAll(':', '%3A').replaceAll(',', '%2C')
}
