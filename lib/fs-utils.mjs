import crypto from 'node:crypto'
import fs from 'node:fs'
import path from 'node:path'

import { SetupKafkaError } from './errors.ts'

export function ensureDirectory(directory, mode = 0o700) {
  fs.mkdirSync(directory, { recursive: true, mode })
  try {
    fs.chmodSync(directory, mode)
  } catch {
    // Filesystems mounted by hosted runners may not preserve Unix modes.
  }
}

export function writeTextAtomic(file, content, mode = 0o600) {
  ensureDirectory(path.dirname(file))
  const temporary = `${file}.tmp-${process.pid}-${crypto.randomBytes(4).toString('hex')}`
  fs.writeFileSync(temporary, content, { encoding: 'utf8', mode })
  fs.renameSync(temporary, file)
  try {
    fs.chmodSync(file, mode)
  } catch {
    // Best effort on non-POSIX filesystems.
  }
}

export function writeJsonAtomic(file, value, mode = 0o600) {
  writeTextAtomic(file, `${JSON.stringify(value, null, 2)}\n`, mode)
}

export function readJson(file, code = 'state_read_failed') {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'))
  } catch (error) {
    throw new SetupKafkaError(code, `failed to read JSON file ${file}: ${error.message}`, { cause: error })
  }
}

export function appendJsonLine(file, value, mode = 0o600) {
  ensureDirectory(path.dirname(file))
  fs.appendFileSync(file, `${JSON.stringify(value)}\n`, { encoding: 'utf8', mode })
}

export function sha256File(file) {
  const hash = crypto.createHash('sha256')
  hash.update(fs.readFileSync(file))
  return hash.digest('hex')
}

export function regularFiles(directory, excluded = new Set()) {
  const files = []
  walk(directory, files, excluded)
  return files.sort()
}

export function relativeUnix(root, file) {
  return path.relative(root, file).split(path.sep).join('/')
}

function walk(directory, files, excluded) {
  if (!fs.existsSync(directory)) {
    return
  }
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const file = path.join(directory, entry.name)
    if (excluded.has(file) || entry.isSymbolicLink()) {
      continue
    }
    if (entry.isDirectory()) {
      walk(file, files, excluded)
    } else if (entry.isFile()) {
      files.push(file)
    }
  }
}
