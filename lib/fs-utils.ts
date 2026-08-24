import crypto from 'node:crypto'
import fs from 'node:fs'
import path from 'node:path'

import { SetupKafkaError } from './errors.ts'

export function ensureDirectory(directory: string, mode = 0o700): void {
  fs.mkdirSync(directory, { recursive: true, mode })
  try {
    fs.chmodSync(directory, mode)
  } catch {
    // Filesystems mounted by hosted runners may not preserve Unix modes.
  }
}

export function writeTextAtomic(file: string, content: string, mode = 0o600): void {
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

export function writeJsonAtomic(file: string, value: unknown, mode = 0o600): void {
  writeTextAtomic(file, `${JSON.stringify(value, null, 2)}\n`, mode)
}

export function readJson(file: string, code = 'state_read_failed'): unknown {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'))
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    throw new SetupKafkaError(code, `failed to read JSON file ${file}: ${message}`, { cause: error })
  }
}

export function appendJsonLine(file: string, value: unknown, mode = 0o600): void {
  ensureDirectory(path.dirname(file))
  fs.appendFileSync(file, `${JSON.stringify(value)}\n`, { encoding: 'utf8', mode })
}

export function sha256File(file: string): string {
  const hash = crypto.createHash('sha256')
  hash.update(fs.readFileSync(file))
  return hash.digest('hex')
}

export function regularFiles(
  directory: string,
  excluded: ReadonlySet<string> = new Set<string>(),
): string[] {
  const files: string[] = []
  walk(directory, files, excluded)
  return files.sort()
}

export function relativeUnix(root: string, file: string): string {
  return path.relative(root, file).split(path.sep).join('/')
}

function walk(directory: string, files: string[], excluded: ReadonlySet<string>): void {
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
