import { spawnSync } from 'node:child_process'
import fs from 'node:fs'
import path from 'node:path'

import { loadCatalog } from '../lib/catalog.ts'
import { repositoryRoot } from '../lib/paths.ts'

const failures: string[] = []
const sourceRoots = ['lib', 'src', 'bin', 'control/src', 'snapshot/src', 'scripts', 'test']
const sourceFiles = sourceRoots.flatMap((root) => files(path.join(repositoryRoot, root), '.ts'))
const javascriptFiles = sourceRoots.flatMap((root) => files(path.join(repositoryRoot, root), '.mjs'))
for (const file of javascriptFiles) {
  fail(`${path.relative(repositoryRoot, file)} must be migrated to TypeScript`)
}

for (const file of sourceFiles) {
  const relative = path.relative(repositoryRoot, file)
  const text = fs.readFileSync(file, 'utf8')
  const lines = text.split(/\r?\n/).length
  if (lines > 300) fail(`${relative} has ${lines} lines; maximum is 300`)
  const syntax = spawnSync(process.execPath, ['--check', file], { encoding: 'utf8' })
  if (syntax.status !== 0) fail(`${relative} failed node --check: ${syntax.stderr.trim()}`)
  if (relative !== 'scripts/lint.ts') {
    for (const forbidden of ['TODO', 'FIXME', 'shell: true', 'eval(', 'child_process.exec']) {
      if (text.includes(forbidden)) fail(`${relative} contains forbidden token ${JSON.stringify(forbidden)}`)
    }
    if (!relative.startsWith('test/') && text.includes("from 'node:test'")) {
      fail(`${relative} contains an in-source test`)
    }
  }
}

const packageJson = JSON.parse(fs.readFileSync(path.join(repositoryRoot, 'package.json'), 'utf8'))
if (Object.keys(packageJson.dependencies || {}).length > 0) fail('runtime dependencies are not allowed')
const expectedDevDependencies = { '@types/node': '24.13.3', typescript: '7.0.2' }
if (JSON.stringify(packageJson.devDependencies) !== JSON.stringify(expectedDevDependencies)) {
  fail('development dependencies must be the pinned TypeScript toolchain')
}
if (packageJson.engines?.node !== '>=24') fail('package.json must require Node >=24')

for (const metadata of ['action.yml', 'control/action.yml', 'snapshot/action.yml']) {
  const text = read(metadata)
  if (!/\n  using: node24\n/.test(text)) fail(`${metadata} must use node24`)
}
const rootAction = read('action.yml')
for (const fragment of [
  'post: src/post.ts',
  'post-if: always()',
  'version:',
  'bootstrap-servers:',
  'evidence-directory:',
  'state-file:',
]) {
  if (!rootAction.includes(fragment)) fail(`action.yml is missing ${fragment}`)
}

const catalog = loadCatalog()
for (const [version, digest] of Object.entries(catalog.versions)) {
  if (!/^\d+\.\d+\.\d+$/.test(version)) fail(`catalog version is not exact: ${version}`)
  if (!/^sha256:[0-9a-f]{64}$/.test(digest)) fail(`catalog digest is invalid for ${version}`)
}

for (const file of files(path.join(repositoryRoot, 'compose'), '.yml')) {
  const relative = path.relative(repositoryRoot, file)
  const text = fs.readFileSync(file, 'utf8')
  if (/image:\s*[^$\s]/.test(text)) fail(`${relative} contains a non-catalog image reference`)
  if (text.includes(':latest')) fail(`${relative} contains a floating latest tag`)
}
for (const base of ['compose/single.yml', 'compose/three.yml']) {
  const text = read(base)
  if (!text.includes('KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"')) {
    fail(`${base} must disable automatic topic creation`)
  }
}

for (const workflow of files(path.join(repositoryRoot, '.github', 'workflows'), '.yml')) {
  const relative = path.relative(repositoryRoot, workflow)
  for (const line of fs.readFileSync(workflow, 'utf8').split(/\r?\n/)) {
    const match = line.match(/\buses:\s*([^\s#]+)/)
    const reference = match?.[1]
    if (!reference || reference.startsWith('./')) continue
    if (!/@[0-9a-f]{40}$/.test(reference)) fail(`${relative} has mutable action reference ${reference}`)
  }
}

if (failures.length > 0) {
  for (const failure of failures) process.stderr.write(`lint: ${failure}\n`)
  process.exitCode = 1
} else {
  process.stdout.write(`lint: ${sourceFiles.length} source modules and repository contracts passed\n`)
}

function read(relative: string): string {
  return fs.readFileSync(path.join(repositoryRoot, relative), 'utf8')
}

function files(root: string, extension: string): string[] {
  if (!fs.existsSync(root)) return []
  const result: string[] = []
  for (const entry of fs.readdirSync(root, { withFileTypes: true })) {
    const file = path.join(root, entry.name)
    if (entry.isDirectory()) result.push(...files(file, extension))
    else if (entry.isFile() && file.endsWith(extension)) result.push(file)
  }
  return result.sort()
}

function fail(message: string): void {
  failures.push(message)
}
