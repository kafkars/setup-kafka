import { fileURLToPath } from 'node:url'
import path from 'node:path'

export const repositoryRoot = path.resolve(fileURLToPath(new URL('..', import.meta.url)))
export const catalogPath = path.join(repositoryRoot, 'catalog', 'apache-kafka.json')
export const composeRoot = path.join(repositoryRoot, 'compose')
