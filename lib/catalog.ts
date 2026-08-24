import fs from 'node:fs'

import { catalogPath } from './paths.ts'
import { SetupKafkaError, invariant } from './errors.ts'

export interface CatalogSource {
  repository: string
  commit: string
}

export interface KafkaCatalog {
  schemaVersion: 1
  implementation: 'apache-kafka'
  imageRepository: string
  extractedFrom: CatalogSource
  versions: Record<string, string>
}

export interface ResolvedImage {
  version: string
  digest: string
  image: string
  implementation: 'apache-kafka'
  extractedFrom: CatalogSource
}

let cachedCatalog: Readonly<KafkaCatalog> | undefined

export function loadCatalog(): Readonly<KafkaCatalog> {
  if (cachedCatalog) {
    return cachedCatalog
  }
  let parsed: unknown
  try {
    parsed = JSON.parse(fs.readFileSync(catalogPath, 'utf8'))
  } catch (error) {
    throw new SetupKafkaError('catalog_read_failed', `failed to read ${catalogPath}`, { cause: error })
  }
  validateCatalog(parsed)
  cachedCatalog = Object.freeze(parsed)
  return cachedCatalog
}

export function supportedVersions(): string[] {
  return Object.keys(loadCatalog().versions).sort(compareVersions)
}

export function resolveImage(version: string): ResolvedImage {
  invariant(/^\d+\.\d+\.\d+$/.test(version), 'version_not_exact', `version must be an exact patch version, got ${JSON.stringify(version)}`)
  const catalog = loadCatalog()
  const digest = catalog.versions[version]
  if (!digest) {
    throw new SetupKafkaError(
      'version_unsupported',
      `unsupported Apache Kafka version ${version}; supported versions: ${supportedVersions().join(', ')}`,
    )
  }
  return {
    version,
    digest,
    image: `${catalog.imageRepository}@${digest}`,
    implementation: catalog.implementation,
    extractedFrom: catalog.extractedFrom,
  }
}

export function verifyImageInspection(output: string, expectedImage: string): void {
  let inspected: unknown
  try {
    inspected = JSON.parse(String(output))
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    throw new SetupKafkaError('image_inspect_invalid', `Docker returned invalid image inspection JSON: ${message}`, { cause: error })
  }
  invariant(Array.isArray(inspected) && inspected.length === 1, 'image_inspect_invalid', 'Docker image inspection must contain exactly one image')
  const image = inspected[0]
  const digests = isRecord(image) && Array.isArray(image.RepoDigests) ? image.RepoDigests : []
  invariant(digests.includes(expectedImage), 'image_digest_mismatch', `Docker image inspection did not retain expected digest ${expectedImage}`)
}

function validateCatalog(catalog: unknown): asserts catalog is KafkaCatalog {
  invariant(isRecord(catalog), 'catalog_schema_invalid', 'catalog must be an object')
  invariant(catalog.schemaVersion === 1, 'catalog_schema_invalid', 'catalog schemaVersion must be 1')
  invariant(catalog.implementation === 'apache-kafka', 'catalog_implementation_invalid', 'catalog implementation must be apache-kafka')
  invariant(typeof catalog.imageRepository === 'string' && catalog.imageRepository.length > 0, 'catalog_image_invalid', 'catalog imageRepository is missing')
  invariant(isRecord(catalog.extractedFrom), 'catalog_source_invalid', 'catalog extractedFrom is missing')
  invariant(typeof catalog.extractedFrom.repository === 'string', 'catalog_source_invalid', 'catalog source repository is missing')
  invariant(typeof catalog.extractedFrom.commit === 'string', 'catalog_source_invalid', 'catalog source commit is missing')
  invariant(isRecord(catalog.versions), 'catalog_versions_invalid', 'catalog versions are missing')
  for (const [version, digest] of Object.entries(catalog.versions)) {
    invariant(/^\d+\.\d+\.\d+$/.test(version), 'catalog_version_invalid', `catalog version ${version} is not exact`)
    invariant(typeof digest === 'string' && /^sha256:[0-9a-f]{64}$/.test(digest), 'catalog_digest_invalid', `catalog digest for ${version} is invalid`)
  }
}

function compareVersions(left: string, right: string): number {
  const a = left.split('.').map(Number)
  const b = right.split('.').map(Number)
  for (let index = 0; index < 3; index += 1) {
    if (a[index] !== b[index]) {
      return a[index]! - b[index]!
    }
  }
  return 0
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
