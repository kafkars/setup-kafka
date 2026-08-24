import fs from 'node:fs'

import { catalogPath } from './paths.mjs'
import { SetupKafkaError, invariant } from './errors.mjs'

let cachedCatalog

export function loadCatalog() {
  if (cachedCatalog) {
    return cachedCatalog
  }
  let parsed
  try {
    parsed = JSON.parse(fs.readFileSync(catalogPath, 'utf8'))
  } catch (error) {
    throw new SetupKafkaError('catalog_read_failed', `failed to read ${catalogPath}`, { cause: error })
  }
  validateCatalog(parsed)
  cachedCatalog = Object.freeze(parsed)
  return cachedCatalog
}

export function supportedVersions() {
  return Object.keys(loadCatalog().versions).sort(compareVersions)
}

export function resolveImage(version) {
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

export function verifyImageInspection(output, expectedImage) {
  let inspected
  try {
    inspected = JSON.parse(String(output))
  } catch (error) {
    throw new SetupKafkaError('image_inspect_invalid', `Docker returned invalid image inspection JSON: ${error.message}`, { cause: error })
  }
  invariant(Array.isArray(inspected) && inspected.length === 1, 'image_inspect_invalid', 'Docker image inspection must contain exactly one image')
  const digests = Array.isArray(inspected[0]?.RepoDigests) ? inspected[0].RepoDigests : []
  invariant(digests.includes(expectedImage), 'image_digest_mismatch', `Docker image inspection did not retain expected digest ${expectedImage}`)
}

function validateCatalog(catalog) {
  invariant(catalog?.schemaVersion === 1, 'catalog_schema_invalid', 'catalog schemaVersion must be 1')
  invariant(catalog.implementation === 'apache-kafka', 'catalog_implementation_invalid', 'catalog implementation must be apache-kafka')
  invariant(typeof catalog.imageRepository === 'string' && catalog.imageRepository.length > 0, 'catalog_image_invalid', 'catalog imageRepository is missing')
  invariant(catalog.versions && typeof catalog.versions === 'object', 'catalog_versions_invalid', 'catalog versions are missing')
  for (const [version, digest] of Object.entries(catalog.versions)) {
    invariant(/^\d+\.\d+\.\d+$/.test(version), 'catalog_version_invalid', `catalog version ${version} is not exact`)
    invariant(/^sha256:[0-9a-f]{64}$/.test(digest), 'catalog_digest_invalid', `catalog digest for ${version} is invalid`)
  }
}

function compareVersions(left, right) {
  const a = left.split('.').map(Number)
  const b = right.split('.').map(Number)
  for (let index = 0; index < 3; index += 1) {
    if (a[index] !== b[index]) {
      return a[index] - b[index]
    }
  }
  return 0
}
