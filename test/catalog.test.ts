import assert from 'node:assert/strict'
import test from 'node:test'

import { resolveImage, supportedVersions, verifyImageInspection } from '../lib/catalog.ts'

const expected = ['3.7.2', '3.8.1', '3.9.2', '4.0.2', '4.1.2', '4.2.1', '4.3.1']

test('catalog exposes only exact qualified versions', () => {
  assert.deepEqual(supportedVersions(), expected)
  for (const version of expected) {
    const resolved = resolveImage(version)
    assert.match(resolved.image, /^apache\/kafka@sha256:[0-9a-f]{64}$/)
    assert.equal(resolved.extractedFrom.commit, 'b1d609d8a43c3ab97a773a711a6e2a59a20c920a')
  }
})

test('catalog rejects floating and unknown versions', () => {
  assert.throws(() => resolveImage('latest'), /exact patch version/)
  assert.throws(() => resolveImage('4.3'), /exact patch version/)
  assert.throws(() => resolveImage('9.9.9'), /unsupported Apache Kafka version/)
})


test('image inspection must retain the selected digest', () => {
  const image = resolveImage('4.3.1').image
  assert.doesNotThrow(() => verifyImageInspection(JSON.stringify([{ RepoDigests: [image] }]), image))
  assert.throws(
    () => verifyImageInspection(JSON.stringify([{ RepoDigests: ['apache/kafka@sha256:' + '0'.repeat(64)] }]), image),
    /did not retain expected digest/,
  )
  assert.throws(() => verifyImageInspection('not-json', image), /invalid image inspection JSON/)
})
