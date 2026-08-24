import assert from 'node:assert/strict'
import test from 'node:test'

import { allTopics, parseTopics, readinessTopic } from '../lib/topics.ts'

test('topic declarations are sorted and default deterministically', () => {
  assert.deepEqual(parseTopics('zeta\nalpha:4\nbeta:6:2\n', 3), [
    { name: 'alpha', partitions: 4, replicationFactor: 3 },
    { name: 'beta', partitions: 6, replicationFactor: 2 },
    { name: 'zeta', partitions: 1, replicationFactor: 3 },
  ])
})

test('readiness topic is always full replication', () => {
  const topics = allTopics(parseTopics('orders:3', 3), 3)
  assert.deepEqual(topics.find((topic) => topic.name === readinessTopic), {
    name: readinessTopic,
    partitions: 1,
    replicationFactor: 3,
    internal: true,
  })
})

test('topic declarations fail closed', () => {
  assert.throws(() => parseTopics('bad name', 1), /invalid Kafka topic name/)
  assert.throws(() => parseTopics('orders:0', 1), /between 1 and 10000/)
  assert.throws(() => parseTopics('orders:1:4', 3), /between 1 and 3/)
  assert.throws(() => parseTopics('orders\norders', 1), /duplicate topic/)
  assert.throws(() => parseTopics(readinessTopic, 1), /reserved/)
})
