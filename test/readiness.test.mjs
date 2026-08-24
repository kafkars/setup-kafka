import assert from 'node:assert/strict'
import test from 'node:test'

import { topicReadinessProblem } from '../lib/readiness.mjs'

const expected = { name: 'orders', partitions: 2, replicationFactor: 3 }

const ready = `
Topic: orders TopicId: abc PartitionCount: 2 ReplicationFactor: 3 Configs:
  Topic: orders Partition: 0 Leader: 1 Replicas: 1,2,3 Isr: 1,2,3
  Topic: orders Partition: 1 Leader: 2 Replicas: 2,3,1 Isr: 2,3,1
`

test('topic readiness requires leaders, replicas, and full ISR', () => {
  assert.equal(topicReadinessProblem(ready, expected), null)
  assert.match(topicReadinessProblem(ready.replace('Leader: 2', 'Leader: -1'), expected), /leader -1/)
  assert.match(topicReadinessProblem(ready.replace('Isr: 2,3,1', 'Isr: 2,3'), expected), /2 in-sync replicas/)
  assert.match(topicReadinessProblem(ready.split('\n').slice(0, 3).join('\n'), expected), /1 of 2 partitions/)
})
