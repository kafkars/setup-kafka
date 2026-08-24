import { SetupKafkaError, invariant } from './errors.mjs'

export const readinessTopic = 'setup-kafka-readiness'

export function parseTopics(text, clusterSize) {
  invariant(Number.isInteger(clusterSize) && clusterSize > 0, 'cluster_size_invalid', 'cluster size must be positive')
  const topics = []
  const seen = new Set()
  for (const [index, rawLine] of String(text ?? '').split(/\r?\n/).entries()) {
    const line = rawLine.trim()
    if (!line || line.startsWith('#')) {
      continue
    }
    const fields = line.split(':')
    if (fields.length > 3) {
      throw topicError(index, 'expected name[:partitions[:replication-factor]]')
    }
    const name = fields[0]?.trim() ?? ''
    validateTopicName(name, index)
    if (name === readinessTopic) {
      throw topicError(index, `${readinessTopic} is reserved by the action`)
    }
    if (seen.has(name)) {
      throw topicError(index, `duplicate topic ${name}`)
    }
    seen.add(name)
    const partitions = parsePositive(fields[1], 1, 10_000, 'partitions', index)
    const replicationFactor = parsePositive(fields[2], clusterSize, clusterSize, 'replication factor', index)
    topics.push({ name, partitions, replicationFactor })
  }
  topics.sort((left, right) => left.name.localeCompare(right.name))
  return topics
}

export function allTopics(userTopics, clusterSize) {
  return [
    ...userTopics,
    { name: readinessTopic, partitions: 1, replicationFactor: clusterSize, internal: true },
  ].sort((left, right) => left.name.localeCompare(right.name))
}

function validateTopicName(name, index) {
  if (!/^[A-Za-z0-9._-]{1,249}$/.test(name) || name === '.' || name === '..') {
    throw topicError(index, `invalid Kafka topic name ${JSON.stringify(name)}`)
  }
}

function parsePositive(value, fallback, maximum, label, index) {
  if (value === undefined || value.trim() === '') {
    return fallback
  }
  if (!/^\d+$/.test(value.trim())) {
    throw topicError(index, `${label} must be an integer`)
  }
  const parsed = Number(value)
  if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > maximum) {
    throw topicError(index, `${label} must be between 1 and ${maximum}`)
  }
  return parsed
}

function topicError(index, message) {
  return new SetupKafkaError('topics_invalid', `topics line ${index + 1}: ${message}`)
}
