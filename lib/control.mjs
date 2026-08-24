import { runCompose } from './compose.mjs'
import { Deadline } from './deadline.ts'
import { SetupKafkaError } from './errors.ts'
import { requireSuccess } from './process.ts'
import { waitForBrokers, waitForTopics } from './readiness.mjs'

export const controlOperations = Object.freeze([
  'restart',
  'pause',
  'unpause',
  'rolling-restart',
])

export async function controlEnvironment(state, operation, broker, timeoutMs = 60_000) {
  if (!controlOperations.includes(operation)) {
    throw new SetupKafkaError('control_operation_invalid', `operation must be one of: ${controlOperations.join(', ')}`)
  }
  const deadline = new Deadline(timeoutMs, `control ${operation}`)
  if (operation === 'rolling-restart') {
    if (broker) {
      throw new SetupKafkaError('control_broker_invalid', 'rolling-restart does not accept a broker')
    }
    for (const service of state.brokerServices) {
      await restart(state, service, deadline)
    }
    return { operation, brokers: [...state.brokerServices] }
  }
  const service = resolveBroker(state, broker)
  if (operation === 'restart') {
    await restart(state, service, deadline)
  } else if (operation === 'pause') {
    await simple(state, ['pause', service], deadline, 'broker-pause', `pause ${service}`)
  } else {
    await simple(state, ['unpause', service], deadline, 'broker-unpause', `unpause ${service}`)
    await waitForBrokers(state, deadline)
    await waitForTopics(state, deadline)
  }
  return { operation, brokers: [service] }
}

async function restart(state, service, deadline) {
  await simple(state, ['restart', '--no-deps', service], deadline, 'broker-restart', `restart ${service}`)
  await waitForBrokers(state, deadline)
  await waitForTopics(state, deadline)
}

async function simple(state, args, deadline, operation, label) {
  const result = await runCompose(state, args, {
    timeoutMs: deadline.remainingMs(30_000),
    operation: `${operation}-${args.at(-1)}`,
  })
  requireSuccess(result, 'control_failed', label, state.secrets)
}

function resolveBroker(state, requested) {
  const broker = requested || state.brokerServices[0]
  if (!state.brokerServices.includes(broker)) {
    throw new SetupKafkaError('control_broker_invalid', `broker must be one of: ${state.brokerServices.join(', ')}`)
  }
  return broker
}
