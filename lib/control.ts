import { runCompose } from './compose.ts'
import { Deadline } from './deadline.ts'
import { SetupKafkaError } from './errors.ts'
import { requireSuccess } from './process.ts'
import { waitForBrokers, waitForTopics } from './readiness.ts'
import type { EnvironmentState } from './state.ts'

export const controlOperations = [
  'restart',
  'pause',
  'unpause',
  'rolling-restart',
] as const

export type ControlOperation = (typeof controlOperations)[number]

export interface ControlResult {
  operation: ControlOperation
  brokers: string[]
}

export async function controlEnvironment(
  state: EnvironmentState,
  operation: string,
  broker: string,
  timeoutMs = 60_000,
): Promise<ControlResult> {
  if (!isControlOperation(operation)) {
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

async function restart(
  state: EnvironmentState,
  service: string,
  deadline: Deadline,
): Promise<void> {
  await simple(state, ['restart', '--no-deps', service], deadline, 'broker-restart', `restart ${service}`)
  await waitForBrokers(state, deadline)
  await waitForTopics(state, deadline)
}

async function simple(
  state: EnvironmentState,
  args: readonly string[],
  deadline: Deadline,
  operation: string,
  label: string,
): Promise<void> {
  const result = await runCompose(state, args, {
    timeoutMs: deadline.remainingMs(30_000),
    operation: `${operation}-${args.at(-1)}`,
  })
  requireSuccess(result, 'control_failed', label, state.secrets)
}

function resolveBroker(state: EnvironmentState, requested: string): string {
  const broker = requested || state.brokerServices[0]!
  if (!state.brokerServices.includes(broker)) {
    throw new SetupKafkaError('control_broker_invalid', `broker must be one of: ${state.brokerServices.join(', ')}`)
  }
  return broker
}

function isControlOperation(operation: string): operation is ControlOperation {
  return controlOperations.includes(operation as ControlOperation)
}
