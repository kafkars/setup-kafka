export class SetupKafkaError extends Error {
  constructor(code, message, options = {}) {
    super(message, options.cause ? { cause: options.cause } : undefined)
    this.name = 'SetupKafkaError'
    this.code = code
    this.details = options.details ?? null
  }
}

export function invariant(condition, code, message, details) {
  if (!condition) {
    throw new SetupKafkaError(code, message, { details })
  }
}

export function asSetupKafkaError(error, code = 'unexpected_error') {
  if (error instanceof SetupKafkaError) {
    return error
  }
  const message = error instanceof Error ? error.message : String(error)
  return new SetupKafkaError(code, message, { cause: error })
}
