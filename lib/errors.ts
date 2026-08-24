export class SetupKafkaError extends Error {
  readonly code: string
  readonly details: unknown | null

  constructor(code: string, message: string, options: SetupKafkaErrorOptions = {}) {
    super(message, options.cause !== undefined ? { cause: options.cause } : undefined)
    this.name = 'SetupKafkaError'
    this.code = code
    this.details = options.details ?? null
  }
}

interface SetupKafkaErrorOptions {
  cause?: unknown
  details?: unknown
}

export function invariant(
  condition: unknown,
  code: string,
  message: string,
  details?: unknown,
): asserts condition {
  if (!condition) {
    throw new SetupKafkaError(code, message, { details })
  }
}

export function asSetupKafkaError(error: unknown, code = 'unexpected_error'): SetupKafkaError {
  if (error instanceof SetupKafkaError) {
    return error
  }
  const message = error instanceof Error ? error.message : String(error)
  return new SetupKafkaError(code, message, { cause: error })
}
