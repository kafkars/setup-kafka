import { SetupKafkaError } from './errors.mjs'

export class Deadline {
  constructor(timeoutMs, label) {
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0) {
      throw new SetupKafkaError('deadline_invalid', `${label} timeout must be a positive integer`)
    }
    this.label = label
    this.expiresAt = Date.now() + timeoutMs
  }

  remainingMs(maximum = Number.MAX_SAFE_INTEGER) {
    const remaining = this.expiresAt - Date.now()
    if (remaining <= 0) {
      throw new SetupKafkaError('deadline_elapsed', `${this.label} deadline elapsed`)
    }
    return Math.max(1, Math.min(remaining, maximum))
  }

  expired() {
    return Date.now() >= this.expiresAt
  }
}
