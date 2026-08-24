import { spawn } from 'node:child_process'

import { appendJsonLine } from './fs-utils.mjs'
import { SetupKafkaError } from './errors.ts'

const defaultCaptureLimit = 1024 * 1024

export async function runCommand({
  command,
  args = [],
  cwd,
  env = process.env,
  timeoutMs,
  stdin,
  captureLimitBytes = defaultCaptureLimit,
  operationLog,
  operation = command,
  secrets = [],
}) {
  const startedAt = new Date()
  const startedNs = process.hrtime.bigint()
  const stdout = new BoundedCapture(captureLimitBytes)
  const stderr = new BoundedCapture(captureLimitBytes)
  let timedOut = false
  let result
  try {
    result = await execute()
  } catch (error) {
    const completedAt = new Date()
    record({
      operation,
      command,
      args: redactArgs(args, secrets),
      startedAt: startedAt.toISOString(),
      completedAt: completedAt.toISOString(),
      durationMs: elapsedMs(startedNs),
      status: 'spawn-failed',
      diagnostic: redactText(error.message, secrets),
    })
    if (error instanceof SetupKafkaError) {
      throw error
    }
    throw new SetupKafkaError('process_start_failed', `failed to start ${command}: ${error.message}`, { cause: error })
  }
  const completedAt = new Date()
  const output = {
    ...result,
    stdout: stdout.text(),
    stderr: stderr.text(),
    stdoutTruncated: stdout.truncated,
    stderrTruncated: stderr.truncated,
    timedOut,
    durationMs: elapsedMs(startedNs),
  }
  record({
    operation,
    command,
    args: redactArgs(args, secrets),
    startedAt: startedAt.toISOString(),
    completedAt: completedAt.toISOString(),
    durationMs: output.durationMs,
    status: timedOut ? 'timed-out' : output.code === 0 ? 'succeeded' : 'failed',
    exitCode: output.code,
    signal: output.signal,
    stdoutTruncated: output.stdoutTruncated,
    stderrTruncated: output.stderrTruncated,
  })
  return output

  function execute() {
    return new Promise((resolve, reject) => {
      let settled = false
      let forceTimer
      const child = spawn(command, args, {
        cwd,
        env,
        stdio: ['pipe', 'pipe', 'pipe'],
      })
      child.stdout.on('data', (chunk) => stdout.push(chunk))
      child.stderr.on('data', (chunk) => stderr.push(chunk))
      child.once('error', (error) => settleReject(error))
      child.once('close', (code, signal) => settleResolve({ code, signal }))
      const timeout = setTimeout(() => {
        timedOut = true
        child.kill('SIGTERM')
        forceTimer = setTimeout(() => child.kill('SIGKILL'), 2_000)
        forceTimer.unref()
      }, timeoutMs)
      timeout.unref()
      if (stdin === undefined) {
        child.stdin.end()
      } else {
        child.stdin.end(stdin)
      }

      function settleResolve(value) {
        if (settled) return
        settled = true
        clearTimeout(timeout)
        clearTimeout(forceTimer)
        resolve(value)
      }

      function settleReject(error) {
        if (settled) return
        settled = true
        clearTimeout(timeout)
        clearTimeout(forceTimer)
        reject(error)
      }
    })
  }

  function record(value) {
    if (operationLog) {
      appendJsonLine(operationLog, value)
    }
  }
}

export function requireSuccess(result, code, label, secrets = []) {
  if (result.code === 0 && !result.timedOut) {
    return result
  }
  const rawDiagnostic = result.stderr.trim() || result.stdout.trim() || 'no command diagnostic'
  const diagnostic = redactText(rawDiagnostic.slice(-4_000), secrets)
  const reason = result.timedOut ? 'timed out' : `exited with ${result.code ?? result.signal ?? 'unknown status'}`
  throw new SetupKafkaError(code, `${label} ${reason}: ${diagnostic}`)
}

export function redactText(value, secrets) {
  let redacted = String(value ?? '')
  for (const secret of secrets.filter(Boolean)) {
    redacted = redacted.split(secret).join('<redacted>')
  }
  return redacted
}

function redactArgs(args, secrets) {
  return args.map((argument) => redactText(argument, secrets))
}

function elapsedMs(startedNs) {
  return Number((process.hrtime.bigint() - startedNs) / 1_000_000n)
}

class BoundedCapture {
  constructor(limit) {
    this.limit = limit
    this.buffer = Buffer.alloc(0)
    this.truncated = false
  }

  push(chunk) {
    const next = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)
    if (next.length >= this.limit) {
      this.buffer = next.subarray(next.length - this.limit)
      this.truncated = true
      return
    }
    if (this.buffer.length + next.length <= this.limit) {
      this.buffer = Buffer.concat([this.buffer, next])
      return
    }
    const overflow = this.buffer.length + next.length - this.limit
    this.buffer = Buffer.concat([this.buffer.subarray(overflow), next])
    this.truncated = true
  }

  text() {
    const marker = this.truncated ? '[output truncated; final bytes follow]\n' : ''
    return marker + this.buffer.toString('utf8')
  }
}
