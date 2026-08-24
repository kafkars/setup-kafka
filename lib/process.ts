import { spawn } from 'node:child_process'

import { appendJsonLine } from './fs-utils.ts'
import { SetupKafkaError } from './errors.ts'

const defaultCaptureLimit = 1024 * 1024

export interface RunCommandOptions {
  command: string
  args?: readonly string[]
  cwd?: string
  env?: NodeJS.ProcessEnv
  timeoutMs: number
  stdin?: string | Uint8Array
  captureLimitBytes?: number
  operationLog?: string
  operation?: string
  secrets?: readonly string[]
}

export interface CommandResult {
  code: number | null
  signal: NodeJS.Signals | null
  stdout: string
  stderr: string
  stdoutTruncated: boolean
  stderrTruncated: boolean
  timedOut: boolean
  durationMs: number
}

interface ExitStatus {
  code: number | null
  signal: NodeJS.Signals | null
}

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
}: RunCommandOptions): Promise<CommandResult> {
  const startedAt = new Date()
  const startedNs = process.hrtime.bigint()
  const stdout = new BoundedCapture(captureLimitBytes)
  const stderr = new BoundedCapture(captureLimitBytes)
  let timedOut = false
  let result: ExitStatus
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
      diagnostic: redactText(error instanceof Error ? error.message : String(error), secrets),
    })
    if (error instanceof SetupKafkaError) {
      throw error
    }
    const message = error instanceof Error ? error.message : String(error)
    throw new SetupKafkaError('process_start_failed', `failed to start ${command}: ${message}`, { cause: error })
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

  function execute(): Promise<ExitStatus> {
    return new Promise<ExitStatus>((resolve, reject) => {
      let settled = false
      let forceTimer: NodeJS.Timeout | undefined
      const child = spawn(command, [...args], {
        cwd,
        env,
        stdio: ['pipe', 'pipe', 'pipe'],
      })
      child.stdout.on('data', (chunk: Buffer) => stdout.push(chunk))
      child.stderr.on('data', (chunk: Buffer) => stderr.push(chunk))
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

      function settleResolve(value: ExitStatus): void {
        if (settled) return
        settled = true
        clearTimeout(timeout)
        clearTimeout(forceTimer)
        resolve(value)
      }

      function settleReject(error: unknown): void {
        if (settled) return
        settled = true
        clearTimeout(timeout)
        clearTimeout(forceTimer)
        reject(error)
      }
    })
  }

  function record(value: unknown): void {
    if (operationLog) {
      appendJsonLine(operationLog, value)
    }
  }
}

export function requireSuccess(
  result: CommandResult,
  code: string,
  label: string,
  secrets: readonly string[] = [],
): CommandResult {
  if (result.code === 0 && !result.timedOut) {
    return result
  }
  const rawDiagnostic = result.stderr.trim() || result.stdout.trim() || 'no command diagnostic'
  const diagnostic = redactText(rawDiagnostic.slice(-4_000), secrets)
  const reason = result.timedOut ? 'timed out' : `exited with ${result.code ?? result.signal ?? 'unknown status'}`
  throw new SetupKafkaError(code, `${label} ${reason}: ${diagnostic}`)
}

export function redactText(value: unknown, secrets: readonly string[]): string {
  let redacted = String(value ?? '')
  for (const secret of secrets.filter(Boolean)) {
    redacted = redacted.split(secret).join('<redacted>')
  }
  return redacted
}

function redactArgs(args: readonly string[], secrets: readonly string[]): string[] {
  return args.map((argument) => redactText(argument, secrets))
}

function elapsedMs(startedNs: bigint): number {
  return Number((process.hrtime.bigint() - startedNs) / 1_000_000n)
}

class BoundedCapture {
  readonly limit: number
  buffer: Buffer
  truncated: boolean

  constructor(limit: number) {
    this.limit = limit
    this.buffer = Buffer.alloc(0)
    this.truncated = false
  }

  push(chunk: string | Uint8Array): void {
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

  text(): string {
    const marker = this.truncated ? '[output truncated; final bytes follow]\n' : ''
    return marker + this.buffer.toString('utf8')
  }
}
