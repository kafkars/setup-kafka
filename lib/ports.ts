import net from 'node:net'

import { SetupKafkaError } from './errors.ts'

export class PortReservations {
  readonly ports: readonly number[]
  private readonly servers: net.Server[]

  static async reserve(count: number): Promise<PortReservations> {
    if (!Number.isInteger(count) || count < 1) {
      throw new SetupKafkaError('host_port_invalid', 'at least one host port must be reserved')
    }
    const servers: net.Server[] = []
    const ports: number[] = []
    try {
      for (let index = 0; index < count; index += 1) {
        const server = net.createServer()
        await listen(server)
        const address = server.address()
        if (!address || typeof address === 'string') {
          throw new SetupKafkaError('host_port_unavailable', 'failed to inspect a loopback port reservation')
        }
        servers.push(server)
        ports.push(address.port)
      }
      return new PortReservations(ports, servers)
    } catch (error) {
      await Promise.allSettled(servers.map(close))
      if (error instanceof SetupKafkaError) {
        throw error
      }
      const message = error instanceof Error ? error.message : String(error)
      throw new SetupKafkaError('host_port_unavailable', `failed to reserve loopback ports: ${message}`, { cause: error })
    }
  }

  constructor(ports: readonly number[], servers: net.Server[]) {
    this.ports = Object.freeze([...ports])
    this.servers = servers
  }

  endpoints(): string[] {
    return this.ports.map((port) => `127.0.0.1:${port}`)
  }

  async release(): Promise<void> {
    const servers = this.servers.splice(0)
    await Promise.all(servers.map(close))
  }
}

function listen(server: net.Server): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen({ host: '127.0.0.1', port: 0, exclusive: true }, () => {
      server.off('error', reject)
      resolve()
    })
  })
}

function close(server: net.Server): Promise<void> {
  return new Promise<void>((resolve) => server.close(() => resolve()))
}
