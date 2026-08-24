import net from 'node:net'

import { SetupKafkaError } from './errors.mjs'

export class PortReservations {
  static async reserve(count) {
    if (!Number.isInteger(count) || count < 1) {
      throw new SetupKafkaError('host_port_invalid', 'at least one host port must be reserved')
    }
    const servers = []
    const ports = []
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
      throw new SetupKafkaError('host_port_unavailable', `failed to reserve loopback ports: ${error.message}`, { cause: error })
    }
  }

  constructor(ports, servers) {
    this.ports = Object.freeze([...ports])
    this.servers = servers
  }

  endpoints() {
    return this.ports.map((port) => `127.0.0.1:${port}`)
  }

  async release() {
    const servers = this.servers.splice(0)
    await Promise.all(servers.map(close))
  }
}

function listen(server) {
  return new Promise((resolve, reject) => {
    server.once('error', reject)
    server.listen({ host: '127.0.0.1', port: 0, exclusive: true }, () => {
      server.off('error', reject)
      resolve()
    })
  })
}

function close(server) {
  return new Promise((resolve) => server.close(() => resolve()))
}
