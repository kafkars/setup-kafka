import assert from 'node:assert/strict'
import net from 'node:net'
import test from 'node:test'

import { PortReservations } from '../lib/ports.ts'

test('host ports remain owned until explicitly released', async () => {
  const reservation = await PortReservations.reserve(3)
  assert.equal(new Set(reservation.ports).size, 3)
  await assert.rejects(bind(reservation.ports[0]!))
  await reservation.release()
  const server = await bind(reservation.ports[0]!)
  await new Promise((resolve) => server.close(resolve))
})

function bind(port: number): Promise<net.Server> {
  return new Promise<net.Server>((resolve, reject) => {
    const server = net.createServer()
    server.once('error', reject)
    server.listen({ host: '127.0.0.1', port, exclusive: true }, () => resolve(server))
  })
}
