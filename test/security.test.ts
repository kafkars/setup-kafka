import assert from 'node:assert/strict'
import test from 'node:test'

import { clientProperties, composeSecurityFile, createSecurity, securityNames } from '../lib/security.ts'

test('security presets map to Kafka client configuration', () => {
  for (const name of securityNames) {
    const security = createSecurity(name)
    const ca = security.tls ? '/tmp/ca.pem' : null
    const properties = clientProperties(security, '127.0.0.1:9092', ca)
    assert.match(properties, new RegExp(`security\\.protocol=${security.securityProtocol}`))
    if (security.sasl) {
      assert.match(properties, new RegExp(`sasl\\.mechanism=${security.saslMechanism}`))
      assert.ok(security.password.length >= 32)
      assert.match(properties, new RegExp(security.password))
    } else {
      assert.equal(security.password, '')
    }
    if (security.tls) {
      assert.match(properties, /ssl\.truststore\.type=PEM/)
      assert.match(properties, /ssl\.truststore\.location=\/tmp\/ca\.pem/)
    }
  }
})

test('security overlay follows topology', () => {
  assert.equal(composeSecurityFile(createSecurity('plaintext'), 'single-broker'), null)
  assert.equal(composeSecurityFile(createSecurity('tls'), 'single-broker'), 'tls-single.yml')
  assert.equal(composeSecurityFile(createSecurity('scram-sha-512'), 'three-broker'), 'sasl-three.yml')
})
