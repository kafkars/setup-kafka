# setup-kafka

Digest-pinned Apache Kafka environments for integration tests.

```yaml
- name: Set up Kafka
  id: kafka
  uses: kafkars/setup-kafka@<full-commit-sha>
  with:
    version: 4.3.1
    topology: three-broker
    security: scram-sha-512
    topics: |
      orders:6:3
      payments:3:3

- name: Run tests
  env:
    KAFKA_BOOTSTRAP_SERVERS: ${{ steps.kafka.outputs.bootstrap-servers }}
    KAFKA_CLIENT_PROPERTIES: ${{ steps.kafka.outputs.client-properties }}
  run: ./scripts/integration-test
```

Pin the Action to a full commit SHA. `version` must be an exact release from the
built-in catalog; floating versions and custom images are rejected.

Setup succeeds only after Kafka is reachable through its exported connection,
every declared topic has a leader and full ISR, and an idempotent producer
completes a write. The post-job hook always captures final evidence and cleans
up the owned containers, networks, and volumes.

The main options are:

- `topology`: `single-broker` or `three-broker`
- `security`: `plaintext`, `tls`, `sasl-plain`, `scram-sha-256`, or `scram-sha-512`
- `topics`: one `name[:partitions[:replication-factor]]` declaration per line

See [`action.yml`](action.yml) for all inputs and outputs. The bundled
`control` and `snapshot` Actions must use the same commit SHA as the root Action.

## Local use

```bash
node bin/setup-kafka.ts catalog
node bin/setup-kafka.ts up --version 4.3.1
node bin/setup-kafka.ts down --state <state-file>
```

Local environments require Linux, Docker Engine, Docker Compose v2, and Node 24.
This project is for tests and qualification, not production deployment.

## Development

```bash
npm ci --ignore-scripts
./scripts/check
```
