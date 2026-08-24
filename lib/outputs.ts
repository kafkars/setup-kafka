import type { EnvironmentState } from './state.ts'

export function publicOutputs(state: EnvironmentState): Record<string, string> {
  return {
    'bootstrap-servers': state.bootstrapServers,
    'security-protocol': state.security.securityProtocol,
    'sasl-mechanism': state.security.saslMechanism,
    username: state.security.username,
    password: state.security.password,
    'ca-certificate': state.caCertificate,
    'client-certificate': state.clientCertificate,
    'client-key': state.clientKey,
    'client-properties': state.clientProperties,
    'compose-project': state.project,
    'state-file': state.stateFile,
    'evidence-directory': state.evidenceDirectory,
    'evidence-archive': state.evidenceArchive,
    'final-evidence-archive': state.finalEvidenceArchive,
    image: state.image,
  }
}
