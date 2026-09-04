//! Hygress container binary entry point (design §11).
//!
//! A thin delegate to [`hygress_gateway::bootstrap::main`], the real entry: it parses the
//! environment config, runs the bootstrap pre-conditions (the `GPUSTACK_API_PORT` readiness
//! probe and — under `integrations` — the fail-fast `jwt_secret_key` resolution, the control-plane
//! `Controller` + first-snapshot `ready()`, and the terminate-mode data plane), then blocks the
//! main thread on the Pingora server until SIGTERM/SIGINT. A failed pre-condition (readiness
//! timeout, jwt fail-fast, control-plane init) is a logged **non-zero** exit.

fn main() {
    // Install a rustls CryptoProvider process-wide BEFORE any TLS use (the kube client +
    // reqwest dial the APIServer and upstreams over TLS; without an installed provider
    // rustls panics at first handshake). Idempotent: a second install is ignored.
    let _ = rustls::crypto::ring::default_provider().install_default();
    hygress_gateway::bootstrap::main();
}
