//! Crate-wide error type for the strategy-2 adapter.
//!
//! The pure translation layer (`translate`) returns `hygress_core::Error` for per-object
//! issues; this type covers the **control-plane wiring** fallible operations — connecting to
//! the apiserver, startup discovery, and the best-effort topology-B IngressClass seed.

use thiserror::Error;

/// Unified error for the adapter (control-plane wiring).
#[derive(Debug, Error)]
pub enum Error {
    /// A kube transport / inference failure (config inference, kubeconfig parsing,
    /// `LIST`/`GET`, connection, ...).
    #[error("kube: {0}")]
    Kube(#[from] kube::Error),

    /// The apiserver did not become ready within the startup discovery budget
    /// (design §5: `GET /api`, 60s / 5s retry).
    #[error("apiserver not ready within the discovery window: {0}")]
    NotReady(String),

    /// Invalid adapter configuration supplied to [`crate::Controller::new`].
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
}

/// Convenience result alias for the adapter.
pub type Result<T> = std::result::Result<T, Error>;
