//! Downstream TLS: the SNI certificate store fed from `ConfigData.tls`
//! (`Secret gpustack-tls-<host>` / `-default`, design §4.4 / §5.3).
//!
//! ## Single source of truth + hot reload
//!
//! The resolved cert table lives in exactly one place: an
//! [`arc_swap::ArcSwap`] of a [`SniMap`] owned by the [`SniStore`].
//! [`SniStore::store_config`] reads the `tls.crt`/`tls.key` PEM content from a
//! [`TlsConfig`], PEM-parses it into rustls `pki_types`, builds a rustls
//! [`CertifiedKey`] per host, and atomically `store()`s the result — so a
//! re-`store_config` (a hot snapshot change) is visible to the very next
//! handshake **that resolves through this store**.
//!
//! R-9⑤ (pingora 0.8 constraint): the data-plane listener is attached with the
//! file-based `pingora` `add_tls` (single default-cert PEM, written once at
//! bind — see `bootstrap::write_default_tls_pem`); the `SniStore` is
//! snapshot-reflected at bind time for the future SNI resolver / integration
//! tests, and the rustls [`ServerConfig`] built here (`server_config`) is what
//! a pingora upgrade with an injectable resolver (PR #599 / issue #832) would
//! attach. Certificate rotation in 0.8 therefore requires a container restart
//! (documented; a rotation-detection loop is added in R-11).
//!
//! ## SNI selection (exact → first-level wildcard → default)
//!
//! `SniResolver` implements rustls's [`ResolvesServerCert`]: it reads the
//! client SNI (`ClientHello::server_name`) and resolves the cert by **exact
//! lowercase host → first-level wildcard (`a.b.c` → `*.b.c`) → the
//! `gpustack-tls-default` fallback**. The pure selection policy
//! ([`match_name`]) is unit-tested independently of the crypto provider.
//!
//! ## Crypto provider
//!
//! rustls 0.23 has no default crypto provider; building a [`CertifiedKey`]
//! (and a [`ServerConfig`]) requires one. Rather than pin `ring`/`aws-lc-rs`
//! here, every entry point that needs the crypto takes `provider: &Arc<CryptoProvider>`
//! and the data plane supplies it. That keeps this module free of a specific
//! provider dependency while still producing a real, attachable [`ServerConfig`].

use std::collections::BTreeMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ResolvesServerCert, ServerConfig};
use rustls::sign::CertifiedKey;
use rustls_pemfile::{certs as pem_certs, private_key as pem_private_key};
use thiserror::Error;

use hygress_core::prelude::TlsConfig;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors raised while parsing PEM or assembling the SNI server config.
#[derive(Debug, Error)]
pub enum TlsError {
    #[error("PEM parse failed: {0}")]
    Pem(String),
    #[error("crypto/protocol-version error: {0}")]
    Provider(String),
}

// ---------------------------------------------------------------------------
// Pure SNI selection policy (no crypto, fully testable)
// ---------------------------------------------------------------------------

/// First-level wildcard label: `foo.example.com` → `*.example.com`.
/// `None` when there is no right-hand label to wildcard.
pub fn wildcard_of(domain: &str) -> Option<String> {
    let rest = domain.split_once('.')?.1;
    if rest.is_empty() {
        return None;
    }
    Some(format!("*.{rest}"))
}

/// The SNI selection policy: **exact (lowercase) → first-level wildcard → None**
/// (the caller then falls back to its default cert).
///
/// Returns the matched key: the host itself (exact) or the wildcard pattern.
/// Returning `None` signals "use the default cert".
///
/// Generic over the value type (the policy only checks key membership), so it is
/// unit-testable without a crypto provider.
pub fn match_name<K>(host: &str, names: &BTreeMap<String, K>) -> Option<String> {
    let h = host.to_ascii_lowercase();
    if names.contains_key(&h) {
        return Some(h);
    }
    if let Some(w) = wildcard_of(&h) {
        if names.contains_key(&w) {
            return Some(w);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// PEM parsing (pure; no provider needed to parse)
// ---------------------------------------------------------------------------

/// Parse a PEM cert/key pair into rustls `pki_types` (DER cert chain + private
/// key). No crypto provider is required to *parse*; one is only needed to build
/// a `CertifiedKey`. Returns the first key found (multi-key PEM: first wins).
pub fn parse_pem(
    cert_pem: &str,
    key_pem: &str,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), TlsError> {
    let mut certs = Vec::new();
    let mut cert_cursor = std::io::Cursor::new(cert_pem.as_bytes());
    for r in pem_certs(&mut cert_cursor) {
        certs.push(r.map_err(|e| TlsError::Pem(e.to_string()))?);
    }
    if certs.is_empty() {
        return Err(TlsError::Pem("no certificates in PEM".to_string()));
    }
    let mut key_cursor = std::io::Cursor::new(key_pem.as_bytes());
    let key = match pem_private_key(&mut key_cursor) {
        Ok(Some(k)) => k,
        Ok(None) => return Err(TlsError::Pem("no private key in PEM".to_string())),
        Err(e) => return Err(TlsError::Pem(e.to_string())),
    };
    Ok((certs, key))
}

// ---------------------------------------------------------------------------
// SniMap — the resolved table (single source of truth)
// ---------------------------------------------------------------------------

/// The resolved SNI table: `lowercase host → CertifiedKey` plus an optional
/// default (the `gpustack-tls-default` fallback cert).
#[derive(Default, Clone)]
pub struct SniMap {
    /// Exact / wildcard host → cert, keyed by lowercase host / wildcard pattern.
    pub by_name: BTreeMap<String, Arc<CertifiedKey>>,
    /// Fallback cert (`gpustack-tls-default`).
    pub default: Option<Arc<CertifiedKey>>,
}

impl SniMap {
    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty() && self.default.is_none()
    }
    /// Total number of configured cert slots (named + default).
    pub fn count(&self) -> usize {
        self.by_name.len() + usize::from(self.default.is_some())
    }
    /// Resolve the cert for a requested SNI: exact → wildcard → default.
    pub fn resolve(&self, host: Option<&str>) -> Option<Arc<CertifiedKey>> {
        if let Some(h) = host {
            if let Some(key) = match_name(h, &self.by_name) {
                return self.by_name.get(&key).cloned();
            }
        }
        self.default.clone()
    }
}

// ---------------------------------------------------------------------------
// SniStore — the shared, ArcSwap-backed handle
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct SniStore {
    inner: Arc<ArcSwap<SniMap>>,
}

impl SniStore {
    /// Build an empty store. It is filled via [`Self::store_config`] once the
    /// first `TlsConfig` arrives (and re-filled on every hot reload).
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(SniMap::default())),
        }
    }

    /// The current (lock-free) resolved table.
    pub fn load(&self) -> Arc<SniMap> {
        self.inner.load_full()
    }

    /// Parse `tls` into a [`SniMap`], build a `CertifiedKey` per host (isolating
    /// per-host failures), and atomically swap it in. Returns the number of
    /// accepted cert slots (named + default).
    ///
    /// Per-host isolation: a malformed PEM for one host is skipped + WARNed;
    /// the others still resolve (design §5.4).
    pub fn store_config(
        &self,
        tls: &TlsConfig,
        provider: &Arc<CryptoProvider>,
    ) -> usize {
        let mut map = SniMap::default();
        for host in &tls.hosts {
            let key = match parse_pem(&host.cert_pem, &host.key_pem) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        host = %host.host,
                        error = %e,
                        "TLS cert for host skipped (others unaffected)"
                    );
                    continue;
                }
            };
            let certified = match CertifiedKey::from_der(key.0, key.1, provider) {
                Ok(ck) => ck,
                Err(e) => {
                    tracing::warn!(
                        host = %host.host,
                        error = %e,
                        "TLS CertifiedKey for host skipped (others unaffected)"
                    );
                    continue;
                }
            };
            let slot = Arc::new(certified);
            if host.is_default {
                map.default = Some(slot);
            } else {
                // Key by the lowercase host so SNI lookups are case-insensitive.
                map.by_name.insert(host.host.to_ascii_lowercase(), slot);
            }
        }
        let accepted = map.count();
        self.inner.store(Arc::new(map));
        accepted
    }

    /// Build the rustls [`ServerConfig`] that terminates TLS with this store's
    /// SNI callback. The resolver shares the store's `ArcSwap`, so later
    /// [`Self::store_config`] calls take effect for new handshakes.
    pub fn server_config(&self, provider: &Arc<CryptoProvider>) -> Result<ServerConfig, TlsError> {
        let resolver: Arc<dyn ResolvesServerCert> =
            Arc::new(SniResolver { inner: self.inner.clone() });
        let config = rustls::server::ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(|e| TlsError::Provider(e.to_string()))?
            .with_no_client_auth()
            .with_cert_resolver(resolver);
        Ok(config)
    }
}

impl Default for SniStore {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// ResolvesServerCert impl
// ---------------------------------------------------------------------------

/// The rustls SNI callback: reads the client SNI and resolves the cert via the
/// shared [`SniMap`] (exact → wildcard → default).
struct SniResolver {
    inner: Arc<ArcSwap<SniMap>>,
}

impl std::fmt::Debug for SniResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SniResolver").finish_non_exhaustive()
    }
}

impl ResolvesServerCert for SniResolver {
    fn resolve(&self, client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        let sni = client_hello.server_name();
        let map = self.inner.load();
        let resolved = map.resolve(sni);
        if resolved.is_none() {
            tracing::warn!(
                sni = ?sni,
                "no cert matched SNI and no default configured; handshake will be rejected by the TLS layer"
            );
        }
        resolved
    }
}

// ---------------------------------------------------------------------------
// Tests — pure parts only (PEM parse + SNI policy). The provider-dependent
// `CertifiedKey` build + real handshake are exercised by the `integrations`
// e2e suite (a crypto provider is supplied there).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    const CERT: &str = include_str!("../tests/fixtures/test-cert.pem");
    const KEY: &str = include_str!("../tests/fixtures/test-key.pem");

    /// A value-agnostic key set (the SNI policy is membership-only, so `()`
    /// stands in for the `Arc<CertifiedKey>` value).
    fn keyset(names: &[&str]) -> BTreeMap<String, ()> {
        names.iter().map(|n| (n.to_ascii_lowercase(), ())).collect()
    }

    #[test]
    fn parse_pem_reads_fixture() {
        let (certs, key) = parse_pem(CERT, KEY).unwrap();
        assert!(!certs.is_empty(), "fixture must parse into at least one cert");
        assert!(!key.secret_der().is_empty(), "fixture key must be non-empty");
    }

    #[test]
    fn parse_pem_rejects_garbage() {
        assert!(parse_pem("not a pem", KEY).is_err());
        assert!(parse_pem(CERT, "not a key").is_err());
        // Empty cert / empty key are rejected, not silently accepted.
        assert!(parse_pem("", "").is_err());
    }

    #[test]
    fn wildcard_first_label_only() {
        assert_eq!(wildcard_of("api.acme.com").as_deref(), Some("*.acme.com"));
        assert_eq!(
            wildcard_of("a.b.example.org").as_deref(),
            Some("*.b.example.org")
        );
        assert_eq!(wildcard_of("com"), None);
        assert_eq!(wildcard_of(""), None);
        assert_eq!(wildcard_of("foo."), None);
    }

    #[test]
    fn sni_exact_match_case_insensitive() {
        let names = keyset(&["acme.com", "*.example.com"]);
        assert_eq!(match_name("ACME.com", &names).as_deref(), Some("acme.com"));
    }

    #[test]
    fn sni_wildcard_fallback() {
        let names = keyset(&["*.example.com"]);
        // Exact miss, first-level wildcard hits.
        assert_eq!(
            match_name("api.example.com", &names).as_deref(),
            Some("*.example.com")
        );
        // Sibling domain misses.
        assert_eq!(match_name("other.com", &names), None);

        // A two-level-deeper host does NOT match `*.example.com` (a single-label
        // wildcard covers exactly one label); it only matches its OWN first-level
        // wildcard, `*.b.example.com`, which is a distinct key.
        let deep = keyset(&["*.example.com", "*.b.example.com"]);
        assert_eq!(
            match_name("a.b.example.com", &deep).as_deref(),
            Some("*.b.example.com")
        );
        // Without the deeper wildcard configured, the deep host falls through
        // (→ None here; the resolver then uses the default cert).
        assert_eq!(match_name("a.b.example.com", &names), None);
    }

    #[test]
    fn sni_no_match_is_none() {
        let names = keyset(&["acme.com"]);
        assert_eq!(match_name("unknown.io", &names), None);
    }

    #[test]
    fn sni_empty_map_never_matches() {
        let names: BTreeMap<String, ()> = BTreeMap::new();
        assert!(match_name("anything.example", &names).is_none());
    }

    #[test]
    fn sni_map_resolve_empty_default() {
        // On an empty table (no named host, no default) every SNI → None, which
        // the resolver then logs + rejects (handled by the TLS layer).
        let map = SniMap::default();
        assert!(map.is_empty());
        assert_eq!(map.count(), 0);
        assert!(map.resolve(None).is_none());
        assert!(map.resolve(Some("anything.example")).is_none());
    }
}
