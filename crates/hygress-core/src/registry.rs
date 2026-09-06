//! McpBridge registry / outbound proxy model + target resolution
//! (design §5.3 / §6.3).
//!
//! A [`Registry`] is one row of `McpBridge.spec.registries`: a
//! `name.type` service id plus addressing (`domain`, `port`) and an optional
//! outbound proxy reference. [`Registry::resolve`] turns it into a concrete
//! [`ResolvedTarget`] with the same semantics as GPUStack's
//! `McpBridgeRegistry` + destination wiring:
//!
//! - `static` → `domain` is already `host:port` (embedded `gpustack.static`,
//!   direct instances) → direct connect;
//! - `dns`     → `domain` is a bare host plus real `port` (workers) →
//!   `domain:port` direct connect;
//! - `proxy`   → egress through the named [`OutboundProxy`];
//! - `tunnel`  → WebSocket relay addressing (L2+, design D10) — the address
//!   is resolved the same way as proxy; the variant marks the transport.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::destination::{parse_service_with_port, ServiceType};
use crate::error::Error;

/// One `McpBridge.spec.registries[]` entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Registry {
    /// Service id — `name.type` (no port), e.g. `model-5-12.static`.
    pub id: String,
    /// Registry kind (= type suffix of the id).
    pub kind: ServiceType,
    /// static: already `host:port`; dns/proxy/tunnel: bare host (or
    /// `host:port` for static-style proxies).
    pub domain: String,
    /// Real port (dns) / port carried alongside the service (static uses 80
    /// by convention but the domain already contains the port).
    pub port: Option<u16>,
    /// Outbound proxy name (`proxyName`) for `proxy` registries.
    pub proxy_ref: Option<String>,
}

impl Registry {
    /// Build a registry from a `name.type[:port]` service string and domain.
    /// The service type suffix determines `kind`; the id is normalized to
    /// `name.type` (port stripped — it lives in the service string form, not
    /// the registry id).
    pub fn new(service: &str, domain: impl Into<String>) -> Result<Self, Error> {
        let ref_ = parse_service_with_port(service)?;
        Ok(Registry {
            id: ref_.service_name(),
            kind: ref_.kind,
            domain: domain.into(),
            port: ref_.port,
            proxy_ref: None,
        })
    }

    /// Attach an outbound proxy reference (for `proxy` registries).
    pub fn with_proxy_ref(mut self, proxy_ref: impl Into<String>) -> Self {
        self.proxy_ref = Some(proxy_ref.into());
        self
    }

    /// `name.type` — matches `McpBridgeRegistry.get_service_name()`.
    pub fn service_name(&self) -> &str {
        &self.id
    }

    /// `name.type:port` (port defaults to 80) — matches
    /// `McpBridgeRegistry.get_service_name_with_port()`.
    pub fn service_name_with_port(&self) -> String {
        format!("{}:{}", self.id, self.port.unwrap_or(80))
    }

    /// Resolve this registry to a concrete connection target.
    ///
    /// `proxies` is the `McpBridge.spec.proxies` list; a `proxy` registry
    /// requires its `proxyRef` to be present there.
    pub fn resolve(&self, proxies: &[OutboundProxy]) -> Result<ResolvedTarget, Error> {
        if self.domain.is_empty() {
            return Err(Error::invalid(format!(
                "registry '{}': empty domain",
                self.id
            )));
        }
        match self.kind {
            ServiceType::Static => {
                // Contract (§2.1.2): a static domain is `host:port`. We
                // normalize defensively so a bare host (or a URL-ish string)
                // still resolves to a usable `host:port`. A well-formed
                // `host:port` is returned verbatim (the common case).
                let address = normalize_static_domain(&self.domain, self.port)?;
                Ok(ResolvedTarget::Direct { address })
            }
            ServiceType::Dns => {
                let port = self.port.ok_or_else(|| {
                    Error::invalid(format!("dns registry '{}' requires a port", self.id))
                })?;
                Ok(ResolvedTarget::Direct {
                    address: format!("{}:{}", self.domain, port),
                })
            }
            ServiceType::Proxy => {
                let proxy_name = self.proxy_ref.as_ref().ok_or_else(|| {
                    Error::invalid(format!("proxy registry '{}' requires proxy_ref", self.id))
                })?;
                let proxy = OutboundProxy::find(proxies, proxy_name).ok_or_else(|| {
                    Error::unknown(format!(
                        "registry '{}' references unknown outbound proxy '{proxy_name}'",
                        self.id
                    ))
                })?;
                let address = match self.port {
                    Some(p) => format!("{}:{}", self.domain, p),
                    None => self.domain.clone(),
                };
                Ok(ResolvedTarget::Proxied {
                    address,
                    proxy_name: proxy_name.clone(),
                    proxy: proxy.clone(),
                })
            }
            ServiceType::Tunnel => {
                let address = match self.port {
                    Some(p) => format!("{}:{}", self.domain, p),
                    None => self.domain.clone(),
                };
                Ok(ResolvedTarget::Tunnel { address })
            }
        }
    }
}

/// A resolved connection target for one registry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResolvedTarget {
    /// Direct connect to `address` (`host:port`).
    Direct {
        /// Upstream `host:port` to connect to directly.
        address: String,
    },
    /// Connect through the named outbound proxy; `address` is the upstream
    /// origin host:port.
    Proxied {
        /// Upstream origin `host:port`.
        address: String,
        /// Name of the outbound proxy to connect through.
        proxy_name: String,
        /// The named outbound proxy definition.
        proxy: OutboundProxy,
    },
    /// WebSocket relay (L2+); `address` is the relay endpoint.
    Tunnel {
        /// Relay endpoint (`host:port`).
        address: String,
    },
}

/// Precomputed registry resolution for one `name.type` (design §6.3 / M8),
/// derived once per snapshot at [`crate::config::SharedConfig`] store time so
/// the per-request path does not rescan `registries`, rebuild a scheme-stripped
/// shadow, or re-`resolve` to reach a connect target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreResolvedRegistry {
    /// `true` when the registry domain carries an `https://` scheme (D8).
    pub https: bool,
    /// Connect address `host:port`.
    pub address: String,
    /// Whether the upstream is reached directly or through an outbound proxy.
    pub proxied: bool,
    /// Outbound forward-proxy address (`host:port`) when `proxied` (D8).
    pub proxy: Option<String>,
}

/// Precompute the registry-resolution index for a whole snapshot:
/// `name.type` (registry id) → the resolved connect target, or the resolve
/// error. The scheme is split off once here; an unresolvable registry is
/// recorded so the request path reports the identical error without retrying
/// the resolution.
///
/// Duplicate registry ids keep the **first** entry (`entry().or_insert`),
/// matching `resolve_destination`'s first-match `find` — a malformed snapshot
/// with duplicate ids resolves identically to the direct resolver.
pub fn precompute_registries(
    registries: &[Registry],
    proxies: &[OutboundProxy],
) -> HashMap<String, Result<PreResolvedRegistry, String>> {
    let mut out = HashMap::with_capacity(registries.len());
    for reg in registries {
        let (https, bare) = split_scheme_is_https(&reg.domain);
        let shadow = Registry {
            domain: bare.to_string(),
            ..reg.clone()
        };
        let entry = match shadow.resolve(proxies) {
            Ok(ResolvedTarget::Direct { address }) => Ok(PreResolvedRegistry {
                https,
                address,
                proxied: false,
                proxy: None,
            }),
            Ok(ResolvedTarget::Proxied { address, proxy, .. }) => Ok(PreResolvedRegistry {
                https,
                address,
                proxied: true,
                proxy: Some(proxy.address()),
            }),
            Ok(ResolvedTarget::Tunnel { address }) => Ok(PreResolvedRegistry {
                https,
                address,
                proxied: false,
                proxy: None,
            }),
            Err(e) => Err(e.to_string()),
        };
        out.entry(reg.id.clone()).or_insert(entry);
    }
    out
}

/// Split an optional `scheme://` prefix off a registry domain, reporting only
/// whether the scheme is `https` (D8). The bare `host[:port]` is returned
/// verbatim when the domain carries no scheme.
fn split_scheme_is_https(domain: &str) -> (bool, &str) {
    match domain.find("://") {
        Some(i) => {
            let scheme = &domain[..i];
            let https = scheme.eq_ignore_ascii_case("https");
            (https, &domain[i + "://".len()..])
        }
        None => (false, domain),
    }
}

/// One `McpBridge.spec.proxies[]` entry (provider egress).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboundProxy {
    /// Proxy name (referenced by `Registry::proxyRef`).
    pub name: String,
    /// Proxy server host.
    pub server_address: String,
    /// Proxy server port.
    pub server_port: u16,
    /// Connect timeout in seconds; `None` = not set.
    pub connect_timeout_secs: Option<u32>,
    /// Local listener port (McpBridge `listenerPort`).
    pub listener_port: Option<u16>,
    /// Proxy type (McpBridge `type`, e.g. `forward`/`reverse`).
    pub kind: Option<String>,
}

impl OutboundProxy {
    /// Create a proxy with a name, server address and port; the optional
    /// fields (`connect_timeout_secs`, `listener_port`, `kind`) start unset.
    pub fn new(
        name: impl Into<String>,
        server_address: impl Into<String>,
        server_port: u16,
    ) -> Self {
        Self {
            name: name.into(),
            server_address: server_address.into(),
            server_port,
            connect_timeout_secs: None,
            listener_port: None,
            kind: None,
        }
    }

    /// Find a proxy by name.
    pub fn find<'a>(proxies: &'a [OutboundProxy], name: &str) -> Option<&'a OutboundProxy> {
        proxies.iter().find(|p| p.name == name)
    }

    /// `host:port` of the proxy server.
    pub fn address(&self) -> String {
        format!("{}:{}", self.server_address, self.server_port)
    }

    /// Attach a connect timeout.
    pub fn with_timeout(mut self, secs: u32) -> Self {
        self.connect_timeout_secs = Some(secs);
        self
    }

    /// Attach a proxy type.
    pub fn with_kind(mut self, kind: impl Into<String>) -> Self {
        self.kind = Some(kind.into());
        self
    }
}

/// Normalize a **static** registry domain to a usable `host:port`.
///
/// - a well-formed `host:port` (the common GPUStack case, domain already
///   carries the port) is returned verbatim;
/// - a **bare host** is normalized to `host:<port>` using the registry port
///   (the port parsed from the service string), defaulting to 80;
/// - a **URL-ish string** (`scheme://host:port[/path]`) has its scheme (and any
///   trailing path) stripped, keeping just `host:port`.
///
/// The adapter is responsible for authoring `host:port` domains; this is a
/// belt-and-braces normalization for the pure resolver.
fn normalize_static_domain(domain: &str, port: Option<u16>) -> Result<String, Error> {
    let trimmed = domain.trim();
    if trimmed.is_empty() {
        return Err(Error::invalid("static registry domain is empty"));
    }
    // Strip an optional `scheme://` prefix.
    let without_scheme = match trimmed.find("://") {
        Some(i) => &trimmed[i + 3..],
        None => trimmed,
    };
    // Strip any trailing path (authority only).
    let authority = match without_scheme.find('/') {
        Some(i) => &without_scheme[..i],
        None => without_scheme,
    };
    if authority.is_empty() {
        return Err(Error::invalid(format!(
            "static registry domain '{domain}' has no host"
        )));
    }
    if has_explicit_port(authority) {
        Ok(authority.to_string())
    } else {
        Ok(format!("{}:{}", authority, port.unwrap_or(80)))
    }
}

/// `true` when `host[:port]` carries an explicit numeric port.
///
/// Handles bracketed IPv6 (`[2001:db8::1]` or `[2001:db8::1]:443`) where the
/// address itself contains `:`.
fn has_explicit_port(authority: &str) -> bool {
    if authority.starts_with('[') {
        return match authority.find(']') {
            Some(close) => {
                let rest = &authority[close + 1..];
                rest.len() > 1 && rest.starts_with(':') && rest[1..].parse::<u16>().is_ok()
            }
            None => false,
        };
    }
    match authority.rfind(':') {
        Some(i) => {
            let port_str = &authority[i + 1..];
            !port_str.is_empty() && port_str.parse::<u16>().is_ok()
        }
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_from_service_string() {
        let r = Registry::new("model-5-12.static:80", "10.0.0.5:8081").unwrap();
        assert_eq!(r.id, "model-5-12.static");
        assert_eq!(r.kind, ServiceType::Static);
        assert_eq!(r.port, Some(80));
        assert_eq!(r.service_name(), "model-5-12.static");
        assert_eq!(r.service_name_with_port(), "model-5-12.static:80");
    }

    #[test]
    fn registry_service_name_with_port_defaults_to_80() {
        let r = Registry::new("gpustack.dns", "127.0.0.1").unwrap();
        assert_eq!(r.service_name_with_port(), "gpustack.dns:80");
    }

    #[test]
    fn registry_new_rejects_bad_service() {
        assert!(Registry::new("no-suffix", "127.0.0.1").is_err());
        assert!(Registry::new("svc.bogus:80", "127.0.0.1").is_err());
        assert!(Registry::new("", "127.0.0.1").is_err());
    }

    #[test]
    fn resolve_static_is_domain_as_is() {
        // Embedded form: gpustack.static:80 with domain = 127.0.0.1:<api_port>.
        let r = Registry::new("gpustack.static:80", "127.0.0.1:8080").unwrap();
        let t = r.resolve(&[]).unwrap();
        assert_eq!(
            t,
            ResolvedTarget::Direct {
                address: "127.0.0.1:8080".to_string(),
            }
        );
    }

    #[test]
    fn resolve_static_bare_host_defaults_to_registry_port() {
        // Bare host (no :port) -> normalized to host:<registry port>.
        let r = Registry::new("inst-1.static:8081", "10.0.0.5").unwrap();
        let t = r.resolve(&[]).unwrap();
        assert_eq!(
            t,
            ResolvedTarget::Direct {
                address: "10.0.0.5:8081".to_string(),
            }
        );
        // No port anywhere -> default 80.
        let r2 = Registry::new("inst-2.static", "10.0.0.6").unwrap();
        assert_eq!(
            r2.resolve(&[]).unwrap(),
            ResolvedTarget::Direct {
                address: "10.0.0.6:80".to_string(),
            }
        );
    }

    #[test]
    fn resolve_static_strips_url_like_scheme_and_path() {
        // URL-ish domain: scheme and trailing path are stripped to host:port.
        let r = Registry::new("inst-3.static:80", "http://10.0.0.7:8080/path").unwrap();
        assert_eq!(
            r.resolve(&[]).unwrap(),
            ResolvedTarget::Direct {
                address: "10.0.0.7:8080".to_string(),
            }
        );
        // Empty domain still rejected.
        let r2 = Registry::new("inst-4.static:80", "   ").unwrap();
        assert!(matches!(r2.resolve(&[]), Err(Error::Invalid(_))));
    }

    #[test]
    fn resolve_dns_uses_domain_and_port() {
        let r = Registry::new("worker-1.dns:30080", "10.1.2.3").unwrap();
        let t = r.resolve(&[]).unwrap();
        assert_eq!(
            t,
            ResolvedTarget::Direct {
                address: "10.1.2.3:30080".to_string(),
            }
        );
    }

    #[test]
    fn resolve_dns_without_port_fails() {
        let r = Registry {
            id: "w.dns".into(),
            kind: ServiceType::Dns,
            domain: "10.0.0.1".into(),
            port: None,
            proxy_ref: None,
        };
        assert!(matches!(r.resolve(&[]), Err(Error::Invalid(_))));
    }

    #[test]
    fn resolve_proxy_uses_outbound_proxy() {
        let proxy = OutboundProxy::new("egress-a", "proxy.internal", 3128)
            .with_timeout(10)
            .with_kind("forward");
        let r = Registry::new("provider-1.proxy:443", "api.example.com")
            .unwrap()
            .with_proxy_ref("egress-a");
        let t = r.resolve(std::slice::from_ref(&proxy)).unwrap();
        match &t {
            ResolvedTarget::Proxied {
                address,
                proxy_name,
                proxy,
            } => {
                assert_eq!(address, "api.example.com:443");
                assert_eq!(proxy_name, "egress-a");
                assert_eq!(proxy.address(), "proxy.internal:3128");
            }
            other => panic!("expected Proxied, got {other:?}"),
        }
    }

    #[test]
    fn resolve_proxy_unknown_or_missing_ref_fails() {
        let r = Registry::new("p1.proxy:443", "api.example.com").unwrap();
        assert!(matches!(r.resolve(&[]), Err(Error::Invalid(_))));
        let r2 = r.with_proxy_ref("ghost");
        assert!(matches!(r2.resolve(&[]), Err(Error::Unknown(_))));
    }

    #[test]
    fn resolve_tunnel() {
        let r = Registry::new("w1.tunnel:8443", "relay.internal").unwrap();
        let t = r.resolve(&[]).unwrap();
        assert_eq!(
            t,
            ResolvedTarget::Tunnel {
                address: "relay.internal:8443".to_string(),
            }
        );
    }

    #[test]
    fn resolve_empty_domain_fails() {
        let r = Registry::new("s.static:80", "").unwrap();
        assert!(matches!(r.resolve(&[]), Err(Error::Invalid(_))));
    }

    #[test]
    fn outbound_proxy_find_and_address() {
        let a = OutboundProxy::new("a", "127.0.0.1", 1).with_timeout(5);
        let b = OutboundProxy::new("b", "127.0.0.1", 2);
        assert!(OutboundProxy::find(&[a.clone(), b.clone()], "a").is_some());
        assert!(OutboundProxy::find(&[a.clone(), b.clone()], "zzz").is_none());
        assert_eq!(b.address(), "127.0.0.1:2");
    }
}
