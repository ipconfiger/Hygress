//! ⑦ registry resolution — pure. Turns a destination's `name.type:port` into a
//! concrete connect target using the `McpBridge` registries + proxies.
//!
//! - `static` → `domain` is already `host:port` (direct connect).
//! - `dns`    → `domain:port` (direct connect).
//! - `proxy`  → through the named `OutboundProxy`.
//! - `tunnel` → WebSocket relay address (L2+).
//!
//! The registry is keyed by `name.type` (**no** port) — the same key space as
//! the model-mapper `name.type` key and the `X-GPUStack-Model-Instance` value.
//!
//! The **hot path** ([`resolve_index`]) reads the snapshot's precomputed
//! registry index (M8) — O(1) per candidate, no per-request linear rescan or
//! scheme-stripping `Registry` clone. [`resolve_destination`] is the direct
//! resolution kept for configs built without the precompute (tests).

use std::collections::HashMap;

use hygress_core::prelude::{ConfigData, Destination, PreResolvedRegistry, ResolvedTarget};

use crate::context::{CandidateTarget, Scheme};
use crate::error::GatewayError;

/// Resolve one destination to a concrete target via the snapshot's
/// **precomputed** registry index (M8) — O(1) per candidate.
///
/// Fails (`GatewayError::RegistryResolve`) when the `name.type` has no
/// registered entry, or its precomputed resolution recorded an error.
pub fn resolve_index(
    index: &HashMap<String, Result<PreResolvedRegistry, String>>,
    dest: &Destination,
) -> Result<CandidateTarget, GatewayError> {
    let service_ref = dest
        .service_ref()
        .map_err(|e| GatewayError::RegistryResolve(e.to_string()))?;
    let service_name = service_ref.service_name(); // `name.type` (no port)
    match index.get(&service_name) {
        Some(Ok(info)) => Ok(CandidateTarget {
            service: dest.service.clone(),
            service_name,
            address: info.address.clone(),
            proxied: info.proxied,
            scheme: if info.https { Scheme::Https } else { Scheme::Http },
            proxy: info.proxy.clone(),
        }),
        Some(Err(e)) => Err(GatewayError::RegistryResolve(format!(
            "'{service_name}': {e}"
        ))),
        None => Err(GatewayError::RegistryResolve(format!(
            "no registry for '{service_name}'"
        ))),
    }
}

/// Resolve one destination to a concrete target by scanning the snapshot's
/// `registries` directly (the precomputed index is derived from these; kept
/// for configs constructed without the precompute, e.g. tests).
pub fn resolve_destination(data: &ConfigData, dest: &Destination) -> Result<CandidateTarget, GatewayError> {
    let service_ref = dest
        .service_ref()
        .map_err(|e| GatewayError::RegistryResolve(e.to_string()))?;
    let service_name = service_ref.service_name(); // `name.type` (no port)

    let registry = data
        .registries
        .iter()
        .find(|r| r.id == service_name)
        .ok_or_else(|| GatewayError::RegistryResolve(format!("no registry for '{service_name}'")))?;

    // D8: the outbound scheme comes from the registry protocol — a domain
    // authored with an `https://` scheme is a TLS endpoint (the core resolver
    // strips the scheme when it builds the `host:port` address). Resolve over
    // the scheme-stripped domain so the address is clean for every kind.
    let (scheme, bare_domain) = split_scheme(&registry.domain);
    let shadow = hygress_core::registry::Registry {
        domain: bare_domain.to_string(),
        ..registry.clone()
    };
    let target = shadow
        .resolve(&data.proxies)
        .map_err(|e| GatewayError::RegistryResolve(format!("'{service_name}': {e}")))?;

    let (address, proxied, proxy) = match target {
        ResolvedTarget::Direct { address } => (address, false, None),
        // D8: carry the outbound proxy address so the data plane can dial
        // through it (HTTP-proxy semantics) instead of the upstream origin.
        ResolvedTarget::Proxied { address, proxy, .. } => (address, true, Some(proxy.address())),
        ResolvedTarget::Tunnel { address } => (address, false, None),
    };

    Ok(CandidateTarget {
        service: dest.service.clone(),
        service_name,
        address,
        proxied,
        scheme,
        proxy,
    })
}

/// Split an optional `scheme://` prefix off a registry domain.
///
/// Returns the parsed scheme (`http`/`https`; anything else → `Http`) and the
/// remaining bare `host[:port]` (a scheme-less domain is returned verbatim).
fn split_scheme(domain: &str) -> (Scheme, &str) {
    match domain.find("://") {
        Some(i) => {
            let scheme = Scheme::parse(&domain[..i]).unwrap_or_default();
            (scheme, &domain[i + "://".len()..])
        }
        None => (Scheme::Http, domain),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hygress_core::prelude::{OutboundProxy, Registry};

    fn data_with(regs: Vec<Registry>, proxies: Vec<OutboundProxy>) -> ConfigData {
        ConfigData {
            registries: regs,
            proxies,
            ..Default::default()
        }
    }

    #[test]
    fn static_resolves_domain_verbatim() {
        let data = data_with(vec![Registry::new("inst.static:80", "10.0.0.5:8081").unwrap()], vec![]);
        let c = resolve_destination(&data, &Destination::new("inst.static:80")).unwrap();
        assert_eq!(c.address, "10.0.0.5:8081");
        assert_eq!(c.service_name, "inst.static");
        assert!(!c.proxied);
    }

    #[test]
    fn dns_resolves_host_and_port() {
        let r = Registry::new("worker.dns:30080", "10.1.2.3").unwrap();
        let data = data_with(vec![r], vec![]);
        let c = resolve_destination(&data, &Destination::new("worker.dns:30080")).unwrap();
        assert_eq!(c.address, "10.1.2.3:30080");
    }

    #[test]
    fn proxy_resolves_through_outbound_proxy() {
        let proxy = OutboundProxy::new("egress-a", "proxy.internal", 3128);
        let r = Registry::new("prov.proxy:443", "api.example.com")
            .unwrap()
            .with_proxy_ref("egress-a");
        let data = data_with(vec![r], vec![proxy.clone()]);
        let c = resolve_destination(&data, &Destination::new("prov.proxy:443")).unwrap();
        assert_eq!(c.address, "api.example.com:443");
        assert!(c.proxied);
        // D8: the outbound proxy address is carried for the data plane to dial
        // through (HTTP-proxy semantics).
        assert_eq!(c.proxy, Some("proxy.internal:3128".to_string()));
        assert_eq!(c.scheme, Scheme::Http);
    }

    // ----- D8: scheme from the registry protocol -----

    #[test]
    fn https_dns_provider_resolves_scheme_and_clean_address() {
        // A `dns` provider endpoint authored with an `https://` domain: the
        // candidate must carry `Scheme::Https` and a scheme-free address.
        let r = Registry::new("provider-1.dns:8443", "https://api.upstream.example.com").unwrap();
        let data = data_with(vec![r], vec![]);
        let c = resolve_destination(&data, &Destination::new("provider-1.dns:8443")).unwrap();
        assert_eq!(c.scheme, Scheme::Https);
        assert_eq!(c.address, "api.upstream.example.com:8443");
        assert!(!c.proxied);
        assert_eq!(c.proxy, None);
    }

    #[test]
    fn https_static_provider_resolves_scheme() {
        let r = Registry::new("p2.static:8443", "https://api.example.com:8443").unwrap();
        let data = data_with(vec![r], vec![]);
        let c = resolve_destination(&data, &Destination::new("p2.static:8443")).unwrap();
        assert_eq!(c.scheme, Scheme::Https);
        assert_eq!(c.address, "api.example.com:8443");
    }

    #[test]
    fn plain_domain_defaults_to_http() {
        let r = Registry::new("w.dns:30080", "10.1.2.3").unwrap();
        let data = data_with(vec![r], vec![]);
        let c = resolve_destination(&data, &Destination::new("w.dns:30080")).unwrap();
        assert_eq!(c.scheme, Scheme::Http);
        assert_eq!(c.address, "10.1.2.3:30080");
    }

    #[test]
    fn split_scheme_handles_variants() {
        assert_eq!(split_scheme("https://api.example.com:443"), (Scheme::Https, "api.example.com:443"));
        assert_eq!(split_scheme("http://10.0.0.5:8081"), (Scheme::Http, "10.0.0.5:8081"));
        assert_eq!(split_scheme("10.0.0.5:8081"), (Scheme::Http, "10.0.0.5:8081"));
        assert_eq!(split_scheme("api.example.com"), (Scheme::Http, "api.example.com"));
        // Unknown schemes fall back to http (only http/https are supported).
        assert_eq!(split_scheme("wss://relay:8443"), (Scheme::Http, "relay:8443"));
    }

    #[test]
    fn unknown_registry_fails() {
        let data = data_with(vec![Registry::new("a.static:80", "10.0.0.9:80").unwrap()], vec![]);
        let e = resolve_destination(&data, &Destination::new("ghost.static:80")).unwrap_err();
        assert!(matches!(e, GatewayError::RegistryResolve(_)));
        assert!(e.to_string().contains("ghost.static"));
    }

    #[test]
    fn bad_destination_service_fails() {
        let data = data_with(vec![], vec![]);
        let e = resolve_destination(&data, &Destination::new("not-a-service")).unwrap_err();
        assert!(matches!(e, GatewayError::RegistryResolve(_)));
    }

    #[test]
    fn dns_without_port_fails() {
        use hygress_core::destination::ServiceType;
        let r = Registry {
            id: "w.dns".into(),
            kind: ServiceType::Dns,
            domain: "10.0.0.1".into(),
            port: None,
            proxy_ref: None,
        };
        let data = data_with(vec![r], vec![]);
        let e = resolve_destination(&data, &Destination::new("w.dns:80")).unwrap_err();
        assert!(matches!(e, GatewayError::RegistryResolve(_)));
    }
}
