//! `higress.io/destination` entry parsing and service reference model.
//!
//! GPUStack writes destination lists in two forms (design §2.1.2):
//!
//! - weighted:      `"<pct>% <name.type:port>"` (Hamilton-computed percents)
//! - mirror (no %): `"<name.type:port>"` (e.g. `gpustack.dns:30080`)
//!
//! A service reference is `name.type` with the *type* suffix in
//! `{static, dns, proxy, tunnel}` and an optional `:port` — exactly the shape
//! produced by `McpBridgeRegistry.get_service_name()` / `get_service_name_with_port()`.

use serde::{Deserialize, Serialize};

use crate::error::Error;

/// Service (registry) type, derived from the `name.type` suffix.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceType {
    /// `domain` is already a `host:port` (embedded `gpustack.static:80`, direct instances).
    Static,
    /// `domain` is a bare host plus a real `port` (workers, incluster instances).
    Dns,
    /// Egress via an `OutboundProxy` (provider endpoints).
    Proxy,
    /// WebSocket relay addressing (L2+, design D10).
    Tunnel,
}

impl ServiceType {
    /// The suffix as it appears in a service name (`static`, `dns`, ...).
    pub fn suffix(self) -> &'static str {
        match self {
            ServiceType::Static => "static",
            ServiceType::Dns => "dns",
            ServiceType::Proxy => "proxy",
            ServiceType::Tunnel => "tunnel",
        }
    }

    /// Parse a service type from its suffix; `None` for unknown suffixes.
    pub fn from_suffix(s: &str) -> Option<Self> {
        match s {
            "static" => Some(ServiceType::Static),
            "dns" => Some(ServiceType::Dns),
            "proxy" => Some(ServiceType::Proxy),
            "tunnel" => Some(ServiceType::Tunnel),
            _ => None,
        }
    }
}

/// A parsed `name.type[:port]` service reference.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ServiceRef {
    /// Service name without type/port (e.g. `model-5-12`, `gpustack`).
    pub name: String,
    /// Registry kind derived from the type suffix.
    pub kind: ServiceType,
    /// Optional explicit port (`None` for the no-port mirror form).
    pub port: Option<u16>,
}

impl ServiceRef {
    /// `name.type` — matches `McpBridgeRegistry.get_service_name()`.
    pub fn service_name(&self) -> String {
        format!("{}.{}", self.name, self.kind.suffix())
    }

    /// `name.type:port` — matches `McpBridgeRegistry.get_service_name_with_port()`
    /// (port defaults to 80 when absent).
    pub fn service_name_with_port(&self) -> String {
        format!("{}:{}", self.service_name(), self.port.unwrap_or(80))
    }
}

/// Parse a `name.type[:port]` service reference.
///
/// Accepts both `name.type` (no port) and `name.type:port`. Fails with
/// [`Error::Parse`] when the string is malformed and [`Error::Unknown`] for an
/// unrecognized type suffix.
pub fn parse_service_with_port(input: &str) -> Result<ServiceRef, Error> {
    let input = input.trim();
    if input.is_empty() {
        return Err(Error::parse("empty service reference"));
    }

    let (name_type, port) = match input.rfind(':') {
        Some(i) => {
            let port_str = &input[i + 1..];
            if port_str.is_empty() {
                return Err(Error::parse(format!(
                    "service '{input}' has a trailing ':' without port"
                )));
            }
            let port: u16 = port_str.parse().map_err(|_| {
                Error::parse(format!("invalid port '{port_str}' in service '{input}'"))
            })?;
            (&input[..i], Some(port))
        }
        None => (input, None),
    };

    let dot = name_type
        .rfind('.')
        .ok_or_else(|| Error::parse(format!("service '{input}' missing '.type' suffix")))?;
    let name = &name_type[..dot];
    let suffix = &name_type[dot + 1..];
    if name.is_empty() {
        return Err(Error::parse(format!("service '{input}' has an empty name")));
    }
    if suffix.is_empty() {
        return Err(Error::parse(format!(
            "service '{input}' has an empty type suffix"
        )));
    }
    let kind = ServiceType::from_suffix(suffix)
        .ok_or_else(|| Error::unknown(format!("unknown service type '{suffix}' in '{input}'")))?;

    Ok(ServiceRef {
        name: name.to_string(),
        kind,
        port,
    })
}

/// One destination entry of a `higress.io/destination` annotation list.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Destination {
    /// Weight percent (0–100). `None` for the mirror no-percent form; the
    /// effective weight is then 100.
    pub percent: Option<u32>,
    /// The `name.type:port` service string.
    pub service: String,
}

impl Destination {
    /// A single unweighted destination (mirror / single-backend form).
    pub fn new(service: impl Into<String>) -> Self {
        Destination {
            percent: None,
            service: service.into(),
        }
    }

    /// A weighted destination.
    pub fn with_percent(percent: u32, service: impl Into<String>) -> Self {
        Destination {
            percent: Some(percent),
            service: service.into(),
        }
    }

    /// Parse one destination entry — either `"<pct>% <name.type:port>"` or
    /// `"<name.type:port>"` (mirror form, percent defaults to 100).
    pub fn parse(entry: &str) -> Result<Self, Error> {
        let entry = entry.trim();
        if entry.is_empty() {
            return Err(Error::parse("empty destination entry"));
        }
        match entry.find('%') {
            Some(idx) => {
                let pct_str = entry[..idx].trim();
                let percent: u32 = pct_str.parse().map_err(|_| {
                    Error::parse(format!("invalid percent '{pct_str}' in entry '{entry}'"))
                })?;
                let service = entry[idx + 1..].trim();
                if service.is_empty() {
                    return Err(Error::parse(format!(
                        "entry '{entry}' has a percent but no service"
                    )));
                }
                Ok(Destination {
                    percent: Some(percent),
                    service: service.to_string(),
                })
            }
            None => Ok(Destination {
                percent: None,
                service: entry.to_string(),
            }),
        }
    }

    /// Effective weight percent (100 for the no-percent form).
    pub fn weight(&self) -> u32 {
        self.percent.unwrap_or(100)
    }

    /// Parse this destination's service reference (`name.type[:port]`).
    pub fn service_ref(&self) -> Result<ServiceRef, Error> {
        parse_service_with_port(&self.service)
    }
}

/// Parse a full `higress.io/destination` block (newline-separated entries,
/// with or without percents). Blank lines are skipped.
pub fn parse_destinations(block: &str) -> Result<Vec<Destination>, Error> {
    block
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(Destination::parse)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ----- ServiceType -----

    #[test]
    fn service_type_suffix_roundtrip() {
        for t in [
            ServiceType::Static,
            ServiceType::Dns,
            ServiceType::Proxy,
            ServiceType::Tunnel,
        ] {
            assert_eq!(ServiceType::from_suffix(t.suffix()), Some(t));
        }
    }

    #[test]
    fn service_type_unknown_suffix() {
        assert_eq!(ServiceType::from_suffix("bogus"), None);
        assert_eq!(ServiceType::from_suffix(""), None);
    }

    // ----- parse_service_with_port -----

    #[test]
    fn parse_service_with_port_full() {
        let s = parse_service_with_port("model-5-12.static:80").unwrap();
        assert_eq!(s.name, "model-5-12");
        assert_eq!(s.kind, ServiceType::Static);
        assert_eq!(s.port, Some(80));
        assert_eq!(s.service_name(), "model-5-12.static");
        assert_eq!(s.service_name_with_port(), "model-5-12.static:80");
    }

    #[test]
    fn parse_service_no_port() {
        let s = parse_service_with_port("gpustack.dns").unwrap();
        assert_eq!(s.kind, ServiceType::Dns);
        assert_eq!(s.port, None);
        // GPUStack get_service_name_with_port() defaults to 80.
        assert_eq!(s.service_name_with_port(), "gpustack.dns:80");
    }

    #[test]
    fn parse_service_dns_proxy_tunnel() {
        assert_eq!(
            parse_service_with_port("provider-1.dns:8443").unwrap().kind,
            ServiceType::Dns
        );
        assert_eq!(
            parse_service_with_port("p1.proxy:443").unwrap().kind,
            ServiceType::Proxy
        );
        assert_eq!(
            parse_service_with_port("w1.tunnel:8080").unwrap().kind,
            ServiceType::Tunnel
        );
    }

    #[test]
    fn parse_service_errors() {
        assert!(matches!(parse_service_with_port(""), Err(Error::Parse(_))));
        assert!(matches!(
            parse_service_with_port("nodots"),
            Err(Error::Parse(_))
        ));
        assert!(matches!(
            parse_service_with_port("model-1.bogus:80"),
            Err(Error::Unknown(_))
        ));
        assert!(matches!(
            parse_service_with_port("model-1.static:abc"),
            Err(Error::Parse(_))
        ));
        assert!(matches!(
            parse_service_with_port(".static:80"),
            Err(Error::Parse(_))
        ));
        assert!(matches!(
            parse_service_with_port("model-1.:80"),
            Err(Error::Parse(_))
        ));
        assert!(matches!(
            parse_service_with_port("model-1.static:"),
            Err(Error::Parse(_))
        ));
    }

    // ----- Destination -----

    #[test]
    fn destination_with_percent() {
        let d = Destination::parse("50% model-1.static:80").unwrap();
        assert_eq!(d.percent, Some(50));
        assert_eq!(d.service, "model-1.static:80");
        assert_eq!(d.weight(), 50);
    }

    #[test]
    fn destination_mirror_no_percent() {
        let d = Destination::parse("gpustack.dns:30080").unwrap();
        assert_eq!(d.percent, None);
        assert_eq!(d.weight(), 100);
    }

    #[test]
    fn destination_percent_without_space() {
        let d = Destination::parse("100%gpustack.static:80").unwrap();
        assert_eq!(d.percent, Some(100));
        assert_eq!(d.service, "gpustack.static:80");
    }

    #[test]
    fn destination_percent_errors() {
        assert!(matches!(
            Destination::parse("abc% svc.static:80"),
            Err(Error::Parse(_))
        ));
        assert!(matches!(Destination::parse("50%"), Err(Error::Parse(_))));
        assert!(matches!(Destination::parse(""), Err(Error::Parse(_))));
        assert!(matches!(Destination::parse("   "), Err(Error::Parse(_))));
    }

    #[test]
    fn destination_service_ref() {
        let ok = Destination::parse("15% provider-1.dns:8443").unwrap();
        let ref_ = ok.service_ref().unwrap();
        assert_eq!(ref_.kind, ServiceType::Dns);

        let bad = Destination::new("provider-1.bogus:8443");
        assert!(matches!(bad.service_ref(), Err(Error::Unknown(_))));
    }

    #[test]
    fn parse_destinations_block() {
        let block = "60% model-1-10.static:80\n40% model-1-11.static:80\n";
        let v = parse_destinations(block).unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(v[0].percent, Some(60));
        assert_eq!(v[1].service, "model-1-11.static:80");
    }

    #[test]
    fn parse_destinations_mixed_forms_and_blank_lines() {
        let v = parse_destinations("gpustack.dns:30080\n\n50% a.static:80\n").unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(v[0].weight(), 100);
        assert_eq!(v[1].weight(), 50);
    }
}
