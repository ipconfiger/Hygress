//! Group/Version/Resource (and [`ApiResource`]) definitions for the CRD + standard kinds the
//! adapter consumes (design §5.1/§5.2).
//!
//! - The three CRDs (`networking.higress.io/v1` McpBridge, `extensions.higress.io/v1alpha1`
//!   WasmPlugin, `networking.istio.io/v1alpha3` EnvoyFilter) are consumed via
//!   `Api<DynamicObject>` with the [`ApiResource`]s below (`k8s-openapi` has no built-in types
//!   for these groups).
//! - The standard kinds (`networking.k8s.io/v1` Ingress/IngressClass, `core/v1` Secret/ConfigMap)
//!   are consumed via their typed `k8s-openapi` resources; their group/version is pinned here for
//!   reference and as [`GroupVersionResource`]s.

use kube::core::{ApiResource, GroupVersionResource};

/// McpBridge: `networking.higress.io/v1` (plural `mcpbridges`).
pub fn mcpbridge_gvr() -> GroupVersionResource {
    GroupVersionResource::gvr("networking.higress.io", "v1", "mcpbridges")
}

/// WasmPlugin: `extensions.higress.io/v1alpha1` (plural `wasmplugins`).
pub fn wasmplugin_gvr() -> GroupVersionResource {
    GroupVersionResource::gvr("extensions.higress.io", "v1alpha1", "wasmplugins")
}

/// EnvoyFilter: `networking.istio.io/v1alpha3` (plural `envoyfilters`).
pub fn envoyfilter_gvr() -> GroupVersionResource {
    GroupVersionResource::gvr("networking.istio.io", "v1alpha3", "envoyfilters")
}

/// Ingress: `networking.k8s.io/v1` (plural `ingresses`).
pub fn ingress_gvr() -> GroupVersionResource {
    GroupVersionResource::gvr("networking.k8s.io", "v1", "ingresses")
}

/// IngressClass: `networking.k8s.io/v1` (plural `ingressclasses`, cluster-scoped).
pub fn ingress_class_gvr() -> GroupVersionResource {
    GroupVersionResource::gvr("networking.k8s.io", "v1", "ingressclasses")
}

/// Secret: `core/v1` (empty group, version `v1`, plural `secrets`).
pub fn secret_gvr() -> GroupVersionResource {
    GroupVersionResource::gvr("", "v1", "secrets")
}

/// ConfigMap: `core/v1` (empty group, version `v1`, plural `configmaps`).
pub fn config_map_gvr() -> GroupVersionResource {
    GroupVersionResource::gvr("", "v1", "configmaps")
}

// ---------------------------------------------------------------------------
// ApiResource (the `DynamicObject` DynamicType for the 3 CRDs)
// ---------------------------------------------------------------------------

/// Build an [`ApiResource`] (the `DynamicObject` `DynamicType`) for a group/version/kind/plural.
fn api_resource(group: &str, version: &str, kind: &str, plural: &str) -> ApiResource {
    let api_version = if group.is_empty() {
        version.to_string()
    } else {
        format!("{group}/{version}")
    };
    ApiResource {
        group: group.to_string(),
        version: version.to_string(),
        api_version,
        kind: kind.to_string(),
        plural: plural.to_string(),
    }
}

/// McpBridge ApiResource.
pub fn mcpbridge() -> ApiResource {
    api_resource("networking.higress.io", "v1", "McpBridge", "mcpbridges")
}

/// WasmPlugin ApiResource.
pub fn wasmplugin() -> ApiResource {
    api_resource("extensions.higress.io", "v1alpha1", "WasmPlugin", "wasmplugins")
}

/// EnvoyFilter ApiResource.
pub fn envoyfilter() -> ApiResource {
    api_resource("networking.istio.io", "v1alpha3", "EnvoyFilter", "envoyfilters")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crd_api_resources() {
        let m = mcpbridge();
        assert_eq!(m.group, "networking.higress.io");
        assert_eq!(m.version, "v1");
        assert_eq!(m.api_version, "networking.higress.io/v1");
        assert_eq!(m.kind, "McpBridge");
        assert_eq!(m.plural, "mcpbridges");

        let w = wasmplugin();
        assert_eq!(w.api_version, "extensions.higress.io/v1alpha1");
        assert_eq!(w.plural, "wasmplugins");

        let e = envoyfilter();
        assert_eq!(e.api_version, "networking.istio.io/v1alpha3");
        assert_eq!(e.plural, "envoyfilters");
    }

    #[test]
    fn gvr_definitions() {
        assert_eq!(mcpbridge_gvr().group, "networking.higress.io");
        assert_eq!(mcpbridge_gvr().resource, "mcpbridges");
        assert_eq!(wasmplugin_gvr().group, "extensions.higress.io");
        assert_eq!(wasmplugin_gvr().version, "v1alpha1");
        assert_eq!(envoyfilter_gvr().resource, "envoyfilters");
        assert_eq!(ingress_gvr().resource, "ingresses");
        assert_eq!(ingress_class_gvr().resource, "ingressclasses");
        // Core resources have an empty group and version `v1`.
        assert_eq!(secret_gvr().group, "");
        assert_eq!(secret_gvr().version, "v1");
        assert_eq!(secret_gvr().resource, "secrets");
        assert_eq!(config_map_gvr().resource, "configmaps");
    }
}
