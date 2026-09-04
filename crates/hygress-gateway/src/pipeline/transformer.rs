//! ① + ③ `gpustack-header-transformer` equivalent — pure wrappers over the core
//! ordered rule engine (`hygress_core::Transformer`).
//!
//! - **Inbound** (runs in stage ①/③, before auth): strips untrusted
//!   `x-gpustack-auth-token` / `x-gpustack-model-instance`, renames
//!   `x-gpustack-model` → `x-higress-llm-model`, renames
//!   `x-gpustack-fallback-path` → `:path`, dedupes the routed model + path
//!   (RETAIN_FIRST/RETAIN_LAST), and backstops `:path` →
//!   `x-gpustack-original-path` (for the fallback restore).
//! - **Outbound** (runs in stage ⑨/before egress): dedupes (keeps)
//!   `X-GPUStack-Model-Instance` / `X-GPUStack-Route-Name` so the egress never
//!   strips the instance headers.

use hygress_core::transform::{HeaderMap, Transformer};

/// Apply the inbound rule set in order (stage ①/③).
pub fn apply_inbound(headers: &mut HeaderMap) {
    Transformer::inbound().apply(headers);
}

/// Apply the outbound rule set (stage ⑨ / pre-forward keep).
pub fn apply_outbound(headers: &mut HeaderMap) {
    Transformer::outbound().apply(headers);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inbound_strips_untrusted_and_renames() {
        let mut h = HeaderMap::from_iter([
            ("X-GPUStack-Auth-Token", "forged"),
            ("X-GPUStack-Model-Instance", "forged"),
            ("x-gpustack-model", "legacy"),
            (":path", "/v1/chat/completions"),
        ]);
        apply_inbound(&mut h);
        assert!(!h.contains("x-gpustack-auth-token"));
        assert!(!h.contains("x-gpustack-model-instance"));
        assert_eq!(h.get("x-higress-llm-model"), Some("legacy"));
        // :path backstopped to original-path.
        assert_eq!(h.get("x-gpustack-original-path"), Some("/v1/chat/completions"));
    }

    #[test]
    fn inbound_prefilled_llm_model_wins() {
        let mut h = HeaderMap::from_iter([
            ("x-higress-llm-model", "prefilled"),
            ("x-gpustack-model", "legacy"),
        ]);
        apply_inbound(&mut h);
        assert_eq!(h.get("x-higress-llm-model"), Some("prefilled"));
        assert_eq!(h.count("x-higress-llm-model"), 1);
    }

    #[test]
    fn inbound_fallback_path_restores_and_backstops() {
        let mut h = HeaderMap::from_iter([
            (":path", "/v1/chat/completions"),
            ("x-gpustack-fallback-path", "/original/path"),
        ]);
        apply_inbound(&mut h);
        assert_eq!(h.get(":path"), Some("/original/path"));
        assert!(!h.contains("x-gpustack-fallback-path"));
        assert_eq!(h.get("x-gpustack-original-path"), Some("/original/path"));
    }

    #[test]
    fn outbound_keeps_and_dedupes_instance_headers() {
        let mut h = HeaderMap::new();
        h.append("X-GPUStack-Model-Instance", "model-1-2.static");
        h.append("x-gpustack-model-instance", "model-9-9.static");
        h.append("X-GPUStack-Route-Name", "ns/route-1");
        apply_outbound(&mut h);
        assert_eq!(h.get("x-gpustack-model-instance"), Some("model-1-2.static"));
        assert_eq!(h.count("x-gpustack-model-instance"), 1);
        assert_eq!(h.get("x-gpustack-route-name"), Some("ns/route-1"));
    }

    #[test]
    fn outbound_never_strips_llm_model() {
        let mut h = HeaderMap::from_iter([("x-higress-llm-model", "m")]);
        apply_outbound(&mut h);
        assert_eq!(h.get("x-higress-llm-model"), Some("m"));
    }
}
