//! ⑨ `gpustack-set-model-pre-route` equivalent — pure. After the selected
//! instance is known, write the outbound instance / route-name headers the
//! worker and the usage attribution consume:
//! - `X-GPUStack-Model-Instance` = the selected instance's `name.type`
//!   (`model-<mid>-<iid>[-alias].<type>` — matches GPUStack's
//!   `get_instance_id_from_header`).
//! - `X-GPUStack-Route-Name` = the origin (ns-qualified) ingress name — the
//!   source of `model_route_id` usage attribution (design §2.1.3 / §6.3).

use hygress_core::transform::HeaderMap;

use crate::context::hdr;

/// Apply the stage-⑨ headers for the selected `service_name`.
pub fn apply(headers: &mut HeaderMap, service_name: &str, route_name: &str) {
    headers.insert(hdr::MODEL_INSTANCE_OUT, service_name);
    headers.insert(hdr::ROUTE_NAME_OUT, route_name);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sets_instance_and_route_name() {
        let mut h = HeaderMap::new();
        apply(&mut h, "model-1-2.static", "higress-system/ai-route-route-5.internal");
        assert_eq!(h.get(hdr::MODEL_INSTANCE_OUT), Some("model-1-2.static"));
        assert_eq!(h.get(hdr::ROUTE_NAME_OUT), Some("higress-system/ai-route-route-5.internal"));
    }

    #[test]
    fn overwrites_preexisting_values() {
        let mut h = HeaderMap::new();
        h.insert(hdr::MODEL_INSTANCE_OUT, "stale");
        apply(&mut h, "model-9-9.dns", "ns/route-1");
        assert_eq!(h.get(hdr::MODEL_INSTANCE_OUT), Some("model-9-9.dns"));
        assert_eq!(h.get(hdr::ROUTE_NAME_OUT), Some("ns/route-1"));
    }
}
