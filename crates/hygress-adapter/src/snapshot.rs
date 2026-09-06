//! Full snapshot build (design §5.3 `snapshot`): a label-selected `LIST` of every managed
//! resource kind in `gateway_namespace` → [`Object`]s → a [`ConfigData`] built by the pure
//! [`translate`](crate::translate) layer.
//!
//! This is the cluster-facing half of the snapshot: it performs the (read-only) `LIST`s and
//! hands the raw objects to the pure translation. All translation semantics live in
//! [`translate`](crate::translate); this module only lists + shapes inputs.
//!
//! ## Change short-circuit (P4)
//!
//! Each LIST pass produces a cheap [`SnapshotFingerprint`] — one `(kind, namespace, name,
//! resource_version)` per object, sorted. The controller's run loop hands in the previous
//! pass's fingerprint (it reconciles on watch events or the convergence poll tick — ~1s on
//! the embedded apiserver, POLL_INTERVAL); when the new one matches, the expensive `build_config_data` translate (and the
//! downstream RouteTable rebuild / regex recompile / ArcSwap swap that [`hygress_core`]
//! performs on `store`) is skipped entirely, so a steady-state cluster never re-triggers a
//! full rebuild on a pass where nothing changed. Any k8s mutation bumps `resource_version`,
//! which always changes the fingerprint.

use kube::core::ResourceExt;

use crate::client::Client;
use crate::translate::{build_config_data, Object, ObjectKind};
use hygress_core::prelude::ConfigData;
use kube::api::ListParams;

/// A cheap change-detector for a snapshot pass: one `(kind, namespace, name, resource_version)`
/// entry per listed object, sorted so that equal content yields an equal fingerprint regardless
/// of LIST order. Comparing two fingerprints is an `O(n)` byte-wise `Vec` equality — vastly
/// cheaper than re-translating + re-storing the whole snapshot.
pub(crate) type SnapshotFingerprint = Vec<(ObjectKind, String, String, u64)>;

/// The identity half of [`to_object`]: extract ONLY `(kind, namespace, name, resource_version)`
/// from a listed item — no `serde_json::to_value`, no [`Object`]/`Value` build — so the
/// fingerprint pass stays orders-of-magnitude cheaper than the translate.
fn fingerprint_of<T: kube::core::Resource + serde::Serialize>(
    kind: ObjectKind,
    item: &T,
    ns: &str,
) -> (ObjectKind, String, String, u64) {
    let name = item.name_any();
    let namespace = item.namespace().unwrap_or_else(|| ns.to_string());
    let resource_version = item
        .resource_version()
        .and_then(|rv| rv.parse::<u64>().ok())
        .unwrap_or(0);
    (kind, namespace, name, resource_version)
}

/// Convert one listed resource (typed or dynamic) into an [`Object`].
fn to_object<T: kube::core::Resource + serde::Serialize>(
    kind: ObjectKind,
    item: &T,
    ns: &str,
) -> Object {
    let name = item.name_any();
    let namespace = item.namespace().unwrap_or_else(|| ns.to_string());
    let uid = item.uid().unwrap_or_default();
    let resource_version = item
        .resource_version()
        .and_then(|rv| rv.parse::<u64>().ok())
        .unwrap_or(0);
    let value = serde_json::to_value(item).unwrap_or(serde_json::Value::Null);
    Object::new(kind, name, namespace, uid, resource_version, value)
}

/// `LIST` every managed resource kind in the gateway namespace (one pass) and, when the
/// fingerprint changed since `prev`, assemble the [`ConfigData`].
///
/// Returns `(fingerprint, data)`: `data` is `None` when `prev` is present and the new
/// fingerprint equals it (steady-state unchanged — skip the expensive translate + the
/// downstream rebuild that `store` triggers), and `Some` on the first tick (`prev = None`) or
/// any real change. `Err` on a transport error (the run loop keeps the last-known-good
/// snapshot on transient failures).
pub(crate) async fn build_snapshot(
    client: &Client,
    gateway_namespace: &str,
    mirror_name: &str,
    prev: Option<&SnapshotFingerprint>,
) -> Result<(SnapshotFingerprint, Option<ConfigData>), kube::Error> {
    let ns = &client.namespace;
    let lp = Client::managed_list_params();

    // One LIST per kind. McpBridge: NO managed-label selector — GPUStack creates the
    // `default` McpBridge WITHOUT the `gpustack.ai/managed=true` label (verified against the
    // live v2.2.3 baseline), so a labeled LIST returns nothing and `config.registries` ends up
    // empty (every destination fail-resolves with `registry_resolve_failed`). The gateway
    // namespace holds only GPUStack's `default` bridge, so a plain list is safe.
    let mcpbridges = client
        .mcpbridges()
        .list(&ListParams::default())
        .await?
        .items;
    let wasmplugins = client.wasmplugins().list(&lp).await?.items;
    let envoyfilters = client.envoyfilters().list(&lp).await?.items;
    let ingresses = client.ingresses().list(&lp).await?.items;
    let secrets = client.secrets().list(&lp).await?.items;
    // ConfigMaps: the managed-label selector only — intentional (ORA3-M16,
    // documented downgrade). GPUStack writes `higress-config` (gateway timing)
    // / `higress-https` / `higress-ca-root-cert` WITHOUT the managed label, so
    // they are never listed here: the embedded topology does NOT consume the
    // `higress-config` timing values, and `config.timing` stays at the built-in
    // seed defaults (downstream 1800s / upstream 10s). `configmap_to_timing`
    // (translate.rs) is therefore unreachable on the embedded topology — it
    // could only fire for a managed, name-matching timing ConfigMap.
    let configmaps = client.configmaps().list(&lp).await?.items;

    // Cheap fingerprint pass: identity only (no `to_value`, no `Object` build).
    let total = mcpbridges.len()
        + wasmplugins.len()
        + envoyfilters.len()
        + ingresses.len()
        + secrets.len()
        + configmaps.len();
    let mut fp: SnapshotFingerprint = Vec::with_capacity(total);
    for it in &mcpbridges {
        fp.push(fingerprint_of(ObjectKind::McpBridge, it, ns));
    }
    for it in &wasmplugins {
        fp.push(fingerprint_of(ObjectKind::WasmPlugin, it, ns));
    }
    for it in &envoyfilters {
        fp.push(fingerprint_of(ObjectKind::EnvoyFilter, it, ns));
    }
    for it in &ingresses {
        fp.push(fingerprint_of(ObjectKind::Ingress, it, ns));
    }
    for it in &secrets {
        fp.push(fingerprint_of(ObjectKind::Secret, it, ns));
    }
    for it in &configmaps {
        fp.push(fingerprint_of(ObjectKind::ConfigMap, it, ns));
    }
    fp.sort();

    // Steady-state short-circuit: nothing changed since the last pass → skip the expensive
    // translate (and the downstream rebuild that `store` triggers). Belt-and-suspenders
    // (oracle Minor): if any listed object lacks a usable resourceVersion (rv == 0 — a
    // non-conforming backend), do NOT trust the fingerprint: a real change could be masked
    // by an unchanged-zero sequence, so fall through to the full build every tick. In
    // practice k8s ALWAYS stamps per-object resourceVersion, so steady-state rv is never 0.
    if let Some(p) = prev {
        if p == &fp && fp.iter().all(|(_, _, _, rv)| *rv != 0) {
            return Ok((fp, None));
        }
    }

    // First tick (`prev = None`) or a real change: build the full objects + ConfigData.
    let mut objects: Vec<Object> = Vec::with_capacity(total);
    for it in &mcpbridges {
        objects.push(to_object(ObjectKind::McpBridge, it, ns));
    }
    for it in &wasmplugins {
        objects.push(to_object(ObjectKind::WasmPlugin, it, ns));
    }
    for it in &envoyfilters {
        objects.push(to_object(ObjectKind::EnvoyFilter, it, ns));
    }
    for it in &ingresses {
        objects.push(to_object(ObjectKind::Ingress, it, ns));
    }
    for it in &secrets {
        objects.push(to_object(ObjectKind::Secret, it, ns));
    }
    for it in &configmaps {
        objects.push(to_object(ObjectKind::ConfigMap, it, ns));
    }

    Ok((
        fp,
        Some(build_config_data(&objects, gateway_namespace, mirror_name)),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::Secret;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn secret(ns: Option<&str>, name: &str, rv: Option<&str>) -> Secret {
        Secret {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: ns.map(str::to_string),
                resource_version: rv.map(str::to_string),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn fingerprint_of_extracts_identity_only() {
        // name / namespace / resource_version are extracted; the value body is never touched.
        let s = secret(Some("higress-system"), "gpustack-tls-default", Some("1001"));
        assert_eq!(
            fingerprint_of(ObjectKind::Secret, &s, "fallback-ns"),
            (
                ObjectKind::Secret,
                "higress-system".to_string(),
                "gpustack-tls-default".to_string(),
                1001,
            )
        );
        // Absent namespace falls back to the gateway namespace; absent rv → 0.
        let s2 = secret(None, "gpustack-tls-api", None);
        assert_eq!(
            fingerprint_of(ObjectKind::Secret, &s2, "fallback-ns"),
            (
                ObjectKind::Secret,
                "fallback-ns".to_string(),
                "gpustack-tls-api".to_string(),
                0,
            )
        );
    }

    #[test]
    fn fingerprint_equal_for_same_content_independent_of_order() {
        // Two passes that list the same objects in a different order yield equal fingerprints
        // once sorted (LIST order is not guaranteed to be stable across ticks).
        let mut a: SnapshotFingerprint = vec![
            (
                ObjectKind::Ingress,
                "ns".into(),
                "ai-route-route-5.internal".into(),
                1,
            ),
            (
                ObjectKind::Secret,
                "ns".into(),
                "gpustack-tls-default".into(),
                2,
            ),
            (ObjectKind::McpBridge, "ns".into(), "default".into(), 3),
        ];
        let mut b: SnapshotFingerprint = vec![
            (ObjectKind::McpBridge, "ns".into(), "default".into(), 3),
            (
                ObjectKind::Ingress,
                "ns".into(),
                "ai-route-route-5.internal".into(),
                1,
            ),
            (
                ObjectKind::Secret,
                "ns".into(),
                "gpustack-tls-default".into(),
                2,
            ),
        ];
        a.sort();
        b.sort();
        assert_eq!(a, b);
    }

    #[test]
    fn fingerprint_changes_when_a_resource_version_bumps() {
        let mut fp: SnapshotFingerprint = vec![
            (
                ObjectKind::Ingress,
                "ns".into(),
                "ai-route-route-5.internal".into(),
                1,
            ),
            (
                ObjectKind::Secret,
                "ns".into(),
                "gpustack-tls-default".into(),
                2,
            ),
        ];
        fp.sort();
        // Bumping any one object's resource_version (a k8s mutation) changes the fingerprint.
        let mut bumped = fp.clone();
        bumped
            .iter_mut()
            .find(|e| e.2 == "gpustack-tls-default")
            .unwrap()
            .3 = 3;
        assert_ne!(fp, bumped);
        // A removal (object count change) also changes it.
        let mut removed = fp.clone();
        removed.pop();
        assert_ne!(fp, removed);
    }
}
