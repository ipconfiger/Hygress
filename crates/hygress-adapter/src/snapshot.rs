//! Full snapshot build (design §5.3 `snapshot`): a label-selected `LIST` of every managed
//! resource kind in `gateway_namespace` → [`Object`]s → a [`ConfigData`] built by the pure
//! [`translate`](crate::translate) layer.
//!
//! This is the cluster-facing half of the snapshot: it performs the (read-only) `LIST`s and
//! hands the raw objects to the pure translation. All translation semantics live in
//! [`translate`](crate::translate); this module only lists + shapes inputs.

use kube::core::ResourceExt;

use crate::client::Client;
use crate::translate::{build_config_data, Object, ObjectKind};
use hygress_core::prelude::ConfigData;
use kube::api::ListParams;

/// Convert one listed resource (typed or dynamic) into an [`Object`].
fn to_object<T: kube::core::Resource + serde::Serialize>(kind: ObjectKind, item: &T, ns: &str) -> Object {
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

/// `LIST` every managed resource kind in the gateway namespace and assemble the snapshot.
///
/// Returns `Ok(ConfigData)` (possibly empty) on a successful LIST pass; `Err` on a transport
/// error (the run loop keeps the last-known-good snapshot on transient failures).
pub(crate) async fn build_snapshot(
    client: &Client,
    gateway_namespace: &str,
    mirror_name: &str,
) -> Result<ConfigData, kube::Error> {
    let ns = &client.namespace;
    let lp = Client::managed_list_params();
    let mut objects: Vec<Object> = Vec::new();

    // CRDs.
    // McpBridge: NO managed-label selector — GPUStack creates the `default` McpBridge
    // WITHOUT the `gpustack.ai/managed=true` label (verified against the live v2.2.3
    // baseline), so a labeled LIST returns nothing and `config.registries` ends up empty
    // (every destination fail-resolves with `registry_resolve_failed`). The gateway
    // namespace holds only GPUStack's `default` bridge, so a plain list is safe.
    for it in client.mcpbridges().list(&ListParams::default()).await?.items {
        objects.push(to_object(ObjectKind::McpBridge, &it, ns));
    }
    for it in client.wasmplugins().list(&lp).await?.items {
        objects.push(to_object(ObjectKind::WasmPlugin, &it, ns));
    }
    for it in client.envoyfilters().list(&lp).await?.items {
        objects.push(to_object(ObjectKind::EnvoyFilter, &it, ns));
    }

    // Standard kinds (typed, namespaced, managed selector).
    for it in client.ingresses().list(&lp).await?.items {
        objects.push(to_object(ObjectKind::Ingress, &it, ns));
    }
    for it in client.secrets().list(&lp).await?.items {
        objects.push(to_object(ObjectKind::Secret, &it, ns));
    }
    for it in client.configmaps().list(&lp).await?.items {
        objects.push(to_object(ObjectKind::ConfigMap, &it, ns));
    }

    Ok(build_config_data(&objects, gateway_namespace, mirror_name))
}
