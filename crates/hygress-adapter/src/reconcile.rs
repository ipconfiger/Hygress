//! Reconcile loop support: startup api-resources discovery, the topology-B
//! IngressClass seed, and (indirectly) the 1s poll diff (the LIST itself lives in
//! [`crate::snapshot`] and the store in [`hygress_core::SharedConfig::store`]).
//!
//! Design:
//! - `wait_for_apiserver_ready` — `GET /api` (via `list_api_groups`) with a 60s / 5s retry
//!   budget (design §5), so the first LIST is not issued against a half-up apiserver.
//! - `ensure_ingress_class` — best-effort, idempotent seed of the `higress` IngressClass for
//!   topology B (external cluster); topology A does not check it, and the seed has no side
//!   effects when it already exists (design §5.2 / D3). GPUStack never creates this object, so
//!   the seed is Hygress' responsibility.
//!
//! No mocks: the discovery + seed paths are exercised only against a real cluster (out of scope
//! for the unit layer). The IngressClass object construction is a pure function and is unit
//! tested here.

use std::time::{Duration, Instant};

use k8s_openapi::api::networking::v1::{IngressClass, IngressClassSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::PostParams;

use crate::client::Client;
use crate::error::Result;
use crate::error::Error;

/// Startup discovery budget (design §5): up to 60s total, 5s between attempts.
pub const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(60);
pub const DISCOVERY_INTERVAL: Duration = Duration::from_secs(5);

/// The `controller` value on the seeded IngressClass. GPUStack's support probe
/// (`is_supported_higress`) only reads the object **by name** (`higress`); it does not check
/// this field, so the value is Hygress'/Higress' internal choice.
pub const INGRESS_CLASS_CONTROLLER: &str = "higress.io/ingress-controller";

/// Block until the apiserver answers `GET /api` (or the budget is exhausted).
pub(crate) async fn wait_for_apiserver_ready(client: &Client) -> Result<()> {
    let deadline = Instant::now() + DISCOVERY_TIMEOUT;
    let mut attempts = 0;
    loop {
        match client.inner.list_api_groups().await {
            Ok(groups) => {
                tracing::info!(
                    attempts,
                    api_groups = groups.groups.len(),
                    "api-resources discovery OK; apiserver ready"
                );
                return Ok(());
            }
            Err(e) => {
                if Instant::now() >= deadline {
                    return Err(Error::NotReady(format!(
                        "{attempts} attempts over {DISCOVERY_TIMEOUT:?}: {e}"
                    )));
                }
                attempts += 1;
                tracing::warn!(
                    attempt = attempts,
                    "apidiscovery not ready yet: {e} (retrying in {DISCOVERY_INTERVAL:?})"
                );
                tokio::time::sleep(DISCOVERY_INTERVAL).await;
            }
        }
    }
}

/// Build the IngressClass object to seed (pure — unit tested).
pub(crate) fn build_ingress_class(name: &str, controller: &str) -> IngressClass {
    IngressClass {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            ..Default::default()
        },
        spec: Some(IngressClassSpec {
            controller: Some(controller.to_string()),
            parameters: None,
        }),
    }
}

/// Idempotently ensure the named IngressClass exists (topology-B seed).
///
/// Best-effort by caller: this returns `Err` on a transport / 409, which the caller logs and
/// tolerates (the embedded topology A never depends on it).
pub(crate) async fn ensure_ingress_class(client: &Client, name: &str) -> Result<()> {
    let api = client.ingress_class();
    match api.get_opt(name).await? {
        Some(_) => {
            tracing::debug!(name, "IngressClass already present; skipping seed");
            Ok(())
        }
        None => {
            let ic = build_ingress_class(name, INGRESS_CLASS_CONTROLLER);
            api.create(&PostParams::default(), &ic).await.map_err(Error::Kube)?;
            tracing::info!(name, controller = INGRESS_CLASS_CONTROLLER, "seeded IngressClass (topology B)");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingress_class_object_shape() {
        let ic = build_ingress_class("higress", INGRESS_CLASS_CONTROLLER);
        assert_eq!(ic.metadata.name.as_deref(), Some("higress"));
        let spec = ic.spec.as_ref().expect("IngressClass spec is set");
        assert_eq!(spec.controller.as_deref(), Some(INGRESS_CLASS_CONTROLLER));
        assert!(spec.parameters.is_none());
    }

    #[test]
    fn ingestion_class_controller_is_higress_internal() {
        // The seed controller is Hygress'/Higress' internal choice; GPUStack's probe reads the
        // object by name only. Pin it so a typo here cannot silently break topology B.
        assert_eq!(INGRESS_CLASS_CONTROLLER, "higress.io/ingress-controller");
    }
}
