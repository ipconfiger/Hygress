//! kube 4.x client wiring for the strategy-2 read-only adapter (design §5.2).
//!
//! - Loads the kube client from the embedded file kubeconfig (`kubeconfig: Some(<path>)`) or
//!   infers it from `KUBECONFIG` / in-cluster (`kubeconfig: None`). The embedded file kubeconfig
//!   already carries `insecure-skip-tls-verify: true` against `https://127.0.0.1:18443`;
//!   kube's loader honours it. No override is forced, so topology-B external clusters with a
//!   real CA are respected.
//! - Exposes the per-resource `Api`s: the three CRDs as `Api<DynamicObject>` (via the
//!   [`gvr`](crate::gvr) `ApiResource`s) and the standard kinds via their typed `k8s-openapi`
//!   types.
//!
//! No writes here: every accessor yields a namespaced `list`/`get`. The single best-effort
//! write (the topology-B IngressClass seed) lives in [`crate::reconcile`].

use std::path::Path;

use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::api::networking::v1::Ingress;
use k8s_openapi::api::networking::v1::IngressClass;
use kube::api::{Api, DynamicObject, ListParams};
use kube::config::{Config, KubeConfigOptions};
use kube::{Client as StdClient, Error as KubeError};

use crate::gvr;

/// The managed-object label selector (design §2.1.1).
pub const MANAGED_SELECTOR: &str = "gpustack.ai/managed=true";

/// Load a kube [`Config`] from an embedded file kubeconfig path, or infer it from the
/// environment (`KUBECONFIG` / `~/.kube/config` / in-cluster).
pub async fn connect_config(kubeconfig: Option<&Path>) -> Result<Config, KubeError> {
    match kubeconfig {
        Some(path) => {
            let kc = kube::config::Kubeconfig::read_from(path)?;
            Config::from_custom_kubeconfig(kc, &KubeConfigOptions::default())
                .await
                .map_err(KubeError::from)
        }
        None => Config::infer().await.map_err(KubeError::InferConfig),
    }
}

/// A connected, namespaced view over the kube API for the gateway namespace.
#[derive(Clone)]
pub struct Client {
    pub(crate) inner: StdClient,
    pub(crate) namespace: String,
}

impl Client {
    /// Build a [`Client`] from a kubeconfig path (embedded file) or by inferring the client
    /// config, scoped to `namespace`.
    pub async fn connect(kubeconfig: Option<&Path>, namespace: impl Into<String>) -> Result<Self, KubeError> {
        Ok(Self {
            inner: StdClient::try_from(connect_config(kubeconfig).await?)?,
            namespace: namespace.into(),
        })
    }

    /// Connect with an explicit [`Config`] (used by tests that point at a local apiserver).
    pub async fn from_config(config: Config, namespace: impl Into<String>) -> Result<Self, KubeError> {
        Ok(Self {
            inner: StdClient::try_from(config)?,
            namespace: namespace.into(),
        })
    }

    // ---- typed resources (namespaced) ----

    /// `networking.k8s.io/v1` Ingress (namespaced, managed-selector list).
    pub fn ingresses(&self) -> Api<Ingress> {
        Api::namespaced(self.inner.clone(), &self.namespace)
    }

    /// `core/v1` Secret (namespaced, managed-selector list).
    pub fn secrets(&self) -> Api<Secret> {
        Api::namespaced(self.inner.clone(), &self.namespace)
    }

    /// `core/v1` ConfigMap (namespaced, managed-selector list).
    pub fn configmaps(&self) -> Api<ConfigMap> {
        Api::namespaced(self.inner.clone(), &self.namespace)
    }

    /// `networking.k8s.io/v1` IngressClass (cluster-scoped; used for topology-B seeding).
    pub fn ingress_class(&self) -> Api<IngressClass> {
        Api::all(self.inner.clone())
    }

    // ---- CRDs as DynamicObject (namespaced, managed-selector list) ----

    /// `networking.higress.io/v1` McpBridge.
    pub fn mcpbridges(&self) -> Api<DynamicObject> {
        Api::namespaced_with(self.inner.clone(), &self.namespace, &gvr::mcpbridge())
    }

    /// `extensions.higress.io/v1alpha1` WasmPlugin.
    pub fn wasmplugins(&self) -> Api<DynamicObject> {
        Api::namespaced_with(self.inner.clone(), &self.namespace, &gvr::wasmplugin())
    }

    /// `networking.istio.io/v1alpha3` EnvoyFilter.
    pub fn envoyfilters(&self) -> Api<DynamicObject> {
        Api::namespaced_with(self.inner.clone(), &self.namespace, &gvr::envoyfilter())
    }

    /// A `ListParams` preconfigured with the managed-object label selector.
    pub fn managed_list_params() -> ListParams {
        ListParams::default().labels(MANAGED_SELECTOR)
    }
}
